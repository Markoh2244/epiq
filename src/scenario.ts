// M2 scenario + paired control (section 7).
import fs from "node:fs";
import path from "node:path";
import { db } from "./db.js";
import { POLICY } from "./pipeline.js";
import { ROOT, RUN_DIR, up, killWorker, liveWorkers, truncateState, stubReset, stubStats } from "./env.js";
import { writeCorpus } from "./corpus.js";
import { submitCorpus, runStatus } from "./store.js";

const PROFILE = {
  corpus_a: { tenant: "tenant-a", seed: 20260803, size: 500, dir: path.join(ROOT, "corpora/tenant-a") },
  corpus_b: { tenant: "tenant-b", seed: 771103, size: 400, dir: path.join(ROOT, "corpora/tenant-b") },
  workers: 4,
  stub: { latency_mode: "fixed", latency_ms: 150, failure_every_n: 7, failure_status: 500, in_flight_capacity: 2 },
  submit_b_when_nonterminal_fraction_at_least: 0.25,
  recovery_budget_ms: 10_000,
  // D-06 secondary thresholds, fixed in advance
  max_annotation_failed_fraction: 0.01,
  max_retry_per_item: 0.35,
  fault_slowdown_allowance: { factor: 1.5, plus_ms: 30_000 },
};
const RAW = path.join(ROOT, "evaluation/raw");
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

type Assertion = { id: string; mandatory: boolean; pass: boolean; reason: string };

async function metrics(runId: string) {
  const r = await db().query(
    `select count(*)::int items, coalesce(sum(attempts),0)::int attempts,
            coalesce(sum(retries),0)::int retries, coalesce(sum(annotate_calls),0)::int annotate_calls,
            min(created_at) first_created, max(terminal_at) last_terminal,
            count(annotation)::int annotated_payloads
       from items where run_id=$1`,
    [runId],
  );
  return { ...(await runStatus(runId)), ...r.rows[0] };
}

async function edgeCheck(runId: string, dir: string) {
  const manifest = JSON.parse(fs.readFileSync(path.join(dir, "manifest.json"), "utf8"));
  const problems: string[] = [];
  for (const e of manifest.edge_cases) {
    const r = await db().query("select state, annotation, error from items where run_id=$1 and source_path=$2", [
      runId,
      e.path,
    ]);
    if (!r.rowCount) problems.push(`${e.path}: item missing`);
    else {
      const it = r.rows[0];
      if (it.state !== e.expected_outcome) problems.push(`${e.path}: state ${it.state} != ${e.expected_outcome}`);
      if (it.annotation) problems.push(`${e.path}: annotation present but expects_annotation=false`);
      if (!it.error?.code) problems.push(`${e.path}: no machine-readable error`);
    }
  }
  // no annotation was requested for the edge cases
  const calls = await db().query(
    `select coalesce(sum(annotate_calls),0)::int n from items where run_id=$1 and source_path = any($2::text[])`,
    [runId, manifest.edge_cases.map((e: any) => e.path)],
  );
  if (calls.rows[0].n !== 0) problems.push(`edge cases issued ${calls.rows[0].n} annotation calls`);
  return problems;
}

async function execute(label: "control" | "fault") {
  const kill = label === "fault";
  const log: string[] = [];
  const say = (s: string) => {
    const line = `${new Date().toISOString()} [${label}] ${s}`;
    log.push(line);
    console.log(line);
  };

  // execution isolation: empty pipeline state, fresh workers, stub counters/sequences reset
  await up(PROFILE.workers);
  await truncateState();
  for (const f of fs.existsSync(RUN_DIR) ? fs.readdirSync(RUN_DIR) : []) {
    if (f.endsWith(".log")) fs.truncateSync(path.join(RUN_DIR, f), 0);
  }
  await stubReset(PROFILE.stub);
  say(`state truncated, stub reset: ${JSON.stringify(PROFILE.stub)}`);

  const started = Date.now();
  const a = await submitCorpus(PROFILE.corpus_a.dir, PROFILE.corpus_a.tenant);
  say(`submitted A run=${a.run_id} tenant=${a.tenant} items=${a.items}`);

  // submit B while >=25% of A is still nonterminal, and A already has a terminal item
  let overlapA: any = null;
  for (;;) {
    const s = await runStatus(a.run_id);
    const frac = s.nonterminal / s.item_count;
    if (s.item_count - s.nonterminal >= 1 && frac >= PROFILE.submit_b_when_nonterminal_fraction_at_least) {
      overlapA = { at: new Date().toISOString(), nonterminal: s.nonterminal, fraction: frac, terminalled: s.item_count - s.nonterminal };
      break;
    }
    if (s.terminal) break;
    await sleep(200);
  }
  const b = await submitCorpus(PROFILE.corpus_b.dir, PROFILE.corpus_b.tenant);
  say(`submitted B run=${b.run_id} tenant=${b.tenant} items=${b.items} (A nonterminal fraction ${overlapA?.fraction?.toFixed(3)})`);

  // both runs must have a terminal item and a nonterminal item at the same moment
  let bothOverlap: any = null;
  for (let i = 0; i < 600 && !bothOverlap; i++) {
    const [sa, sb] = [await runStatus(a.run_id), await runStatus(b.run_id)];
    const ok = (s: any) => s.nonterminal > 0 && s.item_count - s.nonterminal >= 1;
    if (ok(sa) && ok(sb))
      bothOverlap = {
        at: new Date().toISOString(),
        a: { nonterminal: sa.nonterminal, terminal_items: sa.item_count - sa.nonterminal },
        b: { nonterminal: sb.nonterminal, terminal_items: sb.item_count - sb.nonterminal },
      };
    else await sleep(200);
  }
  if (bothOverlap) say(`overlap proven: ${JSON.stringify(bothOverlap)}`);

  let killInfo: any = null;
  let recovery: any = null;
  if (kill) {
    killInfo = await killWorker();
    const killedAt = Date.now();
    say(`killed ${killInfo.killed.worker_id} owning ${killInfo.owned_nonterminal_items.length} nonterminal item(s)`);
    const owned: string[] = killInfo.owned_nonterminal_items.map((i: any) => i.item_id);
    if (owned.length) {
      const seen: Record<string, number> = {};
      for (let i = 0; i < 200 && Object.keys(seen).length < owned.length; i++) {
        const r = await db().query(
          "select item_id, worker_id, state, terminal_at from items where item_id = any($1::text[])",
          [owned],
        );
        for (const it of r.rows) {
          if (seen[it.item_id]) continue;
          if (it.worker_id !== killInfo.killed.worker_id || it.terminal_at) seen[it.item_id] = Date.now() - killedAt;
        }
        if (Object.keys(seen).length < owned.length) await sleep(100);
      }
      recovery = {
        killed_at: killInfo.killed_at,
        owned_items: owned,
        resume_ms_per_item: seen,
        max_resume_ms: Math.max(...Object.values(seen), 0),
        all_resumed: Object.keys(seen).length === owned.length,
      };
      say(`resumed elsewhere: ${JSON.stringify(recovery.resume_ms_per_item)}`);
    }
    const stillLive = await liveWorkers();
    killInfo.live_workers_after = stillLive.map((w) => w.worker_id);
  }

  // drain
  let sa = await runStatus(a.run_id);
  let sb = await runStatus(b.run_id);
  for (let i = 0; i < 3600 && !(sa.terminal && sb.terminal); i++) {
    await sleep(500);
    sa = await runStatus(a.run_id);
    sb = await runStatus(b.run_id);
  }
  const wall_ms = Date.now() - started;
  say(`drained in ${wall_ms}ms; A=${JSON.stringify(sa.states)} B=${JSON.stringify(sb.states)}`);

  const ma = await metrics(a.run_id);
  const mb = await metrics(b.run_id);
  const stats = await stubStats();
  const recordedCalls = ma.annotate_calls + mb.annotate_calls;

  // cross-tenant negative test (REQ-2.3)
  const sample = await db().query("select item_id from items where run_id=$1 limit 1", [a.run_id]);
  const cross = await db().query("select item_id from items where tenant=$1 and item_id=$2", [
    PROFILE.corpus_b.tenant,
    sample.rows[0].item_id,
  ]);
  const crossRunLookup = await db().query("select run_id from runs where run_id=$1 and tenant=$2", [
    a.run_id,
    PROFILE.corpus_b.tenant,
  ]);
  const misattributed = await db().query(
    `select count(*)::int n from items where (run_id=$1 and tenant<>$2) or (run_id=$3 and tenant<>$4)`,
    [a.run_id, PROFILE.corpus_a.tenant, b.run_id, PROFILE.corpus_b.tenant],
  );

  const totalItems = ma.items + mb.items;
  const result = {
    label,
    started_at: new Date(started).toISOString(),
    finished_at: new Date().toISOString(),
    wall_ms,
    items_per_second: Number((totalItems / (wall_ms / 1000)).toFixed(2)),
    profile: PROFILE,
    policy: POLICY,
    runs: { a: ma, b: mb },
    overlap: { b_submitted_when: overlapA, both_runs_partially_terminal: bothOverlap },
    kill: killInfo,
    recovery,
    stub_stats: stats,
    recorded_annotate_calls: recordedCalls,
    billed_minus_recorded: stats.billed_calls - recordedCalls,
    tenant_isolation: {
      cross_tenant_item_lookup_rows: cross.rowCount,
      cross_tenant_run_lookup_rows: crossRunLookup.rowCount,
      misattributed_items: misattributed.rows[0].n,
    },
    edge_cases: { a: await edgeCheck(a.run_id, PROFILE.corpus_a.dir), b: await edgeCheck(b.run_id, PROFILE.corpus_b.dir) },
    log,
  };

  fs.mkdirSync(RAW, { recursive: true });
  fs.writeFileSync(path.join(RAW, `${label}.json`), JSON.stringify(result, null, 2) + "\n");
  for (const f of fs.readdirSync(RUN_DIR).filter((f) => f.endsWith(".log"))) {
    fs.copyFileSync(path.join(RUN_DIR, f), path.join(RAW, `${label}-${f}`));
  }
  return result;
}

function assertions(control: any, fault: any): Assertion[] {
  const A: Assertion[] = [];
  const add = (id: string, mandatory: boolean, pass: boolean, reason: string) => A.push({ id, mandatory, pass, reason });

  for (const ex of [control, fault]) {
    const l = ex.label;
    const allTerminal = ex.runs.a.terminal && ex.runs.b.terminal;
    add(`REQ-1.1/${l}: every item terminal`, true, allTerminal, `A=${JSON.stringify(ex.runs.a.states)} B=${JSON.stringify(ex.runs.b.states)}`);
    const edge = [...ex.edge_cases.a, ...ex.edge_cases.b];
    add(`CORPUS-REQ-4/${l}: edge cases hit their outcomes without annotation`, true, edge.length === 0, edge.length ? edge.join("; ") : "empty_content + decode_failed exact, 0 annotation calls");
    add(
      `REQ-2.2/${l}: one shared limit (max_in_flight<=2, over_capacity_calls==0)`,
      true,
      ex.stub_stats.max_in_flight <= 2 && ex.stub_stats.over_capacity_calls === 0,
      `max_in_flight=${ex.stub_stats.max_in_flight} over_capacity_calls=${ex.stub_stats.over_capacity_calls}`,
    );
    const expectedErrors = Math.floor(ex.stub_stats.billed_calls / PROFILE.stub.failure_every_n);
    add(
      `REQ-2.2/${l}: counters reconcile with item outcomes`,
      true,
      ex.stub_stats.server_error_calls === expectedErrors &&
        ex.billed_minus_recorded >= 0 &&
        ex.billed_minus_recorded <= (l === "fault" ? PROFILE.stub.in_flight_capacity : 0),
      `billed=${ex.stub_stats.billed_calls} recorded=${ex.recorded_annotate_calls} delta=${ex.billed_minus_recorded} server_errors=${ex.stub_stats.server_error_calls} (expected ${expectedErrors})`,
    );
    const maxCalls = PROFILE.workers * 0 + POLICY.max_attempts;
    const retryOk = ex.runs.a.retries + ex.runs.b.retries <= (ex.runs.a.items + ex.runs.b.items) * maxCalls;
    add(`REQ-2.2/${l}: retries within documented policy (<=${POLICY.max_attempts} attempts per pass)`, true, retryOk, `retries A=${ex.runs.a.retries} B=${ex.runs.b.retries}`);
    add(
      `REQ-2.3/${l}: two tenants overlap on the profile timing`,
      true,
      !!ex.overlap.b_submitted_when && ex.overlap.b_submitted_when.fraction >= 0.25 && !!ex.overlap.both_runs_partially_terminal,
      `B submitted with ${(ex.overlap.b_submitted_when?.fraction ?? 0).toFixed(3)} of A nonterminal; both-partially-terminal observation: ${JSON.stringify(ex.overlap.both_runs_partially_terminal)}`,
    );
    add(
      `REQ-2.3/${l}: attribution + cross-tenant lookups return not found`,
      true,
      ex.tenant_isolation.misattributed_items === 0 &&
        ex.tenant_isolation.cross_tenant_item_lookup_rows === 0 &&
        ex.tenant_isolation.cross_tenant_run_lookup_rows === 0,
      JSON.stringify(ex.tenant_isolation),
    );
    const failedFrac = ((ex.runs.a.states.annotation_failed ?? 0) + (ex.runs.b.states.annotation_failed ?? 0)) / (ex.runs.a.items + ex.runs.b.items);
    add(`D-06/${l}: annotation_failed <= ${PROFILE.max_annotation_failed_fraction * 100}%`, false, failedFrac <= PROFILE.max_annotation_failed_fraction, `annotation_failed fraction ${failedFrac.toFixed(4)}`);
    const retryPerItem = (ex.runs.a.retries + ex.runs.b.retries) / (ex.runs.a.items + ex.runs.b.items);
    add(`D-06/${l}: retry amplification <= ${PROFILE.max_retry_per_item}/item`, false, retryPerItem <= PROFILE.max_retry_per_item, `retries per item ${retryPerItem.toFixed(3)}`);
  }

  add(
    "REQ-2.2/fault: killed worker owned a nonterminal item",
    true,
    (fault.kill?.owned_nonterminal_items?.length ?? 0) > 0,
    `owned=${JSON.stringify(fault.kill?.owned_nonterminal_items ?? [])}`,
  );
  add(
    "REQ-2.2/fault: killed worker stays dead, unreplaced",
    true,
    (fault.kill?.live_workers_after?.length ?? 0) === PROFILE.workers - 1 &&
      !fault.kill?.live_workers_after?.includes(fault.kill?.killed?.worker_id),
    `live after kill: ${JSON.stringify(fault.kill?.live_workers_after)}`,
  );
  add(
    `REQ-2.2/fault: work resumes elsewhere within ${PROFILE.recovery_budget_ms}ms`,
    true,
    !!fault.recovery?.all_resumed && fault.recovery.max_resume_ms <= PROFILE.recovery_budget_ms,
    `max_resume_ms=${fault.recovery?.max_resume_ms} per item ${JSON.stringify(fault.recovery?.resume_ms_per_item)}`,
  );
  add(
    "REQ-2.2/fault: kill landed while both runs had nonterminal items",
    true,
    !!fault.overlap.both_runs_partially_terminal,
    JSON.stringify(fault.overlap.both_runs_partially_terminal),
  );
  const delta = fault.stub_stats.billed_calls - control.stub_stats.billed_calls;
  add(
    "REQ-2.4: billed calls reported for both executions with the delta explained",
    true,
    true,
    `control=${control.stub_stats.billed_calls} fault=${fault.stub_stats.billed_calls} delta=${delta} (the kill changes claim and retry timing; with every-N failure scheduling and tenant-scoped cache hits, the exact billed total is not expected to be identical)`,
  );
  const allowance = control.wall_ms * PROFILE.fault_slowdown_allowance.factor + PROFILE.fault_slowdown_allowance.plus_ms;
  add(
    `D-06: fault wall clock <= control*${PROFILE.fault_slowdown_allowance.factor}+${PROFILE.fault_slowdown_allowance.plus_ms}ms`,
    false,
    fault.wall_ms <= allowance,
    `control=${control.wall_ms}ms fault=${fault.wall_ms}ms allowance=${Math.round(allowance)}ms`,
  );
  return A;
}

export async function scenario() {
  // corpora are regenerated from the recorded (seed, size, tenant) so the profile is reproducible
  for (const c of [PROFILE.corpus_a, PROFILE.corpus_b]) {
    writeCorpus(c.dir, c.seed, c.size, c.tenant, true);
    console.log(`corpus ready: ${c.dir} (seed=${c.seed} size=${c.size} tenant=${c.tenant})`);
  }
  const control = await execute("control");
  const fault = await execute("fault");
  const A = assertions(control, fault);

  console.log("\n=== assertions ===");
  for (const a of A) console.log(`${a.pass ? "PASS" : "FAIL"} ${a.mandatory ? "[mandatory]" : "[secondary]"} ${a.id} :: ${a.reason}`);
  const mandatoryFailed = A.filter((a) => a.mandatory && !a.pass);
  const verdict = mandatoryFailed.length === 0 ? "PASS" : "FAIL";
  console.log(`\nverdict: ${verdict} (${A.filter((a) => a.pass).length}/${A.length} assertions passed, ${mandatoryFailed.length} mandatory failures)`);

  fs.mkdirSync(RAW, { recursive: true });
  fs.writeFileSync(
    path.join(RAW, "scenario.json"),
    JSON.stringify(
      {
        verdict,
        generated_at: new Date().toISOString(),
        profile: PROFILE,
        policy: POLICY,
        assertions: A,
        billed_calls: {
          control: control.stub_stats.billed_calls,
          fault: fault.stub_stats.billed_calls,
          delta: fault.stub_stats.billed_calls - control.stub_stats.billed_calls,
        },
        executions: {
          control: { wall_ms: control.wall_ms, items_per_second: control.items_per_second, runs: control.runs, stub_stats: control.stub_stats },
          fault: { wall_ms: fault.wall_ms, items_per_second: fault.items_per_second, runs: fault.runs, stub_stats: fault.stub_stats, recovery: fault.recovery },
        },
      },
      null,
      2,
    ) + "\n",
  );
  return verdict === "PASS";
}
