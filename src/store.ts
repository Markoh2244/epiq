// Durable state writes/reads shared by the CLI and the scenario harness (D-03).
import fs from "node:fs";
import path from "node:path";
import { randomBytes } from "node:crypto";
import { Manifest } from "./corpus.js";
import { db, initSchema, TERMINAL_STATES } from "./db.js";

export const ITEM_COLS = `item_id, run_id, tenant, corpus_id, state, sha256, bytes, declared_bytes, source_path,
  extension, attempts, retries, annotate_calls, extracted_text, annotation, error, worker_id,
  created_at, updated_at, terminal_at`;

export async function submitCorpus(dir: string, tenant: string) {
  const manifest: Manifest = JSON.parse(fs.readFileSync(path.join(dir, "manifest.json"), "utf8"));
  if (manifest.arguments.tenant !== tenant)
    throw new Error(`corpus tenant ${manifest.arguments.tenant} does not match trusted tenant ${tenant}`);
  const runId = `run_${Date.now().toString(36)}${randomBytes(3).toString("hex")}`;
  await initSchema();
  await db().query("insert into runs (run_id, tenant, corpus_id, corpus_dir, item_count) values ($1,$2,$3,$4,$5)", [
    runId,
    tenant,
    manifest.corpus_id,
    path.resolve(dir),
    manifest.files.length,
  ]);
  await db().query(
    `insert into items (item_id, run_id, tenant, corpus_id, source_path, abs_path, extension, declared_bytes, state)
     select t.item_id, $2, $3, $4, t.source_path, t.abs_path, t.extension, t.declared_bytes, 'pending'
       from unnest($1::text[], $5::text[], $6::text[], $7::text[], $8::int[])
         as t(item_id, source_path, abs_path, extension, declared_bytes)`,
    [
      manifest.files.map((f) => `${runId}-i${String(f.order).padStart(4, "0")}`),
      runId,
      tenant,
      manifest.corpus_id,
      manifest.files.map((f) => f.path),
      manifest.files.map((f) => path.resolve(dir, "files", f.path)),
      manifest.files.map((f) => f.extension),
      manifest.files.map((f) => f.bytes),
    ],
  );
  return { run_id: runId, tenant, corpus_id: manifest.corpus_id, items: manifest.files.length };
}

export async function runStatus(runId: string) {
  const run = await db().query("select run_id, tenant, corpus_id, item_count from runs where run_id=$1", [runId]);
  if (!run.rowCount) throw new Error(`run not found: ${runId}`);
  const rows = await db().query("select state, count(*)::int as n from items where run_id=$1 group by state", [runId]);
  const states: Record<string, number> = {};
  for (const r of rows.rows) states[r.state] = r.n;
  const nonterminal = Object.entries(states)
    .filter(([s]) => !TERMINAL_STATES.includes(s))
    .reduce((a, [, n]) => a + n, 0);
  return { ...run.rows[0], states, nonterminal, terminal: nonterminal === 0 };
}
