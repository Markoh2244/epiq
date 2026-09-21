// Unified command interface (section 2a).
import path from "node:path";
import { writeCorpus, verifyCorpus } from "./corpus.js";
import { db, closeDb } from "./db.js";
import { up, down, killWorker, liveWorkers } from "./env.js";
import { createStub } from "./stub.js";
import { ITEM_COLS, submitCorpus, runStatus } from "./store.js";

const argv = process.argv.slice(2);
const cmd = argv[0];

const flag = (name: string) => {
  const i = argv.indexOf(`--${name}`);
  return i >= 0 ? argv[i + 1] : undefined;
};
const has = (name: string) => argv.includes(`--${name}`);
const need = (name: string) => flag(name) ?? fail(`missing --${name}`);
const out = (v: unknown) => console.log(JSON.stringify(v, null, 2));
function fail(msg: string): never {
  console.error(JSON.stringify({ error: msg }));
  process.exit(1);
}

async function main() {
  switch (cmd) {
    case "corpus": {
      const dir = need("out");
      if (has("verify")) {
        const r = verifyCorpus(dir);
        out(r);
        if (!r.ok) process.exit(1);
        return;
      }
      const size = Number(need("size"));
      if (!(size >= 50 && size <= 500)) fail("--size must be 50..500");
      const m = writeCorpus(dir, Number(need("seed")), size, need("tenant"), has("force"));
      out({
        corpus_id: m.corpus_id,
        out: path.resolve(dir),
        ...m.arguments,
        totals: m.totals,
        edge_cases: m.edge_cases,
        digest: m.digest,
      });
      return;
    }
    case "stub": {
      const port = Number(flag("port") ?? 8080);
      createStub().listen(port, () => console.log(`stub listening on ${port}`));
      return; // foreground
    }
    case "up":
      out(await up(Number(flag("workers") ?? 4)));
      return;
    case "down":
      out(await down());
      return;
    case "reset": {
      const w = Number(flag("workers") ?? 4);
      await down();
      out({ reset: true, ...(await up(w)) });
      return;
    }
    case "submit":
      out(await submitCorpus(need("corpus"), need("tenant")));
      return;
    case "status":
      out(await runStatus(need("run")));
      return;
    case "item": {
      const r = await db().query(`select ${ITEM_COLS} from items where tenant=$1 and item_id=$2`, [
        need("tenant"),
        need("id"),
      ]);
      if (!r.rowCount) fail("not found");
      out(r.rows[0]);
      return;
    }
    case "items": {
      const tenant = need("tenant");
      const runId = need("run");
      const state = flag("state");
      const run = await db().query("select run_id from runs where run_id=$1 and tenant=$2", [runId, tenant]);
      if (!run.rowCount) fail("not found");
      const r = await db().query(
        `select ${ITEM_COLS} from items where tenant=$1 and run_id=$2 ${state ? "and state=$3" : ""} order by item_id`,
        state ? [tenant, runId, state] : [tenant, runId],
      );
      out(r.rows);
      return;
    }
    case "kill-worker": {
      const idx = flag("index");
      out(await killWorker(idx === undefined ? undefined : Number(idx)));
      return;
    }
    case "workers":
      out(await liveWorkers());
      return;
    case "scenario": {
      const { scenario } = await import("./scenario.js");
      const ok = await scenario();
      await closeDb();
      process.exit(ok ? 0 : 1);
    }
    default:
      console.error(
        "usage: ./intake <corpus|stub|up|down|reset|submit|status|item|items|kill-worker|workers|scenario> [flags]",
      );
      process.exit(2);
  }
}

main()
  .then(() => closeDb())
  .catch(async (e) => {
    console.error(JSON.stringify({ error: String(e?.message ?? e) }));
    await closeDb().catch(() => {});
    process.exit(1);
  });
