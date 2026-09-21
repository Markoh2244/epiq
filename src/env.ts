// Local environment lifecycle: Postgres (container), stub, workers (REQ-2.1).
import { spawn, execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { db, initSchema, STUB_URL } from "./db.js";

export const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
export const RUN_DIR = path.join(ROOT, ".run");
const COMPOSE = ["compose", "-f", path.join(ROOT, "src/docker-compose.yml")];
const TSX = path.join(ROOT, "node_modules/.bin/tsx");

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const alive = (pid: number) => {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
};

function pidFile(name: string) {
  return path.join(RUN_DIR, `${name}.pid`);
}

function readPid(name: string): number | null {
  const f = pidFile(name);
  if (!fs.existsSync(f)) return null;
  const pid = Number(fs.readFileSync(f, "utf8").trim());
  return pid && alive(pid) ? pid : null;
}

function startDetached(name: string, args: string[], env: Record<string, string> = {}) {
  fs.mkdirSync(RUN_DIR, { recursive: true });
  const out = fs.openSync(path.join(RUN_DIR, `${name}.log`), "a");
  const child = spawn(TSX, args, {
    cwd: ROOT,
    detached: true,
    stdio: ["ignore", out, out],
    env: { ...process.env, ...env },
  });
  child.unref();
  fs.writeFileSync(pidFile(name), String(child.pid));
  return child.pid!;
}

async function stubHealthy() {
  try {
    const r = await fetch(`${STUB_URL}/healthz`, { signal: AbortSignal.timeout(500) });
    return r.ok;
  } catch {
    return false;
  }
}

async function dbReady() {
  try {
    await db().query("select 1");
    return true;
  } catch {
    return false;
  }
}

export async function up(workers: number) {
  const steps: string[] = [];

  execFileSync("docker", [...COMPOSE, "up", "-d"], { stdio: "inherit" });
  for (let i = 0; i < 60; i++) {
    if (await dbReady()) break;
    await sleep(1000);
  }
  if (!(await dbReady())) throw new Error("postgres did not become ready");
  await initSchema();
  steps.push("postgres ready");

  if (await stubHealthy()) steps.push("stub already running");
  else {
    const pid = startDetached("stub", ["src/stub.ts", "--port", "8080"]);
    for (let i = 0; i < 40; i++) {
      if (await stubHealthy()) break;
      await sleep(250);
    }
    if (!(await stubHealthy())) throw new Error("stub did not become healthy");
    steps.push(`stub started (pid ${pid}, port 8080)`);
  }

  const started: number[] = [];
  for (let i = 0; i < workers; i++) {
    if (readPid(`worker-${i}`)) continue;
    started.push(startDetached(`worker-${i}`, ["src/worker.ts", String(i)]));
  }
  // any worker beyond the requested count is stopped so `up` is idempotent per --workers
  for (let i = workers; i < 32; i++) {
    const pid = readPid(`worker-${i}`);
    if (pid) {
      try {
        process.kill(pid, "SIGTERM");
      } catch {}
      fs.rmSync(pidFile(`worker-${i}`), { force: true });
    }
  }
  steps.push(`workers: ${workers} requested, ${started.length} newly started`);
  return { ok: true, workers, steps };
}

export async function down() {
  const stopped: string[] = [];
  if (fs.existsSync(RUN_DIR)) {
    for (const f of fs.readdirSync(RUN_DIR).filter((f) => f.endsWith(".pid"))) {
      const name = f.replace(/\.pid$/, "");
      const pid = readPid(name);
      if (pid) {
        try {
          process.kill(pid, "SIGTERM");
          stopped.push(`${name}(${pid})`);
        } catch {}
      }
      fs.rmSync(path.join(RUN_DIR, f), { force: true });
    }
  }
  await sleep(500);
  execFileSync("docker", [...COMPOSE, "down", "-v"], { stdio: "inherit" });
  return { ok: true, stopped };
}

export async function liveWorkers() {
  const r = await db().query("select worker_id, pid, worker_index from workers where alive order by worker_index");
  return r.rows.filter((w) => alive(w.pid));
}

export async function killWorker(index?: number) {
  const live = await liveWorkers();
  const target = index === undefined ? live[0] : live.find((w) => w.worker_index === index);
  if (!target) throw new Error(`no live worker${index === undefined ? "" : ` with index ${index}`}`);
  const owned = await db().query(
    "select item_id, run_id, tenant, state from items where worker_id=$1 and terminal_at is null",
    [target.worker_id],
  );
  process.kill(target.pid, "SIGKILL"); // no supervisor: it stays dead
  await db().query("update workers set alive=false where worker_id=$1", [target.worker_id]);
  fs.rmSync(pidFile(`worker-${target.worker_index}`), { force: true });
  return {
    killed: { worker_id: target.worker_id, pid: target.pid, index: target.worker_index },
    owned_nonterminal_items: owned.rows,
    killed_at: new Date().toISOString(),
  };
}

export async function truncateState() {
  await initSchema();
  await db().query("truncate items, runs, annotation_cache");
}

export async function stubReset(patch: Record<string, unknown> = {}) {
  const r = await fetch(`${STUB_URL}/v1/reset`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(patch),
  });
  if (!r.ok) throw new Error(`stub reset failed: ${r.status}`);
  return r.json();
}

export async function stubStats() {
  const r = await fetch(`${STUB_URL}/v1/stats`);
  return r.json() as Promise<any>;
}
