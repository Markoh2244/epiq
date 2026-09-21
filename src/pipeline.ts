// Per-item processing (section 4) plus the stub-boundary retry policy (D-05).
import { createHash } from "node:crypto";
import fs from "node:fs/promises";
import { PoolClient } from "pg";
import { db, STUB_URL } from "./db.js";

export const POLICY = {
  request_timeout_ms: 5000,
  max_attempts: 4, // 1 try + 3 retries per item-processing pass
  backoff_ms: [200, 400, 800],
  retryable_statuses: [429, 500, 502, 503, 504],
  lease_ms: 5000,
  heartbeat_ms: 1500,
  annotate_slots: Number(process.env.INTAKE_ANNOTATE_SLOTS ?? 2),
};

const SLOT_LOCK_NS = 918273;

/** Cluster-wide limiter: one Postgres advisory lock per stub in-flight slot, so the
 *  shared cap holds across all worker processes (REQ-2.2). */
async function withSlot<T>(lockClient: PoolClient, fn: () => Promise<T>): Promise<T> {
  for (;;) {
    for (let slot = 1; slot <= POLICY.annotate_slots; slot++) {
      const r = await lockClient.query("select pg_try_advisory_lock($1,$2) as got", [SLOT_LOCK_NS, slot]);
      if (r.rows[0].got) {
        try {
          return await fn();
        } finally {
          await lockClient.query("select pg_advisory_unlock($1,$2)", [SLOT_LOCK_NS, slot]);
        }
      }
    }
    await new Promise((r) => setTimeout(r, 25));
  }
}

type AnnotateResult =
  | { ok: true; annotation: unknown; calls: number; retries: number }
  | { ok: false; code: string; detail: string; calls: number; retries: number };

async function callStub(bytes: Buffer): Promise<{ status: number; body: any } | { status: 0; body: any }> {
  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(), POLICY.request_timeout_ms);
  try {
    const res = await fetch(`${STUB_URL}/v1/annotate`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ content_b64: bytes.toString("base64") }),
      signal: ac.signal,
    });
    return { status: res.status, body: await res.json().catch(() => null) };
  } catch (e) {
    return { status: 0, body: { error: { code: "transport_error", message: String(e) } } };
  } finally {
    clearTimeout(t);
  }
}

async function annotate(
  lockClient: PoolClient,
  bytes: Buffer,
  recordCall: () => Promise<unknown>,
): Promise<AnnotateResult> {
  let calls = 0;
  let retries = 0;
  let last = { code: "unknown", detail: "" };
  for (let attempt = 0; attempt < POLICY.max_attempts; attempt++) {
    if (attempt > 0) {
      retries++;
      await new Promise((r) => setTimeout(r, POLICY.backoff_ms[Math.min(attempt - 1, POLICY.backoff_ms.length - 1)]));
    }
    const res = await withSlot(lockClient, async () => {
      calls++;
      // Persist the attempt before crossing the HTTP boundary. If this worker is
      // SIGKILLed after the stub bills it, the final counter still reconciles.
      await recordCall();
      return callStub(bytes);
    });
    if (res.status === 200 && res.body?.annotation) return { ok: true, annotation: res.body.annotation, calls, retries };
    const code = res.body?.error?.code ?? `http_${res.status}`;
    last = { code, detail: JSON.stringify(res.body ?? null).slice(0, 300) };
    if (!POLICY.retryable_statuses.includes(res.status) && res.status !== 0) break; // 400 etc: not retryable
  }
  return { ok: false, code: last.code, detail: last.detail, calls, retries };
}

const TEXT_EXTS = new Set(["txt", "csv", "json"]);

function decodeUtf8(bytes: Buffer): string {
  const text = new TextDecoder("utf-8", { fatal: true }).decode(bytes); // throws on invalid utf-8
  return text;
}

/** Extract text/metadata for one item; throws {code} style errors for decode failures. */
function extract(ext: string, bytes: Buffer): { text: string | null } {
  if (ext === "png") {
    const sig = Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]);
    if (!bytes.subarray(0, 8).equals(sig)) throw { code: "decode_failed", detail: "not a PNG (bad signature)" };
    return { text: null }; // binary: no text carried by the declared type (ITEM-REQ-2)
  }
  if (!TEXT_EXTS.has(ext)) throw { code: "decode_failed", detail: `unsupported extension .${ext}` };
  let text: string;
  try {
    text = decodeUtf8(bytes);
  } catch {
    throw { code: "decode_failed", detail: "invalid utf-8 for declared text type" };
  }
  if (ext === "json") {
    try {
      JSON.parse(text);
    } catch (e) {
      throw { code: "decode_failed", detail: `json parse error: ${String(e).slice(0, 120)}` };
    }
  }
  if (ext === "csv") {
    const rows = text.split(/\r?\n/).filter((l) => l.length > 0);
    if (rows.length === 0) throw { code: "decode_failed", detail: "csv has no rows" };
    const cols = rows[0].split(",").length;
    if (rows.some((r) => r.split(",").length !== cols))
      throw { code: "decode_failed", detail: "csv rows have inconsistent column counts" };
  }
  return { text };
}

export async function processItem(lockClient: PoolClient, item: any, workerId: string, log: (s: string) => void) {
  const set = async (sql: string, params: unknown[]) =>
    db().query(`update items set updated_at=now(), ${sql} where item_id=$1`, [item.item_id, ...params]);
  const terminal = async (state: string, extra: string, params: unknown[]) =>
    set(`state=$${params.length + 2}, terminal_at=now(), lease_until=null${extra ? ", " + extra : ""}`, [
      ...params,
      state,
    ]);

  let bytes: Buffer;
  try {
    bytes = await fs.readFile(item.abs_path);
  } catch (e) {
    await terminal("unreadable", "error=$2", [{ code: "unreadable", detail: String(e).slice(0, 200) }]);
    return log(`${item.item_id} unreadable`);
  }

  const sha = createHash("sha256").update(bytes).digest("hex");
  await set("bytes=$2, sha256=$3", [bytes.length, sha]);

  if (bytes.length === 0) {
    // pre-annotation terminal error: no annotation request is issued (ITEM-REQ-4)
    await terminal("empty_content", "error=$2", [{ code: "empty_content", detail: "file has zero bytes" }]);
    return log(`${item.item_id} empty_content`);
  }

  let text: string | null;
  try {
    text = extract(item.extension, bytes).text;
  } catch (e: any) {
    await terminal("decode_failed", "error=$2", [{ code: e.code ?? "decode_failed", detail: e.detail ?? String(e) }]);
    return log(`${item.item_id} decode_failed`);
  }
  await set("extracted_text=$2", [text]);

  const cached = await db().query("select annotation from annotation_cache where tenant=$1 and sha256=$2", [
    item.tenant,
    sha,
  ]);
  if (cached.rowCount) {
    await terminal("annotated", "annotation=$2", [cached.rows[0].annotation]);
    return log(`${item.item_id} annotated (cache hit)`);
  }

  const r = await annotate(lockClient, bytes, async () => {
    await db().query("update items set annotate_calls=annotate_calls+1 where item_id=$1", [item.item_id]);
  });
  await db().query("update items set retries=retries+$2 where item_id=$1", [item.item_id, r.retries]);
  if (r.ok === false) {
    await terminal("annotation_failed", "error=$2", [
      { code: "annotation_failed", stub_error: r.code, detail: r.detail, calls: r.calls },
    ]);
    return log(`${item.item_id} annotation_failed (${r.code})`);
  }
  await db().query(
    "insert into annotation_cache (tenant, sha256, annotation) values ($1,$2,$3) on conflict do nothing",
    [item.tenant, sha, r.annotation],
  );
  await terminal("annotated", "annotation=$2", [r.annotation]);
  log(`${item.item_id} annotated`);
}
