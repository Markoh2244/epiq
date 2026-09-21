import { Pool } from "pg";

export const DB_URL = process.env.INTAKE_DB_URL ?? "postgres://intake:intake@127.0.0.1:55432/intake";
export const STUB_URL = process.env.STUB_URL ?? "http://127.0.0.1:8080";

let pool: Pool | null = null;
export function db(): Pool {
  if (!pool) pool = new Pool({ connectionString: DB_URL, max: 4 });
  return pool;
}

export async function closeDb() {
  if (pool) await pool.end();
  pool = null;
}

const SCHEMA = `
create table if not exists runs (
  run_id      text primary key,
  tenant      text not null,
  corpus_id   text not null,
  corpus_dir  text not null,
  item_count  int  not null,
  created_at  timestamptz not null default now()
);
create table if not exists items (
  item_id        text primary key,
  run_id         text not null references runs(run_id),
  tenant         text not null,
  corpus_id      text not null,
  source_path    text not null,
  abs_path       text not null,
  extension      text not null,
  declared_bytes int  not null,
  bytes          int,
  sha256         text,
  state          text not null,
  attempts       int  not null default 0,
  retries        int  not null default 0,
  annotate_calls int  not null default 0,
  error          jsonb,
  extracted_text text,
  annotation     jsonb,
  worker_id      text,
  lease_until    timestamptz,
  created_at     timestamptz not null default now(),
  updated_at     timestamptz not null default now(),
  terminal_at    timestamptz
);
create index if not exists items_claim_idx on items (state, lease_until);
create index if not exists items_run_idx   on items (run_id, state);
create index if not exists items_tenant_idx on items (tenant, item_id);
-- annotation reuse for repeated bytes, scoped per tenant so nothing crosses tenants
create table if not exists annotation_cache (
  tenant     text not null,
  sha256     text not null,
  annotation jsonb not null,
  created_at timestamptz not null default now(),
  primary key (tenant, sha256)
);
create table if not exists workers (
  worker_id  text primary key,
  pid        int not null,
  worker_index int not null,
  started_at timestamptz not null default now(),
  alive      boolean not null default true
);
`;

export async function initSchema() {
  await db().query(SCHEMA);
}

export const TERMINAL_STATES = ["annotated", "empty_content", "decode_failed", "annotation_failed", "unreadable"];
