// One independently killable OS worker process (REQ-1.4, REQ-2.1).
// Ownership lives in Postgres: a claim is a lease, and an expired lease makes the
// item runnable again for any other worker (D-01).
import { db, initSchema, closeDb } from "./db.js";
import { POLICY, processItem } from "./pipeline.js";

const index = Number(process.argv[2] ?? 0);
const workerId = `w${index}-${process.pid}`;
const log = (s: string) => console.log(`${new Date().toISOString()} ${workerId} ${s}`);

const CLAIM = `
update items set state='processing', worker_id=$1, attempts=attempts+1,
       lease_until=now() + ($2::int * interval '1 millisecond'), updated_at=now()
where item_id = (
  select item_id from items
   where state='pending' or (state='processing' and lease_until < now())
   order by created_at, item_id
   limit 1 for update skip locked)
returning *`;

async function main() {
  await initSchema();
  const lockClient = await db().connect(); // dedicated session holding advisory slot locks
  await db().query(
    `insert into workers (worker_id, pid, worker_index, alive) values ($1,$2,$3,true)
     on conflict (worker_id) do update set alive=true`,
    [workerId, process.pid, index],
  );
  log(`up (pid ${process.pid})`);

  let stopping = false;
  const shutdown = async () => {
    stopping = true;
    await db().query("update workers set alive=false where worker_id=$1", [workerId]).catch(() => {});
    await closeDb().catch(() => {});
    process.exit(0);
  };
  process.on("SIGTERM", shutdown);
  process.on("SIGINT", shutdown);

  while (!stopping) {
    const r = await db().query(CLAIM, [workerId, POLICY.lease_ms]);
    if (!r.rowCount) {
      await new Promise((res) => setTimeout(res, 200));
      continue;
    }
    const item = r.rows[0];
    log(`claimed ${item.item_id} attempt ${item.attempts}`);
    const hb = setInterval(() => {
      db()
        .query(
          `update items set lease_until=now() + ($2::int * interval '1 millisecond')
             where item_id=$1 and worker_id=$3 and state='processing'`,
          [item.item_id, POLICY.lease_ms, workerId],
        )
        .catch(() => {});
    }, POLICY.heartbeat_ms);
    try {
      await processItem(lockClient, item, workerId, log);
    } catch (e) {
      log(`ERROR ${item.item_id} ${String(e)}`);
      // leave the lease to expire: another worker picks it up (D-01)
    } finally {
      clearInterval(hb);
    }
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
