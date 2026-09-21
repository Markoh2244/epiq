# Decisions

| ID | Decision | Why / trade-off | Date |
|---|---|---|---|
| D-01 | Workers claim one Postgres-backed item lease at a time with `FOR UPDATE SKIP LOCKED`; leases expire after 5 seconds and heartbeat every 1.5 seconds. | A killed worker leaves recoverable work without a supervisor. Recovery can take up to the lease duration. | 2026-09-21 |
| D-02 | Every manifest entry becomes a distinct item. Annotations are cached by `(tenant, sha256)`. | Duplicate files remain independently queryable while repeated work is avoided. Tenant scoping prevents cache sharing. | 2026-09-21 |
| D-03 | Postgres holds runs, items, leases, annotations, and cache entries. CLI queries read directly from it. | One durable source of truth makes progress visible during a run. | 2026-09-21 |
| D-04 | Terminal states are `annotated`, `empty_content`, `decode_failed`, `annotation_failed`, and `unreadable`; failures carry JSON error codes. | Consumers can distinguish malformed input from an unavailable annotation dependency. | 2026-09-21 |
| D-05 | Annotation calls time out after 5 seconds and retry 429/5xx/transport failures up to 4 total attempts with 200/400/800ms backoff. Every attempt is recorded before its HTTP call. | The stub can bill an attempt before a worker dies; pre-recording preserves counter reconciliation, at the cost of an attempted-but-not-completed record. | 2026-09-21 |

| D-07 | Docker supplies Postgres; local detached processes supply the stub and workers. | This demonstrates leases, process loss, and HTTP failure locally; it does n| D-06 | Secondary thresholds: ≤1% `annotation_failed`, ≤0.35 retries/item, and fault duration ≤1.5× control +30s. | These are visibility thresholds only; they never replace mandatory requirements. | 2026-09-21 |ot model multi-host or database-region failure. | 2026-09-21 |
