# M2 Evaluation

Run the required paired profile with:

```bash
./intake scenario
```

The command regenerates the two recorded corpora, runs a clean control and fault execution, prints every assertion, and writes the current machine-readable evidence to `evaluation/raw/`.

## Fixed profile

- Tenant A: `tenant-a`, seed `20260803`, 500 items
- Tenant B: `tenant-b`, seed `771103`, 400 items
- Four independently killable workers
- Stub: fixed 150ms latency, 500 on every seventh billed request, capacity 2
- Policy: 5s timeout; four total attempts; 200/400/800ms retry backoff; 5s lease; 1.5s heartbeat

## Evidence and verdict

The authoritative result is `evaluation/raw/scenario.json`; companion `control.json`, `fault.json`, and worker logs include run IDs, timestamps, state totals, attempts, retries, recovery timing, and stub counters. A scenario exits zero only when every mandatory assertion passes.

Latest run: **PASS — 24/24 assertions passed, zero mandatory failures**.

- Control: 79.400s, 11.34 items/s, 960 billed calls, 137 scheduled server errors, zero over-capacity calls.
- Fault: 79.858s, 11.27 items/s, 957 billed calls, 136 scheduled server errors, zero over-capacity calls.
- Fault recovery: one worker was killed while owning one nonterminal item; another worker resumed it in 4.736s (within the 10s requirement).
- Both executions processed all 900 items with the required tenant overlap, edge-case outcomes, tenant-isolation negative checks, and terminal states.

The billed-call delta is -3. The only injected difference is the worker kill; it changes claim/retry ordering. Since the stub fails every Nth billed request and cache hits are tenant-scoped, that scheduling change can alter the total number of retries and billed calls. Attempt records are persisted before each HTTP call, so both executions reconcile `billed_calls` exactly with recorded annotation attempts.
