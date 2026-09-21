# Content Intake Pipeline

Local TypeScript implementation of the take-home content intake pipeline. The fixed entrypoint is `./intake`; all source is under `src/`.

## Prerequisites

- Docker Desktop with the Docker daemon running
- Node.js 22+
- Python 3.14+ for the black-box stub conformance suite (standard library only)

The first `./intake` command installs npm dependencies automatically if `node_modules/` is missing.

## Quick Start

```bash
./intake corpus --seed 42 --size 60 --tenant tenant-x --out ./corpora/demo --force
./intake corpus --verify --out ./corpora/demo

./intake reset --workers 4
./intake submit --corpus ./corpora/demo --tenant tenant-x
./intake status --run <run_id>
./intake items --tenant tenant-x --run <run_id>
./intake item --tenant tenant-x --id <item_id>
./intake down
```

`./intake up --workers 4` starts Postgres in Docker, the HTTP annotation stub on port `8080`, and four independently killable worker OS processes. Durable state lives in Postgres on local port `55432`.

## Command Interface

- `./intake corpus --seed <int> --size <50..500> --tenant <t> --out <dir> [--force]`
- `./intake corpus --verify --out <dir>`
- `./intake stub [--port <p>]`
- `./intake up [--workers <n>]`
- `./intake down`
- `./intake reset [--workers <n>]`
- `./intake submit --corpus <dir> --tenant <t>`
- `./intake status --run <run_id>`
- `./intake item --tenant <t> --id <item_id>`
- `./intake items --tenant <t> --run <run_id> [--state <s>]`
- `./intake kill-worker [--index <n>]`
- `./intake scenario`

Query commands print JSON to stdout.

## M2 Scenario Profile

`./intake scenario` regenerates and runs the required paired executions:

- Control: no worker killed
- Fault: one live worker is SIGKILLed and not restarted
- Workers: `4`
- Stub: fixed `150ms` latency, every 7th billed admitted request returns `500`, capacity `2`
- Tenant A: `tenant-a`, seed `20260803`, size `500`
- Tenant B: `tenant-b`, seed `771103`, size `400`

Raw JSON, logs, assertion results, run IDs, timings, retry counts, terminal-state counts, recovery data, and stub stats are written under `evaluation/raw/`.

## Testing

Start the environment, then run the black-box stub conformance tests:

```bash
./intake reset --workers 4
python3 tests/test_stub_conformance.py
```

Run the full robustness harness:

```bash
./intake scenario
```

## Submission Documents

- `DECISIONS.md` records D-01 through D-07.
- `ARCHITECTURE.md` contains the current component/data-flow diagram.
- `EVALUATION.md` describes the M2 profile and evidence.
- `traces/README.md` describes the included coding-agent trace and redactions.

## Known Limitations

- The local environment models one Postgres instance and local worker processes. It does not model multi-host networking or database failover.
- The worker can be killed after an external dependency has billed a call; attempts are persisted before the HTTP request so the final counters remain reconcilable, but exactly-once billing across crashes is not claimed.
- Always run `./intake scenario` after changing the code and review the generated evidence before submission.

