# Chaos soak report — 2026-07-20

Accelerated chaos soak: 36 workload waves against 3 worker OS processes,
`kill -9` roughly every 4s, 2 full Postgres restart(s) mid-run.
This is the rc gate's accelerated soak; the 48h endurance run remains open.

## Verdict: **PASS**

## Load
- Total jobs processed: 511 (4 from cron)
- Ledgered by kind: await=72, budget=9, flow=54, parent=36, step=216, uni=12
- Final states: failed=9, succeeded=502

## Chaos
- Worker kills (kill -9): 27
- Postgres restarts: 2

## At-least-once accounting
- Step bodies executed: 1249 for 1249 distinct steps
- Duplicated step executions (crash windows between side effect and journal
  write): 0 — expected to be > 0 under kill -9 and bounded by the
  kill count; results above prove journaled values stayed correct regardless.

## Invariants
- [x] quiescence
- [x] lost-jobs
- [x] step
- [x] parent
- [x] await
- [x] flow
- [x] flow-order
- [x] uni
- [x] budget
- [x] attempts
- [x] spawn-idempotency
- [x] cron-dedup
- [x] duplicate-rate

## Worker-side observations (from logs)
- Fenced stale acks: 0
- Claim rounds skipped during outages: 14
