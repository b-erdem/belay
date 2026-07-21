# Chaos soak report — 2026-07-21

Accelerated chaos soak: 480 workload waves against 3 worker OS processes,
`kill -9` roughly every 4s, 5 full Postgres restart(s) mid-run.
This is the rc gate's accelerated soak; the 48h endurance run remains open.

## Verdict: **PASS**

## Load
- Total jobs processed: 6790 (30 from cron)
- Ledgered by kind: await=960, budget=120, flow=720, parent=480, step=2880, uni=160
- Final states: failed=120, succeeded=6670

## Chaos
- Worker kills (kill -9): 341
- Postgres restarts: 5

## At-least-once accounting
- Step bodies executed: 16672 for 16630 distinct steps
- Duplicated step executions (crash windows between side effect and journal
  write): 40 — expected to be > 0 under kill -9 and bounded by the
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
- Claim rounds skipped during outages: 81
