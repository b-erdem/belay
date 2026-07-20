# Chaos soak report — 2026-07-20

Accelerated chaos soak: 70 workload waves against 3 worker OS processes,
`kill -9` roughly every 4s, 2 full Postgres restart(s) mid-run.
This is the rc gate's accelerated soak; the 48h endurance run remains open.

## Verdict: **PASS**

## Load
- Total jobs processed: 990 (5 from cron)
- Ledgered by kind: await=140, budget=17, flow=105, parent=70, step=420, uni=23
- Final states: failed=17, succeeded=973

## Chaos
- Worker kills (kill -9): 50
- Postgres restarts: 2

## At-least-once accounting
- Step bodies executed: 2429 for 2426 distinct steps
- Duplicated step executions (crash windows between side effect and journal
  write): 3 — expected to be > 0 under kill -9 and bounded by the
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
- Claim rounds skipped during outages: 36
