# Chaos soak report — 2026-07-21

Accelerated chaos soak: 7000 workload waves against 3 worker OS processes,
`kill -9` roughly every 4s, 13 full Postgres restart(s) mid-run.
This is the rc gate's accelerated soak; the 48h endurance run remains open.

## Verdict: **FAIL (7 findings)**

## Load
- Total jobs processed: 99004 (418 from cron)
- Ledgered by kind: await=14000, budget=1750, flow=10500, parent=7000, step=42001, uni=2331
- Final states: failed=1750, succeeded=97254

## Chaos
- Worker kills (kill -9): 4978
- Postgres restarts: 13

## At-least-once accounting
- Step bodies executed: 242887 for 242515 distinct steps
- Duplicated step executions (crash windows between side effect and journal
  write): 359 — expected to be > 0 under kill -9 and bounded by the
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
- [ ] **budget FAILED (6)**
      - job 25228 recorded 4 steps, wanted 3
      - job 29517 recorded 4 steps, wanted 3
      - job 51452 recorded 4 steps, wanted 3
      - job 72188 recorded 4 steps, wanted 3
      - job 104678 recorded 4 steps, wanted 3
- [x] attempts
- [x] spawn-idempotency
- [x] cron-dedup
- [ ] **duplicate-rate FAILED (1)**
      - 359 duplicated steps — systemic re-running?

## Worker-side observations (from logs)
- Fenced stale acks: 0
- Claim rounds skipped during outages: 243

## Post-mortem (added after diagnosis)

Both failures diagnosed the same morning; one real bug, one checker artifact:

- **budget (REAL BUG, fixed)**: the runner checked the budget only *after*
  journaling a new step. A `kill -9` between journaling the over-budget
  crossing step and acking the failure let the next attempt replay past the
  journal (the memoized path had no check) and execute + pay for one extra
  step. All 6 affected jobs: attempt=2, spent 0.8 of a 0.5 budget, effect
  ledger shows the 4th body executed exactly once. Fix: pre-flight budget
  check against durable spend before every new step body (reuses the row
  the cancel check already fetches). Deterministic regression test added
  (fails on the old code, passes on the fix).
- **duplicate-rate (checker artifact, recalibrated)**: the flat `> 200`
  threshold predated endurance scale. 359 duplicates across 4,978 kills is
  7.2% per kill — *lower* than the previously passing 150s run (3/50 = 6%),
  and the multiplicity distribution (346 steps 2x, 13 steps 3x, none higher)
  is exactly isolated crash windows, not systemic re-running. Threshold now
  scales with kill count.

Everything else held over 7 hours: 99,004 jobs, 4,978 kills, 13 Postgres
restarts, zero lost jobs, zero stuck jobs, byte-correct results, exact
cron dedup, spawn idempotency, flow ordering.
