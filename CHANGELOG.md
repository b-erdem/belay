# Changelog

## 1.0.0-rc.1 — 2026-07-20

First release candidate. Everything below ships open, Apache-2.0, with no
paid tier.

### Engine
- Claim/lease/ack execution with attempt fencing; expired leases reclaimed
  cluster-wide in seconds, stale acks rejected.
- One-rule scheduling: a job is claimable when `ready_at` is due — scheduled
  work, retry backoff, and snoozes share it; there is no staging step and no
  leader election anywhere.
- Poll-first dispatch with in-cluster pokes; no LISTEN/NOTIFY dependency.
- Graceful shutdown: producers stop claiming first; running jobs get
  `shutdown_grace`; the cluster reclaims the rest.
- Per-worker execution `timeout:`; queue `pause`/`resume` at runtime.
- Constraint-backed **unique jobs**: `unique: "key"` (while incomplete) and
  `unique: [key: k, within: seconds]` (per window); duplicates return the
  existing job flagged `duplicate?: true`.
- Retention pruning per terminal state, cascading steps and events; signal
  TTLs; rate-window cleanup.
- Leaderless cron with per-slot dedup via a unique index.
- Admission control per queue: `global_limit` (live-leased counting),
  sliding-window `rate` limits, per-key `partition` fairness.

### The agent layer
- **Durable steps** (`Capstan.step/4`): memoized per job with cost columns —
  retries replay past completed work.
- **Budgets** (`budget: [usd:, tokens:]`): jobs fail with `:budget_exceeded`
  the moment accumulated step costs cross the cap.
- **Signals** (`await/3`, `signal_job/4`): park at zero cost, wake instantly,
  deadline timeouts; **steering** (`steer/3` → `steering/1`) injects guidance
  into running jobs; cooperative cancellation at step boundaries.
- **Dynamic children**: `spawn/3` and `spawn_many/3` (replay-safe, memoized),
  `await_children/1`, `map_children/5` fan-out/fan-in — agents grow their own
  DAGs at runtime.
- **Workflows**: DAG dependencies in the `held` state, released and cascaded
  transactionally inside the completing job's ack; `ignore:` policies.
- **Batches**: transparent sugar over workflows with an any-outcome
  `on_complete` callback.
- **Durable event streams**: `emit/2` + live subscriptions + offset replay.
- **Replay debugging**: `Capstan.Replay.dry_run/2` re-runs job code against
  the recorded journal, side-effect-free, with precise divergence reports.
- **Token-resource rate limits**: shared resource buckets with estimated
  admission and post-hoc true-up via `debit/3`.
- **Input schemas**: insert-time validation raising at the call site.

### Operability
- `stats/1`, `list_jobs/2`, `retry_job/2`, `steps/2`, `events/3`.
- Telemetry spans with durations; `Capstan.Telemetry.attach_default_logger/1`.
- **MCP server** (`mix capstan.mcp`): stats/jobs/steps/events introspection
  plus retry/cancel/signal/steer, over stdio JSON-RPC.

### Storage
- Behaviour with two adapters: deterministic in-memory (test/simulation
  reference) and Postgres (Postgrex, hand-written SQL, clock passed as a
  parameter). The same 64-test suite runs against both.
