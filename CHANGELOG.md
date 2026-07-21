# Changelog

## Unreleased

### Fixed
- **Budgets are now enforced before every step execution, not only after.**
  The 7-hour endurance soak (99,004 jobs, 4,978 worker kills, 13 Postgres
  restarts) caught 6 jobs paying for one step past their budget: the check
  ran only after journaling a *new* step, so a crash between journaling the
  over-budget step and acking the failure let the next attempt replay past
  the journal and execute one more paid step. The runner now pre-flights
  the budget against durable spend before running any step body (no extra
  queries — it reuses the row the cancel check already fetches). A
  deterministic regression test pins the crash window.
- **Cancel requests now survive crashes and retries.** The adapter-
  equivalence property test (below) caught the Postgres ack path clearing
  `cancel_requested` on reclaim/retry while Memory preserved it — silently
  losing a user's cancellation if the worker died before honoring it. The
  flag now persists across all non-terminal transitions on both adapters;
  the wire contract states it normatively.

### Added
- **`SCHEMA.md` — the wire contract.** The Postgres schema specified as a
  versioned protocol: tables, state machine, annotated SQL for every
  operation, advisory-lock discipline, the two soak-learned race rules, and
  a conformance path (the soak driver verifies any foreign worker SDK by
  reading the database). Groundwork for Python/TypeScript SDKs as thin
  contract implementations rather than rewrites.
- **`Capstan.Codec` — cross-language value envelope.** Step values and job
  results now decode as Erlang term format (leading byte 131, written by
  Elixir) *or* UTF-8 JSON (written by any other SDK); tested with
  foreign-written JSON rows replaying through the engine.

## 1.0.0-rc.4 — 2026-07-20

The "better at every dimension" release: the embedded dashboard, transactional
enqueue, runtime CRUD, encryption, and exact partitioned claims.

### Added
- **`Capstan.Dashboard`** — an embedded web dashboard with zero dependencies
  (hand-rolled HTTP over `gen_tcp`, single-file UI, SSE live updates): queue
  tiles with limits and live counts, filterable job list, a journal drawer
  (steps with costs, events, errors, children), a rendered **workflow DAG**,
  and retry/cancel/signal/steer actions behind an optional token and the same
  pluggable authorizer as the MCP server.
- **`Capstan.Txn`** — transactional enqueue inside your own Postgrex or Ecto
  transaction (duck-typed over `query!`; still no Ecto dependency). With the
  `:postgres` notifier, the wake-up is issued via `pg_notify` *inside* the
  transaction, so it delivers exactly on commit and never on rollback.
- **Runtime queue and cron CRUD** — `Capstan.Queues.put/delete/list` and
  `Capstan.Crons.put/delete/pause/resume/list`, persisted in the database,
  validated eagerly, reconciled by every node's `QueueSync` (producers now
  live under a DynamicSupervisor); dynamic entries override static config by
  name. Leaderless, like everything else.
- **Encrypted inputs** — `use Capstan.Worker, encrypted: true` plus
  `encryption: [key: {mod, fun, args}]`: AES-256-GCM envelopes at rest,
  plaintext only inside the executing process; schemas validate before
  encryption; replay decrypts transparently.
- Shared view serializers (lib/capstan/view.ex) so the dashboard and MCP describe jobs
  identically.
- `bench/throughput.exs` — measured ~416 trivial jobs/s end-to-end on a
  laptop (3 worker processes, unbatched acks).

### Changed
- **Partitioned claims are now exact.** Per-key allowances are computed with
  a window-function ranking inside the claim transaction (under the queue
  advisory lock), replacing the bounded over-fetch heuristic — heavy key
  skew can no longer starve minority keys (regression-tested with 30:1 skew).

## 1.0.0-rc.3 — 2026-07-20

Dispatch-latency release: agent workloads are bursts of short tasks, so
insert→result overhead is the product. Measured on stock settings
(`bench/run.sh`): ~9ms p50 same-node, ~11ms p50 / ~25ms p99 across
unconnected OS processes with the new notifier, ~50ms p50 on adaptive
polling alone (vs ~250ms average before).

### Added
- **Notifier layer** (`Capstan.Notifier`): wake-ups as accelerators, never
  load-bearing. `:local` (registry + `:pg`, always on) and opt-in
  `:postgres` — `pg_notify` pokes and result notifications across fleets
  that share Postgres but not an Erlang cluster, with a dedicated
  auto-reconnecting listen connection per node and channel names scoped per
  database. If the channel is down, latency falls back to the polling floor;
  correctness never depends on NOTIFY.
- **Adaptive burst polling**: producers poll at `busy_poll` (default 25ms)
  while claiming work and decay exponentially to `poll_interval` when idle;
  poke storms coalesce into single claim rounds.
- **Fast `await_result`**: wakes on result notifications; otherwise
  re-checks on a 5ms→200ms backoff instead of a fixed 200ms.
- **MCP authorizer hook**: `mix capstan.mcp --authorizer MyGuard` (or
  `authorizer:` on `Capstan.MCP.serve/2`) gates the mutating tools
  (retry/cancel/signal/steer) behind `authorize(tool, args)` — the mount
  point for capability-token systems (e.g. Legant) supervising operating
  agents.
- `bench/` — reproducible latency benchmark across the three topologies.

### Fixed
- Soak/bench harnesses now clean up respawned workers by pattern (stray
  workers were exhausting Postgres connections).

## 1.0.0-rc.2 — 2026-07-20

Hardening release driven by the chaos soak harness (`soak/`), which runs a
mixed workload across multiple worker OS processes under `kill -9` and full
Postgres restarts, then verifies thirteen invariants. The passing report
lives in `soak/REPORT.md`.

### Fixed
- **Parent wake-up races in dynamic children.** Under READ COMMITTED, the
  last two children acking concurrently could each see the other as
  incomplete and both skip the parent's `$children` signal, parking the
  parent forever. Fixed in layers: children now signal unconditionally on
  every terminal ack; signal delivery and awaiting-parking serialize on a
  per-scope advisory lock; and the sweeper re-readies any parent awaiting
  `$children` whose children are all terminal — so even an unknown residual
  ordering degrades to a sweep-interval delay, loudly logged, never a stuck
  job.
- **Database-outage resilience.** Producers, the lease keeper, the sweeper,
  and the cron scheduler now rescue storage failures and skip the cycle with
  a warning instead of crash-looping the supervision tree; restart budgets
  are lenient. A full Postgres restart mid-load is survived with claim
  rounds skipped and no losses.
- **Spawn idempotency across the insert/journal gap.** Children carry
  always-scoped unique keys derived from (parent, spawn name, index), and
  the id list is rebuilt from those keys — a crash between inserting
  children and recording the spawn step can no longer duplicate them.

### Added
- `soak/` chaos harness (workers, driver with ledgered expectations,
  kill/restart orchestration, invariant verification, report generation).
- Third uniqueness scope: `unique: [key: k, scope: :always]`.
- Sub-second worker timeouts: `timeout: {n, :millisecond}`.

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
