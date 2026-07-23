# Changelog

## 1.0.0-rc.6 — 2026-07-23

### Changed
- **Breaking (API naming).** Two job-control functions gained the `_job`
  suffix so the family is consistent and matches Oban. `Belay.cancel_job/2`
  and `Belay.steer_job/3` replace the former `cancel` and `steer`. Update
  call sites; behaviour is unchanged. This is a deliberate pre-1.0 rename to
  freeze a consistent surface before the stable release.

## 1.0.0-rc.5 — 2026-07-21

Launch-prep release: formal verification layer, chunk workers, adaptive
concurrency, the Oban migrator, dashboard v2, and the fixes surfaced by a
pre-launch adversarial review.

### Added
- **Formal verification layer (`verify/`).** A TLA+ model of the
  durable-execution core (journaling, budgets, cooperative cancel,
  crash/reclaim, settlement), generated from source + execution traces and
  exhaustively checked by TLC — 187,975,659 distinct states (depth 49),
  zero violations. Validated by mutation: reverting any of the three known
  production bugs in the model (the budget crash window; the cancel-request
  clear; the operator-retry semantics) yields a TLC counterexample matching
  the real-world failure step for step.
- **Schedule-explored wake protocol (`verify/wake_protocol/`).** The
  parent-wake protocol under Lockstep controlled concurrency: the pre-fix
  count-gated protocol loses the wake on a found, saved, replayable
  schedule (PCT iteration 1); the shipped unconditional protocol survives
  every explored interleaving without the sweeper reconciler — evidence
  the reconciler is a backstop, not load-bearing.
- **`SCHEMA.md` — the wire contract.** The Postgres schema specified as a
  versioned protocol: tables, state machine, annotated SQL for every
  operation, advisory-lock discipline, the two soak-learned race rules, and
  a conformance path (the soak driver verifies any foreign worker SDK by
  reading the database). Groundwork for Python/TypeScript SDKs as thin
  contract implementations rather than rewrites.
- **`Belay.Codec` — cross-language value envelope.** Step values and job
  results now decode as Erlang term format (leading byte 131, written by
  Elixir) *or* UTF-8 JSON (written by any other SDK); tested with
  foreign-written JSON rows replaying through the engine.

- **Chunk workers.** `chunk: [size: n, gather_ms: t]` +
  `run_chunk/1`: the producer gathers claimed jobs per worker and runs them
  as one invocation — one bulk INSERT instead of hundreds, one batch-priced
  embeddings call instead of a hundred singles. Per-job outcome maps retry
  only the failed jobs; gathered jobs are already leased, so crashes
  mid-gather reclaim cleanly. Full chunks dispatch without waiting for the
  gather window.
- **Adaptive per-queue concurrency.** `limit: [min: a, max: b]` scales each
  node's limit up while claim rounds come back saturated and decays it when
  the queue idles — leaderless, like the burst-poll cadence it mirrors, and
  exactly bounded by `global_limit`/rate/partition limits. Policy-driven
  scaling stays a recipe: drive runtime `Queues.put/3` from application code.

- **Dashboard v2.** KPI strip with live throughput and **spend-rate**
  sparklines (trailing-window snapshots persisted client-side), queue rows
  with limit-utilization bars (adaptive ranges shown as min–max), a
  two-line job list with args previews, durations, per-job cost badges and
  state-chip filters, and a restyled journal-timeline drawer. The spend
  KPI rides the same single stats scan the counts already paid for
  (`queue_stats` now returns spend sums); job summaries gained
  `max_attempts`, `input_preview`, `spent_usd_micros`, `started_at`.
  `?sse=0` renders a static snapshot for screenshot tooling.
- **`mix belay.migrate_oban`** — move an Oban installation's pending
  work in one command: dry-run analyzer (state census, per-worker port
  verification, `executing`-row warnings), faithful conversion
  (schedules, retry counts, errors, priority), `--map` renames, and
  idempotent re-runs via `meta.migrated_from_oban_id`. History stays put
  by design — the guide documents the archive pattern instead.

### Fixed
- **Dashboard and MCP mutations now fail closed.** A tokenless dashboard and
  the MCP server are read-only unless an authorizer or explicit mutation
  opt-in is configured. Dashboard writes reject query-string credentials,
  cross-origin requests, non-object/malformed JSON, bodies over 1 MiB, large
  header sets, and ambiguous request framing. Bearer-token comparison uses a
  constant-time digest check.
- **PostgreSQL URLs now match deployment reality.** Percent-encoded
  credentials/database names, Unix-socket hosts, connection query options,
  and secure `sslmode` values are parsed and tested; unsupported ambiguous
  TLS modes fail with a configuration error instead of silently weakening a
  connection. Explicit child-spec options retain precedence over URL values.
- **Queue pause/resume is synchronous.** The API now returns only after the
  producer has changed state, closing the race where a caller could pause a
  queue and immediately observe one more claim.
- **Zombie acknowledgements have a dedicated proof and dual-adapter
  regression.** A stale attempt cannot commit after lease expiry and reclaim;
  the focused TLA+ model completely checks the fence, while its no-fence
  mutant produces the expected `Claim(1) → Expire(1) → Claim(2) → Ack(1)`
  counterexample.
- **Operator retry now clears a pending cancel and respects workflow
  dependencies.** Found by extending the TLA+ model with a `Retry` action:
  (1) retry left `cancel_requested` set, so a cooperatively-cancelled job
  was un-retryable forever — ready, claimed, instantly re-cancelled, in a
  loop — and a retry racing a stale cancel was silently defeated; (2) retry
  sent workflow members straight to `ready`, so a cascade-cancelled
  dependent could run (and succeed) while its dependency sat failed. Retry
  now clears the flag (the operator's later intent wins) and re-holds
  workflow members through a settlement pass under the workflow lock —
  released if deps are satisfied, re-doomed if not. Both adapters, both
  confirmed by failing tests against the real engine before the fix.
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

## 1.0.0-rc.4 — 2026-07-20

The "better at every dimension" release: the embedded dashboard, transactional
enqueue, runtime CRUD, encryption, and exact partitioned claims.

### Added
- **`Belay.Dashboard`** — an embedded web dashboard with zero dependencies
  (hand-rolled HTTP over `gen_tcp`, single-file UI, SSE live updates): queue
  tiles with limits and live counts, filterable job list, a journal drawer
  (steps with costs, events, errors, children), a rendered **workflow DAG**,
  and retry/cancel/signal/steer actions. Tokenless dashboards are read-only;
  writes require a token or the same pluggable authorizer as the MCP server.
- **`Belay.Txn`** — transactional enqueue inside your own Postgrex or Ecto
  transaction (duck-typed over `query!`; still no Ecto dependency). With the
  `:postgres` notifier, the wake-up is issued via `pg_notify` *inside* the
  transaction, so it delivers exactly on commit and never on rollback.
- **Runtime queue and cron CRUD** — `Belay.Queues.put/delete/list` and
  `Belay.Crons.put/delete/pause/resume/list`, persisted in the database,
  validated eagerly, reconciled by every node's `QueueSync` (producers now
  live under a DynamicSupervisor); dynamic entries override static config by
  name. Leaderless, like everything else.
- **Encrypted inputs** — `use Belay.Worker, encrypted: true` plus
  `encryption: [key: {mod, fun, args}]`: AES-256-GCM envelopes at rest,
  plaintext only inside the executing process; schemas validate before
  encryption; replay decrypts transparently.
- Shared view serializers (lib/belay/view.ex) so the dashboard and MCP describe jobs
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
- **Notifier layer** (`Belay.Notifier`): wake-ups as accelerators, never
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
- **MCP authorizer hook**: `mix belay.mcp --authorizer MyGuard` (or
  `authorizer:` on `Belay.MCP.serve/2`) gates the mutating tools
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

First release candidate. Everything below ships open, Apache-2.0.

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
- **Durable steps** (`Belay.step/4`): memoized per job with cost columns —
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
- **Replay debugging**: `Belay.Replay.dry_run/2` re-runs job code against
  the recorded journal, side-effect-free, with precise divergence reports.
- **Token-resource rate limits**: shared resource buckets with estimated
  admission and post-hoc true-up via `debit/3`.
- **Input schemas**: insert-time validation raising at the call site.

### Operability
- `stats/1`, `list_jobs/2`, `retry_job/2`, `steps/2`, `events/3`.
- Telemetry spans with durations; `Belay.Telemetry.attach_default_logger/1`.
- **MCP server** (`mix belay.mcp`): stats/jobs/steps/events introspection
  plus retry/cancel/signal/steer, over stdio JSON-RPC.

### Storage
- Behaviour with two adapters: deterministic in-memory (test/simulation
  reference) and Postgres (Postgrex, hand-written SQL, clock passed as a
  parameter). The same 64-test suite runs against both.
