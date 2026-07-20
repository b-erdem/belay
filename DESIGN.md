# Capstan v2 — Standalone Design

*Rethought 2026-07-20 after the decision to drop the Oban substrate entirely.
The v0 Oban-layer implementation is preserved at git tag `v0-oban-layer`; the
market/pain research behind this design is in PLAN.md and docs/research/.*

## 1. Why standalone (what the freedom actually buys)

Building on Oban forced every v0 feature through three narrow doors: the
`oban_jobs` schema (so workflows/chains/relays lived in `meta` JSON blobs with
expression indexes), Oban's state machine and stager (1-second scheduling
granularity, leader-elected staging), and Oban's dispatch assumptions
(LISTEN/NOTIFY as a load-bearing component). Owning the engine converts each
constraint into a design decision:

| Constraint under Oban | Standalone decision |
|---|---|
| Everything extra lives in `meta` jsonb | **First-class columns**: `workflow_id`, `deps text[]`, `partition_key`, `cron_name/cron_slot`, `budget/spent`, `result` — native indexes, honest schema |
| 8 job states designed pre-agents | **Agent-shaped states** (§3): `awaiting` is first-class, `ready_at` gates claimability, no stager process at all |
| Stager ticks 1s on the leader | **No staging step** — the claim query itself checks `ready_at <= $now`; sub-second scheduling by construction |
| Cron/Stager/Lifeline run on an elected leader ("rare, but insidious" failures) | **Leaderless everything** — cron dedupes via a unique `(cron_name, cron_slot)` index; lease reclaim is idempotent row-level UPDATE; there is no peer election in the codebase |
| Lifeline rescues orphans after 1h and "may cause duplicate execution" | **Leases with fencing** (§5): running jobs hold a `lease_until` renewed in batch; orphan window = lease TTL (~30s), acks are fenced by attempt number |
| LISTEN/NOTIFY load-bearing (breaks under PgBouncer/serverless PG) | **Poll-first dispatch** (§6): 500ms jittered claims + in-cluster `:pg` broadcast when distributed; Postgres NOTIFY is not used at all |
| Args are JSON; step values bolted on | **JSON inputs, term-native step values**, `result` and step `value` as `bytea` term binaries with cost columns beside them |
| Ecto + Oban dependency graph | **Postgrex + Jason + telemetry only**; storage behind a behaviour with a deterministic in-memory adapter |

What we give up, honestly: Oban's five-years-hardened claim path and its
ecosystem (Web UI, Sentry/OTel integrations, community familiarity). The
mitigation is architectural: every semantic lives behind a coarse-op storage
behaviour tested identically against Memory and Postgres, with a virtual clock
so time-dependent logic (backoff, rate windows, leases, await deadlines) is
tested by time-travel rather than sleeps.

## 2. Shape of the system

A `Capstan` instance is a supervision tree you start in your app:

    {Capstan,
     name: MyCapstan,
     storage: [adapter: :postgres, url: "postgres://..."],
     queues: [
       default: 10,
       ai: [limit: 5, global_limit: 2,
            rate: [allowed: 60, period: 60],
            partition: {:input, "tenant"}]
     ]}

Components (all per-instance, all leaderless):

- **Storage** — Postgrex pool or the Memory server. All semantics live in
  coarse atomic operations (§7).
- **Producers** (one per queue) — claim jobs up to local demand on a jittered
  poll, on local completions, and on cluster pokes.
- **Executor** — a `Task.Supervisor`; each job runs in its own process with
  crash isolation. Control flow (await/sleep/budget/cancel) travels as throws
  caught by the runner.
- **LeaseKeeper** — renews all local running leases in one batched UPDATE per
  tick; jobs it fails to renew get brutally killed locally (the cluster will
  reclaim them).
- **Sweeper** — reclaims expired leases (retry or fail by attempts); prunes
  old rate windows and signals.
- **CronScheduler** — every ~20s computes due slots and inserts, relying on
  the unique index for cluster-wide exactly-once-per-slot.

## 3. Job lifecycle

States: `ready` `running` `awaiting` `held` `succeeded` `failed` `cancelled` `paused`.

- `ready` + `ready_at <= now` → claimable. Scheduled jobs, retry backoff, and
  snoozes are all just `ready` with a future `ready_at` — introspection derives
  "scheduled" for display; the engine has one rule.
- `awaiting` — parked on a named signal, with `ready_at` doubling as the wake
  deadline. A matching signal flips it to `ready` immediately; the deadline
  makes `await` return `{:error, :timeout}` to the job code on resume.
- `held` — created but not yet eligible: workflow dependencies unmet, or an
  operator hold. Released transitions to `ready`.
- `running` — claimed under a lease (§5).
- Terminal: `succeeded` (with term-encoded `result`), `failed` (attempts
  exhausted, budget exceeded, or explicit), `cancelled`.
- `paused` — operator freeze of a non-running job; resumable.

Return conventions from `run/1`: `:ok` / `{:ok, result}` / `{:error, reason}`
(retry with backoff) / `{:cancel, reason}` / `{:snooze, seconds}`. Raises and
throws retry; control throws (`await`, `sleep`, budget, cancellation) are
translated by the runner.

## 4. The journal is the core

    Capstan.step(ctx, :transcribe, fn -> whisper!(url) end)
    Capstan.step(ctx, :summarize, fn -> llm!(text) end, cost: [tokens: 1200, usd: 0.018])

Steps are memoized per `(job_id, name)` with a monotonic `seq`, value stored as
a term binary, and **cost columns (`tokens`, `usd_micros`) recorded beside the
value**. That makes three features fall out of the schema instead of being
bolted on:

- **Replay**: a retried job skips completed steps — an agent loop crash
  re-buys zero tokens.
- **Budgets**: `Capstan.insert(..., budget: [usd: 5.00])` — the engine checks
  accumulated spend at every step boundary and fails the job with
  `:budget_exceeded` when crossed. "Kill this agent at $5" is a config line.
  (Per the market research, no shipping product enforces this today.)
- **Auditability**: `capstan_steps` *is* the cost/attribution report; no
  separate metering pipeline.

Signals are a durable per-scope inbox (`capstan_signals`), same semantics
proven in v0: persistent until cleared, so await/signal races only cost
latency, never correctness. `Capstan.steering(ctx)` reads the reserved
`"$steer"` signal so operators (or supervising agents) can inject guidance
into a running job at step boundaries; cooperative cancellation rides the
same check.

## 5. Leases instead of rescue heuristics

Claiming sets `state='running', lease_until = now + lease_ttl, leased_by =
node, attempt += 1` atomically (`FOR UPDATE SKIP LOCKED`). The LeaseKeeper
renews all local leases each `lease_ttl / 3`. The Sweeper (any node) moves
expired-lease jobs back to `ready` with backoff, or `failed` when attempts are
exhausted.

Delivery is at-least-once (as with every SQL queue), but the orphan window is
the lease TTL — seconds, not Lifeline's default hour — and acks are **fenced**:
they update `WHERE id = $id AND attempt = $attempt AND state = 'running'`, so a
reclaimed-and-rerun job can't be clobbered by a zombie's late ack.

## 6. Dispatch without LISTEN/NOTIFY

Every producer polls with jitter (default 500ms) — that alone is a correct
system with worst-case pickup latency of one poll. When nodes are clustered,
completions and inserts broadcast a lightweight poke via `:pg` process groups,
making pickup effectively immediate. Postgres NOTIFY is deliberately absent:
the failure mode research showed it silently degrading under PgBouncer,
Supavisor, RDS Proxy, and serverless Postgres — a queue's signal plane
shouldn't depend on it.

## 7. Storage behaviour

Coarse, semantic, individually-atomic operations — not a query builder:

    insert_jobs/2   claim/3        renew_leases/3   reclaim_expired/2
    ack/4           get_step/3     put_step/4       get_signal/3
    signal/5        clear_signal/3 request_cancel/2 pause/resume/2
    get_job/2       workflow_jobs/2  fire_cron/3    prune/2 ...

`claim/3` embeds admission control (global limit by counting live-leased rows,
sliding-window rate limits, per-key partition caps) inside the claim
transaction. `ack/4` embeds workflow release/cascade in the same transaction as
the terminal write — v0's "advance after the fact" gap is gone.

Two adapters ship: **Postgres** (Postgrex, hand-written SQL, `$now` passed as a
parameter everywhere so time is injectable) and **Memory** (a single
serialized GenServer — deterministic by construction, used by the test suite
and the seeded simulation tests). SQLite is deliberately deferred: Memory
covers dev/test; PG covers production.

Time comes from a `Capstan.Clock` behaviour (`System` / `Sim`). Rate windows,
backoff, lease expiry, await deadlines, and cron slots are all tested by
advancing a SimClock, never by sleeping.

## 8. Only possible because we own the engine (roadmap)

- **Dynamic spawn/graft**: `Capstan.spawn_child(ctx, kind, input)` — agents
  author their own DAGs at runtime (the Cloudflare Dynamic Workflows idea, on
  your own Postgres).
- **Durable streams**: step-level output streaming to subscribers, surviving
  crashes (Temporal Workflow Streams equivalent).
- **Time-travel debugging**: replay a job locally against its recorded step
  journal; diff a fixed implementation against history.
- **Fork/what-if**: fork a workflow's journal and re-run the remainder with
  modified input.
- **MCP server**: introspection + retry/cancel/signal as agent-operable tools.
- **LiveView dashboard** with the workflow DAG view the Elixir community keeps
  asking for.
- Deferred from v0 parity: batches, chains (per-key FIFO ≈ partition cap 1 in
  the meantime), structured args schemas, dynamic queue table, encrypted
  inputs, an Ecto bridge for transactional enqueue from user repos.

## 9. v2 scope shipped in this pass

Engine core (claim/lease/ack/retry/sweep), steps + costs + budgets, signals /
await / steering / cooperative cancel, workflows (held → transactional release
→ cascade with ignore policies), leaderless cron, global/rate/partition
admission, `await_result`, drain-based testing API, Memory + Postgres storage,
SimClock, shared suite run against both adapters.
