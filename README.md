<p align="center"><strong>⚓ Capstan</strong></p>
<p align="center">The durable job engine for the AI age — open, leaderless, Postgres-native.</p>

<p align="center">
  <a href="https://github.com/b-erdem/capstan/actions/workflows/ci.yml"><img src="https://github.com/b-erdem/capstan/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://hex.pm/packages/capstan"><img src="https://img.shields.io/hexpm/v/capstan.svg" alt="Hex"></a>
  <a href="https://hexdocs.pm/capstan"><img src="https://img.shields.io/badge/hex-docs-8e64dd" alt="Docs"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache--2.0-blue" alt="Apache-2.0"></a>
</p>

![The embedded dashboard, live](assets/dashboard-live.gif)

Classic job queues retry *whole jobs*. That was fine when jobs sent emails —
it's ruinous when attempt one spent ninety seconds and $0.40 of tokens
before a network blip. Capstan changes the unit of retry: jobs are made of
**memoized steps with cost accounting**, so a retry replays finished work in
microseconds, a **budget cap kills a runaway agent** mid-flight, and the
journal left behind is a debugging asset you can literally re-execute.

Everything here is open, Apache-2.0. **There is no paid tier — the base
thing is the best version.**

```elixir
defmodule MyApp.ResearchAgent do
  use Capstan.Worker, queue: :ai, max_attempts: 10

  @impl Capstan.Worker
  def run(ctx) do
    # Steps run at most once per job — a crash after :summarize never re-buys :transcribe.
    text    = Capstan.step(ctx, :transcribe, fn -> whisper!(ctx.job.input["url"]) end)
    summary = Capstan.step(ctx, :summarize, fn -> llm!(text) end, cost: [usd: 0.02, tokens: 1200])

    # Fan out real jobs across the cluster; park at zero cost until all land.
    checks = Capstan.map_children(ctx, :verify, MyApp.FactCheck,
               Enum.map(summary.claims, &%{"claim" => &1}))

    # Human in the loop: durable wait, instant wake on signal.
    case Capstan.await(ctx, :approval, timeout: 86_400) do
      %{"approved" => true} -> {:ok, summary}
      _ -> {:cancel, :rejected}
    end
  end
end

# A hard spend cap, enforced by the engine at step boundaries:
Capstan.insert(MyApp.Capstan,
  MyApp.ResearchAgent.new(%{"url" => url}, budget: [usd: 1.00], unique: "research:#{url}"))
```

## No surprise bills

Every AI app that ships a free tier meets the same monster: usage spikes,
retries multiply LLM calls, and the provider invoice arrives before the
dashboard does. Capstan enforces spend **in the engine**, in three layers:

```elixir
# 1. Per-job hard cap — one runaway agent can never exceed its budget.
#    Enforced BEFORE each step against durable spend (model-checked across
#    crash/retry windows — not even kill -9 mid-failure buys an extra step).
MyApp.ResearchAgent.new(input, budget: [usd: 1.00])

# 2. Fleet-wide caps per window — resource buckets span every queue that
#    names them. Units are yours: tokens... or cents.
queues: [
  ai: [limit: 20,
       rate: [resource: "anthropic_tokens", allowed: 2_000_000, period: 60, estimate: 3_000]],
  enrich: [limit: 10,
       rate: [resource: "spend_cents", allowed: 5_000, period: 86_400, estimate: 2]]
]
# ^ that second line is a global kill-switch: at most $50/day, app-wide.
#   When the window is spent, claims stop; work queues instead of billing.

# 3. True-up — estimates are corrected by actuals, so windows converge on
#    what the invoice will actually say:
Capstan.step(ctx, :summarize, fn ->
  %{text: text, usage: usage} = llm!(prompt)
  Capstan.debit(ctx, "anthropic_tokens", usage.total_tokens)
  Capstan.debit(ctx, "spend_cents", usage.cost_cents)
  text
end, cost: [usd: 0.02])
```

Retries make this worse everywhere else — each attempt re-spends. Here they
make it better: finished steps replay from the journal at zero cost, and
the budget check reads *durable* spend, so an attempt can never "forget"
what previous attempts already paid.

## Why Capstan

- **The journal is the core.** Steps, costs, events, and results are
  first-class columns — so budgets ("kill this agent at $5"), token-true-up
  rate limits, streaming, and replay debugging fall out of the schema
  instead of being bolted on.
- **Leaderless everything.** No peer election exists in the codebase: cron
  dedupes through a unique index, recovery is idempotent row-level work any
  node performs. The "leader stalled, nothing runs" failure class is
  structurally absent.
- **Millisecond dispatch on a polling-floor guarantee.** Adaptive burst
  polling plus an opt-in `pg_notify` accelerator — measured **11ms p50
  insert→result across unconnected processes** — while nothing load-bearing
  touches LISTEN/NOTIFY, so PgBouncer transaction pooling and serverless
  Postgres just work.
- **Leases with fencing, not rescue heuristics.** Crashed workers' jobs are
  reclaimed in seconds; zombie acks are rejected by attempt fencing.
- **Deterministic by design.** Storage behind a behaviour with an in-memory
  reference adapter; the clock injectable everywhere (SQL included). One
  suite runs against both adapters, and time-dependent behavior is tested by
  time travel, never sleeps.

## The workflow DAG view

Workflows, fan-outs, and agent-spawned children render as a live graph —
deep-linkable (`#workflow=<id>`), with the full step journal one click away:

![Workflow DAG completing](assets/workflow-dag.gif)

## Feature tour

| | |
|---|---|
| Durable steps + budgets | `step/4` with `cost:`; `budget: [usd:, tokens:]` — retries replay, caps kill |
| Human-in-the-loop | `await/3` parks at zero cost; `signal_job/4` wakes instantly; deadlines |
| Steering & cancellation | `steer/3` injects guidance mid-run; cooperative cancel at step boundaries |
| Dynamic children | `spawn/3`, `await_children/1`, `map_children/5` — replay-safe runtime DAGs |
| Workflows & batches | declared DAGs, transactional release, cascade/ignore policies |
| Event streams | `emit/2` + live subscriptions + offset replay — survives crashes |
| Replay debugging | `Capstan.Replay.dry_run/2` — re-run code against the recorded journal |
| Cluster limits | `global_limit`, sliding-window `rate` (request- or **token**-based with true-up), per-tenant `partition` fairness (exact, skew-proof) |
| Transactional enqueue | `Capstan.Txn.insert/3` in your Postgrex/Ecto transaction; wake-ups deliver exactly on commit |
| Unique jobs | constraint-backed: while-incomplete, windowed, or forever |
| Chunk workers | `chunk: [size:, gather_ms:]` — N jobs, one invocation (batch-priced APIs, bulk INSERTs), per-job partial failure |
| Adaptive concurrency | `limit: [min:, max:]` — per-node scaling under load, exactly bounded by cluster limits |
| Encrypted inputs | AES-256-GCM at rest; plaintext only in the executing process |
| Runtime CRUD | `Capstan.Queues` / `Capstan.Crons` — change queues and schedules with no deploy |
| Scheduling | `schedule_in`, durable `sleep/3`, leaderless cron with exactly-once slots |
| **Embedded dashboard** | zero dependencies, one child spec — everything in the screenshots above |
| **MCP server** | `mix capstan.mcp` — AI assistants inspect and operate the queue, mutations behind a pluggable authorizer |

## Measured, not claimed

Numbers from this repo's reproducible harnesses on a laptop (Postgres 16):

| What | Result | Harness |
|---|---|---|
| Dispatch, same node | **8.6ms** p50 insert→result | `bench/run.sh` |
| Dispatch, cross-process + `pg_notify` | **11.0ms** p50 / 24.5ms p99 | `bench/run.sh` |
| Throughput (unbatched acks, 3 workers) | ~416 jobs/s end-to-end | `bench/throughput.exs` |
| Endurance soak (7h) | 99,004 jobs · 4,978 `kill -9` · 13 DB restarts · **13/13 invariants** | `soak/run.sh` → reports in `soak/reports/` |
| Model checking | **187,975,659 distinct states, zero violations** (TLC, complete to depth 49) | `verify/spec/` |
| Schedule exploration | READ COMMITTED wake race reproduced + fix proven across 400 schedules | `verify/wake_protocol/` |
| Suites | 115 (memory) + 122 (Postgres), same tests, `--warnings-as-errors` | `mix test` |

Four real bugs were found by these harnesses before any user could — two by
chaos, one by adapter-equivalence property testing, two by extending the
formal model — and each layer is validated by *rediscovery*: revert any fix
in the model and TLC produces the production failure, step for step
([CHANGELOG](CHANGELOG.md), [verify/](verify/README.md)). The race lessons
are codified in the [wire contract](SCHEMA.md).

## Proven on a real app

The first design-partner port replaced Oban in a production-shaped Phoenix
ingestion service (4 workers, webhooks, GDPR deletion flows, hourly cron,
uniqueness everywhere): **222/222 tests green and a live-producer run on the
first attempt, with zero engine changes required**. Dead-node recovery
tightened from a conservative 45-minute rescue plugin to ~2× a 60-second
lease — renewed leases replace guessing.

## Quick start

```elixir
# mix.exs
{:capstan, "~> 1.0.0-rc"}

# once, at deploy time
Capstan.Storage.Postgres.migrate!(db_url)

# application.ex
children = [
  {Capstan,
   name: MyApp.Capstan,
   storage: [adapter: :postgres, url: db_url],
   queues: [
     default: 10,
     ai: [limit: 5, global_limit: 2,
          rate: [allowed: 100_000, period: 60, resource: "anthropic", estimate: 2_000]]
   ],
   crons: [[name: "digest", expr: "0 8 * * 1-5", worker: MyApp.Digest]]},
  {Capstan.Dashboard, capstan: MyApp.Capstan, port: 4004}
]
```

Testing is deterministic — drain synchronously, travel through time:

```elixir
{:ok, job} = Capstan.insert(name, MyApp.Pipeline.new(%{"id" => 1}))
assert %{succeeded: 1} = Capstan.Testing.drain(name, :default)

Capstan.Clock.Sim.advance(clock, 3_600)   # backoff, cron, rate windows, leases…
```

## Polyglot by construction

The Postgres schema **is** the protocol — specified in [SCHEMA.md](SCHEMA.md)
with a dual ETF/JSON value envelope already in place. Python and TypeScript
SDKs are planned as thin contract implementations (not rewrites), certified
by the same soak harness, sharing one database with Elixir workers.

## Docs

[Getting started](guides/getting-started.md) ·
[Migrating from Oban](guides/migrating-from-oban.md) ·
[Durable steps](guides/durable-steps.md) ·
[Building agents](guides/agents.md) ·
[Operations](guides/operations.md) ·
[Testing](guides/testing.md) ·
[Honest comparison](guides/comparison.md) ·
[Architecture](DESIGN.md) ·
[Wire contract](SCHEMA.md)

## Status

**1.0.0-rc.** Feature-complete; endurance-soaked, model-checked, and
carrying its first real application — new, honestly: production miles are
the one feature that can't be rushed. The remaining path to 1.0 final is
public in [docs/ZERO_TO_ONE.md](docs/ZERO_TO_ONE.md). Post-1.0: SQLite
storage, batched acking, Python/TypeScript SDKs.

## License

Apache-2.0. Contributions welcome — [CONTRIBUTING.md](CONTRIBUTING.md)'s
five rules are the soul of the codebase.
