# Capstan

**The durable job engine for the AI age — open, leaderless, Postgres-native.**

Classic job queues retry *whole jobs*. That made sense when jobs sent emails;
it fails when attempt one spent ninety seconds and $0.40 of tokens before a
blip. Capstan changes the unit of retry: jobs are made of **memoized steps
with cost accounting**, so a retry replays past completed work in
microseconds, a budget cap kills a runaway agent mid-flight, and the journal
left behind is a debugging asset you can literally re-execute.

Everything is open (Apache-2.0). There is no paid tier — the base thing is
the best version.

```elixir
defmodule MyApp.ResearchAgent do
  use Capstan.Worker, queue: :ai, max_attempts: 10

  @impl Capstan.Worker
  def run(ctx) do
    # Steps run at most once per job — a crash after :summarize never
    # re-buys :transcribe.
    text    = Capstan.step(ctx, :transcribe, fn -> whisper!(ctx.job.input["url"]) end)
    summary = Capstan.step(ctx, :summarize, fn -> llm!(text) end,
                cost: [usd: 0.02, tokens: 1200])

    # Fan out real jobs across the cluster; park at zero cost until all land.
    checks =
      Capstan.map_children(ctx, :verify, MyApp.FactCheck,
        Enum.map(summary.claims, &%{"claim" => &1}))

    # Stream progress to any subscriber, durably.
    Capstan.emit(ctx, %{"phase" => "review", "claims" => length(checks)})

    # Human in the loop: zero-cost wait, instant wake on signal.
    case Capstan.await(ctx, :approval, timeout: 86_400) do
      %{"approved" => true} -> {:ok, summary}
      _ -> {:cancel, :rejected}
    end
  end
end

# Enqueue with a hard spend cap — the engine enforces it at step boundaries.
Capstan.insert(MyApp.Capstan,
  MyApp.ResearchAgent.new(%{"url" => url}, budget: [usd: 1.00], unique: "research:#{url}"))
```

## Why another job library?

Because the workload changed and the incumbents' answers are job-granular,
leader-elected, or paid. Capstan's bets:

- **The journal is the core.** Steps, costs, events, and results are
  first-class columns — which is why budgets ("kill this agent at $5"),
  token-true-up rate limits, streaming, and replay debugging all fall out of
  the schema instead of being bolted on.
- **Leaderless everything.** No peer election exists in the codebase. Cron
  dedupes through a unique index; recovery is idempotent row-level work any
  node performs. The "leader stalled, nothing runs" failure class is
  structurally absent.
- **No LISTEN/NOTIFY.** Jittered polling plus in-cluster pokes. PgBouncer
  transaction pooling and serverless Postgres just work.
- **Leases with fencing, not rescue heuristics.** Crashed workers' jobs are
  reclaimed in seconds; zombie acks are rejected by attempt fencing.
- **One scheduling rule.** A job is claimable when `ready_at` is due —
  scheduled work, backoff, and snoozes are the same thing. No stager, and
  sub-second scheduling by construction.
- **Deterministic by design.** Storage sits behind a behaviour with an
  in-memory reference adapter; the clock is injectable everywhere (SQL
  included). The same 64-test suite runs against Memory and Postgres, and
  time-dependent behavior is tested by time travel, never sleeps.

## Feature tour

| | |
|---|---|
| Durable steps + budgets | `step/4`, `cost:`, `budget: [usd:, tokens:]` — retries replay, caps kill |
| Human-in-the-loop | `await/3` parks at zero cost; `signal_job/4` wakes instantly; deadlines |
| Steering & cancellation | `steer/3` injects guidance mid-run; cooperative cancel at step boundaries |
| Dynamic children | `spawn/3`, `spawn_many/3`, `await_children/1`, `map_children/5` — replay-safe runtime DAGs |
| Workflows & batches | declared DAGs with transactional release and cascade policies; batch sugar |
| Event streams | `emit/2` + live subscriptions + offset replay — token streaming that survives crashes |
| Replay debugging | `Capstan.Replay.dry_run/2` — re-run code against the recorded journal, divergence reported |
| Unique jobs | constraint-backed: `unique: "key"` or `unique: [key: k, within: 3600]` |
| Cluster limits | `global_limit`, sliding-window `rate` (request- or **token**-based with true-up), per-tenant `partition` |
| Scheduling | `schedule_in`, durable `sleep/3`, leaderless cron with slot dedup |
| Operations | `stats`, `list_jobs`, `retry_job`, pause/resume, retention pruning, telemetry, graceful shutdown |
| **MCP server** | `mix capstan.mcp` — AI assistants inspect and operate the queue over stdio |

## Quick start

```elixir
# mix.exs
{:capstan, "~> 1.0.0-rc.1"}

# once, at deploy time
Capstan.Storage.Postgres.migrate!(db_url)

# application.ex
{Capstan,
 name: MyApp.Capstan,
 storage: [adapter: :postgres, url: db_url],
 queues: [
   default: 10,
   ai: [limit: 5, global_limit: 2,
        rate: [allowed: 100_000, period: 60, resource: "anthropic", estimate: 2_000]]
 ],
 crons: [[name: "digest", expr: "0 8 * * 1-5", worker: MyApp.Digest]]}
```

Full walkthrough: [guides/getting-started.md](guides/getting-started.md).
Deep dives: [durable steps](guides/durable-steps.md) ·
[building agents](guides/agents.md) · [operations](guides/operations.md) ·
[testing](guides/testing.md) · [honest comparison](guides/comparison.md) ·
[architecture rationale](DESIGN.md).

## Testing your app

```elixir
{:ok, job} = Capstan.insert(name, MyApp.Pipeline.new(%{"id" => 1}))

assert %{succeeded: 1} = Capstan.Testing.drain(name, :default)

Capstan.Clock.Sim.advance(clock, 3_600)   # time travel, never sleep
```

The in-memory adapter plus the SimClock make every engine behavior —
backoff, waits, rate windows, lease expiry, cron — deterministic in tests.

## Status

**1.0.0-rc.1.** The full 1.0 feature set is implemented and covered by 64
tests running identically against both storage adapters (see
[docs/ZERO_TO_ONE.md](docs/ZERO_TO_ONE.md) for the definition of done and
what remains between rc and final: chaos soak, external design partners,
hex publish). Capstan is new — the design is careful and the suite is
strong, but production miles are the one feature that can't be rushed.
Dev/test:

```
mix test                # in-memory adapter
CAPSTAN_PG=1 mix test   # Postgres 16
```

Post-1.0 roadmap: LiveView dashboard with the workflow DAG view, SQLite
adapter, Ecto bridge for same-transaction enqueue, runtime queue/cron CRUD,
per-key exact partitioned claims, encrypted inputs.

## License

Apache-2.0. Contributions welcome — see [CONTRIBUTING.md](CONTRIBUTING.md);
the five rules at the top are the soul of the codebase.
