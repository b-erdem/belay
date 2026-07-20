# Capstan

**A standalone, agent-native durable job engine for Elixir.**
No Oban, no Ecto — Postgrex, Jason, and telemetry only. Postgres in production,
a deterministic in-memory adapter for tests. Apache-2.0.

Capstan treats the execution journal as the core: jobs are made of **memoized
steps with cost accounting**, so a retry replays past completed work instead of
re-buying it, and a budget cap can kill a runaway agent mid-flight.

```elixir
defmodule MyApp.ResearchAgent do
  use Capstan.Worker, queue: :ai, max_attempts: 10

  @impl Capstan.Worker
  def run(ctx) do
    text = Capstan.step(ctx, :transcribe, fn -> whisper!(ctx.job.input["url"]) end)

    summary =
      Capstan.step(ctx, :summarize, fn -> llm!(text) end,
        cost: [usd: 0.02, tokens: 1200])

    # Park at zero cost until a human decides; signal_job/4 resumes instantly.
    case Capstan.await(ctx, :approval, timeout: 86_400) do
      %{"approved" => true} -> {:ok, summary}
      _ -> {:cancel, :rejected}
    end
  end
end

# Enqueue with a hard spend cap: the engine fails the job at the boundary
# where accumulated step costs cross it.
Capstan.insert(MyCapstan, MyApp.ResearchAgent.new(%{"url" => url}, budget: [usd: 1.0]))
```

## Start an instance

```elixir
children = [
  {Capstan,
   name: MyCapstan,
   storage: [adapter: :postgres, url: "postgres://localhost/my_app"],
   queues: [
     default: 10,
     ai: [limit: 5, global_limit: 2, rate: [allowed: 60, period: 60]],
     tenants: [limit: 10, global_limit: 1, partition: {:input, "tenant"}]
   ],
   crons: [
     [name: "digest", expr: "0 8 * * 1-5", worker: MyApp.Digest]
   ]}
]
```

Create the schema once: `Capstan.Storage.Postgres.migrate!(url)`.

## What's in the box

| Capability | How |
|---|---|
| Durable steps | `Capstan.step/4` — memoized per job, term-native values, cost columns |
| Budgets with teeth | `budget: [usd:, tokens:]` — job fails with `:budget_exceeded` at the cap |
| Human-in-the-loop | `Capstan.await/3` parks the job; `signal_job/4` wakes it with a payload |
| Steering | `Capstan.steer/3` → `Capstan.steering/1` reads guidance at step boundaries |
| Durable sleep | `Capstan.sleep/3` — memoized wake target, survives restarts |
| Workflows | `Capstan.Workflow` — DAG deps, transactional release, cascade or `ignore:` |
| Cluster limits | per-queue `global_limit`, sliding-window `rate`, per-key `partition` fairness |
| Leases, not heuristics | crashed workers reclaimed after the lease TTL (~30s), acks fenced by attempt |
| Leaderless cron | any node fires; a unique `(cron_name, cron_slot)` index dedupes |
| Results | `Capstan.await_result/3` — RPC ergonomics over background work |
| Testing | `Capstan.Testing.drain/2` + `Capstan.Clock.Sim` — time-travel, never sleep |

Design decisions and their rationale — first-class columns over meta blobs, an
8-state agent-shaped lifecycle with no stager, poll-first dispatch with no
LISTEN/NOTIFY dependency, the storage behaviour with a deterministic reference
adapter — are documented in [DESIGN.md](DESIGN.md). The market research and
competitive teardown that motivated all of it are in [PLAN.md](PLAN.md) and
[docs/research/](docs/research/2026-07-20-research-digest.md).

## Status

v0.2 (standalone rewrite). 42 tests — engine core, steps/budgets/steering,
signals/await, workflows, limits, leases/fencing, leaderless cron, live
producers, and 300-seed workflow-settlement invariants — green on both storage
adapters:

```
mix test                # Memory (deterministic, SimClock time-travel)
CAPSTAN_PG=1 mix test   # Postgres 16 (docker, port 55433)
```

Not yet built (deliberately, see DESIGN.md §8): dynamic spawn/graft, durable
streams, time-travel replay tooling, MCP server, dashboard, SQLite adapter,
Ecto bridge for transactional enqueue, dynamic queue/cron tables, batches.
This engine is days old — treat it as a foundation, not yet a production
system.
