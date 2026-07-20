# Capstan

**An open, agent-native pro toolkit for [Oban](https://github.com/oban-bg/oban).**
Durable steps, signals, workflows, batches, chains, relayed results, and a smart
engine with global concurrency and rate limits — on plain Oban tables, fully
compatible with Oban Web, `Oban.Testing`, and telemetry. Apache-2.0.

Capstan is a clean-room alternative to the commercial job-composition layer,
designed from public documentation and rethought for agent workloads: its core
primitive is the **durable step** — retries replay past completed work instead
of re-buying it.

```elixir
defmodule MyApp.ResearchAgent do
  use Capstan.Worker, queue: :ai, max_attempts: 10, recorded: true

  @impl Capstan.Worker
  def process(job) do
    # Each step runs at most once per job — a crash after :summarize
    # never re-pays for :transcribe.
    text    = Capstan.step(job, :transcribe, fn -> transcribe!(job.args) end)
    summary = Capstan.step(job, :summarize, fn -> llm!(text) end)

    # Park until a human approves;
    # Capstan.signal_job(job_id, :approval, %{...}) resumes it instantly.
    %{"approved" => true} = Capstan.await_signal(job, :approval)

    Capstan.step(job, :publish, fn -> publish!(summary) end)
    {:ok, summary}
  end
end
```

## Install

```elixir
# mix.exs
{:capstan, path: "../capstan"}   # not yet on hex
```

Run migrations after Oban's:

```elixir
defmodule MyApp.Repo.Migrations.AddCapstan do
  use Ecto.Migration
  def up, do: Capstan.Migration.up()
  def down, do: Capstan.Migration.down()
end
```

Configure the engine (on SQLite also set `prefix: false`):

```elixir
config :my_app, Oban,
  engine: Capstan.Engine,
  repo: MyApp.Repo,
  queues: [
    mailers: 20,
    ai: [limit: 10, global_limit: 4, rate_limit: [allowed: 60, period: 60]],
    tenants: [limit: 10, global_limit: 2, partition: {:args, "tenant_id"}]
  ]
```

## Features

| Module | What it gives you |
|---|---|
| `Capstan.Engine` | cluster-wide `global_limit`, sliding-window `rate_limit`, per-key `partition` fairness — accounting derived from indexed queries, no hidden state |
| `Capstan.Steps` | `step/3` memoized durable steps; `await_signal/3` + `signal/4` human-in-the-loop |
| `Capstan.Worker` | `args_schema` validation (invalid args cancel, not retry), hooks, recorded results |
| `Capstan.Workflow` | DAG dependencies held in the `suspended` state, cascade or ignore failure policies |
| `Capstan.Batch` | milestone callback jobs (`handle_completed`, `handle_exhausted`), fired exactly once |
| `Capstan.Chain` | strict per-key FIFO with `continue`/`halt` failure policies |
| `Capstan.Relay` | insert a job, `await/2` its recorded result across nodes |

Everything advances through engine transition interception — no polling
coordinator processes.

## Status

v0. 34 tests green on SQLite (Lite engine) and Postgres 16 (Basic engine):

```
mix test                # SQLite
CAPSTAN_PG=1 mix test   # Postgres (docker: port 55433; force recompile on adapter switch)
```

See [PLAN.md](PLAN.md) for the research-backed design rationale, honest
limitations, the parity table against Oban Pro, and the roadmap
(token-budget flow control, cost governance, MCP operability, workflow
visualization).

If you need battle-tested versions of these features in production today, buy
[Oban Pro](https://oban.pro) — it is excellent software and funds Oban itself.
Capstan exists for the layer Pro doesn't sell: step-granular durable execution
and agent-era operability, open.
