# Getting started

Capstan is a durable job engine that runs inside your Elixir application and
stores everything in Postgres. It needs Postgrex, Jason, and telemetry —
no Ecto, no Redis, no separate orchestrator service.

## Install

```elixir
# mix.exs
def deps do
  [{:capstan, "~> 1.0.0-rc.5"}]
end
```

Create the schema once (from a release task, migration, or iex):

```elixir
Capstan.Storage.Postgres.migrate!("postgres://user:pass@localhost/my_app")
```

`migrate!/1` is idempotent and versioned — safe to run on every deploy.

## Start an instance

Add Capstan to your supervision tree:

```elixir
# application.ex
children = [
  MyApp.Repo,
  {Capstan,
   name: MyApp.Capstan,
   storage: [adapter: :postgres, url: "postgres://user:pass@localhost/my_app"],
   queues: [
     default: 10,
     mailers: [limit: 20],
     ai: [limit: 5, global_limit: 2, rate: [allowed: 60, period: 60]]
   ]}
]
```

Every node running this tree becomes a worker node. There is no leader:
scheduling, cron, and recovery are all any-node operations deduplicated by
the database.

## Define work

```elixir
defmodule MyApp.WelcomeEmail do
  use Capstan.Worker, queue: :mailers, max_attempts: 5

  @impl Capstan.Worker
  def run(ctx) do
    user = MyApp.Users.get!(ctx.job.input["user_id"])

    MyApp.Mailer.deliver_welcome(user)
  end
end
```

Return values: `:ok` or `{:ok, result}` succeed (the result is stored and
retrievable), `{:error, reason}` retries with exponential backoff,
`{:cancel, reason}` stops permanently, `{:snooze, seconds}` reschedules
without consuming an attempt. Raised exceptions retry.

## Enqueue

```elixir
{:ok, job} = Capstan.insert(MyApp.Capstan, MyApp.WelcomeEmail.new(%{"user_id" => 42}))

# Options on new/2:
MyApp.WelcomeEmail.new(%{"user_id" => 42},
  queue: :mailers,          # override the worker default
  schedule_in: 300,         # run in five minutes
  priority: 1,              # 0 (highest) .. lower numbers first
  max_attempts: 3,
  unique: "welcome:42",     # at most one incomplete job with this key
  budget: [usd: 0.50]       # fail the job if step costs cross the cap
)
```

Need the outcome? `Capstan.await_result/3` gives background work RPC
ergonomics:

```elixir
{:ok, job} = Capstan.insert(MyApp.Capstan, MyApp.Summarize.new(%{"url" => url}))

case Capstan.await_result(MyApp.Capstan, job.id, 30_000) do
  {:ok, summary} -> summary
  {:error, {:job, :failed}} -> :gave_up
  {:error, :timeout} -> :still_running
end
```

## Recurring jobs

```elixir
{Capstan,
 name: MyApp.Capstan,
 ...,
 crons: [
   [name: "daily-digest", expr: "0 8 * * 1-5", worker: MyApp.Digest],
   [name: "cleanup", expr: "@hourly", worker: MyApp.Cleanup, input: %{"mode" => "fast"}]
 ]}
```

Any node may fire a cron slot; a unique index guarantees each slot inserts
exactly once cluster-wide.

## Where to next

- [Durable steps](durable-steps.md) — the primitive that makes retries cheap
- [Migrating from Oban](migrating-from-oban.md) — porting map + one-command job migration
- [Building agents](agents.md) — budgets, human approval, fan-out, streaming
- [Operations](operations.md) — leases, shutdown, retention, observability
- [Testing](testing.md) — deterministic tests with `drain` and a SimClock
- Add `{Capstan.Dashboard, capstan: MyApp.Capstan, port: 4004}` for the built-in UI
