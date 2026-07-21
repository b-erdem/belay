# Getting started

Belay is a durable job engine that runs inside your Elixir application and
stores everything in Postgres. It needs Postgrex, Jason, and telemetry —
no Ecto, no Redis, no separate orchestrator service.

## Install

```elixir
# mix.exs
def deps do
  [{:belay, "~> 1.0.0-rc.5"}]
end
```

Create the schema once (from a release task, migration, or iex):

```elixir
Belay.Storage.Postgres.migrate!("postgres://user:pass@localhost/my_app")
```

`migrate!/1` is idempotent and versioned — safe to run on every deploy.

## Configure and start an instance

Belay reads its configuration from your application environment, keyed by
the instance name — the same convention as `Ecto.Repo` and
`Phoenix.Endpoint`:

```elixir
# config/config.exs
config :my_app, MyApp.Belay,
  queues: [
    default: 10,
    mailers: [limit: 20],
    ai: [limit: 5, global_limit: 2, rate: [allowed: 60, period: 60]]
  ]

# config/runtime.exs — values you only know at runtime
config :my_app, MyApp.Belay,
  storage: [adapter: :postgres, url: System.fetch_env!("DATABASE_URL")]
```

Then `otp_app` pulls that config in the supervision tree:

```elixir
# application.ex
children = [
  MyApp.Repo,
  {Belay, otp_app: :my_app, name: MyApp.Belay}
]
```

Inline opts on the child spec override the application-env base, so a
computed URL or a test override can be passed directly:

```elixir
{Belay, otp_app: :my_app, name: MyApp.Belay, storage: [adapter: :memory]}
```

Or skip `otp_app` entirely and pass everything inline — both forms accept
the same keys. Every node running this tree becomes a worker node. There is
no leader: scheduling, cron, and recovery are all any-node operations
deduplicated by the database.

## Define work

```elixir
defmodule MyApp.WelcomeEmail do
  use Belay.Worker, queue: :mailers, max_attempts: 5

  @impl Belay.Worker
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
{:ok, job} = Belay.insert(MyApp.Belay, MyApp.WelcomeEmail.new(%{"user_id" => 42}))

# Options on new/2:
MyApp.WelcomeEmail.new(%{"user_id" => 42},
  queue: :mailers,          # override the worker default
  schedule_in: 300,         # run in five minutes
  priority: 1,              # 0 (highest) .. lower numbers first
  max_attempts: 3,
  unique: "welcome:42",     # at most one incomplete job with this key
  budget: [usd: 0.50]       # fail after a step's declared cost crosses the limit
)
```

Need the outcome? `Belay.await_result/3` gives background work RPC
ergonomics:

```elixir
{:ok, job} = Belay.insert(MyApp.Belay, MyApp.Summarize.new(%{"url" => url}))

case Belay.await_result(MyApp.Belay, job.id, 30_000) do
  {:ok, summary} -> summary
  {:error, {:job, :failed}} -> :gave_up
  {:error, :timeout} -> :still_running
end
```

## Recurring jobs

```elixir
{Belay,
 name: MyApp.Belay,
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
- Add `{Belay.Dashboard, belay: MyApp.Belay, port: 4004}` for a read-only
  local UI, or configure `token:` / `authorizer:` to enable operator actions
