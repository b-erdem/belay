# Testing

Capstan is built to make job tests deterministic: a storage adapter with no
I/O, a synchronous drain, and a clock you advance by hand. If a test sleeps,
something is wrong.

## Setup

```elixir
# test/support/capstan_case.ex
defmodule MyApp.CapstanCase do
  use ExUnit.CaseTemplate

  setup do
    {:ok, clock} = Capstan.Clock.Sim.start_link()

    name = Module.concat(MyTest, "C#{System.unique_integer([:positive])}")

    start_supervised!(
      {Capstan,
       name: name,
       storage: [adapter: :memory],
       clock: {Capstan.Clock.Sim, clock},
       queues: [default: [limit: 10, manual: true]]}
    )

    {:ok, name: name, clock: clock}
  end
end
```

Two choices doing the work:

- **`adapter: :memory`** — a deterministic in-memory storage with identical
  semantics to Postgres (the same suite tests both in Capstan's own CI).
- **`manual: true`** — no live producer claims from the queue, so execution
  happens exactly when your test says so.

## Drive execution with drain

```elixir
test "the pipeline completes", %{name: name} do
  {:ok, job} = Capstan.insert(name, MyApp.Pipeline.new(%{"id" => 1}))

  assert %{succeeded: 1} = Capstan.Testing.drain(name, :default)
  assert {:ok, result} = Capstan.await_result(name, job.id, 100)
end
```

`drain/2` claims and runs ready jobs synchronously in the test process until
the queue is quiet — following workflow releases and child spawns as they
happen — and returns outcome counts (`%{succeeded: 3, failed: 1}`).
Non-terminal transitions show up too: a job that parked reports as
`%{awaiting: 1}`, a retry as `%{ready: 1}`.

## Time travel instead of sleeping

```elixir
test "retries back off", %{name: name, clock: clock} do
  {:ok, _} = Capstan.insert(name, MyApp.Flaky.new(%{}))

  assert %{ready: 1} = Capstan.Testing.drain(name, :default)   # attempt 1 failed
  assert %{} == Capstan.Testing.drain(name, :default)          # backoff not due

  Capstan.Clock.Sim.advance(clock, 30)

  assert %{succeeded: 1} = Capstan.Testing.drain(name, :default)
end
```

Everything time-dependent — backoff, `schedule_in`, durable sleep, await
deadlines, rate windows, lease expiry, cron slots, retention — reads the
injected clock, so all of it is testable by advancing time.

## Testing waits and signals

```elixir
{:ok, job} = Capstan.insert(name, MyApp.NeedsApproval.new(%{}))

assert %{awaiting: 1} = Capstan.Testing.drain(name, :default)

Capstan.signal_job(name, job.id, :approval, %{"approved" => true})

assert %{succeeded: 1} = Capstan.Testing.drain(name, :default)
```

## Unit-testing a worker without the engine

For pure logic, call `run/1` yourself with a hand-built ctx — or better,
factor the logic out and keep `run/1` as orchestration, testing the
orchestration through drain as above. The drain path exercises the real
claim/ack machinery, which is where the bugs live.
