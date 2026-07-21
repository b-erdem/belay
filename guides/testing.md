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

## How Capstan itself is tested

If you're contributing (or judging whether to trust this thing in
production), here is the taxonomy of the suite and what each layer is for:

| Layer | Where | What it catches |
|---|---|---|
| Unit | `test/capstan/{logic,cron_expr,input_schema,config,codec}_test.exs` | Pure-function bugs: rate math, cron matching, schema validation, config validation, value encoding |
| Integration | most of `test/capstan/*_test.exs` | Feature behavior through the real claim/ack machinery, run against **both** storage adapters |
| Property | seeded loops in `logic_test.exs`, `adapter_equivalence_test.exs` | Invariants across generated inputs (allowances bounded and monotone; adapters behaviorally identical) |
| Adapter equivalence | `adapter_equivalence_test.exs` | Random command sequences applied to Memory and Postgres in lockstep, full-state dumps compared after every step. Found a real bug on its first run (a pending cancel request was silently dropped by the Postgres ack path) |
| Chaos soak | `soak/` (not in `mix test`) | Crash-consistency: worker `kill -9`, DB restarts, then 13 invariants verified by reading the database — no lost jobs, no double effects, no stuck states |
| Performance | `bench/` (not in `mix test`) | Dispatch latency and throughput, measured and recorded in docs — deliberately outside CI because timing assertions flake |
| Model checking | `verify/` (not in `mix test`) | A TLA+ model of the durable-execution core, exhaustively checked by TLC (1.5M states). Validated by mutation: reverting either of the two known production bugs in the model produces a counterexample. The soak samples the state space; TLC exhausts it |
| Schedule exploration | `verify/wake_protocol/` (not in `mix test`) | The parent-wake protocol under Lockstep controlled concurrency: the pre-fix protocol loses the wake on a found, replayable schedule; the shipped protocol survives every explored interleaving |

Two rules keep the suite honest:

- **Every feature test runs on both adapters.** `mix test` uses Memory;
  `CAPSTAN_PG=1 mix test` reruns the same files on Postgres. A test that
  only passes on one adapter is a bug somewhere — in the adapter or in
  the test.
- **No sleeps for correctness.** Anything time-dependent goes through the
  simulated clock. If a test needs `Process.sleep` to pass, the design is
  wrong (the few sleeps that exist assert *absence* of activity, e.g.
  "a paused queue claims nothing").
