# Deterministic execution traces for formal-spec grounding.
#
# Drives the real engine (Memory adapter + simulated clock, manual queues)
# through the semantically interesting scenarios and emits one JSONL trace
# per scenario under verify/traces/. Traces are byte-stable across runs —
# logical time comes from the simulated clock, never the wall clock.
#
# Crashes are simulated the honest way: a claimed attempt makes real journal
# writes through the storage API, then the lease expires and the sweeper's
# reclaim path runs — exactly what a kill -9 looks like to the database.
#
# Usage: mix run verify/traces.exs
#
# Event vocabulary (one JSON object per line):
#   scenario_init  {scenario}
#   action         {kind: insert|drain|claim|crash|reclaim|cancel|advance, ...}
#   exec           {job, step}            — a step BODY actually executed
#   state          {job, state, attempt, cancel_requested, spent_usd_micros,
#                   steps: [journaled step names]}
#
# The exec/steps contrast is the point: memoized replay shows a step in
# `steps` with no second `exec`; the budget pre-flight check shows a refusal
# with no `exec` at all.

defmodule V do
  @epoch ~U[2026-01-05 00:00:00.000000Z]

  def epoch, do: @epoch

  def start_trace(scenario) do
    Agent.start_link(fn -> %{seq: 0, scenario: scenario, events: []} end, name: __MODULE__)
    emit(%{event: "scenario_init", scenario: scenario})
  end

  def emit(map) do
    Agent.update(__MODULE__, fn state ->
      event =
        map
        |> Map.put(:seq, state.seq + 1)
        |> Map.put(:scenario, state.scenario)

      %{state | seq: state.seq + 1, events: [event | state.events]}
    end)
  end

  def flush!(path) do
    events = Agent.get(__MODULE__, &Enum.reverse(&1.events))
    File.write!(path, Enum.map_join(events, "", &(Jason.encode!(&1) <> "\n")))
    Agent.stop(__MODULE__)
    IO.puts("  #{path} (#{length(events)} events)")
  end

  def exec(ctx, step), do: emit(%{event: "exec", job: ctx.job.id, step: step})

  def boot(scenario_index) do
    name = Module.concat(VerifyTrace, "S#{scenario_index}")
    {:ok, clock} = Belay.Clock.Sim.start_link(@epoch)

    {:ok, sup} =
      Belay.start_link(
        name: name,
        storage: [adapter: :memory],
        clock: {Belay.Clock.Sim, clock},
        queues: [default: [limit: 10, manual: true]],
        poll_interval: 100,
        sweep_interval: 200,
        shutdown_grace: 100
      )

    %{name: name, clock: clock, sup: sup}
  end

  def stop(%{sup: sup}), do: Supervisor.stop(sup)

  def now(name), do: Belay.Config.fetch!(name) |> Belay.Config.now()

  def storage(name), do: Belay.Config.fetch!(name).storage_ref

  def advance(%{name: name, clock: clock}, seconds) do
    Belay.Clock.Sim.advance(clock, seconds)
    emit(%{event: "action", kind: "advance", seconds: seconds, t: unix(now(name))})
  end

  def drain(%{name: name}) do
    emit(%{event: "action", kind: "drain"})
    Belay.Testing.drain(name, :default)
  end

  def insert(%{name: name}, buildable, fields \\ %{}) do
    {:ok, job} = Belay.insert(name, buildable)
    emit(Map.merge(%{event: "action", kind: "insert", job: job.id}, fields))
    job
  end

  # Immediate for parked states, cooperative (:requested) for running.
  def cancel(%{name: name}, id) do
    {:ok, status} = Belay.cancel(name, id)
    emit(%{event: "action", kind: "cancel", job: id, status: status})
  end

  # A real claim through the storage API — used when a scenario needs an
  # attempt that will "die" (lease left to expire) rather than run to ack.
  def claim(%{name: name}, lease_ms) do
    config = Belay.Config.fetch!(name)
    spec = Belay.Queues.resolve_spec!(config, :default)
    {mod, ref} = storage(name)

    {:ok, [job]} = mod.claim(ref, spec, 1, "verify-node", lease_ms, now(name))
    emit(%{event: "action", kind: "claim", job: job.id, attempt: job.attempt})
    job
  end

  # The crashed attempt's journal writes: what put_step leaves behind when
  # the worker executed steps but died before acking.
  def journal_step(%{name: name}, id, step, usd_micros) do
    {mod, ref} = storage(name)

    {:ok, _spent} =
      mod.put_step(ref, id, step, Belay.Codec.encode(1), %{usd_micros: usd_micros, tokens: 0}, now(name))

    emit(%{event: "exec", job: id, step: step})
  end

  def crash_and_reclaim(%{name: name} = env, id) do
    emit(%{event: "action", kind: "crash", job: id})
    advance(env, 2)

    {mod, ref} = storage(name)
    backoff = fn _job -> now(name) end
    {:ok, _} = mod.reclaim_expired(ref, now(name), backoff)
    emit(%{event: "action", kind: "reclaim", job: id})
  end

  def snapshot(%{name: name}, id) do
    {mod, ref} = storage(name)
    {:ok, job} = mod.get_job(ref, id)
    {:ok, steps} = Belay.steps(name, id)

    emit(%{
      event: "state",
      job: id,
      state: job.state,
      attempt: job.attempt,
      cancel_requested: job.cancel_requested,
      spent_usd_micros: job.spent_usd_micros,
      budget_usd_micros: job.budget_usd_micros,
      steps: Enum.sort(Enum.map(steps, & &1.name))
    })
  end

  defp unix(dt), do: DateTime.to_unix(dt, :microsecond)
end

# -- Workers --------------------------------------------------------------------

defmodule V.TwoStep do
  use Belay.Worker, queue: :default

  @impl Belay.Worker
  def run(ctx) do
    Belay.step(ctx, "s1", fn -> V.exec(ctx, "s1") && 1 end)
    Belay.step(ctx, "s2", fn -> V.exec(ctx, "s2") && 2 end)
    {:ok, %{"done" => true}}
  end
end

defmodule V.FlakyBetweenSteps do
  use Belay.Worker, queue: :default, max_attempts: 3

  @impl Belay.Worker
  def run(ctx) do
    Belay.step(ctx, "s1", fn -> V.exec(ctx, "s1") && 1 end)
    if ctx.job.attempt == 1, do: raise("crash between steps")
    Belay.step(ctx, "s2", fn -> V.exec(ctx, "s2") && 2 end)
    :ok
  end

  @impl Belay.Worker
  def backoff(_attempt), do: 5
end

defmodule V.FiveStepBudget do
  use Belay.Worker, queue: :default, max_attempts: 3

  @impl Belay.Worker
  def run(ctx) do
    for i <- 1..5 do
      Belay.step(ctx, "b#{i}", fn -> V.exec(ctx, "b#{i}") && i end, cost: [usd: 0.2])
    end

    :ok
  end
end

defmodule V.SelfCancel do
  use Belay.Worker, queue: :default

  @impl Belay.Worker
  def run(ctx) do
    instance = String.to_existing_atom(ctx.job.input["instance"])

    Belay.step(ctx, "s1", fn -> V.exec(ctx, "s1") && 1 end)

    # An external cancel arriving while the job is mid-run: cooperative,
    # honored at the next step boundary.
    {:ok, :requested} = Belay.cancel(instance, ctx.job.id)
    V.emit(%{event: "action", kind: "cancel", job: ctx.job.id, status: :requested})

    Belay.step(ctx, "s2", fn -> V.exec(ctx, "s2") && 2 end)
    :ok
  end
end

defmodule V.AlwaysFails do
  use Belay.Worker, queue: :default, max_attempts: 1

  @impl Belay.Worker
  def run(_ctx), do: raise("permanent failure")
end

defmodule V.Trivial do
  use Belay.Worker, queue: :default

  @impl Belay.Worker
  def run(ctx) do
    Belay.step(ctx, "t1", fn -> V.exec(ctx, "t1") && 1 end)
    :ok
  end
end

# -- Scenarios ------------------------------------------------------------------

File.mkdir_p!("verify/traces")
IO.puts("Emitting traces:")

# Instances are linked to this script process; instance shutdown between
# scenarios must not take the script down with it.
Process.flag(:trap_exit, true)

# 1. Plain success: both step bodies execute once, both journaled.
env = V.boot(1)
V.start_trace("happy")
job = V.insert(env, V.TwoStep.new(%{}))
V.snapshot(env, job.id)
V.drain(env)
V.snapshot(env, job.id)
V.flush!("verify/traces/happy.jsonl")
V.stop(env)

# 2. Memoization across a retry: s1 executes on attempt 1 only; attempt 2
#    replays it from the journal and executes just s2.
env = V.boot(2)
V.start_trace("retry_memoized")
job = V.insert(env, V.FlakyBetweenSteps.new(%{}))
V.drain(env)
V.snapshot(env, job.id)
V.advance(env, 6)
V.drain(env)
V.snapshot(env, job.id)
V.flush!("verify/traces/retry_memoized.jsonl")
V.stop(env)

# 3. Budget kill on the crossing step: 0.2/step against a 0.5 budget fails
#    the job at exactly three executed + journaled steps.
env = V.boot(3)
V.start_trace("budget_exact")
job = V.insert(env, V.FiveStepBudget.new(%{}, budget: [usd: 0.5]), %{budget_usd_micros: 500_000})
V.drain(env)
V.snapshot(env, job.id)
V.flush!("verify/traces/budget_exact.jsonl")
V.stop(env)

# 4. The endurance-soak crash window: attempt 1 executes and journals the
#    crossing step, then dies before acking the budget failure. Attempt 2
#    must be refused by the pre-flight check — no fourth exec, three steps.
env = V.boot(4)
V.start_trace("budget_crash_window")
job = V.insert(env, V.FiveStepBudget.new(%{}, budget: [usd: 0.5]), %{budget_usd_micros: 500_000})
V.claim(env, 1_000)
V.journal_step(env, job.id, "b1", 200_000)
V.journal_step(env, job.id, "b2", 200_000)
V.journal_step(env, job.id, "b3", 200_000)
V.crash_and_reclaim(env, job.id)
V.snapshot(env, job.id)
V.drain(env)
V.snapshot(env, job.id)
V.flush!("verify/traces/budget_crash_window.jsonl")
V.stop(env)

# 5. Cancel arriving mid-run is honored at the next step boundary: one step
#    executed, second never runs, job cancelled.
env = V.boot(5)
V.start_trace("cancel_mid_run")

job =
  V.insert(env, V.SelfCancel.new(%{"instance" => Atom.to_string(Module.concat(VerifyTrace, "S5"))}))

V.drain(env)
V.snapshot(env, job.id)
V.flush!("verify/traces/cancel_mid_run.jsonl")
V.stop(env)

# 6. The equivalence-test bug class: cancel requested while RUNNING is
#    cooperative; the worker crashes before honoring it; the flag must
#    survive the reclaim/retry boundary and be honored before the next
#    attempt executes anything.
env = V.boot(6)
V.start_trace("cancel_across_crash")
job = V.insert(env, V.Trivial.new(%{}))
V.claim(env, 1_000)
V.cancel(env, job.id)
V.snapshot(env, job.id)
V.crash_and_reclaim(env, job.id)
V.snapshot(env, job.id)
V.drain(env)
V.snapshot(env, job.id)
V.flush!("verify/traces/cancel_across_crash.jsonl")
V.stop(env)

# 7. Workflow settlement: dependent of a permanently failed job is
#    cascade-cancelled without ever running.
env = V.boot(7)
V.start_trace("workflow_settle")

{:ok, jobs} =
  Belay.Workflow.new()
  |> Belay.Workflow.add(:a, V.AlwaysFails.new(%{}))
  |> Belay.Workflow.add(:b, V.Trivial.new(%{}), deps: [:a])
  |> Belay.Workflow.insert(env.name)

V.emit(%{event: "action", kind: "insert", job: jobs["a"].id, wf: "a"})
V.emit(%{event: "action", kind: "insert", job: jobs["b"].id, wf: "b", deps: ["a"]})
V.snapshot(env, jobs["a"].id)
V.snapshot(env, jobs["b"].id)
V.drain(env)
V.snapshot(env, jobs["a"].id)
V.snapshot(env, jobs["b"].id)
V.flush!("verify/traces/workflow_settle.jsonl")
V.stop(env)

IO.puts("done")
