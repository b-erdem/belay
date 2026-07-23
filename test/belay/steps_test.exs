defmodule Belay.StepsTest do
  use Belay.Test.Case, async: false

  alias Belay.Test.{Budgeted, Steered, StepFlaky}

  setup do
    {:ok, start_belay!()}
  end

  test "steps are memoized across retries", %{name: name, clock: clock} do
    {:ok, job} = Belay.insert(name, StepFlaky.new(%{}))

    assert %{ready: 1} = Testing.drain(name, :default)

    advance(clock, 6)

    assert %{succeeded: 1} = Testing.drain(name, :default)
    assert Events.count(:step_ran) == 1
    assert {:ok, 42} = Belay.await_result(name, job.id, 100)
    assert job!(name, job.id).attempt == 2
  end

  test "steps and costs are inspectable", %{name: name} do
    {:ok, job} = Belay.insert(name, Budgeted.new(%{"steps" => 2, "usd" => 0.10}))

    Testing.drain(name, :default)

    {:ok, steps} = Belay.steps(name, job.id)

    assert [%{name: "s1", usd_micros: 100_000}, %{name: "s2", usd_micros: 100_000}] =
             Enum.map(steps, &Map.take(&1, [:name, :usd_micros]))
  end

  test "a crash after journaling the crossing step cannot buy an extra step", %{name: name} do
    # Simulates the endurance-soak finding: attempt 1 dies between journaling
    # the over-budget step and acking the failure, so the journal already
    # holds s1..s3 (spent 0.6 of a 0.5 budget) when the next attempt starts.
    # The retry must die on the pre-flight budget check without executing
    # s4's body — one crash must not buy one more paid step.
    {:ok, job} =
      Belay.insert(name, Budgeted.new(%{"steps" => 5, "usd" => 0.20}, budget: [usd: 0.50]))

    {mod, ref} = storage(name)

    for i <- 1..3 do
      {:ok, _} =
        mod.put_step(
          ref,
          job.id,
          "s#{i}",
          Belay.Codec.encode(i),
          %{usd_micros: 200_000, tokens: 0},
          DateTime.utc_now()
        )
    end

    assert %{failed: 1} = Testing.drain(name, :default)

    job = job!(name, job.id)
    assert job.state == "failed"
    assert [%{"error" => "budget_exceeded"} | _] = Enum.reverse(job.errors)
    assert job.spent_usd_micros == 600_000

    {:ok, steps} = Belay.steps(name, job.id)
    assert Enum.map(steps, & &1.name) == ["s1", "s2", "s3"]

    # No step body ran at all: s1..s3 replayed from the journal, s4 was
    # refused before execution.
    for i <- 1..5, do: assert(Events.count({:step, i}) == 0)
  end

  test "usd budget kills the job at the cap", %{name: name} do
    {:ok, job} =
      Belay.insert(name, Budgeted.new(%{"steps" => 5, "usd" => 0.30}, budget: [usd: 0.50]))

    assert %{failed: 1} = Testing.drain(name, :default)

    job = job!(name, job.id)

    assert job.state == "failed"
    assert [%{"error" => "budget_exceeded"} | _] = Enum.reverse(job.errors)
    assert job.spent_usd_micros == 600_000
    assert Events.count({:step, 2}) == 1
    assert Events.count({:step, 3}) == 0
  end

  test "token budget kills the job at the cap", %{name: name} do
    {:ok, job} =
      Belay.insert(
        name,
        Budgeted.new(%{"steps" => 5, "tokens" => 60}, budget: [tokens: 100])
      )

    assert %{failed: 1} = Testing.drain(name, :default)
    assert job!(name, job.id).spent_tokens == 120
  end

  test "steering payloads reach the running job", %{name: name} do
    {:ok, job} = Belay.insert(name, Steered.new(%{}))

    Belay.steer_job(name, job.id, %{"instruction" => "wrap it up"})

    assert %{succeeded: 1} = Testing.drain(name, :default)

    assert {:ok, %{"steer" => %{"instruction" => "wrap it up"}}} =
             Belay.await_result(name, job.id, 100)
  end
end
