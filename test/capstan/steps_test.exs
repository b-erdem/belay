defmodule Capstan.StepsTest do
  use Capstan.Test.Case, async: false

  alias Capstan.Test.{Budgeted, Steered, StepFlaky}

  setup do
    {:ok, start_capstan!()}
  end

  test "steps are memoized across retries", %{name: name, clock: clock} do
    {:ok, job} = Capstan.insert(name, StepFlaky.new(%{}))

    assert %{ready: 1} = Testing.drain(name, :default)

    advance(clock, 6)

    assert %{succeeded: 1} = Testing.drain(name, :default)
    assert Events.count(:step_ran) == 1
    assert {:ok, 42} = Capstan.await_result(name, job.id, 100)
    assert job!(name, job.id).attempt == 2
  end

  test "steps and costs are inspectable", %{name: name} do
    {:ok, job} = Capstan.insert(name, Budgeted.new(%{"steps" => 2, "usd" => 0.10}))

    Testing.drain(name, :default)

    {:ok, steps} = Capstan.steps(name, job.id)

    assert [%{name: "s1", usd_micros: 100_000}, %{name: "s2", usd_micros: 100_000}] =
             Enum.map(steps, &Map.take(&1, [:name, :usd_micros]))
  end

  test "usd budget kills the job at the cap", %{name: name} do
    {:ok, job} =
      Capstan.insert(name, Budgeted.new(%{"steps" => 5, "usd" => 0.30}, budget: [usd: 0.50]))

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
      Capstan.insert(
        name,
        Budgeted.new(%{"steps" => 5, "tokens" => 60}, budget: [tokens: 100])
      )

    assert %{failed: 1} = Testing.drain(name, :default)
    assert job!(name, job.id).spent_tokens == 120
  end

  test "steering payloads reach the running job", %{name: name} do
    {:ok, job} = Capstan.insert(name, Steered.new(%{}))

    Capstan.steer(name, job.id, %{"instruction" => "wrap it up"})

    assert %{succeeded: 1} = Testing.drain(name, :default)

    assert {:ok, %{"steer" => %{"instruction" => "wrap it up"}}} =
             Capstan.await_result(name, job.id, 100)
  end
end
