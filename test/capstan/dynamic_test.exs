defmodule Capstan.DynamicTest do
  use Capstan.Test.Case, async: false

  alias Capstan.{Crons, Queues}
  alias Capstan.Test.{CronJob, Echo}

  test "dynamic queues start producers at runtime and stop on delete" do
    %{name: name} =
      start_capstan!(sim_clock: false, queues: [], dynamic_sync: 100, poll_interval: 100)

    :ok = Queues.put(name, :imports, limit: 5)

    wait_until(fn ->
      Registry.lookup(Capstan.registry(name), {:producer, "imports"}) != []
    end)

    {:ok, job} = Capstan.insert(name, Echo.new(%{"dyn" => true}, queue: :imports))

    assert {:ok, %{"dyn" => true}} = Capstan.await_result(name, job.id, 3_000)

    :ok = Queues.delete(name, :imports)

    wait_until(fn ->
      Registry.lookup(Capstan.registry(name), {:producer, "imports"}) == []
    end)

    assert Queues.list(name) == []
  end

  test "invalid dynamic queue options fail at the call site" do
    %{name: name} = start_capstan!()

    assert_raise FunctionClauseError, fn ->
      Queues.put(name, :bad, partition: {:nope, "k"})
    end
  end

  test "drain resolves dynamic queue specs" do
    %{name: name} = start_capstan!()

    :ok = Queues.put(name, :dyn_manual, limit: 3)

    {:ok, _} = Capstan.insert(name, Echo.new(%{}, queue: :dyn_manual))

    assert %{succeeded: 1} = Testing.drain(name, :dyn_manual)
  end

  test "dynamic crons fire (deduped) and pause persists" do
    %{name: name} =
      start_capstan!(sim_clock: false, queues: [default: 5], cron_interval: 50)

    :ok = Crons.put(name, "tick", "* * * * *", CronJob)

    wait_until(fn -> Events.count(:cron_ran) >= 1 end)

    Process.sleep(200)

    # Slot dedup holds for dynamic entries too.
    assert Events.count(:cron_ran) in 1..2

    :ok = Crons.pause(name, "tick")

    assert [%{name: "tick", paused: true}] =
             name |> Crons.list() |> Enum.map(&Map.take(&1, [:name, :paused]))

    :ok = Crons.resume(name, "tick")

    assert [%{paused: false}] = name |> Crons.list() |> Enum.map(&Map.take(&1, [:paused]))

    :ok = Crons.delete(name, "tick")

    assert Crons.list(name) == []
  end

  test "a dynamic cron overrides a static one by name" do
    %{name: name} = start_capstan!(crons: [[name: "x", expr: "0 0 1 1 *", worker: CronJob]])

    :ok = Crons.put(name, "x", "30 6 * * *", CronJob)

    entries = Capstan.Crons.schedule_entries(Capstan.Config.fetch!(name))

    assert [%{name: "x", expr: expr}] = entries
    assert expr.source == "30 6 * * *"
  end
end
