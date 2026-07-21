defmodule Belay.CoreTest do
  use Belay.Test.Case, async: false

  alias Belay.Test.{Echo, FailN, NapThenDone, StepOnly}

  setup do
    {:ok, start_belay!()}
  end

  test "insert, drain, and read the result", %{name: name} do
    {:ok, job} = Belay.insert(name, Echo.new(%{"n" => 42}))

    assert %{succeeded: 1} = Testing.drain(name, :default)
    assert {:ok, %{"n" => 42}} = Belay.await_result(name, job.id, 100)
    assert job!(name, job.id).state == "succeeded"
  end

  test "scheduled jobs wait for their time", %{name: name, clock: clock} do
    {:ok, job} = Belay.insert(name, Echo.new(%{}, schedule_in: 60))

    assert Testing.drain(name, :default) == %{}
    assert job!(name, job.id).state == "ready"

    advance(clock, 61)

    assert %{succeeded: 1} = Testing.drain(name, :default)
  end

  test "retries back off and eventually succeed", %{name: name, clock: clock} do
    {:ok, job} = Belay.insert(name, FailN.new(%{"fail_times" => 2}))

    assert %{ready: 1} = Testing.drain(name, :default)
    assert Testing.drain(name, :default) == %{}

    advance(clock, 6)
    assert %{ready: 1} = Testing.drain(name, :default)

    advance(clock, 6)
    assert %{succeeded: 1} = Testing.drain(name, :default)

    job = job!(name, job.id)

    assert job.attempt == 3
    assert length(job.errors) == 2
  end

  test "attempts exhaust into failed", %{name: name, clock: clock} do
    {:ok, job} = Belay.insert(name, FailN.new(%{"fail_times" => 99}, max_attempts: 2))

    Testing.drain(name, :default)
    advance(clock, 6)

    assert %{failed: 1} = Testing.drain(name, :default)
    assert job!(name, job.id).state == "failed"
    assert {:error, {:job, :failed}} = Belay.await_result(name, job.id, 100)
  end

  test "durable sleep parks and resumes past steps", %{name: name, clock: clock} do
    {:ok, job} = Belay.insert(name, NapThenDone.new(%{"seconds" => 120}))

    assert Testing.drain(name, :default) == %{ready: 1}

    parked = job!(name, job.id)

    assert parked.state == "ready"
    assert DateTime.compare(parked.ready_at, Belay.Config.now(config(name))) == :gt
    assert Events.count(:first) == 1

    advance(clock, 121)

    assert %{succeeded: 1} = Testing.drain(name, :default)
    assert Events.count(:first) == 1
    assert {:ok, %{"woke" => true}} = Belay.await_result(name, job.id, 100)
  end

  test "snooze does not burn attempts", %{name: name, clock: clock} do
    {:ok, job} = Belay.insert(name, NapThenDone.new(%{"seconds" => 60}))

    Testing.drain(name, :default)
    assert job!(name, job.id).attempt == 0

    advance(clock, 61)
    Testing.drain(name, :default)

    assert job!(name, job.id).attempt == 1
  end

  test "cancelling a parked job is immediate", %{name: name} do
    {:ok, job} = Belay.insert(name, Echo.new(%{}, schedule_in: 300))

    assert {:ok, :cancelled} = Belay.cancel(name, job.id)
    assert job!(name, job.id).state == "cancelled"
    assert Testing.drain(name, :default) == %{}
  end

  test "cancelling a running job is cooperative at step boundaries", %{name: name} do
    {:ok, job} = Belay.insert(name, StepOnly.new(%{}))

    {mod, ref} = storage(name)
    spec = Belay.Config.queue_spec(config(name), :default)
    now = Belay.Config.now(config(name))

    {:ok, [claimed]} = mod.claim(ref, spec, 1, "test-node", 30_000, now)

    assert {:ok, :requested} = Belay.cancel(name, job.id)

    {:ok, acked, _released} = Belay.Runner.execute(config(name), claimed)

    assert acked.state == "cancelled"
  end

  test "priority orders claims", %{name: name} do
    {:ok, low} = Belay.insert(name, Echo.new(%{}, priority: 5))
    {:ok, high} = Belay.insert(name, Echo.new(%{}, priority: 0))

    {mod, ref} = storage(name)
    spec = Belay.Config.queue_spec(config(name), :default)
    now = Belay.Config.now(config(name))

    {:ok, [first, second]} = mod.claim(ref, spec, 2, "n", 1_000, now)

    assert first.id == high.id
    assert second.id == low.id
  end
end
