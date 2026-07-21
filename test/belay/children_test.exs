defmodule Belay.ChildrenTest do
  use Belay.Test.Case, async: false

  alias Belay.Batch
  alias Belay.Test.{Echo, FanOut, SpawnCrash, Tagged}

  setup do
    {:ok, start_belay!()}
  end

  test "map_children fans out, parks, and returns results in input order", %{name: name} do
    {:ok, parent} = Belay.insert(name, FanOut.new(%{"values" => [1, 2, 3]}))

    counts = Testing.drain(name, :default)

    assert counts[:succeeded] == 4
    assert {:ok, [2, 4, 6]} = Belay.await_result(name, parent.id, 100)

    children = Belay.list_jobs(name, parent_id: parent.id)

    assert length(children) == 3
    assert Enum.all?(children, &(&1.parent_id == parent.id))
  end

  test "spawn is memoized: a crash after spawning cannot duplicate children", %{
    name: name,
    clock: clock
  } do
    {:ok, parent} = Belay.insert(name, SpawnCrash.new(%{}))

    Testing.drain(name, :default)

    advance(clock, 6)

    Testing.drain(name, :default)

    assert length(Belay.list_jobs(name, parent_id: parent.id)) == 2
    assert {:ok, results} = Belay.await_result(name, parent.id, 100)
    assert Enum.sort(results) == [2, 4]
    assert job!(name, parent.id).attempt == 2
  end

  test "a failed child still completes the fan-in (visible in child state)", %{name: name} do
    {:ok, parent} =
      Belay.insert(name, FanOut.new(%{"values" => [1]}))

    # Sneak a failing child in alongside, attached to the same parent.
    {:ok, _bad} =
      Belay.insert(
        name,
        Belay.Test.ChildEcho.new(%{"v" => 9, "fail" => true},
          max_attempts: 1,
          parent_id: parent.id
        )
      )

    counts = Testing.drain(name, :default)

    assert counts[:failed] == 1
    assert {:ok, _results} = Belay.await_result(name, parent.id, 100)

    states =
      name |> Belay.list_jobs(parent_id: parent.id) |> Enum.map(& &1.state) |> Enum.sort()

    assert states == ["failed", "succeeded"]
  end

  test "every child completion signals the parent, not just the last one", %{name: name} do
    # Count-gated signalling is skippable when the last two children ack in
    # concurrent transactions (each sees the other as incomplete under READ
    # COMMITTED) — found by the chaos soak. The contract is now: every child
    # terminal ack upserts the parent's $children signal; parents re-verify.
    {:ok, parent} = Belay.insert(name, Echo.new(%{}, schedule_in: 999))

    {:ok, _c1} =
      Belay.insert(name, Belay.Test.ChildEcho.new(%{"v" => 1}, parent_id: parent.id))

    {:ok, _c2} =
      Belay.insert(name, Belay.Test.ChildEcho.new(%{"v" => 2}, parent_id: parent.id))

    {mod, ref} = storage(name)
    conf = config(name)
    spec = Belay.Config.queue_spec(conf, :default)
    now = Belay.Config.now(conf)

    {:ok, [first_child]} = mod.claim(ref, spec, 1, "n", 30_000, now)
    {:ok, _} = mod.ack(ref, first_child, {:succeeded, nil}, now)

    # One of two children done — the signal must already be there.
    assert {:ok, _} = mod.get_signal(ref, ["job:#{parent.id}"], "$children")
  end

  test "batches run their on_complete callback whatever the outcomes", %{name: name} do
    {:ok, batch_id, jobs} =
      Batch.insert(
        name,
        [
          Tagged.new(%{"tag" => "ok"}),
          Tagged.new(%{"tag" => "bad", "fail" => true}, max_attempts: 1)
        ],
        on_complete: Echo.new(%{"kind" => "batch-done"})
      )

    assert map_size(jobs) == 3

    Testing.drain(name, :default)

    status = Batch.status(name, batch_id)

    assert status.done?
    assert status.state_counts == %{"succeeded" => 2, "failed" => 1}

    callback = Enum.find(Batch.jobs(name, batch_id), &(&1.wf_name == "on-complete"))

    assert callback.state == "succeeded"

    assert {:ok, %{"kind" => "batch-done", "batch_id" => ^batch_id}} =
             {:ok, Belay.Job.result(callback)}
  end
end
