defmodule Belay.EventsReplayTest do
  use Belay.Test.Case, async: false

  alias Belay.Replay
  alias Belay.Test.{Awaiter, Divergent, Emitter, StepFlaky}

  setup do
    :persistent_term.put({Belay.Test.Divergent, :path}, :original)

    {:ok, start_belay!()}
  end

  test "emitted events are delivered live and replayable", %{name: name} do
    {:ok, job} = Belay.insert(name, Emitter.new(%{}))

    Belay.subscribe_events(name, job.id)

    Testing.drain(name, :default)

    assert_received {:belay_event, _, 1, %{"chunk" => "token-1"}}
    assert_received {:belay_event, _, 2, %{"chunk" => "token-2"}}
    assert_received {:belay_event, _, 3, %{"chunk" => "token-3"}}

    all = Belay.events(name, job.id)

    assert Enum.map(all, & &1.payload) == [
             %{"chunk" => "token-1"},
             %{"chunk" => "token-2"},
             %{"chunk" => "token-3"}
           ]

    assert [%{seq: 3}] = Belay.events(name, job.id, 2)
  end

  test "dry_run replays a completed job from its journal", %{name: name, clock: clock} do
    {:ok, job} = Belay.insert(name, StepFlaky.new(%{}))

    Testing.drain(name, :default)
    advance(clock, 6)
    Testing.drain(name, :default)

    assert {:ok, {:ok, 42}, trace} = Replay.dry_run(name, job.id)
    assert Enum.map(trace, & &1.name) == ["expensive"]
    assert Events.count(:step_ran) == 1
  end

  test "dry_run detects divergence after a code-path change", %{name: name} do
    {:ok, job} = Belay.insert(name, Divergent.new(%{}))

    Testing.drain(name, :default)

    assert {:ok, {:ok, 42}, _} = Replay.dry_run(name, job.id)

    :persistent_term.put({Divergent, :path}, :changed)

    assert {:blocked, {:missing_step, "b_new"}, trace} = Replay.dry_run(name, job.id)
    assert Enum.map(trace, & &1.name) == ["a", "b"]
  end

  test "dry_run reports a parked job as blocked on its signal", %{name: name} do
    {:ok, job} = Belay.insert(name, Awaiter.new(%{}))

    Testing.drain(name, :default)

    assert {:blocked, {:blocked_on_signal, "approval"}, _} = Replay.dry_run(name, job.id)
  end

  test "dry_run of a missing job", %{name: name} do
    assert {:error, :not_found} = Replay.dry_run(name, 424_242)
  end
end
