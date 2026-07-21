defmodule Belay.GapsTest do
  # Integration coverage for surfaces the feature suites didn't pin down:
  # pause/resume, encrypted replay, and workflow-retry semantics.
  use Belay.Test.Case, async: false

  alias Belay.Test.{Echo, Secret, Tagged}
  alias Belay.Workflow

  test "pause stops a live producer from claiming; resume drains the backlog" do
    %{name: name} = start_belay!(sim_clock: false, queues: [default: 5], poll_interval: 50)

    assert :ok = Belay.pause_queue(name, :default)

    {:ok, job} = Belay.insert(name, Echo.new(%{"paused" => true}))

    # Well past several poll intervals: nothing may claim it.
    Process.sleep(300)
    assert job!(name, job.id).state == "ready"

    assert :ok = Belay.resume_queue(name, :default)

    assert {:ok, %{"paused" => true}} = Belay.await_result(name, job.id, 2_000)
  end

  test "pausing an unknown queue reports it" do
    %{name: name} = start_belay!()

    assert {:error, :no_producer} = Belay.pause_queue(name, :ghost)
  end

  test "replay decrypts encrypted inputs" do
    %{name: name} = start_belay!(encryption: [key: {Belay.Test.Keys, :test_key, []}])

    {:ok, job} = Belay.insert(name, Secret.new(%{"pin" => "1234"}))

    Testing.drain(name, :default)

    assert {:ok, {:ok, %{"pin" => "1234"}}, _trace} = Belay.Replay.dry_run(name, job.id)
  end

  test "retrying a cascade-failed workflow root does not resurrect cancelled children" do
    # Pinned semantics: settlement cancels are terminal; retrying the root
    # re-runs only the root. Reviving dependents means retrying them too.
    %{name: name} = start_belay!()

    {:ok, jobs} =
      Workflow.new()
      |> Workflow.add(:a, Tagged.new(%{"tag" => "a", "fail" => true}, max_attempts: 1))
      |> Workflow.add(:b, Tagged.new(%{"tag" => "b"}), deps: [:a])
      |> Workflow.insert(name)

    assert %{failed: 1} = Testing.drain(name, :default)
    assert job!(name, jobs["b"].id).state == "cancelled"

    {:ok, _} = Belay.retry_job(name, jobs["a"].id)

    # Root retries and fails again (worker still fails); b stays cancelled.
    assert %{failed: 1} = Testing.drain(name, :default)
    assert job!(name, jobs["b"].id).state == "cancelled"
  end
end
