defmodule Capstan.GapsTest do
  # Integration coverage for surfaces the feature suites didn't pin down:
  # pause/resume, encrypted replay, and workflow-retry semantics.
  use Capstan.Test.Case, async: false

  alias Capstan.Test.{Echo, Secret, Tagged}
  alias Capstan.Workflow

  test "pause stops a live producer from claiming; resume drains the backlog" do
    %{name: name} = start_capstan!(sim_clock: false, queues: [default: 5], poll_interval: 50)

    assert :ok = Capstan.pause_queue(name, :default)

    {:ok, job} = Capstan.insert(name, Echo.new(%{"paused" => true}))

    # Well past several poll intervals: nothing may claim it.
    Process.sleep(300)
    assert job!(name, job.id).state == "ready"

    assert :ok = Capstan.resume_queue(name, :default)

    assert {:ok, %{"paused" => true}} = Capstan.await_result(name, job.id, 2_000)
  end

  test "pausing an unknown queue reports it" do
    %{name: name} = start_capstan!()

    assert {:error, :no_producer} = Capstan.pause_queue(name, :ghost)
  end

  test "replay decrypts encrypted inputs" do
    %{name: name} = start_capstan!(encryption: [key: {Capstan.Test.Keys, :test_key, []}])

    {:ok, job} = Capstan.insert(name, Secret.new(%{"pin" => "1234"}))

    Testing.drain(name, :default)

    assert {:ok, {:ok, %{"pin" => "1234"}}, _trace} = Capstan.Replay.dry_run(name, job.id)
  end

  test "retrying a cascade-failed workflow root does not resurrect cancelled children" do
    # Pinned semantics: settlement cancels are terminal; retrying the root
    # re-runs only the root. Reviving dependents means retrying them too.
    %{name: name} = start_capstan!()

    {:ok, jobs} =
      Workflow.new()
      |> Workflow.add(:a, Tagged.new(%{"tag" => "a", "fail" => true}, max_attempts: 1))
      |> Workflow.add(:b, Tagged.new(%{"tag" => "b"}), deps: [:a])
      |> Workflow.insert(name)

    assert %{failed: 1} = Testing.drain(name, :default)
    assert job!(name, jobs["b"].id).state == "cancelled"

    {:ok, _} = Capstan.retry_job(name, jobs["a"].id)

    # Root retries and fails again (worker still fails); b stays cancelled.
    assert %{failed: 1} = Testing.drain(name, :default)
    assert job!(name, jobs["b"].id).state == "cancelled"
  end
end
