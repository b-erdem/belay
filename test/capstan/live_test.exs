defmodule Capstan.LiveTest do
  # End-to-end through real producers with the real clock: pokes, leases,
  # await/signal wake-ups, workflow releases, global limits, cron dedup.
  use Capstan.Test.Case, async: false

  alias Capstan.Test.{Awaiter, CronJob, Echo, SlowLive, Tagged}
  alias Capstan.Workflow

  defp live!(extra \\ []) do
    start_capstan!(
      Keyword.merge(
        [
          sim_clock: false,
          poll_interval: 50,
          queues: [default: 5, limited: [limit: 5, global_limit: 2]]
        ],
        extra
      )
    )
  end

  test "inserted jobs execute without polling delays" do
    %{name: name} = live!()

    {:ok, job} = Capstan.insert(name, Echo.new(%{"live" => true}))

    assert {:ok, %{"live" => true}} = Capstan.await_result(name, job.id, 2_000)
  end

  test "await parks, signal resumes, result arrives" do
    %{name: name} = live!()

    {:ok, job} = Capstan.insert(name, Awaiter.new(%{}))

    wait_until(fn -> job!(name, job.id).state == "awaiting" end)

    Capstan.signal_job(name, job.id, :approval, %{"go" => 1})

    assert {:ok, %{"go" => 1}} = Capstan.await_result(name, job.id, 2_000)
  end

  test "workflows advance through live producers" do
    %{name: name} = live!()

    {:ok, jobs} =
      Workflow.new()
      |> Workflow.add(:a, Tagged.new(%{"tag" => "a"}))
      |> Workflow.add(:b, Tagged.new(%{"tag" => "b"}), deps: [:a])
      |> Workflow.insert(name)

    wait_until(fn -> Workflow.status(name, jobs["a"].workflow_id).done? end)

    assert Events.all() == [{:ran, "a"}, {:ran, "b"}]
  end

  test "global limit holds under live load" do
    %{name: name} = live!()

    jobs =
      for i <- 1..6 do
        {:ok, job} = Capstan.insert(name, SlowLive.new(%{"i" => i}))
        job
      end

    for job <- jobs do
      assert {:ok, _} = Capstan.await_result(name, job.id, 5_000)
    end

    assert Events.peak_gauge() in 1..2
  end

  test "cron fires exactly once per slot across repeated ticks" do
    %{name: _name} =
      live!(
        cron_interval: 40,
        crons: [[name: "tick", expr: "* * * * *", worker: CronJob]]
      )

    wait_until(fn -> Events.count(:cron_ran) >= 1 end)

    Process.sleep(300)

    # ~8 scheduler ticks elapsed; without slot dedup this would be ~8 runs.
    # Allow 2 for the rare minute-boundary crossing during the test window.
    assert Events.count(:cron_ran) in 1..2
  end
end
