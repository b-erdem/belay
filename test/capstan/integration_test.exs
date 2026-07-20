defmodule Capstan.IntegrationTest do
  # End-to-end through live queue producers (no drain): stager, insert
  # notifications, engine fetch, signal wake-ups, and relay responses.
  use Capstan.Test.Case, async: false

  alias Capstan.{Relay, Workflow}
  alias Capstan.Test.{Awaiter, Echo, Events, Tagged}

  setup do
    Events.clear()

    name =
      start_oban!(
        queues: [
          default: 5,
          limited: [limit: 5, global_limit: 2, rate_limit: [allowed: 50, period: 60]]
        ]
      )

    {:ok, name: name}
  end

  test "extended queue options pass Oban config validation", %{name: name} do
    assert %{limit: 5, capstan: %{global_limit: 2}} = Oban.check_queue(name, queue: :limited)
  end

  test "relay round-trips through live execution", %{name: name} do
    relay = Relay.async(name, Echo.new(%{"live" => true}))

    assert {:ok, %{"live" => true}} = Relay.await(relay, 2_000)
  end

  test "await_signal parks and a signal resumes through live queues", %{name: name} do
    {:ok, %{id: id}} = Oban.insert(name, Awaiter.new(%{}))

    wait_until(fn ->
      match?([%{state: "scheduled"}], all_jobs())
    end)

    Capstan.Steps.signal(name, "job:#{id}", :approval, %{"go" => 1})

    wait_until(fn ->
      match?([%{state: "completed"}], all_jobs())
    end)

    [job] = all_jobs()
    assert {:ok, %{"go" => 1}} = Capstan.Worker.fetch_recorded(job)
  end

  test "workflows advance through live queues", %{name: name} do
    {:ok, _} =
      Workflow.new()
      |> Workflow.add(:a, Tagged.new(%{"tag" => "a"}))
      |> Workflow.add(:b, Tagged.new(%{"tag" => "b"}), deps: [:a])
      |> Workflow.insert(name)

    wait_until(fn ->
      Enum.all?(all_jobs(), &(&1.state == "completed"))
    end)

    assert Events.all() == [{:ran, "a"}, {:ran, "b"}]
  end

  test "global limit holds under live load", %{name: name} do
    for i <- 1..6 do
      {:ok, _} =
        Oban.insert(name, Capstan.Test.Sleeper.new(%{"i" => i}, queue: :limited))
    end

    # Sample concurrency while jobs flow through the limited queue.
    peak =
      Enum.reduce(1..40, 0, fn _, acc ->
        executing = Enum.count(all_jobs(), &(&1.state == "executing"))
        Process.sleep(10)
        max(acc, executing)
      end)

    wait_until(fn -> Enum.all?(all_jobs(), &(&1.state == "completed")) end, 5_000)

    assert peak <= 2
    assert peak > 0
  end
end
