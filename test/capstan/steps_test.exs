defmodule Capstan.StepsTest do
  use Capstan.Test.Case, async: false

  alias Capstan.Test.{Awaiter, Events, StepFlaky}

  setup do
    Events.clear()

    {:ok, name: start_oban!()}
  end

  test "steps are memoized across retries", %{name: name} do
    {:ok, _} = Oban.insert(name, StepFlaky.new(%{}))

    drain!(name, :default, with_scheduled: true)

    [job] = all_jobs()

    assert job.state == "completed"
    assert job.attempt == 2
    assert Events.count(:step_ran) == 1
    assert {:ok, 42} = Capstan.Worker.fetch_recorded(job)
  end

  test "await_signal parks the job, signal wakes and delivers payload", %{name: name} do
    {:ok, %{id: id}} = Oban.insert(name, Awaiter.new(%{}))

    drain!(name, :default)

    [job] = all_jobs()

    assert job.state == "scheduled"
    assert job.meta["awaiting_scope"] == "job:#{id}"
    assert job.meta["awaiting_name"] == "approval"

    Capstan.Steps.signal(name, "job:#{id}", :approval, %{"approved" => true})

    [job] = all_jobs()
    assert job.state == "available"

    drain!(name, :default)

    [job] = all_jobs()
    assert job.state == "completed"
    assert {:ok, %{"approved" => true}} = Capstan.Worker.fetch_recorded(job)
  end

  test "a signal delivered before the job runs is picked up immediately", %{name: name} do
    {:ok, %{id: id}} = Oban.insert(name, Awaiter.new(%{}))

    Capstan.Steps.signal(name, "job:#{id}", :approval, %{"pre" => 1})

    drain!(name, :default)

    [job] = all_jobs()
    assert job.state == "completed"
    assert {:ok, %{"pre" => 1}} = Capstan.Worker.fetch_recorded(job)
  end

  test "clear_signal makes await block again", %{name: name} do
    {:ok, %{id: id}} = Oban.insert(name, Awaiter.new(%{}))

    Capstan.Steps.signal(name, "job:#{id}", :approval, %{})
    Capstan.Steps.clear_signal(name, "job:#{id}", :approval)

    drain!(name, :default)

    [job] = all_jobs()
    assert job.state == "scheduled"
  end
end
