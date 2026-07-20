defmodule Capstan.WorkerTest do
  use Capstan.Test.Case, async: false

  alias Capstan.Test.{Echo, Events, Hooked, Schema}

  setup do
    Events.clear()

    {:ok, name: start_oban!()}
  end

  test "recorded results round-trip through job meta", %{name: name} do
    {:ok, _} = Oban.insert(name, Echo.new(%{"payload" => "hello"}))

    drain!(name, :default)

    [job] = all_jobs()

    assert job.state == "completed"
    assert {:ok, %{"payload" => "hello"}} = Capstan.Worker.fetch_recorded(job)
  end

  test "args schema casts, applies defaults, and validates enums", %{name: name} do
    {:ok, _} = Oban.insert(name, Schema.new(%{"name" => "n", "mode" => "fast"}))

    drain!(name, :default)

    [job] = all_jobs()

    assert job.state == "completed"
    assert {:ok, %{name: "n", count: 7, mode: "fast"}} = Capstan.Worker.fetch_recorded(job)
  end

  test "invalid args cancel instead of retrying", %{name: name} do
    {:ok, _} = Oban.insert(name, Schema.new(%{"count" => 3}))
    {:ok, _} = Oban.insert(name, Schema.new(%{"name" => "n", "mode" => "warp"}))

    drain!(name, :default)

    assert Enum.all?(all_jobs(), &(&1.state == "cancelled"))
    assert Enum.all?(all_jobs(), &(&1.attempt == 1))
  end

  test "hooks run in order around process", %{name: name} do
    {:ok, _} = Oban.insert(name, Hooked.new(%{}))

    drain!(name, :default)

    assert Events.all() == [:before, :process, {:after, :plain}]
  end

  test "before_process can cancel the job", %{name: name} do
    {:ok, _} = Oban.insert(name, Hooked.new(%{"veto" => true}))

    drain!(name, :default)

    [job] = all_jobs()

    assert job.state == "cancelled"
    assert Events.all() == []
  end
end
