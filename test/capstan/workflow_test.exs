defmodule Capstan.WorkflowTest do
  use Capstan.Test.Case, async: false

  alias Capstan.Test.{Events, Tagged}
  alias Capstan.Workflow

  setup do
    Events.clear()

    {:ok, name: start_oban!()}
  end

  defp tagged(tag, extra \\ %{}), do: Tagged.new(Map.merge(%{"tag" => tag}, extra))

  test "linear dependencies run in order", %{name: name} do
    {:ok, jobs} =
      Workflow.new()
      |> Workflow.add(:a, tagged("a"))
      |> Workflow.add(:b, tagged("b"), deps: [:a])
      |> Workflow.add(:c, tagged("c"), deps: [:b])
      |> Workflow.insert(name)

    assert jobs["a"].state == "available"
    assert jobs["b"].state == "suspended"
    assert jobs["c"].state == "suspended"

    drain!(name, :default)

    assert Events.all() == [{:ran, "a"}, {:ran, "b"}, {:ran, "c"}]
    assert Enum.all?(all_jobs(), &(&1.state == "completed"))
  end

  test "diamond: join waits for all parents", %{name: name} do
    {:ok, _} =
      Workflow.new()
      |> Workflow.add(:a, tagged("a"))
      |> Workflow.add(:b, tagged("b"), deps: [:a])
      |> Workflow.add(:c, tagged("c"), deps: [:a])
      |> Workflow.add(:d, tagged("d"), deps: [:b, :c])
      |> Workflow.insert(name)

    drain!(name, :default)

    events = Events.all()

    assert length(events) == 4
    assert List.first(events) == {:ran, "a"}
    assert List.last(events) == {:ran, "d"}
    assert Enum.all?(all_jobs(), &(&1.state == "completed"))
  end

  test "failed upstream cancels dependents transitively", %{name: name} do
    {:ok, wf_jobs} =
      Workflow.new()
      |> Workflow.add(:a, Tagged.new(%{"tag" => "a", "fail" => true}, max_attempts: 1))
      |> Workflow.add(:b, tagged("b"), deps: [:a])
      |> Workflow.add(:c, tagged("c"), deps: [:b])
      |> Workflow.insert(name)

    drain!(name, :default)

    states = Map.new(all_jobs(), &{&1.meta["workflow_name"], &1.state})

    assert states == %{"a" => "discarded", "b" => "cancelled", "c" => "cancelled"}
    assert Events.all() == [{:ran, "a"}]

    status = Workflow.status(name, wf_jobs["a"].meta["workflow_id"])
    assert status.done?
  end

  test "ignore_discarded releases dependents despite failure", %{name: name} do
    {:ok, _} =
      Workflow.new()
      |> Workflow.add(:a, Tagged.new(%{"tag" => "a", "fail" => true}, max_attempts: 1))
      |> Workflow.add(:b, tagged("b"), deps: [:a], ignore_discarded: true)
      |> Workflow.insert(name)

    drain!(name, :default)

    states = Map.new(all_jobs(), &{&1.meta["workflow_name"], &1.state})

    assert states == %{"a" => "discarded", "b" => "completed"}
    assert {:ran, "b"} in Events.all()
  end

  test "unknown deps raise at build time" do
    assert_raise ArgumentError, ~r/unknown workflow deps/, fn ->
      Workflow.new()
      |> Workflow.add(:b, tagged("b"), deps: [:missing])
    end
  end
end
