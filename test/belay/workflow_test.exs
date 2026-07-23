defmodule Belay.WorkflowTest do
  use Belay.Test.Case, async: false

  alias Belay.Test.Tagged
  alias Belay.Workflow

  setup do
    {:ok, start_belay!()}
  end

  defp tagged(tag, extra \\ %{}, opts \\ []) do
    Tagged.new(Map.merge(%{"tag" => tag}, extra), opts)
  end

  test "linear dependencies run in order", %{name: name} do
    {:ok, jobs} =
      Workflow.new()
      |> Workflow.add(:a, tagged("a"))
      |> Workflow.add(:b, tagged("b"), deps: [:a])
      |> Workflow.add(:c, tagged("c"), deps: [:b])
      |> Workflow.insert(name)

    assert jobs["a"].state == "ready"
    assert jobs["b"].state == "held"
    assert jobs["c"].state == "held"

    assert %{succeeded: 3} = Testing.drain(name, :default)
    assert Events.all() == [{:ran, "a"}, {:ran, "b"}, {:ran, "c"}]
  end

  test "diamond joins wait for all parents", %{name: name} do
    {:ok, jobs} =
      Workflow.new()
      |> Workflow.add(:a, tagged("a"))
      |> Workflow.add(:b, tagged("b"), deps: [:a])
      |> Workflow.add(:c, tagged("c"), deps: [:a])
      |> Workflow.add(:d, tagged("d"), deps: [:b, :c])
      |> Workflow.insert(name)

    assert %{succeeded: 4} = Testing.drain(name, :default)

    events = Events.all()

    assert List.first(events) == {:ran, "a"}
    assert List.last(events) == {:ran, "d"}

    wf_id = jobs["a"].workflow_id
    assert %{done?: true, state_counts: %{"succeeded" => 4}} = Workflow.status(name, wf_id)
  end

  test "failed upstreams cancel dependents transitively", %{name: name} do
    {:ok, jobs} =
      Workflow.new()
      |> Workflow.add(:a, tagged("a", %{"fail" => true}, max_attempts: 1))
      |> Workflow.add(:b, tagged("b"), deps: [:a])
      |> Workflow.add(:c, tagged("c"), deps: [:b])
      |> Workflow.insert(name)

    assert %{failed: 1} = Testing.drain(name, :default)

    states =
      name
      |> Workflow.jobs(jobs["a"].workflow_id)
      |> Map.new(&{&1.wf_name, &1.state})

    assert states == %{"a" => "failed", "b" => "cancelled", "c" => "cancelled"}
    assert Events.all() == [{:ran, "a"}]

    assert {:error, {:job, :cancelled}} = Belay.await_result(name, jobs["c"].id, 100)
  end

  test "ignore: [:failed] treats failure as satisfied", %{name: name} do
    {:ok, _jobs} =
      Workflow.new()
      |> Workflow.add(:a, tagged("a", %{"fail" => true}, max_attempts: 1))
      |> Workflow.add(:b, tagged("b"), deps: [:a], ignore: [:failed])
      |> Workflow.insert(name)

    counts = Testing.drain(name, :default)

    assert counts == %{failed: 1, succeeded: 1}
    assert {:ran, "b"} in Events.all()
  end

  test "cancelling a held workflow job cascades to its dependents", %{name: name} do
    {:ok, jobs} =
      Workflow.new()
      |> Workflow.add(:a, tagged("a", %{}, schedule_in: 300))
      |> Workflow.add(:b, tagged("b"), deps: [:a])
      |> Workflow.add(:c, tagged("c"), deps: [:b])
      |> Workflow.insert(name)

    {:ok, :cancelled} = Belay.cancel_job(name, jobs["a"].id)

    states =
      name
      |> Workflow.jobs(jobs["a"].workflow_id)
      |> Map.new(&{&1.wf_name, &1.state})

    assert states == %{"a" => "cancelled", "b" => "cancelled", "c" => "cancelled"}
  end

  test "unknown deps raise at build time" do
    assert_raise ArgumentError, ~r/unknown workflow deps/, fn ->
      Workflow.new() |> Workflow.add(:b, tagged("b"), deps: [:missing])
    end
  end
end
