defmodule Capstan.BatchRelayTest do
  use Capstan.Test.Case, async: false

  alias Capstan.{Batch, Relay}
  alias Capstan.Test.{BatchCb, Echo, Events, Tagged}

  setup do
    Events.clear()

    {:ok, name: start_oban!()}
  end

  test "batch fires completed callback exactly once", %{name: name} do
    changesets = for tag <- ~w(x y z), do: Tagged.new(%{"tag" => tag})

    {:ok, batch_id, jobs} = Batch.insert(name, changesets, callback: BatchCb)

    assert length(jobs) == 3

    drain!(name, :default)

    assert Events.count({:batch_completed, batch_id}) == 1
    assert Events.count({:batch_exhausted, batch_id}) == 0
  end

  test "batch with a discarded job fires exhausted, not completed", %{name: name} do
    changesets = [
      Tagged.new(%{"tag" => "ok"}),
      Tagged.new(%{"tag" => "bad", "fail" => true}, max_attempts: 1)
    ]

    {:ok, batch_id, _jobs} = Batch.insert(name, changesets, callback: BatchCb)

    drain!(name, :default)

    assert Events.count({:batch_completed, batch_id}) == 0
    assert Events.count({:batch_exhausted, batch_id}) == 1
  end

  test "relay awaits a recorded result", %{name: name} do
    relay = Relay.async(name, Echo.new(%{"n" => 42}))

    drain!(name, :default)

    assert {:ok, %{"n" => 42}} = Relay.await(relay, 500)
  end

  test "relay reports terminal failure", %{name: name} do
    relay = Relay.async(name, Tagged.new(%{"tag" => "f", "fail" => true}, max_attempts: 1))

    drain!(name, :default)

    assert {:error, {:job, :discarded}} = Relay.await(relay, 500)
  end

  test "relay times out when nothing runs", %{name: name} do
    relay = Relay.async(name, Echo.new(%{}))

    assert {:error, :timeout} = Relay.await(relay, 50)
  end
end
