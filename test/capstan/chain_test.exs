defmodule Capstan.ChainTest do
  use Capstan.Test.Case, async: false

  alias Capstan.Chain
  alias Capstan.Test.{Events, Tagged}

  setup do
    Events.clear()

    {:ok, name: start_oban!()}
  end

  defp chained(tag, key, extra \\ %{}, opts \\ []) do
    meta = Map.merge(%{"chain_key" => key}, Map.get(extra, :meta, %{}))

    Tagged.new(
      Map.merge(%{"tag" => tag}, Map.get(extra, :args, %{})),
      Keyword.put(opts, :meta, meta)
    )
  end

  test "later links hold while a predecessor is incomplete", %{name: name} do
    {:ok, _} = Oban.insert(name, chained("1", "acct:9"))
    {:ok, _} = Oban.insert(name, chained("2", "acct:9"))
    {:ok, _} = Oban.insert(name, chained("3", "acct:9"))

    assert Enum.map(all_jobs(), & &1.state) == ["available", "suspended", "suspended"]

    drain!(name, :default)

    assert Events.all() == [{:ran, "1"}, {:ran, "2"}, {:ran, "3"}]
    assert Enum.all?(all_jobs(), &(&1.state == "completed"))
  end

  test "independent keys do not block each other", %{name: name} do
    {:ok, _} = Oban.insert(name, chained("a1", "a"))
    {:ok, _} = Oban.insert(name, chained("b1", "b"))

    assert Enum.map(all_jobs(), & &1.state) == ["available", "available"]
  end

  test "default policy continues past discarded links", %{name: name} do
    {:ok, _} = Oban.insert(name, chained("1", "k", %{args: %{"fail" => true}}, max_attempts: 1))
    {:ok, _} = Oban.insert(name, chained("2", "k"))

    drain!(name, :default)

    states = Enum.map(all_jobs(), & &1.state)

    assert states == ["discarded", "completed"]
  end

  test "halt policy holds the chain until resumed", %{name: name} do
    {:ok, _} =
      Oban.insert(
        name,
        chained("1", "h", %{args: %{"fail" => true}, meta: %{"chain_policy" => "halt"}},
          max_attempts: 1
        )
      )

    {:ok, _} = Oban.insert(name, chained("2", "h"))

    drain!(name, :default)

    assert Enum.map(all_jobs(), & &1.state) == ["discarded", "suspended"]

    Chain.resume(name, "h")

    drain!(name, :default)

    assert Enum.map(all_jobs(), & &1.state) == ["discarded", "completed"]
  end
end
