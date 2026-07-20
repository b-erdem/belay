defmodule Capstan.EngineTest do
  use Capstan.Test.Case, async: false

  import Ecto.Query

  alias Capstan.RateLimiter
  alias Capstan.Test.Tagged
  alias Oban.Job

  setup do
    {:ok, name: start_oban!()}
  end

  defp conf(name), do: Oban.config(name)

  defp insert_available!(name, n) do
    for i <- 1..n do
      {:ok, job} = Oban.insert(name, Tagged.new(%{"tag" => "j#{i}", "tenant" => tenant(i)}))
      job
    end
  end

  defp tenant(i), do: if(rem(i, 2) == 0, do: "even", else: "odd")

  defp init_meta!(name, opts) do
    {:ok, meta} = Capstan.Engine.init(conf(name), Keyword.merge([queue: "default"], opts))
    meta
  end

  defp fetch!(name, meta) do
    {:ok, {_meta, jobs}} = Capstan.Engine.fetch_jobs(conf(name), meta, %{})
    jobs
  end

  test "global_limit caps executing jobs across fetches", %{name: name} do
    insert_available!(name, 10)

    meta = init_meta!(name, limit: 10, global_limit: 3)

    first = fetch!(name, meta)
    assert length(first) == 3

    # All three are now executing; a second producer gets nothing.
    assert fetch!(name, meta) == []

    # Complete one and capacity opens up again.
    Repo.update_all(
      from(j in Job, where: j.id == ^hd(first).id),
      set: [state: "completed"]
    )

    assert length(fetch!(name, meta)) == 1
  end

  test "rate_limit admits only the window allowance", %{name: name} do
    insert_available!(name, 10)

    meta = init_meta!(name, limit: 10, rate_limit: [allowed: 4, period: 60])

    assert length(fetch!(name, meta)) == 4
    assert fetch!(name, meta) == []

    windows = Repo.all(from(r in Capstan.RateWindow, select: {r.queue, r.count}))
    assert windows == [{"default", 4}]
  end

  test "rate limiter sliding window math", %{name: name} do
    limit = %{allowed: 10, period: 60}
    t0 = 1_000_020

    assert RateLimiter.allowance(conf(name), "q", limit, t0) == 10

    RateLimiter.debit(conf(name), "q", limit, 10, t0)

    assert RateLimiter.allowance(conf(name), "q", limit, t0 + 1) == 0

    # Next window, 30s in: previous window weighs 50%.
    half = 1_000_080 + 30
    assert RateLimiter.allowance(conf(name), "q", limit, half) == 5

    # Two windows later the old spend ages out entirely.
    assert RateLimiter.allowance(conf(name), "q", limit, t0 + 130) == 10
  end

  test "partitioned global_limit enforces per-key caps and reverts overflow", %{name: name} do
    insert_available!(name, 4)

    meta = init_meta!(name, limit: 10, global_limit: 1, partition: {:args, "tenant"})

    jobs = fetch!(name, meta)

    tenants = jobs |> Enum.map(& &1.args["tenant"]) |> Enum.sort()
    assert tenants == ["even", "odd"]

    reverted =
      Repo.all(from(j in Job, where: j.state == "available", order_by: j.id))

    assert length(reverted) == 2
    assert Enum.all?(reverted, &(&1.attempt == 0))
  end

  test "queues without capstan options behave exactly like stock oban", %{name: name} do
    insert_available!(name, 5)

    meta = init_meta!(name, limit: 3)

    assert length(fetch!(name, meta)) == 3
    assert length(fetch!(name, meta)) == 2
  end

  test "engine rejects invalid capstan options", %{name: name} do
    assert {:error, %ArgumentError{}} =
             Capstan.Engine.init(conf(name), queue: :q, limit: 1, global_limit: -1)

    assert {:error, %ArgumentError{}} =
             Capstan.Engine.init(conf(name), queue: :q, limit: 1, rate_limit: [allowed: 5])
  end
end
