defmodule Capstan.ResourceRateTest do
  # Token-style resource budgets: admission on an estimate, corrected by
  # actual usage (Capstan.debit/3) so the window converges on reality.
  use Capstan.Test.Case, async: false

  alias Capstan.Test.ResourceUser

  setup do
    context =
      start_capstan!(
        queues: [
          metered: [
            limit: 10,
            rate: [allowed: 10_000, period: 60, resource: "prov", estimate: 4_000],
            manual: true
          ]
        ]
      )

    {:ok, context}
  end

  defp claim!(name, demand) do
    {mod, ref} = storage(name)
    conf = config(name)
    spec = Capstan.Config.queue_spec(conf, :metered)

    {:ok, jobs} = mod.claim(ref, spec, demand, "n1", 30_000, Capstan.Config.now(conf))

    jobs
  end

  defp window_total(name) do
    {mod, ref} = storage(name)

    # Peek through the adapter-agnostic surface: allowance math is what matters,
    # but the raw counter makes the true-up arithmetic visible.
    case mod do
      Capstan.Storage.Memory ->
        :sys.get_state(ref).rate |> Map.values() |> Enum.sum()

      Capstan.Storage.Postgres ->
        %{rows: [[sum]]} =
          Postgrex.query!(ref, "SELECT COALESCE(sum(count), 0) FROM capstan_rate", [])

        sum
    end
  end

  test "admission divides the window by the estimate", %{name: name} do
    for i <- 1..5 do
      {:ok, _} = Capstan.insert(name, ResourceUser.new(%{"actual" => 1_000 + i}, queue: :metered))
    end

    # 10_000 allowed / 4_000 estimated = 2 jobs admitted; 8_000 debited.
    claimed = claim!(name, 10)

    assert length(claimed) == 2
    assert window_total(name) == 8_000

    assert claim!(name, 10) == []
  end

  test "actual usage trues the window up, opening real headroom", %{name: name} do
    jobs =
      for i <- 1..5 do
        {:ok, job} = Capstan.insert(name, ResourceUser.new(%{"actual" => 1_000}, queue: :metered))
        _ = i
        job
      end

    _ = jobs

    claimed = claim!(name, 10)
    assert length(claimed) == 2

    for job <- claimed do
      {:ok, _, _} = Capstan.Runner.execute(config(name), job)
    end

    # Each run replaced its 4_000 estimate with 1_000 actual:
    # 8_000 - 2*4_000 + 2*1_000 = 2_000 used → room for 2 more jobs.
    assert window_total(name) == 2_000
    assert length(claim!(name, 10)) == 2
  end
end
