defmodule Belay.LimitsTest do
  use Belay.Test.Case, async: false

  alias Belay.Test.Echo

  setup do
    context =
      start_belay!(
        queues: [
          default: [limit: 10, manual: true],
          capped: [limit: 10, global_limit: 3, manual: true],
          rated: [limit: 10, rate: [allowed: 4, period: 60], manual: true],
          tenants: [limit: 10, global_limit: 1, partition: {:input, "tenant"}, manual: true]
        ]
      )

    {:ok, context}
  end

  defp claim!(name, queue, demand) do
    {mod, ref} = storage(name)
    conf = config(name)
    spec = Belay.Config.queue_spec(conf, queue)

    {:ok, jobs} = mod.claim(ref, spec, demand, "n1", 30_000, Belay.Config.now(conf))

    jobs
  end

  defp fill!(name, queue, n, extra \\ fn _ -> %{} end) do
    for i <- 1..n do
      {:ok, job} = Belay.insert(name, Echo.new(extra.(i), queue: queue))
      job
    end
  end

  test "global limit counts live-leased jobs across claimers", %{name: name, clock: clock} do
    fill!(name, :capped, 10)

    first = claim!(name, :capped, 10)
    assert length(first) == 3

    assert claim!(name, :capped, 10) == []

    {mod, ref} = storage(name)
    now = Belay.Config.now(config(name))
    {:ok, _} = mod.ack(ref, hd(first), {:succeeded, nil}, now)

    assert length(claim!(name, :capped, 10)) == 1

    # Expired leases stop counting against the limit.
    advance(clock, 3_600)
    assert length(claim!(name, :capped, 10)) == 3
  end

  test "rate limit slides across windows", %{name: name, clock: clock} do
    fill!(name, :rated, 10)

    assert length(claim!(name, :rated, 10)) == 4
    assert claim!(name, :rated, 10) == []

    # Half a window later the previous spend weighs 50%: 4 - 2 = 2 admitted.
    advance(clock, 90)
    assert length(claim!(name, :rated, 10)) == 2

    # Two full windows later the old spend has aged out.
    advance(clock, 120)
    assert length(claim!(name, :rated, 10)) == 4
  end

  test "partitioned claims are exact under heavy key skew", %{name: name} do
    # 30 jobs for tenant "a" ahead of a single "b" job. With per-key limit 1
    # and demand 2, an over-fetch heuristic would drown "b" in "a" candidates;
    # exact ranking must claim one of each.
    fill!(name, :tenants, 30, fn _ -> %{"tenant" => "a"} end)
    fill!(name, :tenants, 1, fn _ -> %{"tenant" => "b"} end)

    claimed = claim!(name, :tenants, 2)

    tenants = claimed |> Enum.map(& &1.input["tenant"]) |> Enum.sort()

    assert tenants == ["a", "b"]
  end

  test "partitions cap per key without touching the overflow", %{name: name} do
    [a1, a2, _b1] =
      fill!(name, :tenants, 3, fn i -> %{"tenant" => if(i == 3, do: "b", else: "a")} end)

    claimed = claim!(name, :tenants, 10)

    tenants = claimed |> Enum.map(& &1.input["tenant"]) |> Enum.sort()
    assert tenants == ["a", "b"]

    claimed_ids = Enum.map(claimed, & &1.id)
    assert a1.id in claimed_ids

    # The over-cap job was simply not claimed: no attempt burned, no churn.
    leftover = job!(name, a2.id)

    assert leftover.state == "ready"
    assert leftover.attempt == 0
  end
end
