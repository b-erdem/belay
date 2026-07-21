defmodule Belay.LogicTest do
  # Direct unit + seeded property coverage of the pure shared-logic layer —
  # the functions both storage adapters delegate their semantics to.
  use ExUnit.Case, async: true

  alias Belay.Job
  alias Belay.Storage.Logic

  describe "rate_allowance/5" do
    test "full previous window weighs by overlap" do
      # period 60, now 30s into the window: previous counts 50%.
      assert Logic.rate_allowance(10, 0, 10, 60, 1_000_020 + 90) == 5
    end

    test "seeded properties: bounded, monotone in usage" do
      for seed <- 1..200 do
        :rand.seed(:exsss, {seed, 3, 5})

        allowed = :rand.uniform(1_000)
        period = :rand.uniform(600)
        prev = :rand.uniform(2_000) - 1
        curr = :rand.uniform(2_000) - 1
        now = 1_700_000_000 + :rand.uniform(10_000)

        allowance = Logic.rate_allowance(prev, curr, allowed, period, now)

        # Never negative; never exceeds the configured budget (credits from
        # true-ups can drive counts negative, but allowance stays capped).
        assert allowance >= 0
        assert allowance <= allowed

        # More current-window usage can never increase the allowance.
        assert Logic.rate_allowance(prev, curr + 1, allowed, period, now) <= allowance
      end
    end
  end

  describe "partition_take/5" do
    test "respects per-key allowances and the global take, in order" do
      jobs = for i <- 1..10, do: %Job{id: i, input: %{"k" => "#{rem(i, 2)}"}}
      key_fun = &Logic.partition_key(&1, {:input, "k"})

      # Key "1" already has 1 running; per-key limit 2 → one more "1", two "0"s.
      picked = Logic.partition_take(jobs, 3, %{"1" => 1}, 2, key_fun)

      assert Enum.map(picked, & &1.id) == [1, 2, 4]
    end

    test "missing keys coalesce to one bucket" do
      jobs = [%Job{id: 1, input: %{}}, %Job{id: 2, input: %{}}]

      assert [%Job{id: 1}] =
               Logic.partition_take(jobs, 5, %{}, 1, &Logic.partition_key(&1, {:input, "k"}))
    end
  end

  describe "claimable?/2" do
    @now ~U[2026-01-05 00:00:00Z]

    test "ready is gated by ready_at; awaiting by its deadline" do
      assert Logic.claimable?(%Job{state: "ready", ready_at: nil}, @now)
      assert Logic.claimable?(%Job{state: "ready", ready_at: @now}, @now)
      refute Logic.claimable?(%Job{state: "ready", ready_at: DateTime.add(@now, 1)}, @now)

      refute Logic.claimable?(%Job{state: "awaiting", ready_at: nil}, @now)
      assert Logic.claimable?(%Job{state: "awaiting", ready_at: @now}, @now)

      for state <- ~w(running held paused succeeded failed cancelled) do
        refute Logic.claimable?(%Job{state: state, ready_at: @now}, @now)
      end
    end
  end
end
