defmodule Belay.SettlePropTest do
  # Seeded random-DAG invariants over the workflow settlement fixpoint.
  use ExUnit.Case, async: true

  alias Belay.Storage.Logic

  @seeds 300

  test "settlement invariants hold across #{@seeds} random DAGs" do
    for seed <- 1..@seeds do
      :rand.seed(:exsss, {seed, 7, 13})

      jobs = random_dag()
      {release_ids, cancel_ids} = Logic.settle(jobs)

      by_id = Map.new(jobs, &{&1.id, &1})
      released = MapSet.new(release_ids)
      cancelled = MapSet.new(cancel_ids)

      # Nothing is both released and cancelled.
      assert MapSet.disjoint?(released, cancelled), "seed #{seed}: overlap"

      # Only held jobs settle.
      for id <- release_ids ++ cancel_ids do
        assert by_id[id].state == "held", "seed #{seed}: settled a non-held job"
      end

      # Final states after applying the settlement.
      final =
        Map.new(jobs, fn job ->
          state =
            cond do
              job.id in released -> "ready"
              job.id in cancelled -> "cancelled"
              true -> job.state
            end

          {job.wf_name, state}
        end)

      for job <- jobs, job.id in released do
        assert Enum.all?(job.wf_deps, &dep_ok?(final[&1], job)),
               "seed #{seed}: released #{job.wf_name} with unsatisfied dep"
      end

      for job <- jobs, job.id in cancelled do
        assert Enum.any?(job.wf_deps, &dep_doomed?(final[&1], job)),
               "seed #{seed}: cancelled #{job.wf_name} without a doomed dep"
      end

      # Fixpoint: settling the settled workflow is a no-op.
      settled_jobs =
        Enum.map(jobs, fn job ->
          cond do
            job.id in released -> %{job | state: "ready"}
            job.id in cancelled -> %{job | state: "cancelled"}
            true -> job
          end
        end)

      {re_release, _re_cancel} = Logic.settle(settled_jobs)

      re_release_new = Enum.reject(re_release, &(&1 in release_ids))
      assert re_release_new == [], "seed #{seed}: settlement not a fixpoint"
    end
  end

  defp dep_ok?(state, job) do
    state == "succeeded" or
      (state == "cancelled" and "cancelled" in job.wf_ignore) or
      (state == "failed" and "failed" in job.wf_ignore)
  end

  defp dep_doomed?(state, job) do
    (state == "cancelled" and "cancelled" not in job.wf_ignore) or
      (state == "failed" and "failed" not in job.wf_ignore)
  end

  defp random_dag do
    n = 3 + :rand.uniform(9)

    for i <- 1..n do
      deps =
        if i == 1 do
          []
        else
          count = :rand.uniform(min(i - 1, 3)) - 1
          Enum.take_random(1..(i - 1), count) |> Enum.map(&"j#{&1}")
        end

      state =
        if deps == [] do
          Enum.random(~w(succeeded failed cancelled ready running))
        else
          "held"
        end

      ignore =
        case :rand.uniform(5) do
          1 -> ["failed"]
          2 -> ["cancelled"]
          3 -> ["failed", "cancelled"]
          _ -> []
        end

      %Belay.Job{
        id: i,
        wf_name: "j#{i}",
        state: state,
        wf_deps: deps,
        wf_ignore: ignore,
        workflow_id: "wf"
      }
    end
  end
end
