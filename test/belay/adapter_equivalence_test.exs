# The semantic-parity property: random command sequences applied to the
# Memory and Postgres adapters in lockstep must produce identical observable
# state after EVERY command. This mechanically enforces CONTRIBUTING rule 1
# ("storage semantics live once") — any drift between adapters fails with
# the seed and the exact diverging step.
#
# Runs only on the Postgres leg (it needs both adapters).
if Application.get_env(:belay, :test_storage) == :postgres do
  defmodule Belay.AdapterEquivalenceTest do
    use ExUnit.Case, async: false

    alias Belay.{Clock, Job}
    alias Belay.Test.Echo

    @seeds 25
    @commands_per_seed 30
    @start ~U[2026-01-05 00:00:00.000000Z]

    test "memory and postgres agree on every observable transition (#{@seeds} seeds)" do
      for seed <- 1..@seeds do
        :rand.seed(:exsss, {seed, 11, 23})

        spec = random_spec(seed)
        sides = [start_side(:memory, seed, spec), start_side(:postgres, seed, spec)]

        state = %{running: [], known_ids: [], wf_counter: 0}

        Enum.reduce(1..@commands_per_seed, state, fn step, state ->
          command = random_command(state)

          state = apply_both!(sides, command, state)

          dumps = Enum.map(sides, &dump(&1))

          assert Enum.uniq(dumps) |> length() == 1,
                 """
                 adapters diverged (seed #{seed}, step #{step}, command #{inspect(command)}):

                 memory:   #{inspect(Enum.at(dumps, 0), pretty: true, limit: :infinity)}
                 postgres: #{inspect(Enum.at(dumps, 1), pretty: true, limit: :infinity)}
                 """

          state
        end)

        Enum.each(sides, &stop_side/1)
      end
    end

    # -- Command generation (deterministic under the seed) ----------------------

    defp random_spec(seed) do
      base = %{
        queue: "eq",
        local_limit: 10,
        global_limit: nil,
        rate: nil,
        partition: nil,
        manual: true
      }

      case rem(seed, 4) do
        0 -> base
        1 -> %{base | global_limit: 2}
        2 -> %{base | rate: %{allowed: 5, period: 60, resource: nil, estimate: 1}}
        3 -> %{base | global_limit: 1, partition: {:input, "tenant"}}
      end
    end

    defp random_command(state) do
      choices =
        [
          {6, :insert},
          {2, :insert_unique},
          {2, :insert_workflow},
          {6, :claim},
          {3, :signal},
          {2, :cancel_known},
          {2, :retry_known},
          {2, :advance_clock},
          {2, :reclaim}
        ] ++ if(state.running == [], do: [], else: [{8, :ack}])

      total = Enum.sum(Enum.map(choices, &elem(&1, 0)))
      pick = :rand.uniform(total)

      {_, kind} =
        Enum.reduce_while(choices, {0, nil}, fn {weight, kind}, {acc, _} ->
          if pick <= acc + weight, do: {:halt, {acc, kind}}, else: {:cont, {acc + weight, kind}}
        end)

      materialize(kind, state)
    end

    defp materialize(:insert, _state) do
      {:insert, %{"tenant" => "t#{:rand.uniform(2)}"}, priority: :rand.uniform(3) - 1}
    end

    defp materialize(:insert_unique, _state) do
      {:insert, %{"tenant" => "t1"}, unique: "eq-key-#{:rand.uniform(3)}"}
    end

    defp materialize(:insert_workflow, state) do
      ignore = if :rand.uniform(3) == 1, do: ["failed"], else: []

      {:insert_workflow, "wf-#{state.wf_counter}", ignore}
    end

    defp materialize(:claim, _state), do: {:claim, :rand.uniform(3)}

    defp materialize(:ack, state) do
      outcome =
        case :rand.uniform(6) do
          1 -> {:succeeded, Belay.Codec.encode(%{ok: true})}
          2 -> :retry
          3 -> {:failed, %{"error" => "boom"}}
          4 -> {:cancelled, %{"reason" => "because"}}
          5 -> :snooze
          6 -> :await
        end

      {:ack, Enum.random(state.running), outcome}
    end

    defp materialize(:signal, _state), do: {:signal, "eq-scope", "go"}

    defp materialize(:cancel_known, state) do
      case state.known_ids do
        [] -> {:noop}
        ids -> {:cancel, Enum.random(ids)}
      end
    end

    defp materialize(:retry_known, state) do
      case state.known_ids do
        [] -> {:noop}
        ids -> {:retry, Enum.random(ids)}
      end
    end

    defp materialize(:advance_clock, _state), do: {:advance_clock, Enum.random([3, 30, 65])}
    defp materialize(:reclaim, _state), do: {:reclaim}

    # -- Applying commands to both sides ----------------------------------------

    defp apply_both!(sides, command, state) do
      results = Enum.map(sides, &apply_command(&1, command))

      case command do
        {:insert, _, _} ->
          # Unique-key dedup may skip the row — both sides must skip alike.
          id_lists = Enum.map(results, fn jobs -> Enum.map(jobs, & &1.id) end)
          assert Enum.uniq(id_lists) |> length() == 1, "insert diverged: #{inspect(id_lists)}"
          %{state | known_ids: hd(id_lists) ++ state.known_ids}

        {:insert_workflow, _, _} ->
          ids = results |> Enum.map(fn jobs -> Enum.map(jobs, & &1.id) end) |> Enum.uniq()
          assert length(ids) == 1
          %{state | known_ids: hd(ids) ++ state.known_ids, wf_counter: state.wf_counter + 1}

        {:claim, _} ->
          id_lists = results |> Enum.map(fn jobs -> Enum.map(jobs, &{&1.id, &1.attempt}) end)

          assert Enum.uniq(id_lists) |> length() == 1,
                 "claims diverged: #{inspect(id_lists)}"

          %{
            state
            | running: Enum.map(hd(id_lists), fn {id, att} -> {id, att} end) ++ state.running
          }

        {:ack, claimed, _} ->
          %{state | running: List.delete(state.running, claimed)}

        {:reclaim} ->
          # Reclaimed jobs are no longer ours to ack.
          reclaimed = hd(results)
          %{state | running: Enum.reject(state.running, fn {id, _} -> id in reclaimed end)}

        _ ->
          state
      end
    end

    defp apply_command(side, {:insert, input, opts}) do
      row =
        Job.new(Echo, input, Keyword.merge(opts, now: now(side), queue: "eq"), queue: "eq")

      {:ok, jobs} = side.mod.insert_jobs(side.ref, [row], now(side))

      jobs
    end

    defp apply_command(side, {:insert_workflow, wf_id, ignore}) do
      rows =
        for {name, deps} <- [{"a", []}, {"b", ["a"]}, {"c", ["b"]}] do
          Job.new(
            Echo,
            %{"wf" => name},
            [
              now: now(side),
              queue: "eq",
              workflow_id: wf_id,
              wf_name: name,
              wf_deps: deps,
              wf_ignore: ignore,
              state: if(deps == [], do: "ready", else: "held")
            ],
            queue: "eq"
          )
        end

      {:ok, jobs} = side.mod.insert_jobs(side.ref, rows, now(side))

      jobs
    end

    defp apply_command(side, {:claim, demand}) do
      {:ok, jobs} = side.mod.claim(side.ref, side.spec, demand, "eq-node", 10_000, now(side))

      jobs
    end

    defp apply_command(side, {:ack, {id, attempt}, outcome}) do
      {:ok, job} = side.mod.get_job(side.ref, id)
      job = %{job | attempt: attempt}

      outcome =
        case outcome do
          :retry -> {:retry, %{"error" => "retry"}, DateTime.add(now(side), 5, :second)}
          :snooze -> {:snooze, DateTime.add(now(side), 10, :second)}
          :await -> {:await, "job:#{id}", "go", nil}
          other -> other
        end

      # Stale acks (job reclaimed or state moved) must be rejected identically.
      case side.mod.ack(side.ref, job, outcome, now(side)) do
        {:ok, _} -> :ok
        {:error, :stale} -> :stale
      end
    end

    defp apply_command(side, {:signal, scope, name}) do
      {:ok, _woken} = side.mod.put_signal(side.ref, scope, name, %{"n" => 1}, now(side))

      :ok
    end

    defp apply_command(side, {:cancel, id}) do
      {:ok, _} = side.mod.request_cancel(side.ref, id, now(side))

      :ok
    end

    defp apply_command(side, {:retry, id}) do
      case side.mod.retry(side.ref, id, now(side)) do
        {:ok, _} -> :ok
        {:error, reason} -> reason
      end
    end

    defp apply_command(side, {:advance_clock, seconds}) do
      Clock.Sim.advance(side.clock, seconds)

      :ok
    end

    defp apply_command(side, {:reclaim}) do
      backoff = fn _job -> DateTime.add(now(side), 7, :second) end

      {:ok, %{retried: retried, failed: failed}} =
        side.mod.reclaim_expired(side.ref, now(side), backoff)

      Enum.sort(retried ++ failed)
    end

    defp apply_command(_side, {:noop}), do: :ok

    # -- Normalized observable state --------------------------------------------

    defp dump(side) do
      {:ok, jobs} = side.mod.list_jobs(side.ref, %{limit: 1_000})

      jobs
      |> Enum.sort_by(& &1.id)
      |> Enum.map(fn job ->
        %{
          id: job.id,
          state: job.state,
          attempt: job.attempt,
          ready_at: job.ready_at && DateTime.to_unix(job.ready_at, :microsecond),
          await: {job.await_scope, job.await_name},
          unique: {job.unique_key, job.unique_mode},
          wf: {job.workflow_id, job.wf_name},
          errors: length(job.errors || []),
          result?: job.result != nil,
          cancel_requested: job.cancel_requested
        }
      end)
    end

    # -- Side management ---------------------------------------------------------

    defp start_side(:memory, seed, spec) do
      {:ok, clock} = Clock.Sim.start_link(@start)
      name = Module.concat(EqMem, "S#{seed}")
      ref = Belay.Storage.Memory.ref(name)
      {:ok, pid} = Belay.Storage.Memory.start_link(ref)

      %{mod: Belay.Storage.Memory, ref: ref, clock: clock, spec: spec, pid: pid, kind: :memory}
    end

    defp start_side(:postgres, _seed, spec) do
      {:ok, clock} = Clock.Sim.start_link(@start)
      url = Application.fetch_env!(:belay, :test_pg_url)

      opts =
        url
        |> Belay.Storage.Postgres.parse_url()
        |> Keyword.merge(pool_size: 3, types: Belay.Storage.PostgresTypes)

      {:ok, pid} = Postgrex.start_link(opts)

      Belay.Storage.Postgres.truncate!(pid)

      %{
        mod: Belay.Storage.Postgres,
        ref: pid,
        clock: clock,
        spec: spec,
        pid: pid,
        kind: :postgres
      }
    end

    defp stop_side(side) do
      case side.kind do
        :memory -> GenServer.stop(side.pid)
        :postgres -> GenServer.stop(side.pid)
      end
    end

    defp now(side), do: Clock.Sim.now(side.clock)
  end
end
