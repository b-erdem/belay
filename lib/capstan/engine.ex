defmodule Capstan.Engine do
  @moduledoc """
  An `Oban.Engine` that adds cluster-wide coordination on top of the stock
  engines, while keeping every byte of state in ordinary tables you can query.

      config :my_app, Oban,
        engine: Capstan.Engine,
        queues: [
          mailers: 20,
          ai: [limit: 10, global_limit: 4, rate_limit: [allowed: 60, period: 60]],
          tenants: [limit: 10, global_limit: 2, partition: {:args, "tenant_id"}]
        ]

  Queue options beyond the stock `:limit`:

    * `:global_limit` — cap concurrently executing jobs across all nodes.
      Derived by counting `executing` rows, so it is transparent, has no
      drift, and self-heals with the Lifeline plugin after crashes.
    * `:rate_limit` — `[allowed: n, period: seconds]` sliding-window admission
      across all nodes, tracked in `capstan_rate`.
    * `:partition` — `{:args | :meta, "key"}`; applies `:global_limit`
      per distinct key value (per-tenant fairness).

  The engine also drives Capstan's composition features by intercepting job
  transitions: workflow dependency release, batch callbacks, chain FIFO
  advancement, and relay responses all happen here — no polling plugins.

  Delegates to `Oban.Engines.Basic` (Postgres) or `Oban.Engines.Lite`
  (SQLite3) based on the repo adapter.
  """

  @behaviour Oban.Engine

  import Ecto.Query

  alias Capstan.{Chain, Lifecycle, RateLimiter, Steps}
  alias Ecto.Changeset
  alias Oban.{Config, Engine, Job, Repo}

  @capstan_keys [:global_limit, :rate_limit, :partition]

  # -- Setup --------------------------------------------------------------------

  @impl Engine
  def init(%Config{} = conf, opts) do
    {capstan_opts, base_opts} = Keyword.split(opts, @capstan_keys)

    with {:ok, capstan} <- validate_opts(capstan_opts),
         {:ok, meta} <- base(conf).init(conf, base_opts) do
      {:ok, Map.put(meta, :capstan, capstan)}
    end
  end

  @impl Engine
  def put_meta(conf, meta, key, value), do: base(conf).put_meta(conf, meta, key, value)

  @impl Engine
  def check_meta(conf, meta, running), do: base(conf).check_meta(conf, meta, running)

  @impl Engine
  def refresh(conf, meta), do: base(conf).refresh(conf, meta)

  @impl Engine
  def shutdown(conf, meta), do: base(conf).shutdown(conf, meta)

  # -- Insertion ----------------------------------------------------------------

  @impl Engine
  def insert_job(%Config{} = conf, %Changeset{} = changeset, opts) do
    base(conf).insert_job(conf, Chain.maybe_hold(conf, changeset), opts)
  end

  @impl Engine
  def insert_all_jobs(%Config{} = conf, changesets, opts) do
    changesets = Enum.map(changesets, &Chain.maybe_hold(conf, &1))

    base(conf).insert_all_jobs(conf, changesets, opts)
  end

  # -- Fetching under limits ----------------------------------------------------

  @impl Engine
  def fetch_jobs(%Config{} = conf, %{paused: true} = meta, running) do
    base(conf).fetch_jobs(conf, meta, running)
  end

  def fetch_jobs(%Config{} = conf, meta, running) do
    capstan = Map.get(meta, :capstan, %{})

    if map_size(capstan) == 0 do
      base(conf).fetch_jobs(conf, meta, running)
    else
      fetch_limited(conf, meta, running, capstan)
    end
  end

  defp fetch_limited(conf, meta, running, capstan) do
    local_demand = meta.limit - map_size(running)

    if local_demand <= 0 do
      {:ok, {meta, []}}
    else
      Repo.transaction(conf, fn ->
        acquire_queue_lock(conf, meta.queue)

        allowed =
          local_demand
          |> clamp_global(conf, meta, capstan)
          |> clamp_rate(conf, meta, capstan)

        # Snapshot per-key load before fetching: the fetch itself marks jobs
        # executing, which must not count against their own partition.
        pre_counts = partition_precounts(conf, meta, capstan)

        if allowed <= 0 do
          {meta, []}
        else
          {:ok, {inner_meta, jobs}} =
            base(conf).fetch_jobs(
              conf,
              Map.put(meta, :limit, map_size(running) + allowed),
              running
            )

          jobs = enforce_partition(conf, meta, capstan, jobs, pre_counts)

          if capstan[:rate_limit] do
            RateLimiter.debit(conf, to_string(meta.queue), capstan.rate_limit, length(jobs))
          end

          {Map.put(inner_meta, :limit, meta.limit), jobs}
        end
      end)
    end
  end

  defp clamp_global(demand, conf, meta, %{global_limit: limit} = capstan)
       when is_integer(limit) do
    # With a partition, the limit applies per key and is enforced post-fetch.
    if capstan[:partition] do
      demand
    else
      min(demand, max(limit - executing_count(conf, meta.queue), 0))
    end
  end

  defp clamp_global(demand, _conf, _meta, _capstan), do: demand

  defp clamp_rate(demand, conf, meta, %{rate_limit: limit}) when is_map(limit) do
    min(demand, RateLimiter.allowance(conf, to_string(meta.queue), limit))
  end

  defp clamp_rate(demand, _conf, _meta, _capstan), do: demand

  defp executing_count(conf, queue) do
    query =
      Job
      |> where([j], j.queue == ^to_string(queue) and j.state == "executing")
      |> select([j], count(j.id))

    Repo.one(conf, query) || 0
  end

  defp partition_precounts(conf, meta, %{partition: {source, key}, global_limit: limit})
       when is_integer(limit) do
    partition_counts(conf, to_string(meta.queue), source, key)
  end

  defp partition_precounts(_conf, _meta, _capstan), do: nil

  # Per-key enforcement: fetched jobs beyond a key's allowance are returned to
  # `available` with their attempt restored, in fetch order.
  defp enforce_partition(conf, _meta, %{partition: {source, key}, global_limit: limit}, jobs, executing)
       when is_integer(limit) and is_map(executing) do
    {kept, reverted, _counts} =
      Enum.reduce(jobs, {[], [], executing}, fn job, {kept, reverted, counts} ->
        value = partition_value(job, source, key)

        if Map.get(counts, value, 0) < limit do
          {[job | kept], reverted, Map.update(counts, value, 1, &(&1 + 1))}
        else
          {kept, [job | reverted], counts}
        end
      end)

    if reverted != [] do
      ids = Enum.map(reverted, & &1.id)

      Repo.update_all(
        conf,
        where(Job, [j], j.id in ^ids),
        set: [state: "available"],
        inc: [attempt: -1]
      )
    end

    Enum.reverse(kept)
  end

  defp enforce_partition(_conf, _meta, _capstan, jobs, _pre_counts), do: jobs

  defp partition_value(job, source, key) do
    container =
      case source do
        :args -> job.args
        :meta -> job.meta
      end

    container |> Map.get(key) |> to_string()
  end

  defp partition_counts(conf, queue, source, key) do
    field = to_string(source)

    sql =
      case Capstan.Query.dialect(conf) do
        :pg ->
          "SELECT #{field}->>$1, count(*) FROM oban_jobs " <>
            "WHERE queue = $2 AND state = 'executing' GROUP BY 1"

        :sqlite ->
          "SELECT json_extract(#{field}, '$.' || ?1), count(*) FROM oban_jobs " <>
            "WHERE queue = ?2 AND state = 'executing' GROUP BY 1"
      end

    case Repo.query(conf, sql, [key, queue]) do
      {:ok, %{rows: rows}} -> Map.new(rows, fn [value, count] -> {to_string(value), count} end)
      _ -> %{}
    end
  end

  # Serialize concurrent fetches for one queue so global math is race-free.
  defp acquire_queue_lock(conf, queue) do
    if Capstan.Query.dialect(conf) == :pg do
      key = :erlang.phash2({:capstan, conf.prefix, to_string(queue)})

      Repo.query(conf, "SELECT pg_advisory_xact_lock($1)", [key])
    end

    :ok
  end

  # -- Staging, pruning, rescue, inspection: pure delegation --------------------

  @impl Engine
  def stage_jobs(conf, queryable, opts), do: base(conf).stage_jobs(conf, queryable, opts)

  @impl Engine
  def prune_jobs(conf, queryable, opts), do: base(conf).prune_jobs(conf, queryable, opts)

  @impl Engine
  def rescue_jobs(conf, queryable, opts), do: base(conf).rescue_jobs(conf, queryable, opts)

  @impl Engine
  def check_available(conf), do: base(conf).check_available(conf)

  @impl Engine
  def update_job(conf, job, changes), do: base(conf).update_job(conf, job, changes)

  # -- Transitions with lifecycle interception ----------------------------------

  @impl Engine
  def complete_job(conf, job) do
    :ok = base(conf).complete_job(conf, job)

    Lifecycle.transitioned(conf, job, :completed)

    :ok
  end

  @impl Engine
  def discard_job(conf, job) do
    :ok = base(conf).discard_job(conf, job)

    Lifecycle.transitioned(conf, job, :discarded)

    :ok
  end

  @impl Engine
  def cancel_job(conf, job) do
    :ok = base(conf).cancel_job(conf, job)

    Lifecycle.transitioned(conf, job, :cancelled)

    :ok
  end

  @impl Engine
  def error_job(conf, job, seconds), do: base(conf).error_job(conf, job, seconds)

  @impl Engine
  def snooze_job(conf, job, seconds) do
    :ok = base(conf).snooze_job(conf, job, seconds)

    # Close the await race: a signal delivered while the job was still
    # executing could not flip it to available; re-check now that it's parked.
    with %{"awaiting_scope" => scope, "awaiting_name" => name} <- job.meta,
         {:ok, _} <- Steps.lookup_signal(conf, [scope], name) do
      Steps.wake_awaiting(conf, scope, name)
    end

    :ok
  end

  # -- Bulk operations: delegation (lifecycle interception is per-job only) -----

  @impl Engine
  def cancel_all_jobs(conf, queryable), do: base(conf).cancel_all_jobs(conf, queryable)

  @impl Engine
  def retry_job(conf, job), do: base(conf).retry_job(conf, job)

  @impl Engine
  def retry_all_jobs(conf, queryable), do: base(conf).retry_all_jobs(conf, queryable)

  @impl Engine
  def delete_job(conf, job), do: base(conf).delete_job(conf, job)

  @impl Engine
  def delete_all_jobs(conf, queryable), do: base(conf).delete_all_jobs(conf, queryable)

  # -- Helpers ------------------------------------------------------------------

  defp base(%Config{repo: repo}) do
    if repo.__adapter__() == Ecto.Adapters.SQLite3 do
      Oban.Engines.Lite
    else
      Oban.Engines.Basic
    end
  end

  defp validate_opts(opts) do
    Enum.reduce_while(opts, {:ok, Map.new(opts)}, fn
      {:global_limit, limit}, acc when is_integer(limit) and limit > 0 ->
        {:cont, acc}

      {:rate_limit, rate}, {:ok, capstan} when is_list(rate) ->
        allowed = Keyword.get(rate, :allowed)
        period = Keyword.get(rate, :period)

        if is_integer(allowed) and allowed > 0 and is_integer(period) and period > 0 do
          {:cont, {:ok, Map.put(capstan, :rate_limit, Map.new(rate))}}
        else
          {:halt, error("expected rate_limit [allowed: pos, period: seconds], got: #{inspect(rate)}")}
        end

      {:partition, {source, key}}, acc when source in [:args, :meta] and is_binary(key) ->
        {:cont, acc}

      {key, value}, _acc ->
        {:halt, error("invalid #{key} option: #{inspect(value)}")}
    end)
  end

  defp error(message), do: {:error, ArgumentError.exception(message)}
end
