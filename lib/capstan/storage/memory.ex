defmodule Capstan.Storage.Memory do
  @moduledoc """
  In-memory storage: a single serialized GenServer, deterministic given call
  order. The reference implementation for engine semantics and the substrate
  for simulation tests. Not durable — for tests and ephemeral dev only.
  """

  @behaviour Capstan.Storage

  use GenServer

  alias Capstan.Job
  alias Capstan.Storage.Logic

  defstruct seq: 0,
            jobs: %{},
            steps: %{},
            step_seqs: %{},
            signals: %{},
            rate: %{},
            cron_slots: MapSet.new()

  # -- Setup --------------------------------------------------------------------

  @impl Capstan.Storage
  def child_spec({config, _opts}) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [ref(config.name)]}}
  end

  def start_link(name), do: GenServer.start_link(__MODULE__, :ok, name: name)

  def ref(instance_name), do: Module.concat(instance_name, "Storage")

  @impl GenServer
  def init(:ok), do: {:ok, %__MODULE__{}}

  # -- Storage API --------------------------------------------------------------

  @impl Capstan.Storage
  def insert_jobs(ref, rows, now), do: GenServer.call(ref, {:insert_jobs, rows, now})

  @impl Capstan.Storage
  def claim(ref, spec, demand, node_id, lease_ttl, now) do
    GenServer.call(ref, {:claim, spec, demand, node_id, lease_ttl, now})
  end

  @impl Capstan.Storage
  def renew_leases(ref, ids, node_id, until) do
    GenServer.call(ref, {:renew_leases, ids, node_id, until})
  end

  @impl Capstan.Storage
  def reclaim_expired(ref, now, backoff_fun) do
    GenServer.call(ref, {:reclaim_expired, now, backoff_fun})
  end

  @impl Capstan.Storage
  def ack(ref, job, outcome, now), do: GenServer.call(ref, {:ack, job, outcome, now})

  @impl Capstan.Storage
  def get_job(ref, id), do: GenServer.call(ref, {:get_job, id})

  @impl Capstan.Storage
  def get_step(ref, job_id, name), do: GenServer.call(ref, {:get_step, job_id, name})

  @impl Capstan.Storage
  def put_step(ref, job_id, name, bin, cost, now) do
    GenServer.call(ref, {:put_step, job_id, name, bin, cost, now})
  end

  @impl Capstan.Storage
  def list_steps(ref, job_id), do: GenServer.call(ref, {:list_steps, job_id})

  @impl Capstan.Storage
  def get_signal(ref, scopes, name), do: GenServer.call(ref, {:get_signal, scopes, name})

  @impl Capstan.Storage
  def put_signal(ref, scope, name, payload, now) do
    GenServer.call(ref, {:put_signal, scope, name, payload, now})
  end

  @impl Capstan.Storage
  def clear_signal(ref, scope, name), do: GenServer.call(ref, {:clear_signal, scope, name})

  @impl Capstan.Storage
  def request_cancel(ref, id, now), do: GenServer.call(ref, {:request_cancel, id, now})

  @impl Capstan.Storage
  def workflow_jobs(ref, workflow_id), do: GenServer.call(ref, {:workflow_jobs, workflow_id})

  @impl Capstan.Storage
  def prune_rate(ref, before_unix), do: GenServer.call(ref, {:prune_rate, before_unix})

  # -- Server -------------------------------------------------------------------

  @impl GenServer
  def handle_call({:insert_jobs, rows, _now}, _from, state) do
    {jobs, state} =
      Enum.reduce(rows, {[], state}, fn row, {acc, state} ->
        cron_key = {row[:cron_name], row[:cron_slot]}

        if row[:cron_name] && MapSet.member?(state.cron_slots, cron_key) do
          {acc, state}
        else
          id = state.seq + 1
          job = struct(Job, Map.put(row, :id, id))

          state = %{
            state
            | seq: id,
              jobs: Map.put(state.jobs, id, job),
              cron_slots:
                if(row[:cron_name],
                  do: MapSet.put(state.cron_slots, cron_key),
                  else: state.cron_slots
                )
          }

          {[job | acc], state}
        end
      end)

    {:reply, {:ok, Enum.reverse(jobs)}, state}
  end

  def handle_call({:claim, spec, demand, node_id, lease_ttl, now}, _from, state) do
    candidates =
      state.jobs
      |> Map.values()
      |> Enum.filter(&(&1.queue == spec.queue and Logic.claimable?(&1, now)))
      |> Enum.sort_by(&{&1.priority, sort_dt(&1.ready_at), &1.id})

    now_unix = DateTime.to_unix(now)

    take =
      demand
      |> clamp_global(state, spec, now)
      |> clamp_rate(state, spec, now_unix)

    picked =
      case spec.partition do
        nil ->
          Enum.take(candidates, take)

        partition ->
          per_key = spec.global_limit || spec.local_limit

          running_counts =
            state.jobs
            |> Map.values()
            |> Enum.filter(&(&1.queue == spec.queue and Logic.live_running?(&1, now)))
            |> Enum.frequencies_by(&Logic.partition_key(&1, partition))

          Logic.partition_take(candidates, take, running_counts, per_key, fn job ->
            Logic.partition_key(job, partition)
          end)
      end

    lease_until = DateTime.add(now, lease_ttl, :millisecond)

    claimed =
      Enum.map(picked, fn job ->
        %{
          job
          | state: "running",
            attempt: job.attempt + 1,
            lease_until: lease_until,
            leased_by: node_id,
            started_at: job.started_at || now
        }
      end)

    state = put_jobs(state, claimed)
    state = debit_rate(state, spec, now_unix, length(claimed))

    {:reply, {:ok, claimed}, state}
  end

  def handle_call({:renew_leases, ids, node_id, until}, _from, state) do
    {renewed, state} =
      Enum.reduce(ids, {[], state}, fn id, {acc, state} ->
        case state.jobs[id] do
          %Job{state: "running", leased_by: ^node_id} = job ->
            {[id | acc], put_jobs(state, [%{job | lease_until: until}])}

          _ ->
            {acc, state}
        end
      end)

    {:reply, {:ok, Enum.reverse(renewed)}, state}
  end

  def handle_call({:reclaim_expired, now, backoff_fun}, _from, state) do
    expired =
      state.jobs
      |> Map.values()
      |> Enum.filter(fn job ->
        job.state == "running" and job.lease_until != nil and
          DateTime.compare(job.lease_until, now) != :gt
      end)

    error = %{"error" => "lease_expired", "at" => DateTime.to_iso8601(now)}

    {retried, failed, state} =
      Enum.reduce(expired, {[], [], state}, fn job, {retried, failed, state} ->
        if job.attempt >= job.max_attempts do
          updated = Logic.apply_outcome(job, {:failed, error}, now)
          state = settle_and_put(state, updated, now) |> elem(0)
          {retried, [job.id | failed], state}
        else
          updated = Logic.apply_outcome(job, {:retry, error, backoff_fun.(job)}, now)
          {[job.id | retried], failed, put_jobs(state, [updated])}
        end
      end)

    {:reply, {:ok, %{retried: retried, failed: failed}}, state}
  end

  def handle_call({:ack, job, outcome, now}, _from, state) do
    case state.jobs[job.id] do
      %Job{state: "running", attempt: attempt} = current when attempt == job.attempt ->
        updated = Logic.apply_outcome(%{current | cancel_requested: false}, outcome, now)

        # Close the await race: a signal delivered while the job was still
        # running couldn't wake it; if it exists now, park-and-wake atomically.
        updated =
          with {:await, scope, name, _deadline} <- outcome,
               true <- Map.has_key?(state.signals, {scope, name}) do
            %{updated | state: "ready", ready_at: now, await_scope: nil, await_name: nil}
          else
            _ -> updated
          end

        {state, released, cancelled} = settle_and_put(state, updated, now)

        {:reply, {:ok, %{job: updated, released: released, cancelled: cancelled}}, state}

      _ ->
        {:reply, {:error, :stale}, state}
    end
  end

  def handle_call({:get_job, id}, _from, state) do
    case state.jobs[id] do
      nil -> {:reply, :error, state}
      job -> {:reply, {:ok, job}, state}
    end
  end

  def handle_call({:get_step, job_id, name}, _from, state) do
    case state.steps[{job_id, name}] do
      nil -> {:reply, :none, state}
      step -> {:reply, {:ok, step.value}, state}
    end
  end

  def handle_call({:put_step, job_id, name, bin, cost, now}, _from, state) do
    seq = Map.get(state.step_seqs, job_id, 0) + 1

    step = %{
      seq: seq,
      name: name,
      value: bin,
      usd_micros: cost[:usd_micros] || 0,
      tokens: cost[:tokens] || 0,
      inserted_at: now
    }

    job = state.jobs[job_id]

    job = %{
      job
      | spent_usd_micros: (job.spent_usd_micros || 0) + step.usd_micros,
        spent_tokens: (job.spent_tokens || 0) + step.tokens
    }

    state = %{
      state
      | steps: Map.put(state.steps, {job_id, name}, step),
        step_seqs: Map.put(state.step_seqs, job_id, seq),
        jobs: Map.put(state.jobs, job_id, job)
    }

    {:reply, {:ok, %{spent_usd_micros: job.spent_usd_micros, spent_tokens: job.spent_tokens}},
     state}
  end

  def handle_call({:list_steps, job_id}, _from, state) do
    steps =
      state.steps
      |> Enum.filter(fn {{id, _name}, _step} -> id == job_id end)
      |> Enum.map(fn {{_id, _name}, step} -> step end)
      |> Enum.sort_by(& &1.seq)

    {:reply, {:ok, steps}, state}
  end

  def handle_call({:get_signal, scopes, name}, _from, state) do
    found =
      Enum.find_value(scopes, fn scope ->
        case state.signals[{scope, name}] do
          nil -> nil
          payload -> {:ok, payload}
        end
      end)

    {:reply, found || :none, state}
  end

  def handle_call({:put_signal, scope, name, payload, now}, _from, state) do
    state = %{state | signals: Map.put(state.signals, {scope, name}, payload)}

    woken =
      state.jobs
      |> Map.values()
      |> Enum.filter(&(&1.state == "awaiting" and &1.await_scope == scope and &1.await_name == name))
      |> Enum.map(fn job ->
        %{job | state: "ready", ready_at: now, await_scope: nil, await_name: nil}
      end)

    {:reply, {:ok, woken}, put_jobs(state, woken)}
  end

  def handle_call({:clear_signal, scope, name}, _from, state) do
    {:reply, :ok, %{state | signals: Map.delete(state.signals, {scope, name})}}
  end

  def handle_call({:request_cancel, id, now}, _from, state) do
    case state.jobs[id] do
      nil ->
        {:reply, {:ok, %{status: :noop, cancelled: [], released: []}}, state}

      %Job{state: s} when s in ["succeeded", "failed", "cancelled"] ->
        {:reply, {:ok, %{status: :noop, cancelled: [], released: []}}, state}

      %Job{state: "running"} = job ->
        state = put_jobs(state, [%{job | cancel_requested: true}])
        {:reply, {:ok, %{status: :requested, cancelled: [], released: []}}, state}

      job ->
        updated = %{job | state: "cancelled", finished_at: now}
        {state, released, cancelled} = settle_and_put(state, updated, now)
        {:reply, {:ok, %{status: :cancelled, cancelled: cancelled, released: released}}, state}
    end
  end

  def handle_call({:workflow_jobs, workflow_id}, _from, state) do
    jobs =
      state.jobs
      |> Map.values()
      |> Enum.filter(&(&1.workflow_id == workflow_id))
      |> Enum.sort_by(& &1.id)

    {:reply, {:ok, jobs}, state}
  end

  def handle_call({:prune_rate, before_unix}, _from, state) do
    rate = for {{_q, win} = k, v} <- state.rate, win >= before_unix, into: %{}, do: {k, v}

    {:reply, :ok, %{state | rate: rate}}
  end

  # -- Internals ----------------------------------------------------------------

  defp put_jobs(state, jobs) do
    %{state | jobs: Enum.reduce(jobs, state.jobs, &Map.put(&2, &1.id, &1))}
  end

  # After a terminal transition, settle the workflow inside the same call.
  defp settle_and_put(state, %Job{} = job, now) do
    state = put_jobs(state, [job])

    if job.state in ["succeeded", "failed", "cancelled"] and job.workflow_id do
      wf_jobs =
        state.jobs
        |> Map.values()
        |> Enum.filter(&(&1.workflow_id == job.workflow_id))

      {release_ids, cancel_ids} = Logic.settle(wf_jobs)

      released =
        for id <- release_ids do
          %{state.jobs[id] | state: "ready", ready_at: now}
        end

      cancelled =
        for id <- cancel_ids do
          %{state.jobs[id] | state: "cancelled", finished_at: now}
        end

      {put_jobs(state, released ++ cancelled), released, cancelled}
    else
      {state, [], []}
    end
  end

  defp clamp_global(demand, _state, %{global_limit: nil}, _now), do: demand

  defp clamp_global(demand, state, %{partition: nil, global_limit: limit} = spec, now) do
    live =
      state.jobs
      |> Map.values()
      |> Enum.count(&(&1.queue == spec.queue and Logic.live_running?(&1, now)))

    min(demand, max(limit - live, 0))
  end

  # With a partition, the global limit applies per key inside partition_take.
  defp clamp_global(demand, _state, _spec, _now), do: demand

  defp clamp_rate(demand, _state, %{rate: nil}, _now_unix), do: demand

  defp clamp_rate(demand, state, %{rate: rate} = spec, now_unix) do
    win = Logic.window_start(now_unix, rate.period)
    prev = Map.get(state.rate, {spec.queue, win - rate.period}, 0)
    curr = Map.get(state.rate, {spec.queue, win}, 0)

    min(demand, Logic.rate_allowance(prev, curr, rate.allowed, rate.period, now_unix))
  end

  defp debit_rate(state, %{rate: nil}, _now_unix, _count), do: state
  defp debit_rate(state, _spec, _now_unix, 0), do: state

  defp debit_rate(state, %{rate: rate} = spec, now_unix, count) do
    win = Logic.window_start(now_unix, rate.period)

    %{state | rate: Map.update(state.rate, {spec.queue, win}, count, &(&1 + count))}
  end

  defp sort_dt(nil), do: ~U[1970-01-01 00:00:00Z]
  defp sort_dt(%DateTime{} = dt), do: dt
end
