defmodule Capstan.Storage.Memory do
  @moduledoc """
  In-memory storage: a single serialized GenServer, deterministic given call
  order. The reference implementation for engine semantics and the substrate
  for simulation tests. Not durable — for tests and ephemeral dev only.

  Every operation mirrors `Capstan.Storage.Postgres` exactly; the shared test
  suite runs against both to keep them honest.
  """

  @behaviour Capstan.Storage

  use GenServer

  alias Capstan.Job
  alias Capstan.Storage.Logic

  @terminal ~w(succeeded failed cancelled)
  @incomplete ~w(ready running awaiting held paused)

  defstruct seq: 0,
            jobs: %{},
            steps: %{},
            step_seqs: %{},
            events: %{},
            event_seqs: %{},
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

  # -- Storage API (all serialized through the server) --------------------------

  @impl Capstan.Storage
  def insert_jobs(ref, rows, now), do: call(ref, {:insert_jobs, rows, now})

  @impl Capstan.Storage
  def claim(ref, spec, demand, node_id, lease_ttl, now) do
    call(ref, {:claim, spec, demand, node_id, lease_ttl, now})
  end

  @impl Capstan.Storage
  def renew_leases(ref, ids, node_id, until), do: call(ref, {:renew_leases, ids, node_id, until})

  @impl Capstan.Storage
  def reclaim_expired(ref, now, backoff_fun), do: call(ref, {:reclaim_expired, now, backoff_fun})

  @impl Capstan.Storage
  def ack(ref, job, outcome, now), do: call(ref, {:ack, job, outcome, now})

  @impl Capstan.Storage
  def get_job(ref, id), do: call(ref, {:get_job, id})

  @impl Capstan.Storage
  def get_step(ref, job_id, name), do: call(ref, {:get_step, job_id, name})

  @impl Capstan.Storage
  def put_step(ref, job_id, name, bin, cost, now) do
    call(ref, {:put_step, job_id, name, bin, cost, now})
  end

  @impl Capstan.Storage
  def list_steps(ref, job_id), do: call(ref, {:list_steps, job_id})

  @impl Capstan.Storage
  def get_signal(ref, scopes, name), do: call(ref, {:get_signal, scopes, name})

  @impl Capstan.Storage
  def put_signal(ref, scope, name, payload, now) do
    call(ref, {:put_signal, scope, name, payload, now})
  end

  @impl Capstan.Storage
  def clear_signal(ref, scope, name), do: call(ref, {:clear_signal, scope, name})

  @impl Capstan.Storage
  def request_cancel(ref, id, now), do: call(ref, {:request_cancel, id, now})

  @impl Capstan.Storage
  def workflow_jobs(ref, workflow_id), do: call(ref, {:workflow_jobs, workflow_id})

  @impl Capstan.Storage
  def get_by_unique_key(ref, key), do: call(ref, {:get_by_unique_key, key})

  @impl Capstan.Storage
  def children(ref, parent_id), do: call(ref, {:children, parent_id})

  @impl Capstan.Storage
  def append_event(ref, job_id, payload, now), do: call(ref, {:append_event, job_id, payload, now})

  @impl Capstan.Storage
  def list_events(ref, job_id, after_seq), do: call(ref, {:list_events, job_id, after_seq})

  @impl Capstan.Storage
  def queue_stats(ref), do: call(ref, :queue_stats)

  @impl Capstan.Storage
  def list_jobs(ref, filters), do: call(ref, {:list_jobs, filters})

  @impl Capstan.Storage
  def retry(ref, id, now), do: call(ref, {:retry, id, now})

  @impl Capstan.Storage
  def prune_jobs(ref, state, now, keep, limit), do: call(ref, {:prune_jobs, state, now, keep, limit})

  @impl Capstan.Storage
  def prune_signals(ref, now, ttl), do: call(ref, {:prune_signals, now, ttl})

  @impl Capstan.Storage
  def debit_rate(ref, bucket, period, amount, now) do
    call(ref, {:debit_rate, bucket, period, amount, now})
  end

  @impl Capstan.Storage
  def prune_rate(ref, before_unix), do: call(ref, {:prune_rate, before_unix})

  defp call(ref, request), do: GenServer.call(ref, request)

  # -- Server -------------------------------------------------------------------

  @impl GenServer
  def handle_call({:insert_jobs, rows, _now}, _from, state) do
    {jobs, state} =
      Enum.reduce(rows, {[], state}, fn row, {acc, state} ->
        cond do
          duplicate_cron?(state, row) ->
            {acc, state}

          duplicate_unique?(state, row) ->
            {acc, state}

          true ->
            id = state.seq + 1
            job = struct(Job, Map.put(row, :id, id))

            state = %{
              state
              | seq: id,
                jobs: Map.put(state.jobs, id, job),
                cron_slots:
                  if(row[:cron_name],
                    do: MapSet.put(state.cron_slots, {row[:cron_name], row[:cron_slot]}),
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
    state = debit_claim(state, spec, now_unix, length(claimed))

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
          {state, _released, _cancelled} = settle_and_put(state, updated, now)
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
          %{payload: payload} -> {:ok, payload}
        end
      end)

    {:reply, found || :none, state}
  end

  def handle_call({:put_signal, scope, name, payload, now}, _from, state) do
    {state, woken} = do_put_signal(state, scope, name, payload, now)

    {:reply, {:ok, woken}, state}
  end

  def handle_call({:clear_signal, scope, name}, _from, state) do
    {:reply, :ok, %{state | signals: Map.delete(state.signals, {scope, name})}}
  end

  def handle_call({:request_cancel, id, now}, _from, state) do
    case state.jobs[id] do
      nil ->
        {:reply, {:ok, %{status: :noop, cancelled: [], released: []}}, state}

      %Job{state: s} when s in @terminal ->
        {:reply, {:ok, %{status: :noop, cancelled: [], released: []}}, state}

      %Job{state: "running"} = job ->
        state = put_jobs(state, [%{job | cancel_requested: true}])
        {:reply, {:ok, %{status: :requested, cancelled: [], released: []}}, state}

      job ->
        updated = Logic.apply_outcome(job, {:cancelled, %{"reason" => "cancel"}}, now)
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

  def handle_call({:get_by_unique_key, key}, _from, state) do
    found =
      state.jobs
      |> Map.values()
      |> Enum.filter(&(&1.unique_key == key))
      |> Enum.max_by(& &1.id, fn -> nil end)

    {:reply, if(found, do: {:ok, found}, else: :error), state}
  end

  def handle_call({:children, parent_id}, _from, state) do
    children =
      state.jobs
      |> Map.values()
      |> Enum.filter(&(&1.parent_id == parent_id))
      |> Enum.sort_by(& &1.id)

    {:reply, {:ok, children}, state}
  end

  def handle_call({:append_event, job_id, payload, now}, _from, state) do
    seq = Map.get(state.event_seqs, job_id, 0) + 1
    event = %{seq: seq, payload: payload, inserted_at: now}

    state = %{
      state
      | events: Map.update(state.events, job_id, [event], &[event | &1]),
        event_seqs: Map.put(state.event_seqs, job_id, seq)
    }

    {:reply, {:ok, seq}, state}
  end

  def handle_call({:list_events, job_id, after_seq}, _from, state) do
    events =
      state.events
      |> Map.get(job_id, [])
      |> Enum.filter(&(&1.seq > after_seq))
      |> Enum.sort_by(& &1.seq)

    {:reply, {:ok, events}, state}
  end

  def handle_call(:queue_stats, _from, state) do
    stats =
      state.jobs
      |> Map.values()
      |> Enum.frequencies_by(&{&1.queue, &1.state})
      |> Enum.map(fn {{queue, job_state}, count} ->
        %{queue: queue, state: job_state, count: count}
      end)
      |> Enum.sort_by(&{&1.queue, &1.state})

    {:reply, {:ok, stats}, state}
  end

  def handle_call({:list_jobs, filters}, _from, state) do
    limit = Map.get(filters, :limit, 50)

    jobs =
      state.jobs
      |> Map.values()
      |> Enum.filter(&matches_filters?(&1, filters))
      |> Enum.sort_by(& &1.id, :desc)
      |> Enum.take(limit)

    {:reply, {:ok, jobs}, state}
  end

  def handle_call({:retry, id, now}, _from, state) do
    case state.jobs[id] do
      nil ->
        {:reply, {:error, :not_found}, state}

      %Job{state: s} = job when s in ~w(failed cancelled) ->
        updated = %{
          job
          | state: "ready",
            ready_at: now,
            max_attempts: max(job.max_attempts, job.attempt + 1),
            lease_until: nil,
            leased_by: nil,
            finished_at: nil
        }

        {:reply, {:ok, updated}, put_jobs(state, [updated])}

      _ ->
        {:reply, {:error, :not_retryable}, state}
    end
  end

  def handle_call({:prune_jobs, prune_state, now, keep, limit}, _from, state) do
    cutoff = DateTime.add(now, -keep, :second)

    doomed_ids =
      state.jobs
      |> Map.values()
      |> Enum.filter(fn job ->
        job.state == prune_state and job.finished_at != nil and
          DateTime.compare(job.finished_at, cutoff) == :lt
      end)
      |> Enum.take(limit)
      |> MapSet.new(& &1.id)

    state = %{
      state
      | jobs: Map.drop(state.jobs, MapSet.to_list(doomed_ids)),
        steps: Map.reject(state.steps, fn {{id, _}, _} -> MapSet.member?(doomed_ids, id) end),
        events: Map.drop(state.events, MapSet.to_list(doomed_ids))
    }

    {:reply, {:ok, MapSet.size(doomed_ids)}, state}
  end

  def handle_call({:prune_signals, now, ttl}, _from, state) do
    cutoff = DateTime.add(now, -ttl, :second)

    signals =
      Map.reject(state.signals, fn {_key, %{inserted_at: at}} ->
        DateTime.compare(at, cutoff) == :lt
      end)

    {:reply, :ok, %{state | signals: signals}}
  end

  def handle_call({:debit_rate, bucket, period, amount, now}, _from, state) do
    win = Logic.window_start(DateTime.to_unix(now), period)

    {:reply, :ok, %{state | rate: Map.update(state.rate, {bucket, win}, amount, &(&1 + amount))}}
  end

  def handle_call({:prune_rate, before_unix}, _from, state) do
    rate = for {{_b, win} = k, v} <- state.rate, win >= before_unix, into: %{}, do: {k, v}

    {:reply, :ok, %{state | rate: rate}}
  end

  # -- Internals ----------------------------------------------------------------

  defp put_jobs(state, jobs) do
    %{state | jobs: Enum.reduce(jobs, state.jobs, &Map.put(&2, &1.id, &1))}
  end

  # After a terminal transition: settle the workflow, then notify parents of
  # finished children — all inside this single serialized call.
  defp settle_and_put(state, %Job{} = job, now) do
    state = put_jobs(state, [job])

    {state, released, cancelled} =
      if job.state in @terminal and job.workflow_id do
        wf_jobs =
          state.jobs
          |> Map.values()
          |> Enum.filter(&(&1.workflow_id == job.workflow_id))

        {release_ids, cancel_ids} = Logic.settle(wf_jobs)

        released = for id <- release_ids, do: %{state.jobs[id] | state: "ready", ready_at: now}

        cancelled =
          for id <- cancel_ids do
            %{state.jobs[id] | state: "cancelled", finished_at: now}
          end

        {put_jobs(state, released ++ cancelled), released, cancelled}
      else
        {state, [], []}
      end

    terminal_now = if job.state in @terminal, do: [job | cancelled], else: cancelled

    {state, parent_woken} =
      Enum.reduce(terminal_now, {state, []}, fn finished, {state, woken} ->
        {state, newly} = wake_parent(state, finished, now)

        {state, woken ++ newly}
      end)

    {state, released ++ parent_woken, cancelled}
  end

  defp wake_parent(state, %Job{parent_id: nil}, _now), do: {state, []}

  defp wake_parent(state, %Job{parent_id: parent_id}, now) do
    siblings = state.jobs |> Map.values() |> Enum.filter(&(&1.parent_id == parent_id))

    if Enum.all?(siblings, &(&1.state in @terminal)) do
      do_put_signal(state, "job:#{parent_id}", "$children", %{"count" => length(siblings)}, now)
    else
      {state, []}
    end
  end

  defp do_put_signal(state, scope, name, payload, now) do
    state = %{
      state
      | signals: Map.put(state.signals, {scope, name}, %{payload: payload, inserted_at: now})
    }

    woken =
      state.jobs
      |> Map.values()
      |> Enum.filter(
        &(&1.state == "awaiting" and &1.await_scope == scope and &1.await_name == name)
      )
      |> Enum.map(fn job ->
        %{job | state: "ready", ready_at: now, await_scope: nil, await_name: nil}
      end)

    {put_jobs(state, woken), woken}
  end

  defp duplicate_cron?(_state, %{cron_name: nil}), do: false
  defp duplicate_cron?(state, row), do: MapSet.member?(state.cron_slots, {row.cron_name, row.cron_slot})

  defp duplicate_unique?(_state, %{unique_key: nil}), do: false

  defp duplicate_unique?(state, %{unique_key: key, unique_mode: mode}) do
    Enum.any?(Map.values(state.jobs), fn job ->
      job.unique_key == key and job.unique_mode == mode and
        (mode == "window" or job.state in @incomplete)
    end)
  end

  defp matches_filters?(job, filters) do
    Enum.all?(filters, fn
      {:queue, queue} -> job.queue == to_string(queue)
      {:state, state} -> job.state == to_string(state)
      {:worker, worker} -> job.kind == worker |> to_string() |> String.replace_prefix("Elixir.", "")
      {:workflow_id, id} -> job.workflow_id == id
      {:parent_id, id} -> job.parent_id == id
      {:before_id, id} -> job.id < id
      {:limit, _} -> true
    end)
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
    bucket = Logic.rate_bucket(rate, spec.queue)
    win = Logic.window_start(now_unix, rate.period)
    prev = Map.get(state.rate, {bucket, win - rate.period}, 0)
    curr = Map.get(state.rate, {bucket, win}, 0)

    allowance = Logic.rate_allowance(prev, curr, rate.allowed, rate.period, now_unix)

    min(demand, div(allowance, max(rate.estimate, 1)))
  end

  defp debit_claim(state, %{rate: nil}, _now_unix, _count), do: state
  defp debit_claim(state, _spec, _now_unix, 0), do: state

  defp debit_claim(state, %{rate: rate} = spec, now_unix, count) do
    bucket = Logic.rate_bucket(rate, spec.queue)
    win = Logic.window_start(now_unix, rate.period)
    amount = count * max(rate.estimate, 1)

    %{state | rate: Map.update(state.rate, {bucket, win}, amount, &(&1 + amount))}
  end

  defp sort_dt(nil), do: ~U[1970-01-01 00:00:00Z]
  defp sort_dt(%DateTime{} = dt), do: dt
end
