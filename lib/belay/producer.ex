defmodule Belay.Producer do
  @moduledoc false

  # One per queue per node: claims jobs up to local demand on a jittered poll,
  # on pokes (local inserts/releases + cluster broadcasts), and on completions.
  #
  # Two adaptive behaviours, both node-local and leaderless:
  #
  #   * Chunk gathering — jobs whose worker declares `chunk:` are buffered
  #     per worker until `size` is reached or `gather_ms` elapses, then run
  #     as ONE `run_chunk/1` invocation. Buffered jobs are already claimed
  #     and leased, so a crash mid-gather reclaims them like any crash; the
  #     gather window is clamped to half the lease TTL so a healthy buffer
  #     can never outlive its lease.
  #
  #   * Adaptive concurrency — `limit: [min: a, max: b]` scales this node's
  #     limit up while claim rounds come back full and decays it when the
  #     queue idles, mirroring the burst-poll cadence. `global_limit`, rate
  #     limits, and partition fairness still bound the fleet exactly.

  use GenServer

  require Logger

  alias Belay.{Config, Job, Runner}

  def start_link({config, queue, spec}) do
    GenServer.start_link(__MODULE__, {config, to_string(queue), spec},
      name: {:via, Registry, {Belay.registry(config.name), {:producer, to_string(queue)}}}
    )
  end

  def child_spec({config, queue, spec}) do
    %{id: {:producer, queue}, start: {__MODULE__, :start_link, [{config, queue, spec}]}}
  end

  @impl GenServer
  def init({config, queue, spec}) do
    :pg.join(Belay.pg_scope(config.name), {:producers, queue}, self())

    state = %{
      config: config,
      queue: queue,
      spec: spec,
      # ref => slot count (1 per plain job, chunk length per chunk task).
      running: %{},
      # kind => %{jobs: [...], opts: %{size, gather_ms}, timer: ref | nil}
      buffer: %{},
      paused: false,
      # Adaptive cadence: busy_poll while work is flowing, decaying to
      # poll_interval when idle — burst latency without idle DB load.
      interval: config.busy_poll,
      # Adaptive concurrency: nil for static limits, else scales between
      # limit_min and local_limit (the max).
      cur_limit: spec.limit_min || spec.local_limit
    }

    {:ok, schedule_poll(state), {:continue, :claim}}
  end

  @impl GenServer
  def handle_continue(:claim, state), do: {:noreply, claim(state)}

  @impl GenServer
  def handle_call(:belay_pause, _from, state), do: {:reply, :ok, %{state | paused: true}}

  def handle_call(:belay_resume, _from, state) do
    {:reply, :ok, claim(%{state | paused: false})}
  end

  @impl GenServer
  def handle_info(:poll, state) do
    {:noreply, state |> claim() |> schedule_poll()}
  end

  def handle_info(:poke, state) do
    # Coalesce poke storms (one wake per burst, not one claim per insert).
    flush_pokes()

    {:noreply, claim(%{state | interval: state.config.busy_poll})}
  end

  # A job task finished; free the slot and immediately look for more work.
  def handle_info({ref, _result}, state) when is_reference(ref) do
    Process.demonitor(ref, [:flush])

    {:noreply, claim(%{state | running: Map.delete(state.running, ref)})}
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    {:noreply, claim(%{state | running: Map.delete(state.running, ref)})}
  end

  # Gather deadline for a chunked worker: dispatch whatever is buffered.
  # Fires even while paused — buffered jobs hold live leases and must run.
  def handle_info({:flush_chunk, kind}, state) do
    {:noreply, flush_buffer(state, kind)}
  end

  defp claim(%{paused: true} = state), do: state

  # A storage failure (database restart, network blip) skips this round and
  # logs — the next poll retries. Claiming must never crash-loop the producer.
  defp claim(%{config: config, spec: spec} = state) do
    demand = state.cur_limit - slots_used(state)

    if demand <= 0 do
      state
    else
      {storage, ref} = config.storage_ref

      {:ok, jobs} =
        storage.claim(ref, spec, demand, config.node_id, config.lease_ttl, Config.now(config))

      {chunked, plain} = Enum.split_with(jobs, &chunk_opts(&1))

      state = Enum.reduce(plain, state, &start_plain(&2, &1))
      state = Enum.reduce(Enum.group_by(chunked, & &1.kind), state, &buffer_chunk(&2, &1))

      adapt(state, length(jobs), demand)
    end
  rescue
    error ->
      Logger.warning(
        "[belay] queue #{state.queue} claim skipped (storage unavailable?): " <>
          Exception.message(error)
      )

      adapt(state, 0, 0)
  end

  defp start_plain(state, job) do
    task =
      Task.Supervisor.async_nolink(Belay.task_sup(state.config.name), Runner, :execute, [
        state.config,
        job
      ])

    %{state | running: Map.put(state.running, task.ref, 1)}
  end

  # -- Chunk gathering ----------------------------------------------------------

  defp buffer_chunk(state, {kind, jobs}) do
    opts = chunk_opts(hd(jobs))

    entry =
      state.buffer
      |> Map.get(kind, %{jobs: [], opts: opts, timer: nil})
      |> Map.update!(:jobs, &(&1 ++ jobs))

    state = put_in(state.buffer[kind], entry)
    state = dispatch_full_chunks(state, kind)

    case state.buffer[kind] do
      nil ->
        state

      %{jobs: []} ->
        state

      %{opts: %{gather_ms: 0}} ->
        # No gathering: run each claim round's remainder as its own chunk.
        flush_buffer(state, kind)

      %{timer: nil} = entry ->
        # Clamped so a healthy buffer can never outlive its claim lease.
        gather = min(entry.opts.gather_ms, div(state.config.lease_ttl, 2))
        timer = Process.send_after(self(), {:flush_chunk, kind}, gather)

        put_in(state.buffer[kind].timer, timer)

      _ ->
        state
    end
  end

  defp dispatch_full_chunks(state, kind) do
    case state.buffer[kind] do
      %{jobs: jobs, opts: %{size: size}} = entry when length(jobs) >= size ->
        {chunk, rest} = Enum.split(jobs, size)

        state
        |> start_chunk(chunk)
        |> put_in([:buffer, kind], %{entry | jobs: rest})
        |> dispatch_full_chunks(kind)

      _ ->
        state
    end
  end

  defp flush_buffer(state, kind) do
    case state.buffer[kind] do
      nil ->
        state

      %{jobs: [], timer: timer} ->
        cancel_timer(timer)

        %{state | buffer: Map.delete(state.buffer, kind)}

      %{jobs: jobs, timer: timer} ->
        cancel_timer(timer)

        state
        |> start_chunk(jobs)
        |> Map.update!(:buffer, &Map.delete(&1, kind))
    end
  end

  defp start_chunk(state, jobs) do
    task =
      Task.Supervisor.async_nolink(Belay.task_sup(state.config.name), Runner, :execute_chunk, [
        state.config,
        jobs
      ])

    %{state | running: Map.put(state.running, task.ref, length(jobs))}
  end

  defp cancel_timer(nil), do: :ok
  defp cancel_timer(ref), do: Process.cancel_timer(ref)

  defp slots_used(state) do
    running = state.running |> Map.values() |> Enum.sum()
    buffered = state.buffer |> Map.values() |> Enum.map(&length(&1.jobs)) |> Enum.sum()

    running + buffered
  end

  defp chunk_opts(%Job{} = job) do
    case Job.worker_module!(job).__belay_defaults__()[:chunk] do
      nil ->
        nil

      opts ->
        %{size: Keyword.get(opts, :size, 10), gather_ms: Keyword.get(opts, :gather_ms, 0)}
    end
  rescue
    # Unknown/invalid worker modules take the plain path, where the runner
    # produces the proper per-job failure.
    _ -> nil
  end

  # -- Adaptive cadence and concurrency -----------------------------------------

  defp adapt(%{config: config, spec: spec} = state, claimed, demand) do
    interval =
      if claimed > 0 do
        config.busy_poll
      else
        min(state.interval * 2, config.poll_interval)
      end

    cur =
      cond do
        spec.limit_min == nil ->
          state.cur_limit

        # Saturated round: everything we asked for was there. Scale up.
        claimed > 0 and claimed >= demand and state.cur_limit < spec.local_limit ->
          scale(state, min(spec.local_limit, state.cur_limit * 2))

        # Fully idle: decay toward the floor.
        claimed == 0 and slots_used(state) == 0 and state.cur_limit > spec.limit_min ->
          scale(state, max(spec.limit_min, div(state.cur_limit, 2)))

        true ->
          state.cur_limit
      end

    %{state | interval: interval, cur_limit: cur}
  end

  defp scale(state, new_limit) do
    :telemetry.execute([:belay, :queue, :scale], %{limit: new_limit}, %{
      name: state.config.name,
      queue: state.queue,
      from: state.cur_limit
    })

    new_limit
  end

  defp flush_pokes do
    receive do
      :poke -> flush_pokes()
    after
      0 -> :ok
    end
  end

  defp schedule_poll(%{interval: interval} = state) do
    jitter = :rand.uniform(div(interval, 4) + 1)

    Process.send_after(self(), :poll, interval + jitter)

    state
  end
end

defmodule Belay.LeaseKeeper do
  @moduledoc false

  # Renews every local running lease in one storage call per tick. Jobs whose
  # lease could not be renewed (reclaimed or cancelled elsewhere) get their
  # executor killed — the rest of the cluster owns them now.

  use GenServer

  require Logger

  alias Belay.Config

  def start_link(config), do: GenServer.start_link(__MODULE__, config)

  @impl GenServer
  def init(config) do
    {:ok, schedule(config), {:continue, :noop}}
  end

  @impl GenServer
  def handle_continue(:noop, config), do: {:noreply, config}

  @impl GenServer
  def handle_info(:renew, config) do
    entries = Registry.lookup(Belay.run_registry(config.name), :running)

    unless entries == [] do
      ids = Enum.map(entries, fn {_pid, id} -> id end)
      {storage, ref} = config.storage_ref
      until = DateTime.add(Config.now(config), config.lease_ttl, :millisecond)

      {:ok, renewed} = storage.renew_leases(ref, ids, config.node_id, until)

      lost = ids -- renewed

      for {pid, id} <- entries, id in lost do
        Logger.warning("[belay] lost lease for job #{id}; killing local executor")

        Process.exit(pid, :kill)
      end
    end

    {:noreply, schedule(config)}
  rescue
    # When storage is unreachable we can't tell "lost lease" from "outage";
    # keep local work running (at-least-once already covers the double-run
    # risk) and try again next tick.
    error ->
      Logger.warning("[belay] lease renewal skipped: #{Exception.message(error)}")

      {:noreply, schedule(config)}
  end

  defp schedule(config) do
    Process.send_after(self(), :renew, max(div(config.lease_ttl, 3), 250))

    config
  end
end

defmodule Belay.Sweeper do
  @moduledoc false

  # Leaderless maintenance: reclaim expired leases (retry or fail by
  # attempts) and prune old rate windows. Idempotent; safe on every node.

  use GenServer

  alias Belay.{Config, Runner}

  def start_link(config), do: GenServer.start_link(__MODULE__, config)

  @impl GenServer
  def init(config) do
    {:ok, schedule(config)}
  end

  @impl GenServer
  def handle_info(:sweep, config) do
    now = Config.now(config)
    {storage, ref} = config.storage_ref

    backoff_fun = fn job -> DateTime.add(now, Runner.backoff(job), :second) end

    {:ok, %{retried: retried, failed: failed}} = storage.reclaim_expired(ref, now, backoff_fun)
    sweep_rest(config, storage, ref, now, retried, failed)

    {:noreply, schedule(config)}
  rescue
    error ->
      require Logger

      Logger.warning("[belay] sweep skipped: #{Exception.message(error)}")

      {:noreply, schedule(config)}
  end

  defp sweep_rest(config, storage, ref, now, retried, failed) do
    # Backstop for any missed parent wake-up: a parent awaiting $children
    # whose children are all terminal becomes ready again. Idempotent and
    # leaderless — worst-case wake latency is one sweep interval.
    {:ok, resettled} = storage.resettle_parents(ref, now)

    for job <- resettled do
      require Logger

      Logger.warning("[belay] resettled parent #{job.id} (missed $children wake)")

      Belay.poke(config, job.queue)
    end

    # Failed reclaims can cascade-release workflow jobs, so poke on both.
    unless retried == [] and failed == [] do
      for {queue, _spec} <- config.queues, do: Belay.poke(config, queue)
    end

    # Retention: terminal jobs age out with their steps and events; signals
    # expire on their TTL; rate windows older than a day are dead weight.
    for {state, keep} <- config.retention, is_integer(keep) do
      storage.prune_jobs(ref, state, now, keep, 500)
    end

    storage.prune_signals(ref, now, config.signal_ttl)
    storage.prune_rate(ref, DateTime.to_unix(now) - 86_400)

    :ok
  end

  defp schedule(config) do
    Process.send_after(self(), :sweep, config.sweep_interval)

    config
  end
end

defmodule Belay.CronScheduler do
  @moduledoc false

  # Leaderless cron: every node computes the current minute slot and inserts;
  # the unique (cron_name, cron_slot) constraint dedupes cluster-wide.

  use GenServer

  alias Belay.{Config, CronExpr, Job}

  def start_link(config), do: GenServer.start_link(__MODULE__, config)

  @impl GenServer
  def init(config) do
    {:ok, schedule(config)}
  end

  @impl GenServer
  def handle_info(:tick, config) do
    now = Config.now(config)
    slot = CronExpr.slot(now)
    {storage, ref} = config.storage_ref

    rows =
      for cron <- Belay.Crons.schedule_entries(config), CronExpr.matches?(cron.expr, slot) do
        opts =
          cron.opts
          |> Keyword.merge(now: now, cron_name: cron.name, cron_slot: slot)
          |> Keyword.put(:encryption_key, Config.encryption_key(config))

        Job.new(cron.worker, cron.input, opts, cron.worker.__belay_defaults__())
      end

    unless rows == [] do
      {:ok, inserted} = storage.insert_jobs(ref, rows, now)

      inserted |> Enum.map(& &1.queue) |> Enum.uniq() |> Enum.each(&Belay.poke(config, &1))
    end

    {:noreply, schedule(config)}
  rescue
    error ->
      require Logger

      Logger.warning("[belay] cron tick skipped: #{Exception.message(error)}")

      {:noreply, schedule(config)}
  end

  defp schedule(config) do
    Process.send_after(self(), :tick, config.cron_interval)

    config
  end
end
