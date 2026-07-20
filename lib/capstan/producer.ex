defmodule Capstan.Producer do
  @moduledoc false

  # One per queue per node: claims jobs up to local demand on a jittered poll,
  # on pokes (local inserts/releases + cluster broadcasts), and on completions.

  use GenServer

  alias Capstan.{Config, Runner}

  def start_link({config, queue}) do
    GenServer.start_link(__MODULE__, {config, to_string(queue)},
      name: {:via, Registry, {Capstan.registry(config.name), {:producer, to_string(queue)}}}
    )
  end

  def child_spec({config, queue}) do
    %{id: {:producer, queue}, start: {__MODULE__, :start_link, [{config, queue}]}}
  end

  @impl GenServer
  def init({config, queue}) do
    spec = Config.queue_spec(config, queue)

    :pg.join(Capstan.pg_scope(config.name), {:producers, queue}, self())

    state = %{config: config, queue: queue, spec: spec, running: %{}}

    {:ok, schedule_poll(state), {:continue, :claim}}
  end

  @impl GenServer
  def handle_continue(:claim, state), do: {:noreply, claim(state)}

  @impl GenServer
  def handle_info(:poll, state) do
    {:noreply, state |> claim() |> schedule_poll()}
  end

  def handle_info(:poke, state), do: {:noreply, claim(state)}

  # A job task finished; free the slot and immediately look for more work.
  def handle_info({ref, _result}, state) when is_reference(ref) do
    Process.demonitor(ref, [:flush])

    {:noreply, claim(%{state | running: Map.delete(state.running, ref)})}
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    {:noreply, claim(%{state | running: Map.delete(state.running, ref)})}
  end

  defp claim(%{config: config, spec: spec} = state) do
    demand = spec.local_limit - map_size(state.running)

    if demand <= 0 do
      state
    else
      {storage, ref} = config.storage_ref

      {:ok, jobs} =
        storage.claim(ref, spec, demand, config.node_id, config.lease_ttl, Config.now(config))

      running =
        Enum.reduce(jobs, state.running, fn job, acc ->
          task =
            Task.Supervisor.async_nolink(Capstan.task_sup(config.name), Runner, :execute, [
              config,
              job
            ])

          Map.put(acc, task.ref, job.id)
        end)

      %{state | running: running}
    end
  end

  defp schedule_poll(%{config: config} = state) do
    jitter = :rand.uniform(div(config.poll_interval, 4) + 1)

    Process.send_after(self(), :poll, config.poll_interval + jitter)

    state
  end
end

defmodule Capstan.LeaseKeeper do
  @moduledoc false

  # Renews every local running lease in one storage call per tick. Jobs whose
  # lease could not be renewed (reclaimed or cancelled elsewhere) get their
  # executor killed — the rest of the cluster owns them now.

  use GenServer

  require Logger

  alias Capstan.Config

  def start_link(config), do: GenServer.start_link(__MODULE__, config)

  @impl GenServer
  def init(config) do
    {:ok, schedule(config), {:continue, :noop}}
  end

  @impl GenServer
  def handle_continue(:noop, config), do: {:noreply, config}

  @impl GenServer
  def handle_info(:renew, config) do
    entries = Registry.lookup(Capstan.run_registry(config.name), :running)

    unless entries == [] do
      ids = Enum.map(entries, fn {_pid, id} -> id end)
      {storage, ref} = config.storage_ref
      until = DateTime.add(Config.now(config), config.lease_ttl, :millisecond)

      {:ok, renewed} = storage.renew_leases(ref, ids, config.node_id, until)

      lost = ids -- renewed

      for {pid, id} <- entries, id in lost do
        Logger.warning("[capstan] lost lease for job #{id}; killing local executor")

        Process.exit(pid, :kill)
      end
    end

    {:noreply, schedule(config)}
  end

  defp schedule(config) do
    Process.send_after(self(), :renew, max(div(config.lease_ttl, 3), 250))

    config
  end
end

defmodule Capstan.Sweeper do
  @moduledoc false

  # Leaderless maintenance: reclaim expired leases (retry or fail by
  # attempts) and prune old rate windows. Idempotent; safe on every node.

  use GenServer

  alias Capstan.{Config, Runner}

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

    # Failed reclaims can cascade-release workflow jobs, so poke on both.
    unless retried == [] and failed == [] do
      for {queue, _spec} <- config.queues, do: Capstan.poke(config, queue)
    end

    storage.prune_rate(ref, DateTime.to_unix(now) - 86_400)

    {:noreply, schedule(config)}
  end

  defp schedule(config) do
    Process.send_after(self(), :sweep, config.sweep_interval)

    config
  end
end

defmodule Capstan.CronScheduler do
  @moduledoc false

  # Leaderless cron: every node computes the current minute slot and inserts;
  # the unique (cron_name, cron_slot) constraint dedupes cluster-wide.

  use GenServer

  alias Capstan.{Config, CronExpr, Job}

  def start_link(config), do: GenServer.start_link(__MODULE__, config)

  @impl GenServer
  def init(config) do
    if config.crons == [] do
      :ignore
    else
      {:ok, schedule(config)}
    end
  end

  @impl GenServer
  def handle_info(:tick, config) do
    now = Config.now(config)
    slot = CronExpr.slot(now)
    {storage, ref} = config.storage_ref

    rows =
      for cron <- config.crons, CronExpr.matches?(cron.expr, slot) do
        opts =
          cron.opts
          |> Keyword.merge(now: now, cron_name: cron.name, cron_slot: slot)

        Job.new(cron.worker, cron.input, opts, cron.worker.__capstan_defaults__())
      end

    unless rows == [] do
      {:ok, inserted} = storage.insert_jobs(ref, rows, now)

      inserted |> Enum.map(& &1.queue) |> Enum.uniq() |> Enum.each(&Capstan.poke(config, &1))
    end

    {:noreply, schedule(config)}
  end

  defp schedule(config) do
    Process.send_after(self(), :tick, config.cron_interval)

    config
  end
end
