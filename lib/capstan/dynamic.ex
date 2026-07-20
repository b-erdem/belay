defmodule Capstan.Queues do
  @moduledoc """
  Runtime queue management, persisted in the database and applied by every
  node — no leader, no deploy.

      Capstan.Queues.put(MyApp.Capstan, :imports,
        limit: 10, global_limit: 4, rate: [allowed: 100, period: 60])

      Capstan.Queues.delete(MyApp.Capstan, :imports)
      Capstan.Queues.list(MyApp.Capstan)

  Each node's queue-sync process reconciles its local producers against the
  table every `dynamic_sync` interval (default 5s): new entries start
  producers, deletions stop them, changed options restart them. A dynamic
  entry with the same name as a static queue overrides it. Options are
  validated on `put/3`, so a bad entry never reaches producers.
  """

  alias Capstan.Config

  @doc "Create or update a dynamic queue. Options as in the static config."
  def put(name, queue, opts) when is_list(opts) do
    config = Config.fetch!(name)
    {storage, ref} = config.storage_ref

    # Validate eagerly — raises on nonsense before anything is stored.
    _spec = Config.normalize_queue_opts(to_string(queue), opts)

    :ok =
      storage.put_dynamic_queue(ref, to_string(queue), encode_opts(opts), Config.now(config))

    :ok
  end

  @doc "Delete a dynamic queue. Its node-local producers stop on the next sync."
  def delete(name, queue) do
    config = Config.fetch!(name)
    {storage, ref} = config.storage_ref

    storage.delete_dynamic_queue(ref, to_string(queue))
  end

  @doc "List dynamic queues as `{name, opts}` pairs."
  def list(name) do
    config = Config.fetch!(name)
    {storage, ref} = config.storage_ref
    {:ok, rows} = storage.list_dynamic_queues(ref)

    Enum.map(rows, fn {queue, opts} -> {queue, decode_opts(opts)} end)
  end

  @doc false
  # Resolve a queue spec: dynamic entries override static config.
  def resolve_spec!(%Config{} = config, queue) do
    queue = to_string(queue)

    case dynamic_spec(config, queue) do
      nil -> Config.queue_spec(config, queue)
      spec -> spec
    end
  end

  @doc false
  def dynamic_specs(%Config{} = config) do
    {storage, ref} = config.storage_ref
    {:ok, rows} = storage.list_dynamic_queues(ref)

    Map.new(rows, fn {queue, opts} ->
      {queue, Config.normalize_queue_opts(queue, decode_opts(opts))}
    end)
  end

  defp dynamic_spec(config, queue) do
    config |> dynamic_specs() |> Map.get(queue)
  end

  # Stored as JSON; keywords/tuples round-trip through a plain map encoding.
  @doc false
  def encode_opts(opts) do
    Map.new(opts, fn
      {:rate, rate} when is_list(rate) -> {"rate", Map.new(rate, fn {k, v} -> {to_string(k), v} end)}
      {:partition, {source, key}} -> {"partition", [to_string(source), key]}
      {key, value} -> {to_string(key), value}
    end)
  end

  @doc false
  def decode_opts(stored) do
    Enum.map(stored, fn
      {"rate", rate} -> {:rate, Enum.map(rate, fn {k, v} -> {String.to_existing_atom(k), v} end)}
      {"partition", [source, key]} -> {:partition, {String.to_existing_atom(source), key}}
      {key, value} -> {String.to_existing_atom(key), value}
    end)
  end
end

defmodule Capstan.Crons do
  @moduledoc """
  Runtime cron management, persisted in the database, fired leaderlessly by
  every node with per-slot dedup — change schedules without deploys.

      Capstan.Crons.put(MyApp.Capstan, "digest", "0 8 * * 1-5", MyApp.Digest,
        input: %{"edition" => "morning"})

      Capstan.Crons.pause(MyApp.Capstan, "digest")
      Capstan.Crons.resume(MyApp.Capstan, "digest")
      Capstan.Crons.delete(MyApp.Capstan, "digest")

  Dynamic entries are merged with the static `crons:` config on every
  scheduler tick; a dynamic entry with the same name overrides the static one.
  """

  alias Capstan.{Config, CronExpr}

  @doc "Create or update a cron entry. The expression is validated eagerly."
  def put(name, cron_name, expression, worker, opts \\ []) do
    config = Config.fetch!(name)
    {storage, ref} = config.storage_ref

    _validated = CronExpr.parse!(expression)

    entry = %{
      name: to_string(cron_name),
      expression: expression,
      worker: worker |> to_string() |> String.replace_prefix("Elixir.", ""),
      input: Keyword.get(opts, :input, %{}),
      opts: Capstan.Queues.encode_opts(Keyword.drop(opts, [:input]))
    }

    :ok = storage.put_dynamic_cron(ref, entry, Config.now(config))

    :ok
  end

  def delete(name, cron_name) do
    config = Config.fetch!(name)
    {storage, ref} = config.storage_ref

    storage.delete_dynamic_cron(ref, to_string(cron_name))
  end

  def pause(name, cron_name), do: set_paused(name, cron_name, true)
  def resume(name, cron_name), do: set_paused(name, cron_name, false)

  defp set_paused(name, cron_name, paused?) do
    config = Config.fetch!(name)
    {storage, ref} = config.storage_ref

    storage.set_cron_paused(ref, to_string(cron_name), paused?)
  end

  @doc "List dynamic cron entries."
  def list(name) do
    config = Config.fetch!(name)
    {storage, ref} = config.storage_ref
    {:ok, entries} = storage.list_dynamic_crons(ref)

    entries
  end

  @doc false
  # Entries for the scheduler: static config merged with (and overridden by)
  # dynamic rows; paused entries dropped; unparseable/unloadable rows skipped
  # with a warning rather than wedging the tick.
  def schedule_entries(%Config{} = config) do
    {storage, ref} = config.storage_ref
    {:ok, dynamic} = storage.list_dynamic_crons(ref)

    dynamic_entries =
      dynamic
      |> Enum.reject(& &1.paused)
      |> Enum.flat_map(fn entry ->
        with {:ok, expr} <- CronExpr.parse(entry.expression),
             {:ok, worker} <- resolve_worker(entry.worker) do
          [
            %{
              name: entry.name,
              expr: expr,
              worker: worker,
              input: entry.input || %{},
              opts: Capstan.Queues.decode_opts(entry.opts || %{})
            }
          ]
        else
          _ ->
            require Logger

            Logger.warning("[capstan] skipping invalid dynamic cron #{inspect(entry.name)}")

            []
        end
      end)

    dynamic_names = MapSet.new(dynamic_entries, & &1.name)

    Enum.reject(config.crons, &MapSet.member?(dynamic_names, &1.name)) ++ dynamic_entries
  end

  defp resolve_worker(kind) do
    module = String.to_existing_atom("Elixir." <> kind)

    if Code.ensure_loaded?(module), do: {:ok, module}, else: :error
  rescue
    ArgumentError -> :error
  end
end

defmodule Capstan.QueueSync do
  @moduledoc false

  # Per-node reconciler: keeps this node's producers matching static config +
  # dynamic queue rows. Leaderless — every node runs its own producers.

  use GenServer

  require Logger

  def start_link(config), do: GenServer.start_link(__MODULE__, config)

  @impl GenServer
  def init(config) do
    static =
      for {queue, spec} <- config.queues, not spec.manual, into: %{}, do: {queue, spec}

    state = %{config: config, desired_static: static, running: %{}}

    {:ok, schedule(sync(state))}
  end

  @impl GenServer
  def handle_info(:sync, state) do
    {:noreply, state |> sync() |> schedule()}
  end

  defp sync(%{config: config} = state) do
    dynamic =
      try do
        Capstan.Queues.dynamic_specs(config)
      rescue
        error ->
          Logger.warning("[capstan] queue sync skipped: #{Exception.message(error)}")

          :unavailable
      end

    case dynamic do
      :unavailable ->
        state

      dynamic ->
        desired = Map.merge(state.desired_static, dynamic)
        apply_diff(state, desired)
    end
  end

  defp apply_diff(%{config: config, running: running} = state, desired) do
    sup = Capstan.producer_sup(config.name)

    # Stop removed or changed producers first.
    running =
      running
      |> Enum.reject(fn {queue, {pid, spec}} ->
        keep? = Map.get(desired, queue) == spec and Process.alive?(pid)

        unless keep? do
          DynamicSupervisor.terminate_child(sup, pid)
        end

        keep?
      end)
      |> Map.new()

    # Start missing producers.
    running =
      Enum.reduce(desired, running, fn {queue, spec}, running ->
        case running do
          %{^queue => _} ->
            running

          _ ->
            case DynamicSupervisor.start_child(sup, {Capstan.Producer, {config, queue, spec}}) do
              {:ok, pid} -> Map.put(running, queue, {pid, spec})
              {:error, {:already_started, pid}} -> Map.put(running, queue, {pid, spec})
              {:error, reason} ->
                Logger.warning("[capstan] producer #{queue} failed to start: #{inspect(reason)}")

                running
            end
        end
      end)

    %{state | running: running}
  end

  defp schedule(%{config: config} = state) do
    Process.send_after(self(), :sync, config.dynamic_sync)

    state
  end
end
