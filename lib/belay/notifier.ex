defmodule Belay.Notifier do
  @moduledoc """
  Wake-up delivery for low-latency dispatch.

  Notifiers are **accelerators, never load-bearing**: polling remains the
  correctness floor, so a lost or unavailable notifier costs latency, not
  jobs. Two ship built-in:

    * `Belay.Notifier.Local` (always on) — in-process registry pokes plus
      `:pg` broadcast when BEAM nodes are clustered with distributed Erlang.
      Same-node dispatch is effectively instant; clustered dispatch is a
      message send away.
    * `Belay.Notifier.Postgres` (opt-in) — `pg_notify` wake-ups through the
      database, for fleets that share Postgres but not an Erlang cluster:

          {Belay,
           name: MyApp.Belay,
           storage: [adapter: :postgres, url: url],
           notifiers: [:local, :postgres],
           ...}

      The listening side uses one dedicated connection per node
      (`Postgrex.Notifications`, auto-reconnecting) — point `listen_url:` at
      a direct connection if your main URL goes through a transaction pooler.
      NOTIFY payloads here are tiny (a queue name or job id), and if the
      channel is down you fall back to `busy_poll`/`poll_interval` pickup.
  """

  alias Belay.Config

  @type message :: {:poke, String.t()} | {:result, integer()}

  @doc "Deliver a wake-up through this notifier."
  @callback broadcast(Config.t(), message()) :: :ok

  @doc "Optional listener process for the receiving side."
  @callback child_spec({Config.t(), keyword()}) :: Supervisor.child_spec()

  @optional_callbacks child_spec: 1

  @doc false
  def broadcast_all(%Config{notifiers: notifiers} = config, message) do
    for {module, _opts} <- notifiers do
      try do
        module.broadcast(config, message)
      catch
        # A notifier must never fail the caller; polling covers the gap.
        _, _ -> :ok
      end
    end

    :ok
  end

  @doc false
  def children(%Config{notifiers: notifiers} = config) do
    for {module, opts} <- notifiers,
        Code.ensure_loaded?(module) and function_exported?(module, :child_spec, 1) do
      module.child_spec({config, opts})
    end
  end
end

defmodule Belay.Notifier.Local do
  @moduledoc """
  The always-on notifier: registry pokes on this node, `:pg` fan-out to
  clustered BEAM nodes. Configured as `:local`.
  """

  # Result delivery is handled directly by the runner's local registry
  # dispatch, so it's a no-op here.

  @behaviour Belay.Notifier

  @impl Belay.Notifier
  def broadcast(config, {:poke, queue}) do
    case Registry.lookup(Belay.registry(config.name), {:producer, queue}) do
      [{pid, _}] -> send(pid, :poke)
      _ -> :ok
    end

    config.name
    |> Belay.pg_scope()
    |> :pg.get_members({:producers, queue})
    |> Enum.each(&send(&1, :poke))

    :ok
  catch
    :exit, _ -> :ok
  end

  def broadcast(_config, {:result, _id}), do: :ok
end

defmodule Belay.Notifier.Postgres do
  @moduledoc """
  The opt-in `pg_notify` accelerator (configured as `:postgres`): wake-ups
  ride the database itself for fleets that share Postgres without an Erlang
  cluster. See `Belay.Notifier` for semantics and caveats.
  """

  # pg_notify accelerator. The channel is derived from the database name, so
  # separate Belay databases sharing a Postgres cluster don't cross-talk;
  # instances sharing one database wake each other regardless of node
  # topology. Listening requires a direct (non-transaction-pooled)
  # connection; Postgrex.Notifications reconnects on its own.

  @behaviour Belay.Notifier

  use GenServer

  require Logger

  alias Belay.Config

  @impl Belay.Notifier
  def broadcast(config, message) do
    {_mod, ref} = config.storage_ref

    payload =
      case message do
        {:poke, queue} -> Jason.encode!(%{"t" => "p", "q" => queue})
        {:result, job_id} -> Jason.encode!(%{"t" => "r", "id" => job_id})
      end

    case Postgrex.query(ref, "SELECT pg_notify($1, $2)", [channel(config), payload]) do
      {:ok, _} -> :ok
      {:error, _} -> :ok
    end
  end

  @impl Belay.Notifier
  def child_spec({config, opts}) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [{config, opts}]}}
  end

  def start_link({config, opts}), do: GenServer.start_link(__MODULE__, {config, opts})

  @impl GenServer
  def init({config, opts}) do
    {_storage_mod, storage_opts} = config.storage

    url = Keyword.get(opts, :listen_url) || Keyword.fetch!(storage_opts, :url)

    conn_opts =
      url
      |> Belay.Storage.Postgres.parse_url()
      |> Keyword.merge(auto_reconnect: true, sync_connect: false)

    {:ok, conn} = Postgrex.Notifications.start_link(conn_opts)

    # With auto_reconnect the subscription is (re-)established as the
    # connection comes up; :eventual is the async-connect success shape.
    case Postgrex.Notifications.listen(conn, channel(config)) do
      {:ok, _ref} -> :ok
      {:eventual, _ref} -> :ok
    end

    {:ok, %{config: config}}
  end

  @impl GenServer
  def handle_info({:notification, _pid, _ref, _channel, payload}, %{config: config} = state) do
    case Jason.decode(payload) do
      {:ok, %{"t" => "p", "q" => queue}} ->
        case Registry.lookup(Belay.registry(config.name), {:producer, queue}) do
          [{pid, _}] -> send(pid, :poke)
          _ -> :ok
        end

      {:ok, %{"t" => "r", "id" => job_id}} ->
        Registry.dispatch(Belay.run_registry(config.name), {:result, job_id}, fn entries ->
          for {pid, _} <- entries, do: send(pid, {:belay_result, job_id, :remote})
        end)

      _ ->
        :ok
    end

    {:noreply, state}
  rescue
    _ -> {:noreply, state}
  end

  def handle_info(_other, state), do: {:noreply, state}

  @doc false
  def channel(%Config{} = config) do
    {_mod, storage_opts} = config.storage

    database =
      storage_opts
      |> Keyword.fetch!(:url)
      |> Belay.Storage.Postgres.parse_url()
      |> Keyword.fetch!(:database)

    "belay_" <> String.replace(database, ~r/[^a-zA-Z0-9_]/, "_")
  end
end
