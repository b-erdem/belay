defmodule Capstan.Config do
  @moduledoc """
  The resolved runtime configuration of a Capstan instance.

  Built from the options passed to `Capstan.start_link/1`; carried through
  every storage and notifier callback. Applications normally never construct
  one — it is documented because custom storage adapters and notifiers
  receive it.
  """

  defstruct name: Capstan,
            storage: nil,
            storage_ref: nil,
            queues: %{},
            crons: [],
            clock: Capstan.Clock.System,
            node_id: nil,
            poll_interval: 500,
            busy_poll: 25,
            notifiers: [{Capstan.Notifier.Local, []}],
            lease_ttl: 30_000,
            sweep_interval: 5_000,
            cron_interval: 20_000,
            shutdown_grace: 15_000,
            retention: %{},
            signal_ttl: 604_800,
            encryption: nil,
            dynamic_sync: 5_000

  @type t :: %__MODULE__{}

  def new(opts) do
    opts = merge_otp_app(opts)
    name = Keyword.get(opts, :name, Capstan)

    %__MODULE__{
      name: name,
      storage: normalize_storage(Keyword.fetch!(opts, :storage)),
      queues: normalize_queues(Keyword.get(opts, :queues, [])),
      crons: normalize_crons(Keyword.get(opts, :crons, [])),
      clock: Keyword.get(opts, :clock, Capstan.Clock.System),
      node_id: Keyword.get(opts, :node_id, default_node_id()),
      poll_interval: Keyword.get(opts, :poll_interval, 500),
      busy_poll: Keyword.get(opts, :busy_poll, 25),
      notifiers: normalize_notifiers(Keyword.get(opts, :notifiers, [:local]), opts),
      lease_ttl: Keyword.get(opts, :lease_ttl, 30_000),
      sweep_interval: Keyword.get(opts, :sweep_interval, 5_000),
      cron_interval: Keyword.get(opts, :cron_interval, 20_000),
      shutdown_grace: Keyword.get(opts, :shutdown_grace, 15_000),
      retention: normalize_retention(Keyword.get(opts, :retention, [])),
      signal_ttl: Keyword.get(opts, :signal_ttl, 604_800),
      encryption: normalize_encryption(Keyword.get(opts, :encryption)),
      dynamic_sync: Keyword.get(opts, :dynamic_sync, 5_000)
    }
  end

  # `otp_app:` reads `config :my_app, MyApp.Capstan, ...` from application
  # env as the base (the Ecto/Phoenix convention, keyed by the instance
  # `name`), with any inline child-spec opts merged on top — so environment
  # config lives in config/*.exs while runtime values (a computed storage
  # URL from runtime.exs, test overrides) can still be passed directly.
  defp merge_otp_app(opts) do
    case Keyword.pop(opts, :otp_app) do
      {nil, opts} ->
        opts

      {app, opts} ->
        name = Keyword.get(opts, :name, Capstan)
        base = Application.get_env(app, name, [])
        Keyword.merge(base, opts)
    end
  end

  @doc "Resolve the configured 32-byte encryption key, or nil."
  def encryption_key(%__MODULE__{encryption: nil}), do: nil

  def encryption_key(%__MODULE__{encryption: {module, fun, args}}) do
    case apply(module, fun, args) do
      key when is_binary(key) and byte_size(key) == 32 ->
        key

      other ->
        raise ArgumentError,
              "encryption key must be a 32-byte binary, got: #{byte_size(to_string(other))} bytes"
    end
  end

  defp normalize_encryption(nil), do: nil
  defp normalize_encryption(key: {module, fun, args}), do: {module, fun, args}

  @default_retention %{"succeeded" => 86_400, "failed" => 604_800, "cancelled" => 604_800}

  defp normalize_retention(opts) do
    Enum.reduce(opts, @default_retention, fn {state, keep}, acc ->
      Map.put(acc, to_string(state), keep)
    end)
  end

  def now(%__MODULE__{clock: clock}), do: Capstan.Clock.now(clock)

  def put(%__MODULE__{name: name} = config) do
    :persistent_term.put({Capstan, name}, config)
    config
  end

  def fetch!(name) do
    :persistent_term.get({Capstan, name})
  rescue
    ArgumentError ->
      reraise(ArgumentError, "no Capstan instance named #{inspect(name)}", __STACKTRACE__)
  end

  def queue_spec(%__MODULE__{queues: queues}, queue), do: Map.fetch!(queues, to_string(queue))

  @doc "Validate and normalize queue options into a producer spec."
  def normalize_queue_opts(queue, opts) when is_list(opts) do
    {limit, opts} = Keyword.pop(opts, :limit, 10)

    queue_defaults(to_string(queue), limit, opts)
  end

  defp normalize_storage({adapter, opts}), do: {storage_module(adapter), opts}
  defp normalize_storage(adapter) when is_atom(adapter), do: {storage_module(adapter), []}

  defp normalize_storage(opts) when is_list(opts) do
    {storage_module(Keyword.fetch!(opts, :adapter)), Keyword.delete(opts, :adapter)}
  end

  defp storage_module(:memory), do: Capstan.Storage.Memory
  defp storage_module(:postgres), do: Capstan.Storage.Postgres
  defp storage_module(module) when is_atom(module), do: module

  defp normalize_notifiers(notifiers, opts) do
    storage_mod = opts |> Keyword.fetch!(:storage) |> normalize_storage() |> elem(0)

    Enum.map(notifiers, fn entry ->
      {module, nopts} =
        case entry do
          :local ->
            {Capstan.Notifier.Local, []}

          :postgres ->
            {Capstan.Notifier.Postgres, []}

          {shorthand, nopts} when shorthand in [:local, :postgres] ->
            {elem(normalize_notifiers([shorthand], opts) |> hd(), 0), nopts}

          module when is_atom(module) ->
            {module, []}

          {module, nopts} when is_atom(module) ->
            {module, nopts}
        end

      if module == Capstan.Notifier.Postgres and storage_mod != Capstan.Storage.Postgres do
        raise ArgumentError, "the :postgres notifier requires the :postgres storage adapter"
      end

      {module, nopts}
    end)
  end

  defp normalize_queues(queues) do
    Map.new(queues, fn
      {queue, limit} when is_integer(limit) ->
        {to_string(queue), queue_defaults(to_string(queue), limit, [])}

      {queue, opts} when is_list(opts) ->
        {limit, opts} = Keyword.pop(opts, :limit, 10)
        {to_string(queue), queue_defaults(to_string(queue), limit, opts)}
    end)
  end

  defp queue_defaults(queue, limit, opts) do
    # `limit: n` is static; `limit: [min: a, max: b]` scales the producer's
    # local concurrency between the bounds (leaderless — each node adapts its
    # own limit; `global_limit` still caps the fleet).
    {local_limit, limit_min} =
      case limit do
        n when is_integer(n) and n >= 1 ->
          {n, nil}

        bounds when is_list(bounds) ->
          min = Keyword.fetch!(bounds, :min)
          max = Keyword.fetch!(bounds, :max)

          unless is_integer(min) and is_integer(max) and 1 <= min and min <= max do
            raise ArgumentError,
                  "queue #{queue}: limit bounds must satisfy 1 <= min <= max, got #{inspect(bounds)}"
          end

          {max, min}

        other ->
          raise ArgumentError, "queue #{queue}: invalid limit #{inspect(other)}"
      end

    %{
      queue: queue,
      local_limit: local_limit,
      limit_min: limit_min,
      global_limit: Keyword.get(opts, :global_limit),
      rate: normalize_rate(Keyword.get(opts, :rate)),
      partition: normalize_partition(Keyword.get(opts, :partition)),
      # Manual queues get no producer — claimed only via Capstan.Testing.drain.
      manual: Keyword.get(opts, :manual, false)
    }
  end

  defp normalize_rate(nil), do: nil

  defp normalize_rate(opts) do
    %{
      allowed: Keyword.fetch!(opts, :allowed),
      period: Keyword.fetch!(opts, :period),
      # A shared resource bucket (e.g. "anthropic") makes the limit span every
      # queue that names it; :estimate is the per-job admission cost in
      # resource units, corrected post-hoc via Capstan.debit/3.
      resource: Keyword.get(opts, :resource),
      estimate: Keyword.get(opts, :estimate, 1)
    }
  end

  defp normalize_partition(nil), do: nil

  defp normalize_partition({source, key}) when source in [:input, :meta] do
    {source, to_string(key)}
  end

  defp normalize_crons(crons) do
    Enum.map(crons, fn cron ->
      %{
        name: to_string(Keyword.fetch!(cron, :name)),
        expr: Capstan.CronExpr.parse!(Keyword.fetch!(cron, :expr)),
        worker: Keyword.fetch!(cron, :worker),
        input: Keyword.get(cron, :input, %{}),
        opts: Keyword.get(cron, :opts, [])
      }
    end)
  end

  defp default_node_id do
    suffix = :crypto.strong_rand_bytes(3) |> Base.encode16(case: :lower)

    "#{node()}-#{suffix}"
  end
end
