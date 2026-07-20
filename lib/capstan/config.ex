defmodule Capstan.Config do
  @moduledoc false

  defstruct name: Capstan,
            storage: nil,
            storage_ref: nil,
            queues: %{},
            crons: [],
            clock: Capstan.Clock.System,
            node_id: nil,
            poll_interval: 500,
            lease_ttl: 30_000,
            sweep_interval: 5_000,
            cron_interval: 20_000,
            shutdown_grace: 5_000

  @type t :: %__MODULE__{}

  def new(opts) do
    name = Keyword.get(opts, :name, Capstan)

    %__MODULE__{
      name: name,
      storage: normalize_storage(Keyword.fetch!(opts, :storage)),
      queues: normalize_queues(Keyword.get(opts, :queues, [])),
      crons: normalize_crons(Keyword.get(opts, :crons, [])),
      clock: Keyword.get(opts, :clock, Capstan.Clock.System),
      node_id: Keyword.get(opts, :node_id, default_node_id()),
      poll_interval: Keyword.get(opts, :poll_interval, 500),
      lease_ttl: Keyword.get(opts, :lease_ttl, 30_000),
      sweep_interval: Keyword.get(opts, :sweep_interval, 5_000),
      cron_interval: Keyword.get(opts, :cron_interval, 20_000)
    }
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

  defp normalize_storage({adapter, opts}), do: {storage_module(adapter), opts}
  defp normalize_storage(adapter) when is_atom(adapter), do: {storage_module(adapter), []}

  defp normalize_storage(opts) when is_list(opts) do
    {storage_module(Keyword.fetch!(opts, :adapter)), Keyword.delete(opts, :adapter)}
  end

  defp storage_module(:memory), do: Capstan.Storage.Memory
  defp storage_module(:postgres), do: Capstan.Storage.Postgres
  defp storage_module(module) when is_atom(module), do: module

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
    %{
      queue: queue,
      local_limit: limit,
      global_limit: Keyword.get(opts, :global_limit),
      rate: normalize_rate(Keyword.get(opts, :rate)),
      partition: normalize_partition(Keyword.get(opts, :partition)),
      # Manual queues get no producer — claimed only via Capstan.Testing.drain.
      manual: Keyword.get(opts, :manual, false)
    }
  end

  defp normalize_rate(nil), do: nil

  defp normalize_rate(opts) do
    %{allowed: Keyword.fetch!(opts, :allowed), period: Keyword.fetch!(opts, :period)}
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
