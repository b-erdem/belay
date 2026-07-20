defmodule Capstan.Worker do
  @moduledoc """
  An enhanced Oban worker with structured args, lifecycle hooks, and recorded
  results.

      defmodule MyApp.Summarize do
        use Capstan.Worker,
          queue: :ai,
          max_attempts: 5,
          recorded: true,
          args_schema: [
            url: [type: :string, required: true],
            style: [type: :string, default: "tight"]
          ]

        @impl Capstan.Worker
        def process(%Oban.Job{args: args} = job) do
          {:ok, summarize(args.url, args.style)}
        end
      end

  Differences from a plain `Oban.Worker`:

    * implement `c:process/1` instead of `perform/1`
    * `:args_schema` validates and casts string-keyed args into an atom-keyed
      map before `process/1` runs; invalid args cancel the job immediately
      instead of burning retries
    * `:recorded` persists the `{:ok, value}` result into the job's meta so
      `Capstan.Relay` and workflow siblings can read it
    * optional `c:before_process/1` and `c:after_process/2` hooks
  """

  alias Oban.{Job, Repo}

  import Ecto.Query, only: [where: 2]

  @max_recorded_bytes 64 * 1024

  @doc "Called in place of `perform/1`. Return values follow `Oban.Worker` results."
  @callback process(Job.t()) :: Oban.Worker.result()

  @doc "Runs before `process/1`. Return `{:cancel, reason}` to cancel the job."
  @callback before_process(Job.t()) :: :ok | {:cancel, term()}

  @doc "Runs after `process/1` with the result. Failures are logged, never raised."
  @callback after_process(Job.t(), term()) :: term()

  @optional_callbacks before_process: 1, after_process: 2

  defmacro __using__(opts) do
    {capstan_opts, oban_opts} = Keyword.split(opts, [:recorded, :args_schema])

    quote location: :keep do
      use Oban.Worker, unquote(oban_opts)

      @behaviour Capstan.Worker

      @capstan_recorded Keyword.get(unquote(capstan_opts), :recorded, false)
      @capstan_args_schema Keyword.get(unquote(capstan_opts), :args_schema)

      @impl Oban.Worker
      def perform(%Oban.Job{} = job) do
        Capstan.Worker.__execute__(__MODULE__, job, @capstan_recorded, @capstan_args_schema)
      end
    end
  end

  @doc false
  def __execute__(module, job, recorded?, args_schema) do
    with {:ok, job} <- cast_args(job, args_schema),
         :ok <- run_before(module, job) do
      result =
        try do
          module.process(job)
        catch
          :throw, {:capstan_snooze, seconds} -> {:snooze, seconds}
        end

      result = maybe_record(job, recorded?, result)

      run_after(module, job, result)

      result
    end
  end

  defp cast_args(job, nil), do: {:ok, job}

  defp cast_args(%Job{args: args} = job, schema) do
    schema
    |> Enum.reduce_while({:ok, %{}}, fn {key, spec}, {:ok, acc} ->
      case cast_field(args, key, spec) do
        {:ok, value} -> {:cont, {:ok, Map.put(acc, key, value)}}
        :skip -> {:cont, {:ok, acc}}
        {:error, reason} -> {:halt, {:error, {key, reason}}}
      end
    end)
    |> case do
      {:ok, cast} -> {:ok, %{job | args: cast}}
      {:error, {key, reason}} -> {:cancel, {:invalid_args, %{key => reason}}}
    end
  end

  defp cast_field(args, key, spec) do
    type = Keyword.get(spec, :type, :string)
    required? = Keyword.get(spec, :required, false)

    case Map.fetch(args, to_string(key)) do
      {:ok, raw} ->
        case cast_type(type, raw) do
          {:ok, value} -> {:ok, value}
          :error -> {:error, "expected #{inspect(type)}, got: #{inspect(raw)}"}
        end

      :error ->
        cond do
          Keyword.has_key?(spec, :default) -> {:ok, Keyword.fetch!(spec, :default)}
          required? -> {:error, "is required"}
          true -> :skip
        end
    end
  end

  defp cast_type(:string, val) when is_binary(val), do: {:ok, val}
  defp cast_type(:integer, val) when is_integer(val), do: {:ok, val}
  defp cast_type(:float, val) when is_float(val), do: {:ok, val}
  defp cast_type(:float, val) when is_integer(val), do: {:ok, val * 1.0}
  defp cast_type(:boolean, val) when is_boolean(val), do: {:ok, val}
  defp cast_type(:map, val) when is_map(val), do: {:ok, val}
  defp cast_type(:list, val) when is_list(val), do: {:ok, val}
  defp cast_type({:enum, allowed}, val), do: if(val in allowed, do: {:ok, val}, else: :error)
  defp cast_type(_type, _val), do: :error

  defp run_before(module, job) do
    if function_exported?(module, :before_process, 1) do
      case module.before_process(job) do
        {:cancel, reason} -> {:cancel, reason}
        _ -> :ok
      end
    else
      :ok
    end
  end

  defp run_after(module, job, result) do
    if function_exported?(module, :after_process, 2) do
      module.after_process(job, result)
    end
  rescue
    error ->
      require Logger
      Logger.error("Capstan after_process hook failed: #{Exception.message(error)}")
  end

  defp maybe_record(_job, false, result), do: result

  defp maybe_record(%Job{conf: conf, id: id, meta: meta}, true, {:ok, value} = result) do
    bin = value |> :erlang.term_to_binary() |> Base.encode64()

    if byte_size(bin) > @max_recorded_bytes do
      {:cancel, :recorded_result_too_large}
    else
      meta = Map.put(meta, "recorded", bin)
      Repo.update_all(conf, where(Job, id: ^id), set: [meta: meta])

      result
    end
  end

  defp maybe_record(_job, true, result), do: result

  @doc "Decode a recorded result from a job's meta. Returns `{:ok, term}` or `:error`."
  def fetch_recorded(%Job{meta: %{"recorded" => bin}}), do: decode_recorded(bin)
  def fetch_recorded(%Job{}), do: :error

  @doc false
  def decode_recorded(bin) when is_binary(bin) do
    with {:ok, raw} <- Base.decode64(bin) do
      {:ok, :erlang.binary_to_term(raw)}
    end
  end

  def decode_recorded(_), do: :error
end
