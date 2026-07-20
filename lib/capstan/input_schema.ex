defmodule Capstan.InputError do
  @moduledoc "Raised at insert time when a job's input fails its worker's `input_schema`."
  defexception [:worker, :errors]

  @impl Exception
  def message(%{worker: worker, errors: errors}) do
    details = Enum.map_join(errors, "; ", fn {field, reason} -> "#{field} #{reason}" end)

    "invalid input for #{inspect(worker)}: #{details}"
  end
end

defmodule Capstan.InputSchema do
  @moduledoc """
  Insert-time input validation, declared on the worker:

      use Capstan.Worker,
        queue: :ai,
        input_schema: [
          url: [type: :string, required: true],
          style: [type: {:enum, ["tight", "loose"]}, default: "tight"],
          limit: [type: :integer]
        ]

  Validation runs when the job is built, so bad inputs raise
  `Capstan.InputError` at the call site instead of failing on attempt one.
  Defaults are written into the stored input. Keys are strings in the stored
  input (they round-trip through JSON); unknown keys pass through untouched.

  Types: `:string`, `:integer`, `:float` (integers accepted), `:boolean`,
  `:map`, `:list`, `{:enum, values}`.
  """

  @doc false
  def validate!(_worker, nil, input), do: input

  def validate!(worker, schema, input) when is_map(input) do
    {input, errors} =
      Enum.reduce(schema, {input, []}, fn {field, spec}, {input, errors} ->
        key = to_string(field)

        case Map.fetch(input, key) do
          {:ok, value} ->
            case cast(Keyword.get(spec, :type, :string), value) do
              :ok -> {input, errors}
              :error -> {input, [{key, bad_type(spec, value)} | errors]}
            end

          :error ->
            cond do
              Keyword.has_key?(spec, :default) ->
                {Map.put(input, key, Keyword.fetch!(spec, :default)), errors}

              Keyword.get(spec, :required, false) ->
                {input, [{key, "is required"} | errors]}

              true ->
                {input, errors}
            end
        end
      end)

    case errors do
      [] -> input
      errors -> raise Capstan.InputError, worker: worker, errors: Enum.reverse(errors)
    end
  end

  def validate!(worker, _schema, input) do
    raise Capstan.InputError, worker: worker, errors: [{"input", "must be a map, got: #{inspect(input)}"}]
  end

  defp cast(:string, value) when is_binary(value), do: :ok
  defp cast(:integer, value) when is_integer(value), do: :ok
  defp cast(:float, value) when is_float(value) or is_integer(value), do: :ok
  defp cast(:boolean, value) when is_boolean(value), do: :ok
  defp cast(:map, value) when is_map(value), do: :ok
  defp cast(:list, value) when is_list(value), do: :ok
  defp cast({:enum, values}, value), do: if(value in values, do: :ok, else: :error)
  defp cast(_type, _value), do: :error

  defp bad_type(spec, value) do
    "expected #{inspect(Keyword.get(spec, :type, :string))}, got: #{inspect(value)}"
  end
end
