defmodule Capstan.CronExpr do
  @moduledoc """
  Minimal five-field cron expressions: `minute hour day-of-month month day-of-week`.

  Supports `*`, numbers, ranges (`a-b`), steps (`*/n`, `a-b/n`), lists
  (`a,b,c`), and the nicknames `@hourly`, `@daily`, `@midnight`, `@weekly`,
  `@monthly`. Day-of-week is 0-6 with 0 = Sunday (7 also accepted as Sunday).
  """

  defstruct [:minutes, :hours, :days, :months, :weekdays, :source]

  @type t :: %__MODULE__{}

  @nicknames %{
    "@hourly" => "0 * * * *",
    "@daily" => "0 0 * * *",
    "@midnight" => "0 0 * * *",
    "@weekly" => "0 0 * * 0",
    "@monthly" => "0 0 1 * *"
  }

  @ranges %{minutes: 0..59, hours: 0..23, days: 1..31, months: 1..12, weekdays: 0..6}

  @doc "Parse an expression. Returns `{:ok, t}` or `{:error, reason}`."
  def parse(expression) when is_binary(expression) do
    source = String.trim(expression)
    expanded = Map.get(@nicknames, source, source)

    with [m, h, d, mo, w] <- String.split(expanded, ~r/\s+/, trim: true),
         {:ok, minutes} <- field(m, @ranges.minutes),
         {:ok, hours} <- field(h, @ranges.hours),
         {:ok, days} <- field(d, @ranges.days),
         {:ok, months} <- field(mo, @ranges.months),
         {:ok, weekdays} <- field(w, @ranges.weekdays) do
      {:ok,
       %__MODULE__{
         minutes: minutes,
         hours: hours,
         days: days,
         months: months,
         weekdays: weekdays,
         source: source
       }}
    else
      fields when is_list(fields) -> {:error, "expected 5 fields in #{inspect(source)}"}
      {:error, reason} -> {:error, reason}
    end
  end

  def parse!(expression) do
    case parse(expression) do
      {:ok, expr} -> expr
      {:error, reason} -> raise ArgumentError, "invalid cron expression: #{reason}"
    end
  end

  @doc "Does the minute containing `datetime` (UTC) match?"
  def matches?(%__MODULE__{} = expr, %DateTime{} = datetime) do
    weekday = datetime |> Date.day_of_week() |> rem(7)

    MapSet.member?(expr.minutes, datetime.minute) and
      MapSet.member?(expr.hours, datetime.hour) and
      MapSet.member?(expr.months, datetime.month) and
      day_matches?(expr, datetime.day, weekday)
  end

  # Standard cron: when both day fields are restricted, either may match.
  defp day_matches?(expr, day, weekday) do
    day_restricted? = expr.days != MapSet.new(@ranges.days)
    week_restricted? = expr.weekdays != MapSet.new(@ranges.weekdays)

    case {day_restricted?, week_restricted?} do
      {true, true} -> MapSet.member?(expr.days, day) or MapSet.member?(expr.weekdays, weekday)
      _ -> MapSet.member?(expr.days, day) and MapSet.member?(expr.weekdays, weekday)
    end
  end

  @doc "Truncate a datetime to its minute slot."
  def slot(%DateTime{} = datetime) do
    %{datetime | second: 0, microsecond: {0, 6}}
  end

  defp field(spec, range) do
    spec
    |> String.split(",", trim: true)
    |> Enum.reduce_while({:ok, MapSet.new()}, fn part, {:ok, acc} ->
      case part(part, range) do
        {:ok, values} -> {:cont, {:ok, MapSet.union(acc, values)}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp part(spec, range) do
    case String.split(spec, "/", parts: 2) do
      [base] ->
        with {:ok, first..last//_} <- base_range(base, range) do
          {:ok, MapSet.new(first..last)}
        end

      [base, step] ->
        with {:ok, first..last//_} <- base_range(base, range),
             {step, ""} when step > 0 <- Integer.parse(step) do
          {:ok, MapSet.new(first..last//step)}
        else
          _ -> {:error, "bad step in #{inspect(spec)}"}
        end
    end
  end

  defp base_range("*", range), do: {:ok, range}

  defp base_range(base, range) do
    case String.split(base, "-", parts: 2) do
      [single] ->
        with {:ok, value} <- int_in(single, range), do: {:ok, value..value}

      [first, last] ->
        with {:ok, first} <- int_in(first, range),
             {:ok, last} <- int_in(last, range),
             true <- first <= last do
          {:ok, first..last}
        else
          _ -> {:error, "bad range #{inspect(base)}"}
        end
    end
  end

  defp int_in(string, range) do
    case Integer.parse(string) do
      # Weekday 7 is an alias for Sunday.
      {7, ""} when range == 0..6 -> {:ok, 0}
      {value, ""} -> if value in range, do: {:ok, value}, else: {:error, "#{value} out of range"}
      _ -> {:error, "not a number: #{inspect(string)}"}
    end
  end
end
