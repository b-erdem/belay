defmodule Capstan.Clock do
  @moduledoc """
  Injectable time source. Everything time-dependent in the engine (backoff,
  lease expiry, rate windows, await deadlines, cron slots) reads through this,
  so tests advance a `Capstan.Clock.Sim` instead of sleeping.
  """

  @callback now(term()) :: DateTime.t()

  def now({mod, arg}), do: mod.now(arg)
  def now(mod) when is_atom(mod), do: mod.now(nil)
end

defmodule Capstan.Clock.System do
  @moduledoc "Real time."
  @behaviour Capstan.Clock

  @impl true
  def now(_), do: DateTime.utc_now()
end

defmodule Capstan.Clock.Sim do
  @moduledoc "A settable, advanceable clock for deterministic tests."
  @behaviour Capstan.Clock

  def start_link(start \\ ~U[2026-01-01 00:00:00.000000Z]) do
    Agent.start_link(fn -> start end)
  end

  @impl true
  def now(pid), do: Agent.get(pid, & &1)

  def advance(pid, seconds) do
    Agent.update(pid, &DateTime.add(&1, round(seconds * 1_000_000), :microsecond))
  end

  def set(pid, %DateTime{} = at), do: Agent.update(pid, fn _ -> at end)
end
