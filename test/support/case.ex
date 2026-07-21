defmodule Belay.Test.Case do
  @moduledoc false

  use ExUnit.CaseTemplate

  using do
    quote do
      import Belay.Test.Case

      alias Belay.Clock.Sim
      alias Belay.Test.Events
      alias Belay.Testing
    end
  end

  setup do
    Belay.Test.Events.clear()

    :ok
  end

  @doc """
  Start an isolated Belay instance. Returns `%{name:, clock:}` where clock
  is a SimClock pid (unless `sim_clock: false`).

  Queues default to a single manual `:default` queue so tests drive execution
  via `Belay.Testing.drain/2`; pass explicit `queues:` for live producers.
  """
  def start_belay!(opts \\ []) do
    name = Module.concat(BelayTest, "T#{System.unique_integer([:positive])}")

    {clock, clock_pid} =
      if Keyword.get(opts, :sim_clock, true) do
        {:ok, pid} = Belay.Clock.Sim.start_link(~U[2026-01-05 00:00:00.000000Z])
        {{Belay.Clock.Sim, pid}, pid}
      else
        {Belay.Clock.System, nil}
      end

    storage =
      case Application.get_env(:belay, :test_storage, :memory) do
        :memory -> [adapter: :memory]
        :postgres -> [adapter: :postgres, url: Application.fetch_env!(:belay, :test_pg_url)]
      end

    defaults = [
      name: name,
      storage: storage,
      clock: clock,
      queues: [default: [limit: 10, manual: true]],
      poll_interval: 100,
      sweep_interval: 200,
      shutdown_grace: 500
    ]

    # Everything else passes straight through, so tests can exercise any
    # instance option without touching this helper.
    belay_opts = Keyword.merge(defaults, Keyword.drop(opts, [:sim_clock]))

    ExUnit.Callbacks.start_supervised!({Belay, belay_opts})

    if storage[:adapter] == :postgres do
      Belay.Storage.Postgres.truncate!(storage_ref(name))
    end

    %{name: name, clock: clock_pid}
  end

  def storage_ref(name) do
    %{storage_ref: {_mod, ref}} = Belay.Config.fetch!(name)
    ref
  end

  def storage(name) do
    %{storage_ref: storage_ref} = Belay.Config.fetch!(name)
    storage_ref
  end

  def config(name), do: Belay.Config.fetch!(name)

  def advance(nil, _seconds), do: raise("test instance started with sim_clock: false")
  def advance(clock, seconds), do: Belay.Clock.Sim.advance(clock, seconds)

  def job!(name, id) do
    {:ok, job} = Belay.get_job(name, id)
    job
  end

  def wait_until(fun, timeout \\ 3_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_wait(fun, deadline)
  end

  defp do_wait(fun, deadline) do
    case fun.() do
      result when result in [nil, false] ->
        if System.monotonic_time(:millisecond) > deadline do
          ExUnit.Assertions.flunk("wait_until timed out")
        else
          Process.sleep(20)
          do_wait(fun, deadline)
        end

      result ->
        result
    end
  end
end
