defmodule Capstan.Test.Case do
  @moduledoc false

  use ExUnit.CaseTemplate

  using do
    quote do
      import Capstan.Test.Case

      alias Capstan.Clock.Sim
      alias Capstan.Test.Events
      alias Capstan.Testing
    end
  end

  setup do
    Capstan.Test.Events.clear()

    :ok
  end

  @doc """
  Start an isolated Capstan instance. Returns `%{name:, clock:}` where clock
  is a SimClock pid (unless `sim_clock: false`).

  Queues default to a single manual `:default` queue so tests drive execution
  via `Capstan.Testing.drain/2`; pass explicit `queues:` for live producers.
  """
  def start_capstan!(opts \\ []) do
    name = Module.concat(CapstanTest, "T#{System.unique_integer([:positive])}")

    {clock, clock_pid} =
      if Keyword.get(opts, :sim_clock, true) do
        {:ok, pid} = Capstan.Clock.Sim.start_link(~U[2026-01-05 00:00:00.000000Z])
        {{Capstan.Clock.Sim, pid}, pid}
      else
        {Capstan.Clock.System, nil}
      end

    storage =
      case Application.get_env(:capstan, :test_storage, :memory) do
        :memory -> [adapter: :memory]
        :postgres -> [adapter: :postgres, url: Application.fetch_env!(:capstan, :test_pg_url)]
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
    capstan_opts = Keyword.merge(defaults, Keyword.drop(opts, [:sim_clock]))

    ExUnit.Callbacks.start_supervised!({Capstan, capstan_opts})

    if storage[:adapter] == :postgres do
      Capstan.Storage.Postgres.truncate!(storage_ref(name))
    end

    %{name: name, clock: clock_pid}
  end

  def storage_ref(name) do
    %{storage_ref: {_mod, ref}} = Capstan.Config.fetch!(name)
    ref
  end

  def storage(name) do
    %{storage_ref: storage_ref} = Capstan.Config.fetch!(name)
    storage_ref
  end

  def config(name), do: Capstan.Config.fetch!(name)

  def advance(nil, _seconds), do: raise("test instance started with sim_clock: false")
  def advance(clock, seconds), do: Capstan.Clock.Sim.advance(clock, seconds)

  def job!(name, id) do
    {:ok, job} = Capstan.get_job(name, id)
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
