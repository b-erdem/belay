defmodule Capstan.Test.Case do
  @moduledoc false

  use ExUnit.CaseTemplate

  alias Capstan.Test.Repo

  using do
    quote do
      import Capstan.Test.Case

      alias Capstan.Test.Repo
    end
  end

  setup do
    for table <- ~w(oban_jobs capstan_steps capstan_signals capstan_rate capstan_crons) do
      Repo.query!("DELETE FROM #{table}")
    end

    :ok
  end

  @doc """
  Start an isolated Oban instance wired to the Capstan engine.

  Returns the instance name. Queues default to none; pass e.g. `queues: [default: 5]`.
  """
  def start_oban!(opts \\ []) do
    name = Module.concat(ObanTest, "T#{System.unique_integer([:positive])}")

    opts =
      Keyword.merge(
        [
          name: name,
          repo: Repo,
          engine: Capstan.Engine,
          # Oban only auto-disables the prefix for its own Lite engine, so a
          # custom engine on SQLite must disable it explicitly.
          prefix: false,
          notifier: Oban.Notifiers.Isolated,
          peer: false,
          plugins: false,
          queues: false,
          stage_interval: 50,
          shutdown_grace_period: 250
        ],
        opts
      )

    start_supervised!({Oban, opts})

    name
  end

  @doc "Drain a queue synchronously through the configured engine."
  def drain!(name, queue, opts \\ []) do
    Oban.drain_queue(name, Keyword.merge([queue: queue, with_recursion: true], opts))
  end

  @doc "Fetch all jobs as a list of maps for assertions."
  def all_jobs do
    import Ecto.Query

    Repo.all(from(j in Oban.Job, order_by: j.id))
  end

  @doc "Wait until fun returns truthy or timeout (for async queue tests)."
  def wait_until(fun, timeout \\ 2_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_wait(fun, deadline)
  end

  defp do_wait(fun, deadline) do
    case fun.() do
      result when result in [nil, false] ->
        if System.monotonic_time(:millisecond) > deadline do
          flunk("wait_until timed out")
        else
          Process.sleep(25)
          do_wait(fun, deadline)
        end

      result ->
        result
    end
  end
end
