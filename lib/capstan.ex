defmodule Capstan do
  @moduledoc """
  Capstan is an open toolkit that turns [Oban](https://hexdocs.pm/oban) into an
  agent-era job platform: durable steps, signals, workflows, batches, chains,
  relayed results, and a smart engine with global and rate limits — all on
  plain Oban tables, compatible with Oban Web and `Oban.Testing`.

  ## Setup

  Run `Capstan.Migration` after Oban's migrations, then configure the engine:

      config :my_app, Oban,
        engine: Capstan.Engine,
        repo: MyApp.Repo,
        queues: [ai: [limit: 10, global_limit: 4, rate_limit: [allowed: 60, period: 60]]]

  ## The pieces

    * `Capstan.Engine` — global concurrency, sliding-window rate limits, and
      per-key partitions; drives everything below via transition interception
    * `Capstan.Steps` — memoized durable steps and signal waits inside jobs
      (`step/3`, `await_signal/3`, `signal/3`)
    * `Capstan.Worker` — structured args, hooks, recorded results
    * `Capstan.Workflow` — DAG-dependent jobs with failure cascades
    * `Capstan.Batch` — grouped jobs with milestone callbacks
    * `Capstan.Chain` — strict per-key FIFO
    * `Capstan.Relay` — insert a job and await its result
  """

  alias Capstan.Steps
  alias Oban.Job

  @doc "Run `fun` at most once per job. See `Capstan.Steps.step/3`."
  defdelegate step(job, name, fun), to: Steps

  @doc "Wait for a signal, snoozing until it arrives. See `Capstan.Steps.await_signal/3`."
  defdelegate await_signal(job, name, opts \\ []), to: Steps

  @doc "Deliver a signal to the default Oban instance. See `Capstan.Steps.signal/4`."
  def signal(scope, name, payload \\ %{}), do: Steps.signal(Oban, scope, name, payload)

  @doc "Deliver a signal to a specific Oban instance."
  def signal_via(oban, scope, name, payload \\ %{}), do: Steps.signal(oban, scope, name, payload)

  @doc "Signal a specific job (by struct or id) on the default Oban instance."
  def signal_job(job_or_id, name, payload \\ %{})

  def signal_job(%Job{id: id}, name, payload), do: signal_job(id, name, payload)

  def signal_job(id, name, payload) when is_integer(id) do
    Steps.signal(Oban, "job:#{id}", name, payload)
  end
end
