defmodule Capstan.Testing do
  @moduledoc """
  Synchronous, deterministic execution for tests: `drain/3` claims and runs
  ready jobs in the calling process until the queue is empty, following
  workflow releases as they happen.
  """

  alias Capstan.{Config, Runner}

  @doc """
  Drain a queue synchronously. Returns counts by terminal outcome, e.g.
  `%{succeeded: 3, failed: 1}`. Only claims jobs that are due at the current
  clock time — advance a `Capstan.Clock.Sim` to reach scheduled work.
  """
  def drain(name, queue, opts \\ []) do
    config = Config.fetch!(name)
    spec = Capstan.Queues.resolve_spec!(config, queue)
    max_iterations = Keyword.get(opts, :max_iterations, 1_000)

    do_drain(config, spec, %{}, max_iterations)
  end

  defp do_drain(_config, _spec, acc, 0), do: acc

  defp do_drain(config, spec, acc, iterations) do
    {storage, ref} = config.storage_ref

    {:ok, jobs} =
      storage.claim(ref, spec, spec.local_limit, config.node_id, config.lease_ttl,
        Config.now(config))

    case jobs do
      [] ->
        acc

      jobs ->
        acc =
          Enum.reduce(jobs, acc, fn job, acc ->
            case Runner.execute(config, job) do
              {:ok, acked, _released} ->
                Map.update(acc, String.to_atom(acked.state), 1, &(&1 + 1))

              {:error, :stale} ->
                Map.update(acc, :stale, 1, &(&1 + 1))
            end
          end)

        do_drain(config, spec, acc, iterations - 1)
    end
  end
end
