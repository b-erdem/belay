defmodule Belay.Replay do
  @moduledoc """
  Time-travel debugging: re-run a job's *code* against its *recorded journal*.

      case Belay.Replay.dry_run(MyBelay, job_id) do
        {:ok, result, trace} -> ...      # code path fully covered by the journal
        {:blocked, why, trace} -> ...    # hit something the recording never saw
        {:raised, error, trace} -> ...   # the code itself raised during replay
      end

  In a dry run every memoized read (steps, sleeps, spawns) returns its
  recorded value, `emit/2` and `debit/3` are inert, and nothing else touches
  the world. That makes two workflows possible:

    * **Post-mortem** — replay a failed job locally and step through the exact
      decisions it made, with the exact intermediate values it saw.
    * **Divergence detection** — after changing worker code, replay recorded
      jobs: `{:blocked, {:missing_step, name}, _}` pinpoints where the new
      code's path departs from what production actually executed.

  Dry runs never ack, never consume attempts, and are safe against live jobs.
  """

  alias Belay.{Config, Ctx, Job}

  @type trace :: [map()]

  @doc "Replay `job_id`'s worker against its journal without side effects."
  @spec dry_run(term(), integer()) ::
          {:ok, term(), trace}
          | {:blocked, term(), trace}
          | {:raised, {atom(), term()}, trace}
          | {:error, :not_found}
  def dry_run(name, job_id) do
    config = Config.fetch!(name)
    {storage, ref} = config.storage_ref

    with {:ok, job} <- fetch(storage, ref, job_id) do
      {:ok, trace} = storage.list_steps(ref, job_id)
      job = Belay.Runner.decrypt_for_replay(config, job)
      ctx = %Ctx{job: job, belay: name, config: config, replay?: true}

      try do
        {:ok, Job.worker_module!(job).run(ctx), trace}
      catch
        :throw, {:belay_replay, reason} -> {:blocked, reason, trace}
        :throw, {:belay_control, control} -> {:blocked, {:control, elem(control, 0)}, trace}
        kind, reason -> {:raised, {kind, reason}, trace}
      end
    end
  end

  defp fetch(storage, ref, job_id) do
    case storage.get_job(ref, job_id) do
      {:ok, job} -> {:ok, job}
      :error -> {:error, :not_found}
    end
  end
end
