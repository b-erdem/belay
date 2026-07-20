defmodule Capstan.Storage do
  @moduledoc """
  The storage contract: coarse, semantic, individually-atomic operations.

  Engine semantics that must be transactional (admission control inside claim,
  workflow settlement inside ack) live inside single operations, so adapters
  guarantee atomicity per call. Two adapters ship: `Capstan.Storage.Memory`
  (serialized GenServer, deterministic — the test/simulation reference) and
  `Capstan.Storage.Postgres`.

  All time-dependent operations take `now` explicitly; adapters never read the
  wall clock.
  """

  alias Capstan.Job

  @type ref :: term()
  @type now :: DateTime.t()
  @type queue_spec :: %{
          queue: String.t(),
          local_limit: pos_integer(),
          global_limit: pos_integer() | nil,
          rate: %{allowed: pos_integer(), period: pos_integer()} | nil,
          partition: {:input | :meta, String.t()} | nil
        }
  @type outcome ::
          {:succeeded, binary() | nil}
          | {:retry, map(), now()}
          | {:failed, map()}
          | {:cancelled, map()}
          | {:snooze, now()}
          | {:await, String.t(), String.t(), now() | nil}
  @type settle_result :: %{job: Job.t(), released: [Job.t()], cancelled: [Job.t()]}

  @callback child_spec({Capstan.Config.t(), keyword()}) :: Supervisor.child_spec()
  @callback insert_jobs(ref, [map()], now) :: {:ok, [Job.t()]}
  @callback claim(ref, queue_spec, demand :: pos_integer(), node_id :: String.t(),
              lease_ttl_ms :: pos_integer(), now) :: {:ok, [Job.t()]}
  @callback renew_leases(ref, [integer()], String.t(), now) :: {:ok, [integer()]}
  @callback reclaim_expired(ref, now, (Job.t() -> now())) ::
              {:ok, %{retried: [integer()], failed: [integer()]}}
  @callback ack(ref, Job.t(), outcome, now) :: {:ok, settle_result} | {:error, :stale}
  @callback get_job(ref, integer()) :: {:ok, Job.t()} | :error
  @callback get_step(ref, integer(), String.t()) :: {:ok, binary()} | :none
  @callback put_step(ref, integer(), String.t(), binary(), %{usd_micros: integer(), tokens: integer()}, now) ::
              {:ok, %{spent_usd_micros: integer(), spent_tokens: integer()}}
  @callback list_steps(ref, integer()) :: {:ok, [map()]}
  @callback get_signal(ref, [String.t()], String.t()) :: {:ok, map()} | :none
  @callback put_signal(ref, String.t(), String.t(), map(), now) :: {:ok, woken :: [Job.t()]}
  @callback clear_signal(ref, String.t(), String.t()) :: :ok
  @callback request_cancel(ref, integer(), now) ::
              {:ok, %{status: :cancelled | :requested | :noop, cancelled: [Job.t()], released: [Job.t()]}}
  @callback workflow_jobs(ref, String.t()) :: {:ok, [Job.t()]}
  @callback get_by_unique_key(ref, String.t()) :: {:ok, Job.t()} | :error
  @callback children(ref, integer()) :: {:ok, [Job.t()]}
  @callback append_event(ref, integer(), map(), now) :: {:ok, seq :: integer()}
  @callback list_events(ref, integer(), after_seq :: integer()) :: {:ok, [map()]}
  @callback queue_stats(ref) :: {:ok, [%{queue: String.t(), state: String.t(), count: integer()}]}
  @callback list_jobs(ref, map()) :: {:ok, [Job.t()]}
  @callback retry(ref, integer(), now) :: {:ok, Job.t()} | {:error, :not_retryable | :not_found}
  @callback prune_jobs(ref, state :: String.t(), now, keep_seconds :: integer(), limit :: pos_integer()) ::
              {:ok, non_neg_integer()}
  @callback resettle_parents(ref, now) :: {:ok, [Job.t()]}
  @callback prune_signals(ref, now, ttl_seconds :: integer()) :: :ok
  @callback debit_rate(ref, bucket :: String.t(), period :: pos_integer(), amount :: integer(), now) :: :ok
  @callback prune_rate(ref, before_unix :: integer()) :: :ok
end

defmodule Capstan.Storage.Logic do
  @moduledoc false

  # Pure engine logic shared by every storage adapter, so semantics can't
  # drift between Memory and Postgres.

  alias Capstan.Job

  @doc """
  Workflow settlement fixpoint over the full set of a workflow's jobs.

  A held job releases when every dep is satisfied (succeeded, or terminal-failed
  with a matching ignore flag) and dooms when any dep terminally failed without
  an ignore flag. Doomed jobs count as cancelled for their own dependents.

  Returns `{release_ids, cancel_ids}`.
  """
  def settle(jobs) do
    states = Map.new(jobs, &{&1.wf_name, &1.state})
    held = Enum.filter(jobs, &(&1.state == "held" and &1.wf_deps != []))

    do_settle(held, states, [], [])
  end

  defp do_settle(held, states, releases, cancels) do
    {new_releases, new_cancels, new_states} =
      Enum.reduce(held, {releases, cancels, states}, fn job, {rel, can, st} ->
        cond do
          st[job.wf_name] != "held" -> {rel, can, st}
          doomed?(job, st) -> {rel, [job.id | can], Map.put(st, job.wf_name, "cancelled")}
          satisfied?(job, st) -> {[job.id | rel], can, Map.put(st, job.wf_name, "ready")}
          true -> {rel, can, st}
        end
      end)

    if new_states == states do
      {new_releases, new_cancels}
    else
      do_settle(held, new_states, new_releases, new_cancels)
    end
  end

  defp satisfied?(job, states) do
    Enum.all?(job.wf_deps, fn dep ->
      case states[dep] do
        "succeeded" -> true
        "cancelled" -> "cancelled" in job.wf_ignore
        "failed" -> "failed" in job.wf_ignore
        _ -> false
      end
    end)
  end

  defp doomed?(job, states) do
    Enum.any?(job.wf_deps, fn dep ->
      case states[dep] do
        "cancelled" -> "cancelled" not in job.wf_ignore
        "failed" -> "failed" not in job.wf_ignore
        _ -> false
      end
    end)
  end

  @doc """
  Sliding-window allowance: current window plus overlap-weighted previous.
  Capped at `allowed` so post-hoc credits (actual usage below the estimate)
  can't inflate the window beyond its configured budget.
  """
  def rate_allowance(prev_count, curr_count, allowed, period, now_unix) do
    win = window_start(now_unix, period)
    elapsed_frac = (now_unix - win) / period
    used = curr_count + round(prev_count * (1.0 - elapsed_frac))

    allowed |> min(allowed - used) |> max(0)
  end

  @doc "The rate-counter bucket for a queue's rate spec."
  def rate_bucket(%{resource: nil}, queue), do: "queue:" <> queue
  def rate_bucket(%{resource: resource}, _queue), do: "resource:" <> resource

  def window_start(now_unix, period), do: div(now_unix, period) * period

  @doc """
  Pick claimable candidates under a per-key partition limit. Never picks more
  than `take` total. Candidates must already be sorted.
  """
  def partition_take(candidates, take, running_counts, per_key_limit, key_fun) do
    {picked, _counts} =
      Enum.reduce(candidates, {[], running_counts}, fn job, {picked, counts} ->
        key = key_fun.(job)

        if length(picked) < take and Map.get(counts, key, 0) < per_key_limit do
          {[job | picked], Map.update(counts, key, 1, &(&1 + 1))}
        else
          {picked, counts}
        end
      end)

    Enum.reverse(picked)
  end

  @doc "Extract a job's partition key value."
  def partition_key(job, {:input, key}), do: to_string(Map.get(job.input || %{}, key))
  def partition_key(job, {:meta, key}), do: to_string(Map.get(job.meta || %{}, key))

  @doc "Apply an ack outcome to a job struct (pure). Returns the updated job."
  def apply_outcome(%Job{} = job, outcome, now) do
    case outcome do
      {:succeeded, result_bin} ->
        %{job | state: "succeeded", result: result_bin, finished_at: now}
        |> clear_execution()

      {:retry, error, ready_at} ->
        %{job | state: "ready", ready_at: ready_at, errors: job.errors ++ [error]}
        |> clear_execution()

      {:failed, error} ->
        %{job | state: "failed", errors: job.errors ++ [error], finished_at: now}
        |> clear_execution()

      {:cancelled, reason} ->
        %{job | state: "cancelled", errors: job.errors ++ [reason], finished_at: now}
        |> clear_execution()

      {:snooze, ready_at} ->
        %{job | state: "ready", ready_at: ready_at, attempt: job.attempt - 1}
        |> clear_execution()

      {:await, scope, name, deadline} ->
        %{
          job
          | state: "awaiting",
            await_scope: scope,
            await_name: name,
            ready_at: deadline,
            attempt: job.attempt - 1
        }
        |> clear_execution()
    end
  end

  defp clear_execution(job) do
    %{job | lease_until: nil, leased_by: nil}
  end

  @doc "Is the job claimable at `now`? (ready and due, or awaiting past deadline)"
  def claimable?(%Job{state: "ready", ready_at: at}, now), do: due?(at, now)

  def claimable?(%Job{state: "awaiting", ready_at: at}, now) when not is_nil(at) do
    due?(at, now)
  end

  def claimable?(_job, _now), do: false

  defp due?(nil, _now), do: true
  defp due?(at, now), do: DateTime.compare(at, now) != :gt

  @doc "Live-leased running check."
  def live_running?(%Job{state: "running", lease_until: until}, now) when not is_nil(until) do
    DateTime.compare(until, now) == :gt
  end

  def live_running?(_job, _now), do: false
end
