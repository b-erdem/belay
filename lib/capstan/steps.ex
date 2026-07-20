defmodule Capstan.Steps do
  @moduledoc """
  Durable, memoized steps inside a job — the primitive that makes retries cheap
  and long agent-style jobs practical.

  A step runs at most once per job: its return value is persisted keyed by
  `(job_id, name)`, and every later attempt of the same job returns the stored
  value without re-running the function. Combined with `await_signal/3` this
  gives Oban jobs a durable-execution flavor: a job can crash, retry, and resume
  past its expensive completed work (LLM calls, payments, API requests).

      def process(job) do
        text = Capstan.step(job, :transcribe, fn -> transcribe!(job.args) end)
        summary = Capstan.step(job, :summarize, fn -> llm_summarize!(text) end)

        %{"approved" => true} = Capstan.await_signal(job, :approval)

        Capstan.step(job, :publish, fn -> publish!(summary) end)
        :ok
      end

  Semantics:

    * A value **returned** from the step function is memoized — including
      `{:error, _}` tuples. Raise or throw to leave the step un-memoized so a
      retry re-runs it.
    * Results are serialized with `:erlang.term_to_binary/1`. Do not return
      pids, refs, or functions.
    * `await_signal/3` snoozes the job when the signal has not arrived yet.
      Completed steps make the replay on wake-up effectively free.
  """

  import Ecto.Query

  alias Capstan.{Query, Signal, Step}
  alias Oban.{Job, Notifier, Repo}

  @default_resnooze 60
  @max_result_bytes 512 * 1024

  @doc "Run `fun` at most once per job, memoizing its return value."
  def step(%Job{id: job_id, conf: conf} = job, name, fun)
      when not is_nil(conf) and is_function(fun, 0) do
    name = to_string(name)

    query = from(s in Step, where: s.job_id == ^job_id and s.name == ^name, select: s.result)

    case Repo.all(conf, query) do
      [bin | _] ->
        :erlang.binary_to_term(bin)

      [] ->
        result = fun.()
        bin = :erlang.term_to_binary(result)

        if byte_size(bin) > @max_result_bytes do
          raise ArgumentError,
                "step #{name} result is #{byte_size(bin)} bytes, exceeding the " <>
                  "#{@max_result_bytes} byte limit for memoized step results"
        end

        row = %{
          job_id: job_id,
          name: name,
          attempt: job.attempt,
          result: bin,
          inserted_at: DateTime.utc_now()
        }

        Repo.insert_all(conf, Step, [row], on_conflict: :nothing)

        result
    end
  end

  @doc """
  Wait for a named signal, snoozing the job until it arrives.

  Checks the `"job:<id>"` scope and, when the job belongs to a workflow, the
  `"wf:<workflow_id>"` scope. Returns the signal payload (a map). When the
  signal is missing the job snoozes for `:resnooze` seconds (default 60) —
  `Capstan.signal/4` wakes it early, so the snooze is only a fallback bound.

  Options: `:scope` (extra scope string), `:resnooze` (seconds).
  """
  def await_signal(%Job{conf: conf} = job, name, opts \\ []) when not is_nil(conf) do
    name = to_string(name)
    scopes = signal_scopes(job, opts)
    resnooze = Keyword.get(opts, :resnooze, @default_resnooze)

    stamp_awaiting(job, scopes, name, resnooze)

    case lookup_signal(conf, scopes, name) do
      {:ok, payload} ->
        clear_awaiting(job)
        payload

      :missing ->
        throw({:capstan_snooze, resnooze})
    end
  end

  @doc """
  Deliver a signal: persist it and wake any jobs awaiting it.

  `scope` is a string such as `"job:123"`, `"wf:<workflow-id>"`, or any
  application-defined key. Signals persist until explicitly deleted, so an
  awaiting job that races the delivery still finds the signal on its next
  attempt.
  """
  def signal(oban, scope, name, payload) when is_map(payload) do
    conf = Oban.config(oban)
    scope = to_string(scope)
    name = to_string(name)

    row = %{scope: scope, name: name, payload: payload, inserted_at: DateTime.utc_now()}

    Repo.insert_all(conf, Signal, [row],
      on_conflict: {:replace, [:payload, :inserted_at]},
      conflict_target: [:scope, :name]
    )

    wake_awaiting(conf, scope, name)

    :ok
  end

  @doc "Delete a signal so future `await_signal/3` calls block again."
  def clear_signal(oban, scope, name) do
    conf = Oban.config(oban)

    query =
      from(s in Signal, where: s.scope == ^to_string(scope) and s.name == ^to_string(name))

    Repo.delete_all(conf, query)

    :ok
  end

  @doc false
  def wake_awaiting(conf, scope, name) do
    d = Query.dialect(conf)

    wake_query =
      Job
      |> where([j], j.state == "scheduled")
      |> where(^Query.meta_eq(d, "awaiting_scope", scope))
      |> where(^Query.meta_eq(d, "awaiting_name", name))

    queues = Repo.all(conf, select(wake_query, [j], j.queue))

    if queues != [] do
      Repo.update_all(conf, wake_query,
        set: [state: "available", scheduled_at: DateTime.utc_now()]
      )

      payload = for queue <- Enum.uniq(queues), do: %{queue: queue}
      safe_notify(conf, payload)
    end

    :ok
  end

  @doc false
  def lookup_signal(conf, scopes, name) do
    query =
      from(s in Signal,
        where: s.scope in ^scopes and s.name == ^name,
        select: s.payload,
        limit: 1
      )

    case Repo.all(conf, query) do
      [payload | _] -> {:ok, payload || %{}}
      [] -> :missing
    end
  end

  defp signal_scopes(%Job{id: id, meta: meta}, opts) do
    base = ["job:#{id}"]
    base = if wf = meta["workflow_id"], do: ["wf:#{wf}" | base], else: base

    case Keyword.get(opts, :scope) do
      nil -> base
      scope -> [to_string(scope) | base]
    end
  end

  defp stamp_awaiting(%Job{conf: conf, id: id, meta: meta}, [scope | _], name, resnooze) do
    awaiting = %{
      "awaiting_scope" => scope,
      "awaiting_name" => name,
      "awaiting_resnooze" => resnooze
    }

    # The job process owns its row while executing, so read-modify-write is safe.
    Repo.update_all(conf, where(Job, id: ^id), set: [meta: Map.merge(meta, awaiting)])
  end

  defp clear_awaiting(%Job{conf: conf, id: id, meta: meta}) do
    meta = Map.drop(meta, ["awaiting_scope", "awaiting_name", "awaiting_resnooze"])

    Repo.update_all(conf, where(Job, id: ^id), set: [meta: meta])
  end

  defp safe_notify(conf, payload) do
    Notifier.notify(conf, :insert, payload)
  catch
    _, _ -> :ok
  end
end
