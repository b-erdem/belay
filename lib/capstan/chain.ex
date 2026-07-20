defmodule Capstan.Chain do
  @moduledoc """
  Strict FIFO execution per key: at most one incomplete job per chain key, in
  insertion order.

  Stamp jobs with a chain key in meta:

      MyApp.SyncAccount.new(%{"id" => 1}, meta: %{"chain_key" => "account:1"})

  The engine holds a new job in `suspended` whenever an earlier job with the
  same key is incomplete, and releases the next held job when its predecessor
  reaches a final state.

  Failure policy via `meta: %{"chain_policy" => "continue" | "halt"}`:
  `"continue"` (default) advances past cancelled/discarded links, `"halt"`
  leaves the chain held until `resume/2` or a successful retry. Chain keys are
  global (not scoped per queue). Requires `Capstan.Engine`.
  """

  import Ecto.Query

  alias Capstan.Query
  alias Ecto.Changeset
  alias Oban.{Job, Notifier, Repo}

  @incomplete ~w(suspended available scheduled executing retryable)

  @doc false
  def maybe_hold(conf, %Changeset{} = changeset) do
    meta = Changeset.get_field(changeset, :meta) || %{}

    case meta["chain_key"] do
      nil ->
        changeset

      key ->
        if predecessor?(conf, key) do
          Changeset.put_change(changeset, :state, "suspended")
        else
          changeset
        end
    end
  end

  defp predecessor?(conf, key) do
    d = Query.dialect(conf)

    query =
      Job
      |> where([j], j.state in @incomplete)
      |> where(^Query.meta_eq(d, "chain_key", key))
      |> limit(1)

    Repo.all(conf, query) != []
  end

  # -- Engine-driven advancement ------------------------------------------------

  @doc false
  def advance(conf, job, state) do
    policy = job.meta["chain_policy"] || "continue"

    cond do
      state == :completed -> release_next(conf, job.meta["chain_key"])
      policy == "continue" -> release_next(conf, job.meta["chain_key"])
      true -> :ok
    end
  end

  @doc "Manually release the next held job in a chain (for `\"halt\"` policies)."
  def resume(key) when is_binary(key), do: resume(Oban, key)

  def resume(oban, key) do
    release_next(Oban.config(oban), key)
  end

  @doc false
  def release_next(conf, key) do
    d = Query.dialect(conf)

    next =
      Repo.all(
        conf,
        Job
        |> where([j], j.state == "suspended")
        |> where(^Query.meta_eq(d, "chain_key", key))
        |> order_by(asc: :id)
        |> limit(1)
      )

    case next do
      [job] ->
        Repo.update_all(
          conf,
          Job |> where([j], j.id == ^job.id and j.state == "suspended"),
          set: [state: "available", scheduled_at: DateTime.utc_now()]
        )

        safe_notify(conf, job.queue)

      [] ->
        :ok
    end

    :ok
  end

  defp safe_notify(conf, queue) do
    Notifier.notify(conf, :insert, [%{queue: queue}])
  catch
    _, _ -> :ok
  end
end
