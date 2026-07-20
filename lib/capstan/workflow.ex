defmodule Capstan.Workflow do
  @moduledoc """
  Composed jobs with directed acyclic dependencies.

      alias Capstan.Workflow

      Workflow.new()
      |> Workflow.add(:fetch, MyApp.Fetch.new(%{id: 1}))
      |> Workflow.add(:parse, MyApp.Parse.new(%{}), deps: [:fetch])
      |> Workflow.add(:store, MyApp.Store.new(%{}), deps: [:parse])
      |> Workflow.insert(MyOban)

  Jobs with dependencies are inserted in the `suspended` state and released to
  `available` by the engine as their upstream jobs complete. When an upstream
  job is cancelled or discarded, dependents are cancelled by default; pass
  `ignore_cancelled: true` / `ignore_discarded: true` (workflow-wide via
  `new/1`, or per job via `add/4`) to treat those outcomes as completion.

  Requires `Capstan.Engine` to be configured, since dependency release rides on
  engine completion interception.
  """

  import Ecto.Query

  alias Capstan.Query
  alias Ecto.Changeset
  alias Oban.{Job, Notifier, Repo}

  defstruct id: nil, jobs: [], names: MapSet.new(), opts: %{}

  @type t :: %__MODULE__{}

  @doc "Create a new workflow. Options: `:workflow_id`, `:ignore_cancelled`, `:ignore_discarded`."
  def new(opts \\ []) do
    %__MODULE__{
      id: Keyword.get(opts, :workflow_id, Ecto.UUID.generate()),
      opts: %{
        ignore_cancelled: Keyword.get(opts, :ignore_cancelled, false),
        ignore_discarded: Keyword.get(opts, :ignore_discarded, false)
      }
    }
  end

  @doc """
  Add a named job to the workflow.

  `deps` must reference previously added names, which makes cycles impossible
  by construction.
  """
  def add(%__MODULE__{} = workflow, name, %Changeset{} = changeset, opts \\ []) do
    name = to_string(name)
    deps = opts |> Keyword.get(:deps, []) |> Enum.map(&to_string/1)

    if MapSet.member?(workflow.names, name) do
      raise ArgumentError, "workflow already contains a job named #{inspect(name)}"
    end

    case Enum.reject(deps, &MapSet.member?(workflow.names, &1)) do
      [] -> :ok
      missing -> raise ArgumentError, "unknown workflow deps: #{inspect(missing)}"
    end

    entry = %{name: name, changeset: changeset, deps: deps, opts: Map.new(opts)}

    %{workflow | jobs: [entry | workflow.jobs], names: MapSet.put(workflow.names, name)}
  end

  @doc "Convert the workflow into insertable changesets (in add order)."
  def to_changesets(%__MODULE__{} = workflow) do
    workflow.jobs
    |> Enum.reverse()
    |> Enum.map(fn %{name: name, changeset: changeset, deps: deps, opts: opts} ->
      meta =
        changeset
        |> Changeset.get_field(:meta)
        |> Kernel.||(%{})
        |> Map.merge(%{
          "workflow_id" => workflow.id,
          "workflow_name" => name,
          "workflow_deps" => deps,
          "workflow_ignore_cancelled" =>
            Map.get(opts, :ignore_cancelled, workflow.opts.ignore_cancelled),
          "workflow_ignore_discarded" =>
            Map.get(opts, :ignore_discarded, workflow.opts.ignore_discarded)
        })

      changeset = Changeset.put_change(changeset, :meta, meta)

      if deps == [] do
        changeset
      else
        Changeset.put_change(changeset, :state, "suspended")
      end
    end)
  end

  @doc "Insert all workflow jobs (pipe-friendly). Returns `{:ok, %{name => job}}`."
  def insert(%__MODULE__{} = workflow), do: insert(workflow, Oban)

  def insert(%__MODULE__{} = workflow, oban) do
    jobs = Oban.insert_all(oban, to_changesets(workflow))

    {:ok, Map.new(jobs, &{&1.meta["workflow_name"], &1})}
  end

  @doc "All jobs currently in the given workflow."
  def all_jobs(workflow_id) when is_binary(workflow_id), do: all_jobs(Oban, workflow_id)

  def all_jobs(oban, workflow_id) do
    conf = Oban.config(oban)
    d = Query.dialect(conf)

    Repo.all(conf, Job |> where(^Query.meta_eq(d, "workflow_id", workflow_id)) |> order_by(:id))
  end

  @doc "Summarized status: `%{state_counts: %{...}, total: n, done?: bool}`."
  def status(workflow_id) when is_binary(workflow_id), do: status(Oban, workflow_id)

  def status(oban, workflow_id) do
    jobs = all_jobs(oban, workflow_id)
    counts = Enum.frequencies_by(jobs, & &1.state)
    done? = Enum.all?(jobs, &(&1.state in ~w(completed cancelled discarded)))

    %{total: length(jobs), state_counts: counts, done?: done?}
  end

  # -- Engine-driven advancement ------------------------------------------------

  @doc false
  def advance(conf, job, :completed) do
    release_dependents(conf, job.meta["workflow_id"], job.meta["workflow_name"])
  end

  def advance(conf, job, failure) when failure in [:cancelled, :discarded] do
    cascade_failure(conf, job.meta["workflow_id"])
  end

  @doc false
  def release_dependents(conf, wf_id, completed_name) do
    d = Query.dialect(conf)

    candidates =
      Repo.all(
        conf,
        Job
        |> where([j], j.state == "suspended")
        |> where(^Query.meta_eq(d, "workflow_id", wf_id))
        |> where(^Query.deps_contain(d, completed_name))
      )

    if candidates != [], do: release_satisfied(conf, wf_id, candidates)

    :ok
  end

  defp release_satisfied(conf, wf_id, candidates) do
    states = name_states(conf, wf_id)

    releasable = Enum.filter(candidates, &deps_satisfied?(&1.meta, states))

    if releasable != [] do
      ids = Enum.map(releasable, & &1.id)

      Repo.update_all(
        conf,
        Job |> where([j], j.id in ^ids) |> where([j], j.state == "suspended"),
        set: [state: "available", scheduled_at: DateTime.utc_now()]
      )

      notify_queues(conf, releasable)
    end
  end

  defp deps_satisfied?(meta, states) do
    Enum.all?(meta["workflow_deps"] || [], fn dep ->
      case states[dep] do
        "completed" -> true
        "cancelled" -> meta["workflow_ignore_cancelled"] == true
        "discarded" -> meta["workflow_ignore_discarded"] == true
        _ -> false
      end
    end)
  end

  # On a failed upstream: cancel every suspended job that can no longer run,
  # release any that became satisfied through ignore flags. Transitive effects
  # are computed in one pass from a full in-memory view of the workflow.
  defp cascade_failure(conf, wf_id) do
    jobs = all_jobs_light(conf, wf_id)
    states = Map.new(jobs, &{&1.name, &1.state})

    {victims, released, _} = settle(jobs, states)

    if victims != [] do
      Repo.update_all(
        conf,
        Job |> where([j], j.id in ^Enum.map(victims, & &1.id) and j.state == "suspended"),
        set: [state: "cancelled", cancelled_at: DateTime.utc_now()]
      )

      for victim <- victims, victim.full != nil do
        Capstan.Lifecycle.transitioned(conf, %{victim.full | state: "cancelled"}, :cancelled)
      end
    end

    if released != [] do
      Repo.update_all(
        conf,
        Job |> where([j], j.id in ^Enum.map(released, & &1.id) and j.state == "suspended"),
        set: [state: "available", scheduled_at: DateTime.utc_now()]
      )

      notify_queues(conf, Enum.map(released, & &1.full))
    end

    :ok
  end

  # Iterate to a fixpoint: a suspended job whose dep is terminally failed is a
  # victim (unless it ignores that outcome); victims count as cancelled for
  # their own dependents.
  defp settle(jobs, states) do
    suspended = Enum.filter(jobs, &(&1.state == "suspended"))

    Enum.reduce_while(1..length(suspended)//1, {[], [], states}, fn _, {victims, released, states} ->
      {new_victims, new_released, new_states} =
        Enum.reduce(suspended, {victims, released, states}, fn job, {v, r, s} ->
          cond do
            s[job.name] != "suspended" ->
              {v, r, s}

            doomed?(job, s) ->
              {[job | v], r, Map.put(s, job.name, "cancelled")}

            deps_satisfied?(job.meta, s) ->
              {v, [job | r], Map.put(s, job.name, "available")}

            true ->
              {v, r, s}
          end
        end)

      if new_states == states do
        {:halt, {new_victims, new_released, new_states}}
      else
        {:cont, {new_victims, new_released, new_states}}
      end
    end)
    |> then(fn {v, r, s} -> {v, r, s} end)
  end

  defp doomed?(job, states) do
    Enum.any?(job.meta["workflow_deps"] || [], fn dep ->
      case states[dep] do
        "cancelled" -> job.meta["workflow_ignore_cancelled"] != true
        "discarded" -> job.meta["workflow_ignore_discarded"] != true
        _ -> false
      end
    end)
  end

  defp all_jobs_light(conf, wf_id) do
    conf
    |> all_jobs_by_conf(wf_id)
    |> Enum.map(fn job ->
      %{id: job.id, name: job.meta["workflow_name"], state: job.state, meta: job.meta, full: job}
    end)
  end

  defp all_jobs_by_conf(conf, wf_id) do
    d = Query.dialect(conf)

    Repo.all(conf, Job |> where(^Query.meta_eq(d, "workflow_id", wf_id)) |> order_by(:id))
  end

  defp name_states(conf, wf_id) do
    conf
    |> all_jobs_by_conf(wf_id)
    |> Map.new(&{&1.meta["workflow_name"], &1.state})
  end

  defp notify_queues(conf, jobs) do
    payload = for job <- jobs, uniq: true, do: %{queue: job.queue}

    Notifier.notify(conf, :insert, payload)
  catch
    _, _ -> :ok
  end
end
