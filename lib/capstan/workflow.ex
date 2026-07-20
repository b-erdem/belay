defmodule Capstan.Workflow do
  @moduledoc """
  Jobs composed with directed acyclic dependencies.

      alias Capstan.Workflow

      {:ok, jobs} =
        Workflow.new()
        |> Workflow.add(:fetch, MyApp.Fetch.new(%{"id" => 1}))
        |> Workflow.add(:parse, MyApp.Parse.new(%{}), deps: [:fetch])
        |> Workflow.add(:store, MyApp.Store.new(%{}), deps: [:parse])
        |> Workflow.insert(MyCapstan)

  Dependent jobs are inserted `held` and released transactionally as their
  upstreams succeed. When an upstream fails or is cancelled, dependents cancel
  by default; `ignore: [:failed]` / `ignore: [:cancelled]` (per job or
  workflow-wide) treats that outcome as satisfied instead.
  """

  alias Capstan.Config

  defstruct id: nil, entries: [], names: MapSet.new(), ignore: []

  @type t :: %__MODULE__{}

  @doc "Create a workflow. Options: `:workflow_id`, `:ignore` ([:failed | :cancelled])."
  def new(opts \\ []) do
    %__MODULE__{
      id: Keyword.get(opts, :workflow_id, random_id()),
      ignore: opts |> Keyword.get(:ignore, []) |> Enum.map(&to_string/1)
    }
  end

  @doc "Add a named job. `deps` must reference previously added names."
  def add(%__MODULE__{} = workflow, name, {Capstan.Worker, worker, input, opts}, wf_opts \\ []) do
    name = to_string(name)
    deps = wf_opts |> Keyword.get(:deps, []) |> Enum.map(&to_string/1)

    if MapSet.member?(workflow.names, name) do
      raise ArgumentError, "workflow already contains a job named #{inspect(name)}"
    end

    case Enum.reject(deps, &MapSet.member?(workflow.names, &1)) do
      [] -> :ok
      missing -> raise ArgumentError, "unknown workflow deps: #{inspect(missing)}"
    end

    ignore =
      wf_opts |> Keyword.get(:ignore, workflow.ignore) |> Enum.map(&to_string/1)

    entry = %{name: name, worker: worker, input: input, opts: opts, deps: deps, ignore: ignore}

    %{workflow | entries: [entry | workflow.entries], names: MapSet.put(workflow.names, name)}
  end

  @doc "Insert all workflow jobs atomically. Returns `{:ok, %{name => job}}`."
  def insert(%__MODULE__{} = workflow, name) do
    config = Config.fetch!(name)
    {storage, ref} = config.storage_ref
    now = Config.now(config)

    rows =
      workflow.entries
      |> Enum.reverse()
      |> Enum.map(fn entry ->
        opts =
          entry.opts
          |> Keyword.merge(
            now: now,
            workflow_id: workflow.id,
            wf_name: entry.name,
            wf_deps: entry.deps,
            wf_ignore: entry.ignore
          )
          |> Keyword.put(:state, if(entry.deps == [], do: "ready", else: "held"))
          |> Keyword.put(:encryption_key, Config.encryption_key(config))

        Capstan.Job.new(entry.worker, entry.input, opts, entry.worker.__capstan_defaults__())
      end)

    {:ok, jobs} = storage.insert_jobs(ref, rows, now)

    jobs |> Enum.map(& &1.queue) |> Enum.uniq() |> Enum.each(&Capstan.poke(config, &1))

    {:ok, Map.new(jobs, &{&1.wf_name, &1})}
  end

  @doc "All jobs in a workflow."
  def jobs(name, workflow_id) do
    config = Config.fetch!(name)
    {storage, ref} = config.storage_ref

    {:ok, jobs} = storage.workflow_jobs(ref, workflow_id)

    jobs
  end

  @doc "Status summary: `%{total:, state_counts:, done?:}`."
  def status(name, workflow_id) do
    jobs = jobs(name, workflow_id)
    counts = Enum.frequencies_by(jobs, & &1.state)

    %{
      total: length(jobs),
      state_counts: counts,
      done?: jobs != [] and Enum.all?(jobs, &Capstan.Job.terminal?/1)
    }
  end

  defp random_id do
    :crypto.strong_rand_bytes(16) |> Base.encode16(case: :lower)
  end
end
