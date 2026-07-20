defmodule Capstan do
  @moduledoc """
  A standalone, agent-native durable job engine on Postgres (or in-memory for
  tests). No Oban, no Ecto — Postgrex, Jason, and telemetry only.

  ## Start an instance

      children = [
        {Capstan,
         name: MyCapstan,
         storage: [adapter: :postgres, url: "postgres://localhost/my_app"],
         queues: [
           default: 10,
           ai: [limit: 5, global_limit: 2, rate: [allowed: 60, period: 60]]
         ],
         crons: [
           [name: "digest", expr: "0 8 * * *", worker: MyApp.Digest]
         ]}
      ]

  ## Define work

      defmodule MyApp.Agent do
        use Capstan.Worker, queue: :ai, max_attempts: 10

        @impl Capstan.Worker
        def run(ctx) do
          text = Capstan.step(ctx, :fetch, fn -> fetch!(ctx.job.input["url"]) end)

          summary =
            Capstan.step(ctx, :summarize, fn -> llm!(text) end,
              cost: [usd: 0.02, tokens: 1200]
            )

          %{"approved" => true} = Capstan.await(ctx, :approval)

          {:ok, summary}
        end
      end

      Capstan.insert(MyCapstan, MyApp.Agent.new(%{"url" => url}, budget: [usd: 1.0]))

  Steps are memoized per job — retries replay past completed work. Budgets
  fail the job with `:budget_exceeded` when step costs cross the cap. Signals
  (`Capstan.signal_job/4`) wake awaiting jobs instantly; `steer/3` injects
  guidance readable via `steering/1` at step boundaries.
  """

  use Supervisor

  alias Capstan.{Config, Ctx, Job, Runner}

  # -- Supervision --------------------------------------------------------------

  def start_link(opts) do
    config = Config.new(opts)

    Supervisor.start_link(__MODULE__, config, name: Module.concat(config.name, "Supervisor"))
  end

  def child_spec(opts) do
    name = Keyword.get(opts, :name, Capstan)

    %{id: {__MODULE__, name}, start: {__MODULE__, :start_link, [opts]}, type: :supervisor}
  end

  @impl Supervisor
  def init(config) do
    {storage_mod, storage_opts} = config.storage

    config = %{config | storage_ref: {storage_mod, storage_mod.ref(config.name)}}

    Config.put(config)

    producers =
      for {queue, spec} <- config.queues, not spec.manual do
        Supervisor.child_spec({Capstan.Producer, {config, queue}}, id: {:producer, queue})
      end

    children =
      [
        {Registry, keys: :unique, name: registry(config.name)},
        {Registry, keys: :duplicate, name: run_registry(config.name)},
        %{id: :pg, start: {:pg, :start_link, [pg_scope(config.name)]}},
        storage_mod.child_spec({config, storage_opts}),
        {Task.Supervisor, name: task_sup(config.name)},
        {Capstan.LeaseKeeper, config},
        {Capstan.Sweeper, config},
        {Capstan.CronScheduler, config}
      ] ++ producers

    Supervisor.init(children, strategy: :one_for_one)
  end

  @doc false
  def registry(name), do: Module.concat(name, "Registry")

  @doc false
  def run_registry(name), do: Module.concat(name, "RunRegistry")

  @doc false
  def task_sup(name), do: Module.concat(name, "Tasks")

  # -- Inserting ----------------------------------------------------------------

  @doc """
  Insert a job built with `WorkerModule.new(input, opts)`.

  Options on `new/2`: `:queue`, `:priority`, `:max_attempts`, `:schedule_in`,
  `:partition_key`, `:meta`, `:budget` (`[usd: 5.0, tokens: 100_000]`).
  """
  def insert(name, {Capstan.Worker, worker, input, opts}) do
    config = Config.fetch!(name)
    {storage, ref} = config.storage_ref
    now = Config.now(config)

    row = Job.new(worker, input, Keyword.put(opts, :now, now), worker.__capstan_defaults__())

    case storage.insert_jobs(ref, [row], now) do
      {:ok, [job]} ->
        poke(config, job.queue)

        {:ok, job}

      {:ok, []} when row.unique_key != nil ->
        # Deduped by uniqueness — hand back the live holder of the key.
        {:ok, existing} = storage.get_by_unique_key(ref, row.unique_key)

        {:ok, %{existing | duplicate?: true}}
    end
  end

  @doc """
  Insert many jobs at once. Rows deduped by uniqueness or cron slots are
  silently skipped; only inserted jobs are returned.
  """
  def insert_all(name, buildables) do
    config = Config.fetch!(name)
    {storage, ref} = config.storage_ref
    now = Config.now(config)

    rows =
      Enum.map(buildables, fn {Capstan.Worker, worker, input, opts} ->
        Job.new(worker, input, Keyword.put(opts, :now, now), worker.__capstan_defaults__())
      end)

    {:ok, jobs} = storage.insert_jobs(ref, rows, now)

    jobs |> Enum.map(& &1.queue) |> Enum.uniq() |> Enum.each(&poke(config, &1))

    jobs
  end

  # -- Introspection & control --------------------------------------------------

  def get_job(name, id) do
    config = Config.fetch!(name)
    {storage, ref} = config.storage_ref

    storage.get_job(ref, id)
  end

  @doc "Cancel a job: immediate for parked states, cooperative for running."
  def cancel(name, id) do
    config = Config.fetch!(name)
    {storage, ref} = config.storage_ref

    {:ok, result} = storage.request_cancel(ref, id, Config.now(config))

    {:ok, result.status}
  end

  @doc "List a job's recorded steps with costs."
  def steps(name, id) do
    config = Config.fetch!(name)
    {storage, ref} = config.storage_ref

    storage.list_steps(ref, id)
  end

  @doc "Per-queue, per-state job counts."
  def stats(name) do
    config = Config.fetch!(name)
    {storage, ref} = config.storage_ref
    {:ok, rows} = storage.queue_stats(ref)

    Enum.reduce(rows, %{}, fn %{queue: q, state: s, count: c}, acc ->
      Map.update(acc, q, %{s => c}, &Map.put(&1, s, c))
    end)
  end

  @doc """
  List jobs newest-first. Filters: `:queue`, `:state`, `:worker`,
  `:workflow_id`, `:parent_id`, `:before_id` (pagination cursor), `:limit`
  (default 50).
  """
  def list_jobs(name, filters \\ %{}) do
    config = Config.fetch!(name)
    {storage, ref} = config.storage_ref
    {:ok, jobs} = storage.list_jobs(ref, Map.new(filters))

    jobs
  end

  @doc "Resurrect a failed or cancelled job (grants one more attempt if exhausted)."
  def retry_job(name, id) do
    config = Config.fetch!(name)
    {storage, ref} = config.storage_ref

    with {:ok, job} <- storage.retry(ref, id, Config.now(config)) do
      poke(config, job.queue)

      {:ok, job}
    end
  end

  @doc "Stop a queue's local producer from claiming (running jobs finish)."
  def pause_queue(name, queue), do: send_producer(name, queue, :capstan_pause)

  @doc "Resume a paused queue."
  def resume_queue(name, queue), do: send_producer(name, queue, :capstan_resume)

  defp send_producer(name, queue, message) do
    case Registry.lookup(registry(name), {:producer, to_string(queue)}) do
      [{pid, _}] ->
        send(pid, message)
        :ok

      _ ->
        {:error, :no_producer}
    end
  end

  @doc """
  Await a job's terminal result. Returns `{:ok, value}` for success,
  `{:error, {:job, state}}` for failure/cancellation, `{:error, :timeout}`.
  """
  def await_result(name, id, timeout \\ 5_000) do
    config = Config.fetch!(name)

    Registry.register(run_registry(name), {:result, id}, nil)

    check = fn ->
      case get_job(name, id) do
        {:ok, %Job{state: "succeeded"} = job} -> {:ok, Job.result(job)}
        {:ok, %Job{state: state}} when state in ~w(failed cancelled) ->
          {:error, {:job, String.to_atom(state)}}
        _ -> :pending
      end
    end

    result = await_loop(check, config, id, monotonic_ms() + timeout)

    Registry.unregister(run_registry(name), {:result, id})

    result
  end

  defp await_loop(check, config, id, deadline) do
    case check.() do
      :pending ->
        remaining = deadline - monotonic_ms()

        if remaining <= 0 do
          {:error, :timeout}
        else
          receive do
            {:capstan_result, ^id, _job} -> await_loop(check, config, id, deadline)
          after
            min(remaining, 200) -> await_loop(check, config, id, deadline)
          end
        end

      result ->
        result
    end
  end

  defp monotonic_ms, do: System.monotonic_time(:millisecond)

  # -- Signals ------------------------------------------------------------------

  @doc "Deliver a durable signal to a scope, waking any awaiting jobs."
  def signal(name, scope, signal_name, payload \\ %{}) when is_map(payload) do
    config = Config.fetch!(name)
    {storage, ref} = config.storage_ref

    {:ok, woken} =
      storage.put_signal(ref, to_string(scope), to_string(signal_name), payload,
        Config.now(config))

    woken |> Enum.map(& &1.queue) |> Enum.uniq() |> Enum.each(&poke(config, &1))

    :ok
  end

  def signal_job(name, job_id, signal_name, payload \\ %{}) do
    signal(name, "job:#{job_id}", signal_name, payload)
  end

  @doc "Inject steering guidance readable by the running job via `steering/1`."
  def steer(name, job_id, payload) when is_map(payload) do
    signal_job(name, job_id, "$steer", payload)
  end

  def clear_signal(name, scope, signal_name) do
    config = Config.fetch!(name)
    {storage, ref} = config.storage_ref

    storage.clear_signal(ref, to_string(scope), to_string(signal_name))
  end

  # -- In-job APIs (take the ctx) -----------------------------------------------

  @doc "Run `fun` at most once per job; memoized with optional `cost: [usd:, tokens:]`."
  def step(%Ctx{} = ctx, step_name, fun, opts \\ []) when is_function(fun, 0) do
    Runner.step(ctx, step_name, fun, opts)
  end

  @doc """
  Wait for a signal. Returns the payload, or `{:error, :timeout}` after
  `:timeout` seconds. Parks the job (no process held) until signalled.
  """
  def await(%Ctx{} = ctx, signal_name, opts \\ []) do
    Runner.await(ctx, signal_name, opts)
  end

  @doc """
  Durably sleep: parks the job (freeing its slot) and resumes after the
  target. The wake time is memoized under `name`, so replays skip past it.
  """
  def sleep(%Ctx{} = ctx, name, seconds), do: Runner.sleep(ctx, name, seconds)

  @doc "Read the latest steering payload, or nil."
  def steering(%Ctx{} = ctx), do: Runner.steering(ctx)

  @doc """
  Spawn a child job from inside a running job. Memoized under `name`, so a
  crash after spawning cannot duplicate the child. Returns the child job id.
  """
  def spawn(%Ctx{} = ctx, name, buildable), do: Runner.spawn_child(ctx, name, buildable)

  @doc "Spawn many children as one memoized step. Returns their ids in order."
  def spawn_many(%Ctx{} = ctx, name, buildables), do: Runner.spawn_many(ctx, name, buildables)

  @doc """
  Park (at zero cost) until every spawned child reaches a terminal state.
  Returns the children ordered by id.
  """
  def await_children(%Ctx{} = ctx), do: Runner.await_children(ctx)

  @doc """
  Fan out one child per input and wait for all of them — the map phase of a
  map-reduce inside a single durable job. Returns child jobs in input order.

      results =
        Capstan.map_children(ctx, :chunks, MyApp.Summarize, chunk_inputs)
        |> Enum.map(&Capstan.Job.result/1)
  """
  def map_children(%Ctx{} = ctx, name, worker, inputs, opts \\ []) do
    ids = Runner.spawn_many(ctx, name, Enum.map(inputs, &{Capstan.Worker, worker, &1, opts}))
    by_id = ctx |> Runner.await_children() |> Map.new(&{&1.id, &1})

    Enum.map(ids, &Map.fetch!(by_id, &1))
  end

  @doc """
  Append to the job's durable event stream (progress, tokens, partial output).
  Subscribers receive `{:capstan_event, job_id, seq, payload}` live;
  `events/3` replays from any offset.
  """
  def emit(%Ctx{} = ctx, payload), do: Runner.emit(ctx, payload)

  @doc "Record actual resource usage against the queue's rate resource bucket."
  def debit(%Ctx{} = ctx, resource, units), do: Runner.debit(ctx, resource, units)

  @doc "Subscribe the calling process to a job's event stream."
  def subscribe_events(name, job_id) do
    {:ok, _} = Registry.register(run_registry(name), {:events, job_id}, nil)

    :ok
  end

  def unsubscribe_events(name, job_id) do
    Registry.unregister(run_registry(name), {:events, job_id})
  end

  @doc "Replay a job's event stream from an offset (0 for everything)."
  def events(name, job_id, after_seq \\ 0) do
    config = Config.fetch!(name)
    {storage, ref} = config.storage_ref
    {:ok, events} = storage.list_events(ref, job_id, after_seq)

    events
  end

  # -- Internal: producer poking ------------------------------------------------

  @doc false
  def poke(%Config{} = config, queue) do
    case Registry.lookup(registry(config.name), {:producer, to_string(queue)}) do
      [{pid, _}] -> send(pid, :poke)
      _ -> :ok
    end

    :pg.get_members(pg_scope(config.name), {:producers, to_string(queue)})
    |> Enum.each(&send(&1, :poke))

    :ok
  catch
    # :pg scope may not be running (bare drain in tests).
    :exit, _ -> :ok
  end

  @doc false
  def pg_scope(name), do: Module.concat(name, "PG")
end
