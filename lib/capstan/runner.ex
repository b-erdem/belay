defmodule Capstan.Runner do
  @moduledoc false

  # Executes one claimed job: runs the worker (optionally under a timeout),
  # translates returns/raises/control throws into an ack outcome, acks
  # (fenced), broadcasts results, and pokes producers for released jobs.
  #
  # Also implements the in-job APIs surfaced on `Capstan`: step, await, sleep,
  # spawn/await_children, emit, debit, steering — including their replay-mode
  # behavior (see `Capstan.Replay`).

  require Logger

  alias Capstan.{Config, Ctx, Job, Worker}

  # -- Execution ----------------------------------------------------------------

  def execute(%Config{} = config, %Job{} = job) do
    register_running(config, job.id)

    job = maybe_decrypt(config, job)

    ctx = %Ctx{job: job, capstan: config.name, config: config}
    started = System.monotonic_time()

    :telemetry.execute([:capstan, :job, :start], %{system_time: System.system_time()}, %{
      job: job,
      name: config.name
    })

    outcome =
      try do
        run_worker(Job.worker_module!(job), ctx) |> map_return(config, job)
      catch
        :throw, {:capstan_control, control} ->
          control_outcome(control, config, job)

        kind, reason ->
          error_outcome({kind, reason, __STACKTRACE__}, config, job)
      end

    {storage, ref} = config.storage_ref

    case storage.ack(ref, job, outcome, Config.now(config)) do
      {:ok, %{job: acked, released: released, cancelled: cancelled}} ->
        :telemetry.execute(
          [:capstan, :job, :stop],
          %{duration: System.monotonic_time() - started},
          %{job: acked, name: config.name, state: acked.state}
        )

        broadcast_terminal(config, [acked | cancelled])
        poke_released(config, released)

        {:ok, acked, released}

      {:error, :stale} ->
        Logger.warning("[capstan] stale ack for job #{job.id} (attempt #{job.attempt})")

        {:error, :stale}
    end
  end

  @doc false
  # Chunk execution: one worker invocation over N gathered jobs, then one
  # fenced ack per job with its own outcome. Runs from the producer (gathered
  # dispatch) and from Testing.drain (per claim round, ungathered).
  def execute_chunk(%Config{} = config, [%Job{} | _] = jobs) do
    Enum.each(jobs, &register_running(config, &1.id))

    jobs = Enum.map(jobs, &maybe_decrypt(config, &1))
    ctxs = Enum.map(jobs, &%Ctx{job: &1, capstan: config.name, config: config})
    module = Job.worker_module!(hd(jobs))
    started = System.monotonic_time()

    for job <- jobs do
      :telemetry.execute([:capstan, :job, :start], %{system_time: System.system_time()}, %{
        job: job,
        name: config.name
      })
    end

    outcomes =
      try do
        cond do
          not function_exported?(module, :run_chunk, 1) ->
            # A config error, not a transient one: fail directly instead of
            # burning every job's attempts on retries.
            failure = %{"error" => "#{inspect(module)} declares chunk: but not run_chunk/1"}
            Map.new(jobs, &{&1.id, {:failed, failure}})

          true ->
            with_timeout(module, fn -> module.run_chunk(ctxs) end)
            |> normalize_chunk(jobs, config)
        end
      catch
        # Control throws (budget kill, honored cancel) cannot be attributed
        # to one job of the chunk — retry them all; per-job conditions
        # re-assert themselves on the retried singles. Documented tradeoff.
        :throw, {:capstan_control, control} ->
          error = %{"error" => "chunk aborted by control: #{inspect(control)}"}
          Map.new(jobs, fn job -> {job.id, map_return({:error, error}, config, job)} end)

        kind, reason ->
          Map.new(jobs, fn job ->
            {job.id, error_outcome({kind, reason, __STACKTRACE__}, config, job)}
          end)
      end

    {storage, ref} = config.storage_ref

    {acked, released} =
      Enum.reduce(jobs, {[], []}, fn job, {acked_acc, released_acc} ->
        outcome = Map.fetch!(outcomes, job.id)

        case storage.ack(ref, job, outcome, Config.now(config)) do
          {:ok, %{job: acked, released: rel, cancelled: cancelled}} ->
            :telemetry.execute(
              [:capstan, :job, :stop],
              %{duration: System.monotonic_time() - started},
              %{job: acked, name: config.name, state: acked.state}
            )

            broadcast_terminal(config, [acked | cancelled])

            {[acked | acked_acc], rel ++ released_acc}

          {:error, :stale} ->
            Logger.warning("[capstan] stale ack for job #{job.id} (attempt #{job.attempt})")

            {acked_acc, released_acc}
        end
      end)

    poke_released(config, released)

    {:ok, Enum.reverse(acked)}
  end

  # Map a chunk return onto per-job outcomes.
  defp normalize_chunk(:ok, jobs, _config), do: Map.new(jobs, &{&1.id, {:succeeded, nil}})

  defp normalize_chunk({:error, reason}, jobs, config) do
    Map.new(jobs, fn job -> {job.id, map_return({:error, reason}, config, job)} end)
  end

  defp normalize_chunk({:ok, %{} = results}, jobs, _config) do
    Map.new(jobs, fn job ->
      case Map.fetch(results, job.id) do
        {:ok, value} -> {job.id, {:succeeded, :erlang.term_to_binary(value)}}
        :error -> {job.id, {:succeeded, nil}}
      end
    end)
  end

  defp normalize_chunk(%{} = by_id, jobs, config) do
    Map.new(jobs, fn job ->
      case Map.fetch(by_id, job.id) do
        {:ok, ret} ->
          {job.id, map_return(ret, config, job)}

        :error ->
          {job.id, map_return({:error, %{"error" => "no chunk outcome for job"}}, config, job)}
      end
    end)
  end

  defp normalize_chunk(other, jobs, config) do
    Map.new(jobs, fn job -> {job.id, map_return(other, config, job)} end)
  end

  # Workers with a `timeout:` run in a linked inner task so a hung run can be
  # cut off; the timeout is handled like any other error (retry, then fail).
  defp run_worker(module, ctx) do
    with_timeout(module, fn -> module.run(ctx) end)
  end

  defp with_timeout(module, fun) do
    case module.__capstan_defaults__()[:timeout] do
      nil ->
        fun.()

      timeout ->
        # The inner task is linked, so it must catch EVERY class — a bare
        # raise/exit/throw would otherwise kill this runner before yield
        # returns (the job would sit leased until the sweeper reclaims it,
        # with no error journaled). We capture the failure and re-raise it
        # in the caller, where execute/2's own try maps it to an outcome.
        task =
          Task.async(fn ->
            try do
              {:returned, fun.()}
            catch
              :throw, {:capstan_control, control} -> {:control, control}
              kind, reason -> {:caught, kind, reason, __STACKTRACE__}
            end
          end)

        case Task.yield(task, timeout_ms(timeout)) || Task.shutdown(task, :brutal_kill) do
          {:ok, {:returned, value}} -> value
          {:ok, {:control, control}} -> throw({:capstan_control, control})
          {:ok, {:caught, kind, reason, stack}} -> :erlang.raise(kind, reason, stack)
          nil -> {:error, :timeout}
        end
    end
  end

  defp timeout_ms(seconds) when is_integer(seconds), do: seconds * 1_000
  defp timeout_ms({n, :second}), do: n * 1_000
  defp timeout_ms({n, :millisecond}), do: n

  # -- Return mapping -----------------------------------------------------------

  defp map_return(:ok, _config, _job), do: {:succeeded, nil}
  defp map_return({:ok, value}, _config, _job), do: {:succeeded, :erlang.term_to_binary(value)}

  defp map_return({:error, reason}, config, job) do
    retry_or_fail(config, job, %{"error" => inspect(reason)})
  end

  defp map_return({:cancel, reason}, _config, _job) do
    {:cancelled, %{"reason" => inspect(reason)}}
  end

  defp map_return({:snooze, seconds}, config, _job) do
    {:snooze, DateTime.add(Config.now(config), seconds, :second)}
  end

  defp map_return(other, config, job) do
    retry_or_fail(config, job, %{"error" => "bad return: #{inspect(other)}"})
  end

  defp control_outcome({:await, scope, name, deadline}, _config, _job) do
    {:await, scope, name, deadline}
  end

  defp control_outcome({:sleep, seconds}, config, _job) do
    {:snooze, DateTime.add(Config.now(config), seconds, :second)}
  end

  defp control_outcome({:budget_exceeded, info}, _config, _job) do
    {:failed, %{"error" => "budget_exceeded", "detail" => inspect(info)}}
  end

  defp control_outcome({:cancelled, reason}, _config, _job) do
    {:cancelled, %{"reason" => inspect(reason)}}
  end

  defp error_outcome({kind, reason, stacktrace}, config, job) do
    formatted = Exception.format_banner(kind, reason, stacktrace)

    :telemetry.execute([:capstan, :job, :exception], %{}, %{
      job: job,
      name: config.name,
      kind: kind,
      reason: reason,
      stacktrace: stacktrace
    })

    retry_or_fail(config, job, %{"error" => formatted})
  end

  defp retry_or_fail(config, job, error) do
    error = Map.put(error, "attempt", job.attempt)

    if job.attempt >= job.max_attempts do
      {:failed, error}
    else
      {:retry, error, DateTime.add(Config.now(config), backoff(job), :second)}
    end
  end

  @doc false
  def backoff(%Job{} = job) do
    module = Job.worker_module!(job)

    if function_exported?(module, :backoff, 1) do
      module.backoff(job.attempt)
    else
      Worker.default_backoff(job.attempt)
    end
  end

  # -- Steps --------------------------------------------------------------------

  @max_step_bytes 1024 * 1024

  def step(%Ctx{job: job, config: config} = ctx, name, fun, opts) do
    name = to_string(name)
    {storage, ref} = config.storage_ref

    case storage.get_step(ref, job.id, name) do
      {:ok, bin} ->
        Capstan.Codec.decode(bin)

      :none when ctx.replay? ->
        throw({:capstan_replay, {:missing_step, name}})

      :none ->
        fresh = check_cancel!(config, job)

        # Pre-flight, against the durable spend: a crash between journaling
        # the over-budget step and acking the failure must not let the next
        # attempt replay past the journal and pay for one more step. (Found
        # by the 7h endurance soak: 6 of 1750 budget jobs ran a 4th step.)
        check_budget!(job, fresh)

        result = fun.()
        bin = Capstan.Codec.encode(result)

        if byte_size(bin) > @max_step_bytes do
          raise ArgumentError,
                "step #{name} result is #{byte_size(bin)} bytes (limit #{@max_step_bytes})"
        end

        cost = normalize_cost(Keyword.get(opts, :cost))

        {:ok, spent} = storage.put_step(ref, job.id, name, bin, cost, Config.now(config))

        check_budget!(job, spent)

        result
    end
  end

  # -- Signals ------------------------------------------------------------------

  def await(%Ctx{job: job, config: config} = ctx, name, opts) do
    name = to_string(name)
    {storage, ref} = config.storage_ref
    scopes = signal_scopes(job, opts)

    case storage.get_signal(ref, scopes, name) do
      {:ok, payload} ->
        payload

      :none when ctx.replay? ->
        throw({:capstan_replay, {:blocked_on_signal, name}})

      :none ->
        if job.await_name == name and deadline_passed?(job, config) do
          {:error, :timeout}
        else
          deadline =
            case Keyword.get(opts, :timeout) do
              nil -> nil
              seconds -> DateTime.add(Config.now(config), seconds, :second)
            end

          throw({:capstan_control, {:await, hd(scopes), name, deadline}})
        end
    end
  end

  # Durable sleep: the wake target is memoized as a step, so replays pass it
  # and resumes continue after it, no matter how many times the job restarts.
  def sleep(%Ctx{config: config} = ctx, name, seconds)
      when is_integer(seconds) and seconds >= 0 do
    target =
      step(
        ctx,
        "$sleep:#{name}",
        fn -> DateTime.add(Config.now(config), seconds, :second) end,
        []
      )

    now = Config.now(config)

    if DateTime.compare(now, target) == :lt and not ctx.replay? do
      throw({:capstan_control, {:sleep, max(DateTime.diff(target, now, :second), 1)}})
    end

    :ok
  end

  def steering(%Ctx{job: job, config: config}) do
    {storage, ref} = config.storage_ref

    case storage.get_signal(ref, signal_scopes(job, []), "$steer") do
      {:ok, payload} -> payload
      :none -> nil
    end
  end

  # -- Dynamic children ---------------------------------------------------------

  # Spawning is idempotent twice over: the memoizing step skips it entirely on
  # replay, and each child carries an always-scoped unique key derived from
  # (parent, spawn name, index) — so even a crash *between* inserting children
  # and recording the step cannot duplicate them. The id list is rebuilt from
  # the unique keys, never trusted from a possibly-partial insert.
  def spawn_child(%Ctx{} = ctx, name, buildable) do
    [id] = spawn_many(ctx, name, [buildable])

    id
  end

  def spawn_many(%Ctx{job: parent, config: config} = ctx, name, buildables) do
    step(
      ctx,
      "$spawn:#{name}",
      fn ->
        keyed =
          buildables
          |> Enum.with_index()
          |> Enum.map(fn {{Capstan.Worker, worker, input, opts}, index} ->
            key = "$spawn:#{parent.id}:#{name}:#{index}"

            opts =
              opts
              |> Keyword.put(:parent_id, parent.id)
              |> Keyword.put(:unique, key: key, scope: :always)

            {key, {Capstan.Worker, worker, input, opts}}
          end)

        _inserted = Capstan.insert_all(ctx.capstan, Enum.map(keyed, &elem(&1, 1)))

        {storage, ref} = config.storage_ref

        Enum.map(keyed, fn {key, _buildable} ->
          {:ok, child} = storage.get_by_unique_key(ref, key)

          child.id
        end)
      end,
      []
    )
  end

  # Park (at zero cost) until every spawned child is terminal, then return
  # them ordered by id. Re-checks on every wake, so late spawns are safe as
  # long as they happen before the await.
  def await_children(%Ctx{job: job, config: config} = ctx) do
    {storage, ref} = config.storage_ref
    {:ok, children} = storage.children(ref, job.id)

    cond do
      # Children are inserted synchronously by spawn_many before this runs,
      # so an empty set means none were spawned (or a fan-out of zero) —
      # there is nothing to await. Parking here would hang forever, since
      # the resettle_parents backstop needs children to exist to fire.
      children == [] ->
        []

      Enum.all?(children, &Job.terminal?/1) ->
        children

      ctx.replay? ->
        throw({:capstan_replay, {:blocked_on_children, length(children)}})

      true ->
        # Drop any stale completion signal, then re-check before parking: a
        # child finishing in between will have re-signalled, and the engine's
        # park-and-wake check closes the final gap.
        storage.clear_signal(ref, "job:#{job.id}", "$children")

        {:ok, children} = storage.children(ref, job.id)

        if children != [] and Enum.all?(children, &Job.terminal?/1) do
          children
        else
          throw({:capstan_control, {:await, "job:#{job.id}", "$children", nil}})
        end
    end
  end

  # -- Events -------------------------------------------------------------------

  def emit(%Ctx{replay?: true}, _payload), do: {:ok, :replayed}

  def emit(%Ctx{job: job, config: config}, payload) when is_map(payload) do
    {storage, ref} = config.storage_ref

    {:ok, seq} = storage.append_event(ref, job.id, payload, Config.now(config))

    Registry.dispatch(Capstan.run_registry(config.name), {:events, job.id}, fn entries ->
      for {pid, _} <- entries, do: send(pid, {:capstan_event, job.id, seq, payload})
    end)

    {:ok, seq}
  rescue
    ArgumentError -> {:ok, :no_registry}
  end

  # -- Resource debits (post-hoc rate true-up) ----------------------------------

  def debit(%Ctx{replay?: true}, _resource, _units), do: :ok

  def debit(%Ctx{job: job, config: config}, resource, units) when is_integer(units) do
    spec = Config.queue_spec(config, job.queue)

    case spec.rate do
      %{resource: ^resource, period: period, estimate: estimate} ->
        {storage, ref} = config.storage_ref

        # The claim already debited the estimate; credit it back exactly once
        # per execution so the window converges on actual usage.
        credit =
          if Process.get({:capstan_credited, job.id}) do
            0
          else
            Process.put({:capstan_credited, job.id}, true)
            estimate
          end

        storage.debit_rate(
          ref,
          "resource:" <> resource,
          period,
          units - credit,
          Config.now(config)
        )

      _ ->
        raise ArgumentError,
              "queue #{job.queue} has no rate resource #{inspect(resource)} configured"
    end
  end

  # -- Helpers ------------------------------------------------------------------

  # Ciphertext never leaves the database row; plaintext exists only in the
  # executing process.
  defp maybe_decrypt(config, %Job{input: %{"$enc" => _}} = job) do
    Job.decrypt_input(job, Config.encryption_key(config))
  end

  defp maybe_decrypt(_config, job), do: job

  @doc false
  def decrypt_for_replay(config, job), do: maybe_decrypt(config, job)

  # Returns the freshly-read job row so callers can check durable spend
  # without a second query. Falls back to the claim-time row if the read
  # fails — its spend is stale-low, which is the safe direction (the
  # post-write check still catches the crossing step).
  defp check_cancel!(config, job) do
    {storage, ref} = config.storage_ref

    case storage.get_job(ref, job.id) do
      {:ok, %Job{cancel_requested: true}} ->
        throw({:capstan_control, {:cancelled, :cancel_requested}})

      {:ok, %Job{} = fresh} ->
        fresh

      _ ->
        job
    end
  end

  defp check_budget!(job, spent) do
    over_usd? =
      is_integer(job.budget_usd_micros) and spent.spent_usd_micros > job.budget_usd_micros

    over_tokens? = is_integer(job.budget_tokens) and spent.spent_tokens > job.budget_tokens

    if over_usd? or over_tokens? do
      throw(
        {:capstan_control,
         {:budget_exceeded,
          %{
            spent_usd_micros: spent.spent_usd_micros,
            spent_tokens: spent.spent_tokens,
            budget_usd_micros: job.budget_usd_micros,
            budget_tokens: job.budget_tokens
          }}}
      )
    end

    :ok
  end

  defp normalize_cost(nil), do: %{usd_micros: 0, tokens: 0}

  defp normalize_cost(cost) do
    usd = Keyword.get(cost, :usd, 0)

    %{
      usd_micros: round(usd * 1_000_000),
      tokens: Keyword.get(cost, :tokens, 0)
    }
  end

  defp signal_scopes(job, opts) do
    base = ["job:#{job.id}"]
    base = if job.workflow_id, do: base ++ ["wf:#{job.workflow_id}"], else: base

    case Keyword.get(opts, :scope) do
      nil -> base
      scope -> [to_string(scope) | base]
    end
  end

  defp deadline_passed?(%Job{ready_at: nil}, _config), do: false

  defp deadline_passed?(%Job{ready_at: deadline}, config) do
    DateTime.compare(Config.now(config), deadline) != :lt
  end

  # -- Registry integration -----------------------------------------------------

  defp register_running(config, job_id) do
    Registry.register(Capstan.run_registry(config.name), :running, job_id)
  rescue
    # Bare Runner.execute calls (drain/tests) may run outside the tree.
    ArgumentError -> :ok
  end

  defp broadcast_terminal(config, jobs) do
    for %Job{} = job <- jobs, job.state in ~w(succeeded failed cancelled) do
      Registry.dispatch(Capstan.run_registry(config.name), {:result, job.id}, fn entries ->
        for {pid, _} <- entries, do: send(pid, {:capstan_result, job.id, job})
      end)

      Capstan.Notifier.broadcast_all(config, {:result, job.id})
    end
  rescue
    ArgumentError -> :ok
  end

  defp poke_released(config, released) do
    for queue <- released |> Enum.map(& &1.queue) |> Enum.uniq() do
      Capstan.poke(config, queue)
    end

    :ok
  end
end
