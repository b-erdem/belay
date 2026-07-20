defmodule Capstan.Runner do
  @moduledoc false

  # Executes one claimed job: runs the worker, translates returns/raises/control
  # throws into an ack outcome, acks (fenced), broadcasts the result, and pokes
  # producers for any workflow jobs released by the ack.

  require Logger

  alias Capstan.{Config, Ctx, Job, Worker}

  def execute(%Config{} = config, %Job{} = job) do
    register_running(config, job.id)

    ctx = %Ctx{job: job, capstan: config.name, config: config}

    :telemetry.execute([:capstan, :job, :start], %{}, %{job: job, name: config.name})

    outcome =
      try do
        job |> Job.worker_module!() |> apply(:run, [ctx]) |> map_return(config, job)
      catch
        :throw, {:capstan_control, control} ->
          control_outcome(control, config, job)

        kind, reason ->
          error_outcome({kind, reason, __STACKTRACE__}, config, job)
      end

    {storage, ref} = config.storage_ref

    case storage.ack(ref, job, outcome, Config.now(config)) do
      {:ok, %{job: acked, released: released, cancelled: cancelled}} ->
        :telemetry.execute([:capstan, :job, :stop], %{}, %{job: acked, name: config.name})

        broadcast_terminal(config, [acked | cancelled])
        poke_released(config, released)

        {:ok, acked, released}

      {:error, :stale} ->
        Logger.warning("[capstan] stale ack for job #{job.id} (attempt #{job.attempt})")

        {:error, :stale}
    end
  end

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
      reason: reason
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

  # -- Ctx APIs (called via Capstan.step/await/sleep/steering) ------------------

  @max_step_bytes 1024 * 1024

  def step(%Ctx{job: job, config: config}, name, fun, opts) do
    name = to_string(name)
    {storage, ref} = config.storage_ref

    case storage.get_step(ref, job.id, name) do
      {:ok, bin} ->
        :erlang.binary_to_term(bin)

      :none ->
        check_cancel!(config, job)

        result = fun.()
        bin = :erlang.term_to_binary(result)

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

  def await(%Ctx{job: job, config: config}, name, opts) do
    name = to_string(name)
    {storage, ref} = config.storage_ref
    scopes = signal_scopes(job, opts)

    case storage.get_signal(ref, scopes, name) do
      {:ok, payload} ->
        payload

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

  # Durable sleep: the wake target is memoized as a step, so replays past it
  # and resumes after it, no matter how many times the job restarts.
  def sleep(%Ctx{config: config} = ctx, name, seconds)
      when is_integer(seconds) and seconds >= 0 do
    target =
      step(ctx, "$sleep:#{name}", fn -> DateTime.add(Config.now(config), seconds, :second) end,
        [])

    now = Config.now(config)

    if DateTime.compare(now, target) == :lt do
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

  # -- Helpers ------------------------------------------------------------------

  defp check_cancel!(config, job) do
    {storage, ref} = config.storage_ref

    case storage.get_job(ref, job.id) do
      {:ok, %Job{cancel_requested: true}} ->
        throw({:capstan_control, {:cancelled, :cancel_requested}})

      _ ->
        :ok
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
