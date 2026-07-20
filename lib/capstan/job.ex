defmodule Capstan.Job do
  @moduledoc """
  The durable job record.

  States:

    * `"ready"` — claimable once `ready_at` is due (covers scheduled, retry
      backoff, and snoozes; there is no separate staging step)
    * `"awaiting"` — parked on a named signal; `ready_at` doubles as the wake
      deadline (nil waits forever)
    * `"running"` — claimed under a lease
    * `"held"` — created but ineligible (unmet workflow deps, operator hold)
    * `"succeeded"` / `"failed"` / `"cancelled"` — terminal
    * `"paused"` — operator freeze, resumable
  """

  defstruct [
    :id,
    :kind,
    :queue,
    :state,
    :input,
    :meta,
    :priority,
    :attempt,
    :max_attempts,
    :partition_key,
    :ready_at,
    :lease_until,
    :leased_by,
    :await_scope,
    :await_name,
    :workflow_id,
    :wf_name,
    :wf_deps,
    :wf_ignore,
    :cron_name,
    :cron_slot,
    :unique_key,
    :unique_mode,
    :parent_id,
    :budget_usd_micros,
    :budget_tokens,
    :spent_usd_micros,
    :spent_tokens,
    :result,
    :errors,
    :cancel_requested,
    :inserted_at,
    :started_at,
    :finished_at,
    duplicate?: false
  ]

  @type t :: %__MODULE__{}

  @terminal ~w(succeeded failed cancelled)
  @incomplete ~w(ready awaiting running held paused)

  def terminal_states, do: @terminal
  def incomplete_states, do: @incomplete

  def terminal?(%__MODULE__{state: state}), do: state in @terminal

  @doc "Decode a succeeded job's result (ETF or JSON — see `Capstan.Codec`)."
  def result(%__MODULE__{result: bin}), do: Capstan.Codec.decode(bin)

  @doc false
  def worker_module!(%__MODULE__{kind: kind}) do
    String.to_existing_atom("Elixir." <> kind)
  rescue
    ArgumentError -> reraise(ArgumentError, "unknown worker module #{kind}", __STACKTRACE__)
  end

  @doc false
  def new(worker, input, opts, defaults) do
    now = Keyword.fetch!(opts, :now)
    budget = Keyword.get(opts, :budget, [])
    {unique_key, unique_mode} = unique_fields(Keyword.get(opts, :unique), now)

    input = Capstan.InputSchema.validate!(worker, defaults[:input_schema], input)

    input =
      if defaults[:encrypted] do
        encrypt_input(worker, input, Keyword.get(opts, :encryption_key))
      else
        input
      end

    %{
      kind: worker |> to_string() |> String.replace_prefix("Elixir.", ""),
      queue: to_string(Keyword.get(opts, :queue, defaults[:queue] || "default")),
      state: Keyword.get(opts, :state, "ready"),
      input: input,
      unique_key: unique_key,
      unique_mode: unique_mode,
      parent_id: Keyword.get(opts, :parent_id),
      meta: Keyword.get(opts, :meta, %{}),
      priority: Keyword.get(opts, :priority, 0),
      attempt: 0,
      max_attempts: Keyword.get(opts, :max_attempts, defaults[:max_attempts] || 20),
      partition_key: opt_string(Keyword.get(opts, :partition_key)),
      ready_at: Keyword.get(opts, :ready_at) || schedule_at(opts, now),
      workflow_id: Keyword.get(opts, :workflow_id),
      wf_name: Keyword.get(opts, :wf_name),
      wf_deps: Keyword.get(opts, :wf_deps, []),
      wf_ignore: Keyword.get(opts, :wf_ignore, []),
      cron_name: Keyword.get(opts, :cron_name),
      cron_slot: Keyword.get(opts, :cron_slot),
      budget_usd_micros: money_micros(Keyword.get(budget, :usd)),
      budget_tokens: Keyword.get(budget, :tokens),
      spent_usd_micros: 0,
      spent_tokens: 0,
      errors: [],
      cancel_requested: false,
      inserted_at: now
    }
  end

  defp schedule_at(opts, now) do
    case Keyword.get(opts, :schedule_in) do
      nil -> now
      seconds -> DateTime.add(now, seconds, :second)
    end
  end

  # `unique: "key"` holds while a job with the key is incomplete;
  # `unique: [key: k, within: seconds]` dedupes per fixed time window
  # regardless of outcome (the window bucket becomes part of the key);
  # `unique: [key: k, scope: :always]` dedupes forever regardless of outcome
  # (used internally to make child-spawning idempotent).
  defp unique_fields(nil, _now), do: {nil, nil}
  defp unique_fields(key, _now) when is_binary(key), do: {key, "incomplete"}

  defp unique_fields(opts, now) when is_list(opts) do
    key = Keyword.fetch!(opts, :key)

    case {Keyword.get(opts, :within), Keyword.get(opts, :scope)} do
      {nil, :always} ->
        {key, "always"}

      {nil, _} ->
        {key, "incomplete"}

      {seconds, _} when is_integer(seconds) and seconds > 0 ->
        bucket = now |> DateTime.to_unix() |> div(seconds)

        {"#{key}@#{bucket}", "window"}
    end
  end

  defp opt_string(nil), do: nil
  defp opt_string(value), do: to_string(value)

  # AES-256-GCM with a random 96-bit IV; ciphertext stored as
  # %{"$enc" => base64(iv <> tag <> ciphertext)}. Validation runs on the
  # plaintext first, so schemas and encryption compose.
  @aad "capstan.input.v1"

  defp encrypt_input(worker, _input, nil) do
    raise ArgumentError,
          "#{inspect(worker)} declares encrypted: true but the Capstan instance has no " <>
            "encryption: [key: {mod, fun, args}] configured"
  end

  defp encrypt_input(_worker, input, key) do
    iv = :crypto.strong_rand_bytes(12)
    plaintext = Jason.encode!(input)

    {ciphertext, tag} =
      :crypto.crypto_one_time_aead(:aes_256_gcm, key, iv, plaintext, @aad, true)

    %{"$enc" => Base.encode64(iv <> tag <> ciphertext)}
  end

  @doc false
  def decrypt_input(%__MODULE__{input: %{"$enc" => encoded}} = job, key)
      when is_binary(key) do
    <<iv::binary-12, tag::binary-16, ciphertext::binary>> = Base.decode64!(encoded)

    case :crypto.crypto_one_time_aead(:aes_256_gcm, key, iv, ciphertext, @aad, tag, false) do
      plaintext when is_binary(plaintext) -> %{job | input: Jason.decode!(plaintext)}
      :error -> raise ArgumentError, "input decryption failed for job #{job.id} (wrong key?)"
    end
  end

  def decrypt_input(%__MODULE__{input: %{"$enc" => _}} = job, nil) do
    raise ArgumentError,
          "job #{job.id} has an encrypted input but no encryption key is configured"
  end

  def decrypt_input(job, _key), do: job

  defp money_micros(nil), do: nil
  defp money_micros(usd) when is_number(usd), do: round(usd * 1_000_000)
end

defmodule Capstan.Ctx do
  @moduledoc """
  Execution context passed to `c:Capstan.Worker.run/1`. Carries the job and
  everything the step/signal/budget APIs need. `replay?: true` marks a
  `Capstan.Replay` dry run: memoized reads succeed, side effects are inert,
  and anything unrecorded halts with a precise report.
  """

  defstruct [:job, :capstan, :config, replay?: false]

  @type t :: %__MODULE__{job: Capstan.Job.t()}
end

defmodule Capstan.Worker do
  @moduledoc """
  Define a durable worker.

      defmodule MyApp.Summarize do
        use Capstan.Worker, queue: :ai, max_attempts: 5

        @impl Capstan.Worker
        def run(ctx) do
          text = Capstan.step(ctx, :fetch, fn -> fetch!(ctx.job.input["url"]) end)
          {:ok, summarize(text)}
        end
      end

  Return values: `:ok` | `{:ok, result}` | `{:error, reason}` (retry with
  backoff) | `{:cancel, reason}` | `{:snooze, seconds}`. Raised exceptions
  retry. `Capstan.step/4`, `Capstan.await/3`, and `Capstan.sleep/3` manage
  control flow internally.
  """

  @callback run(Capstan.Ctx.t()) ::
              :ok
              | {:ok, term()}
              | {:error, term()}
              | {:cancel, term()}
              | {:snooze, non_neg_integer()}

  @doc "Per-attempt retry backoff in seconds. Overridable."
  @callback backoff(attempt :: pos_integer()) :: non_neg_integer()

  @optional_callbacks backoff: 1

  defmacro __using__(opts) do
    quote location: :keep do
      @behaviour Capstan.Worker

      @capstan_defaults unquote(opts)

      @doc false
      def __capstan_defaults__, do: @capstan_defaults

      @doc "Build insert attrs for this worker (used by `Capstan.insert/4`)."
      def new(input, opts \\ []) do
        {unquote(__MODULE__), __MODULE__, input, opts}
      end
    end
  end

  @doc false
  def default_backoff(attempt) do
    trunc(:math.pow(2, min(attempt, 8))) + :rand.uniform(3) - 1
  end
end
