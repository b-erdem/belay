defmodule Capstan.Storage.Postgres do
  @moduledoc """
  Postgres storage over Postgrex — no Ecto. Claims use `FOR UPDATE SKIP
  LOCKED`; workflow settlement happens inside the acking transaction; the
  clock is always a query parameter, never `now()`.
  """

  @behaviour Capstan.Storage

  alias Capstan.Job
  alias Capstan.Storage.Logic

  @jobs_columns ~w(id kind queue state input meta priority attempt max_attempts partition_key
                   ready_at lease_until leased_by await_scope await_name workflow_id wf_name
                   wf_deps wf_ignore cron_name cron_slot budget_usd_micros budget_tokens
                   spent_usd_micros spent_tokens result errors cancel_requested inserted_at
                   started_at finished_at)

  # -- Setup --------------------------------------------------------------------

  @impl Capstan.Storage
  def child_spec({config, opts}) do
    start_opts =
      opts
      |> Keyword.fetch!(:url)
      |> parse_url()
      |> Keyword.merge(
        name: ref(config.name),
        pool_size: Keyword.get(opts, :pool_size, 10),
        types: Capstan.Storage.PostgresTypes
      )

    %{id: __MODULE__, start: {Postgrex, :start_link, [start_opts]}}
  end

  def ref(instance_name), do: Module.concat(instance_name, "Storage")

  def parse_url(url) do
    uri = URI.parse(url)
    [username, password] = String.split(uri.userinfo || "postgres", ":", parts: 2) |> pad2()

    [
      hostname: uri.host || "localhost",
      port: uri.port || 5432,
      username: username,
      password: password || "",
      database: String.trim_leading(uri.path || "/capstan", "/")
    ]
  end

  defp pad2([a]), do: [a, ""]
  defp pad2([a, b]), do: [a, b]

  # -- Storage API --------------------------------------------------------------

  @impl Capstan.Storage
  def insert_jobs(ref, rows, _now) do
    {sql, params} = build_insert(rows)

    %{rows: returned, columns: columns} = q!(ref, sql, params)

    {:ok, decode_jobs(returned, columns)}
  end

  @impl Capstan.Storage
  def claim(ref, spec, demand, node_id, lease_ttl, now) do
    Postgrex.transaction(ref, fn conn ->
      if spec.global_limit || spec.rate || spec.partition do
        q!(conn, "SELECT pg_advisory_xact_lock(hashtext($1))", ["capstan:" <> spec.queue])
      end

      now_unix = DateTime.to_unix(now)

      take =
        demand
        |> clamp_global(conn, spec, now)
        |> clamp_rate(conn, spec, now_unix)

      if take <= 0 do
        []
      else
        candidate_limit = if spec.partition, do: take * 4 + 16, else: take

        %{rows: rows, columns: columns} =
          q!(
            conn,
            """
            SELECT * FROM capstan_jobs
            WHERE queue = $1
              AND ((state = 'ready' AND (ready_at IS NULL OR ready_at <= $2))
                OR (state = 'awaiting' AND ready_at IS NOT NULL AND ready_at <= $2))
            ORDER BY priority ASC, ready_at ASC NULLS FIRST, id ASC
            LIMIT $3
            FOR UPDATE SKIP LOCKED
            """,
            [spec.queue, now, candidate_limit]
          )

        candidates = decode_jobs(rows, columns)

        picked =
          case spec.partition do
            nil ->
              Enum.take(candidates, take)

            partition ->
              per_key = spec.global_limit || spec.local_limit
              counts = partition_counts(conn, spec.queue, partition, now)

              Logic.partition_take(candidates, take, counts, per_key, fn job ->
                Logic.partition_key(job, partition)
              end)
          end

        ids = Enum.map(picked, & &1.id)

        claimed =
          if ids == [] do
            []
          else
            lease_until = DateTime.add(now, lease_ttl, :millisecond)

            %{rows: rows, columns: columns} =
              q!(
                conn,
                """
                UPDATE capstan_jobs
                SET state = 'running', attempt = attempt + 1, lease_until = $2,
                    leased_by = $3, started_at = COALESCE(started_at, $4)
                WHERE id = ANY($1)
                RETURNING *
                """,
                [ids, lease_until, node_id, now]
              )

            rows |> decode_jobs(columns) |> sort_like(ids)
          end

        debit_rate(conn, spec, now_unix, length(claimed))

        claimed
      end
    end)
    |> unwrap()
  end

  @impl Capstan.Storage
  def renew_leases(ref, ids, node_id, until) do
    %{rows: rows} =
      q!(
        ref,
        """
        UPDATE capstan_jobs SET lease_until = $3
        WHERE id = ANY($1) AND state = 'running' AND leased_by = $2
        RETURNING id
        """,
        [ids, node_id, until]
      )

    {:ok, Enum.map(rows, fn [id] -> id end)}
  end

  @impl Capstan.Storage
  def reclaim_expired(ref, now, backoff_fun) do
    Postgrex.transaction(ref, fn conn ->
      %{rows: rows, columns: columns} =
        q!(
          conn,
          """
          SELECT * FROM capstan_jobs
          WHERE state = 'running' AND lease_until <= $1
          FOR UPDATE SKIP LOCKED
          """,
          [now]
        )

      error = %{"error" => "lease_expired", "at" => DateTime.to_iso8601(now)}

      rows
      |> decode_jobs(columns)
      |> Enum.reduce(%{retried: [], failed: []}, fn job, acc ->
        if job.attempt >= job.max_attempts do
          apply_and_settle(conn, job, {:failed, error}, now, fence: false)

          %{acc | failed: [job.id | acc.failed]}
        else
          apply_and_settle(conn, job, {:retry, error, backoff_fun.(job)}, now, fence: false)

          %{acc | retried: [job.id | acc.retried]}
        end
      end)
    end)
    |> unwrap()
  end

  @impl Capstan.Storage
  def ack(ref, job, outcome, now) do
    Postgrex.transaction(ref, fn conn ->
      case apply_and_settle(conn, job, outcome, now, fence: true) do
        :stale -> Postgrex.rollback(conn, :stale)
        result -> result
      end
    end)
    |> case do
      {:ok, result} -> {:ok, result}
      {:error, :stale} -> {:error, :stale}
    end
  end

  @impl Capstan.Storage
  def get_job(ref, id) do
    case q!(ref, "SELECT * FROM capstan_jobs WHERE id = $1", [id]) do
      %{rows: [row], columns: columns} -> {:ok, decode_job(row, columns)}
      _ -> :error
    end
  end

  @impl Capstan.Storage
  def get_step(ref, job_id, name) do
    case q!(ref, "SELECT value FROM capstan_steps WHERE job_id = $1 AND name = $2", [job_id, name]) do
      %{rows: [[value]]} -> {:ok, value}
      _ -> :none
    end
  end

  @impl Capstan.Storage
  def put_step(ref, job_id, name, bin, cost, now) do
    Postgrex.transaction(ref, fn conn ->
      q!(
        conn,
        """
        INSERT INTO capstan_steps (job_id, seq, name, value, usd_micros, tokens, inserted_at)
        VALUES ($1, (SELECT COALESCE(MAX(seq), 0) + 1 FROM capstan_steps WHERE job_id = $1),
                $2, $3, $4, $5, $6)
        ON CONFLICT (job_id, name) DO NOTHING
        """,
        [job_id, name, bin, cost[:usd_micros] || 0, cost[:tokens] || 0, now]
      )

      %{rows: [[usd, tokens]]} =
        q!(
          conn,
          """
          UPDATE capstan_jobs
          SET spent_usd_micros = spent_usd_micros + $2, spent_tokens = spent_tokens + $3
          WHERE id = $1
          RETURNING spent_usd_micros, spent_tokens
          """,
          [job_id, cost[:usd_micros] || 0, cost[:tokens] || 0]
        )

      %{spent_usd_micros: usd, spent_tokens: tokens}
    end)
  end

  @impl Capstan.Storage
  def list_steps(ref, job_id) do
    %{rows: rows} =
      q!(
        ref,
        """
        SELECT seq, name, value, usd_micros, tokens, inserted_at
        FROM capstan_steps WHERE job_id = $1 ORDER BY seq
        """,
        [job_id]
      )

    steps =
      for [seq, name, value, usd, tokens, at] <- rows do
        %{seq: seq, name: name, value: value, usd_micros: usd, tokens: tokens, inserted_at: at}
      end

    {:ok, steps}
  end

  @impl Capstan.Storage
  def get_signal(ref, scopes, name) do
    case q!(
           ref,
           "SELECT payload FROM capstan_signals WHERE scope = ANY($1) AND name = $2 LIMIT 1",
           [scopes, name]
         ) do
      %{rows: [[payload]]} -> {:ok, payload || %{}}
      _ -> :none
    end
  end

  @impl Capstan.Storage
  def put_signal(ref, scope, name, payload, now) do
    Postgrex.transaction(ref, fn conn ->
      q!(
        conn,
        """
        INSERT INTO capstan_signals (scope, name, payload, inserted_at)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (scope, name) DO UPDATE SET payload = EXCLUDED.payload,
                                                inserted_at = EXCLUDED.inserted_at
        """,
        [scope, name, payload, now]
      )

      %{rows: rows, columns: columns} =
        q!(
          conn,
          """
          UPDATE capstan_jobs
          SET state = 'ready', ready_at = $3, await_scope = NULL, await_name = NULL
          WHERE state = 'awaiting' AND await_scope = $1 AND await_name = $2
          RETURNING *
          """,
          [scope, name, now]
        )

      decode_jobs(rows, columns)
    end)
  end

  @impl Capstan.Storage
  def clear_signal(ref, scope, name) do
    q!(ref, "DELETE FROM capstan_signals WHERE scope = $1 AND name = $2", [scope, name])

    :ok
  end

  @impl Capstan.Storage
  def request_cancel(ref, id, now) do
    Postgrex.transaction(ref, fn conn ->
      case q!(conn, "SELECT * FROM capstan_jobs WHERE id = $1 FOR UPDATE", [id]) do
        %{rows: []} ->
          %{status: :noop, cancelled: [], released: []}

        %{rows: [row], columns: columns} ->
          job = decode_job(row, columns)

          cond do
            job.state in ~w(succeeded failed cancelled) ->
              %{status: :noop, cancelled: [], released: []}

            job.state == "running" ->
              q!(conn, "UPDATE capstan_jobs SET cancel_requested = true WHERE id = $1", [id])

              %{status: :requested, cancelled: [], released: []}

            true ->
              %{released: released, cancelled: cancelled} =
                apply_and_settle(conn, job, {:cancelled, %{"reason" => "cancel"}}, now,
                  fence: false)

              %{status: :cancelled, cancelled: cancelled, released: released}
          end
      end
    end)
  end

  @impl Capstan.Storage
  def workflow_jobs(ref, workflow_id) do
    %{rows: rows, columns: columns} =
      q!(ref, "SELECT * FROM capstan_jobs WHERE workflow_id = $1 ORDER BY id", [workflow_id])

    {:ok, decode_jobs(rows, columns)}
  end

  @impl Capstan.Storage
  def prune_rate(ref, before_unix) do
    q!(ref, "DELETE FROM capstan_rate WHERE window_start < $1", [before_unix])

    :ok
  end

  # -- Shared outcome application -----------------------------------------------

  # Applies the outcome to the row (optionally fenced on state+attempt), then
  # settles the workflow inside the same transaction. Returns the settle map
  # or :stale.
  defp apply_and_settle(conn, job, outcome, now, fence: fence?) do
    updated = Logic.apply_outcome(%{job | cancel_requested: false}, outcome, now)

    # Close the await race: park-and-wake atomically when the signal exists.
    updated =
      with {:await, scope, name, _deadline} <- outcome,
           %{rows: [_ | _]} <-
             q!(conn, "SELECT 1 FROM capstan_signals WHERE scope = $1 AND name = $2", [
               scope,
               name
             ]) do
        %{updated | state: "ready", ready_at: now, await_scope: nil, await_name: nil}
      else
        _ -> updated
      end

    fence_sql = if fence?, do: " AND state = 'running' AND attempt = $12", else: ""
    fence_params = if fence?, do: [job.attempt], else: []

    %{num_rows: num_rows} =
      q!(
        conn,
        """
        UPDATE capstan_jobs
        SET state = $2, ready_at = $3, attempt = $4, lease_until = $5, leased_by = $6,
            await_scope = $7, await_name = $8, result = $9, errors = $10,
            cancel_requested = false, finished_at = $11
        WHERE id = $1#{fence_sql}
        """,
        [
          job.id,
          updated.state,
          updated.ready_at,
          updated.attempt,
          updated.lease_until,
          updated.leased_by,
          updated.await_scope,
          updated.await_name,
          updated.result,
          updated.errors,
          updated.finished_at
        ] ++ fence_params
      )

    cond do
      num_rows == 0 ->
        :stale

      updated.state in ~w(succeeded failed cancelled) and updated.workflow_id != nil ->
        {released, cancelled} = settle_workflow(conn, updated.workflow_id, now)

        %{job: updated, released: released, cancelled: cancelled}

      true ->
        %{job: updated, released: [], cancelled: []}
    end
  end

  defp settle_workflow(conn, workflow_id, now) do
    q!(conn, "SELECT pg_advisory_xact_lock(hashtext($1))", ["capstan_wf:" <> workflow_id])

    %{rows: rows, columns: columns} =
      q!(conn, "SELECT * FROM capstan_jobs WHERE workflow_id = $1", [workflow_id])

    {release_ids, cancel_ids} = rows |> decode_jobs(columns) |> Logic.settle()

    released = settle_update(conn, release_ids, "ready", now)
    cancelled = settle_update(conn, cancel_ids, "cancelled", now)

    {released, cancelled}
  end

  defp settle_update(_conn, [], _state, _now), do: []

  defp settle_update(conn, ids, "ready", now) do
    %{rows: rows, columns: columns} =
      q!(
        conn,
        """
        UPDATE capstan_jobs SET state = 'ready', ready_at = $2
        WHERE id = ANY($1) AND state = 'held' RETURNING *
        """,
        [ids, now]
      )

    decode_jobs(rows, columns)
  end

  defp settle_update(conn, ids, "cancelled", now) do
    %{rows: rows, columns: columns} =
      q!(
        conn,
        """
        UPDATE capstan_jobs SET state = 'cancelled', finished_at = $2
        WHERE id = ANY($1) AND state = 'held' RETURNING *
        """,
        [ids, now]
      )

    decode_jobs(rows, columns)
  end

  # -- Claim helpers ------------------------------------------------------------

  defp clamp_global(demand, _conn, %{global_limit: nil}, _now), do: demand

  defp clamp_global(demand, conn, %{partition: nil, global_limit: limit} = spec, now) do
    %{rows: [[running]]} =
      q!(
        conn,
        """
        SELECT count(*) FROM capstan_jobs
        WHERE queue = $1 AND state = 'running' AND lease_until > $2
        """,
        [spec.queue, now]
      )

    min(demand, max(limit - running, 0))
  end

  defp clamp_global(demand, _conn, _spec, _now), do: demand

  defp clamp_rate(demand, _conn, %{rate: nil}, _now_unix), do: demand

  defp clamp_rate(demand, conn, %{rate: rate} = spec, now_unix) do
    win = Logic.window_start(now_unix, rate.period)

    %{rows: rows} =
      q!(
        conn,
        "SELECT window_start, count FROM capstan_rate WHERE queue = $1 AND window_start = ANY($2)",
        [spec.queue, [win - rate.period, win]]
      )

    counts = Map.new(rows, fn [w, c] -> {w, c} end)

    min(
      demand,
      Logic.rate_allowance(
        Map.get(counts, win - rate.period, 0),
        Map.get(counts, win, 0),
        rate.allowed,
        rate.period,
        now_unix
      )
    )
  end

  defp debit_rate(_conn, %{rate: nil}, _now_unix, _count), do: :ok
  defp debit_rate(_conn, _spec, _now_unix, 0), do: :ok

  defp debit_rate(conn, %{rate: rate} = spec, now_unix, count) do
    win = Logic.window_start(now_unix, rate.period)

    q!(
      conn,
      """
      INSERT INTO capstan_rate (queue, window_start, count) VALUES ($1, $2, $3)
      ON CONFLICT (queue, window_start) DO UPDATE SET count = capstan_rate.count + EXCLUDED.count
      """,
      [spec.queue, win, count]
    )

    :ok
  end

  defp partition_counts(conn, queue, {source, key}, now) do
    field = if source == :input, do: "input", else: "meta"

    %{rows: rows} =
      q!(
        conn,
        """
        SELECT COALESCE(#{field}->>$2, ''), count(*) FROM capstan_jobs
        WHERE queue = $1 AND state = 'running' AND lease_until > $3
        GROUP BY 1
        """,
        [queue, key, now]
      )

    Map.new(rows, fn [k, c] -> {k, c} end)
  end

  # -- Insert building ----------------------------------------------------------

  @insert_fields ~w(kind queue state input meta priority attempt max_attempts partition_key
                    ready_at workflow_id wf_name wf_deps wf_ignore cron_name cron_slot
                    budget_usd_micros budget_tokens spent_usd_micros spent_tokens errors
                    cancel_requested inserted_at)a

  defp build_insert(rows) do
    field_count = length(@insert_fields)

    placeholders =
      rows
      |> Enum.with_index()
      |> Enum.map_join(", ", fn {_row, i} ->
        cols = Enum.map_join(1..field_count, ", ", fn j -> "$#{i * field_count + j}" end)
        "(#{cols})"
      end)

    params =
      Enum.flat_map(rows, fn row ->
        Enum.map(@insert_fields, fn
          :errors -> row[:errors] || []
          field -> row[field]
        end)
      end)

    sql = """
    INSERT INTO capstan_jobs (#{Enum.map_join(@insert_fields, ", ", &to_string/1)})
    VALUES #{placeholders}
    ON CONFLICT (cron_name, cron_slot) WHERE cron_name IS NOT NULL DO NOTHING
    RETURNING *
    """

    {sql, params}
  end

  # -- Decoding -----------------------------------------------------------------

  defp decode_jobs(rows, columns), do: Enum.map(rows, &decode_job(&1, columns))

  defp decode_job(row, columns) do
    map =
      columns
      |> Enum.zip(row)
      |> Map.new(fn {col, val} -> {String.to_existing_atom(col), val} end)

    struct(Job, map)
  end

  defp sort_like(jobs, ids) do
    order = ids |> Enum.with_index() |> Map.new()

    Enum.sort_by(jobs, &order[&1.id])
  end

  defp unwrap({:ok, value}), do: {:ok, value}
  defp unwrap({:error, reason}), do: raise("capstan postgres transaction failed: #{inspect(reason)}")

  defp q!(conn_or_ref, sql, params), do: Postgrex.query!(conn_or_ref, sql, params)

  # Ensure the column-name atoms exist for decode_job.
  for col <- @jobs_columns do
    _ = String.to_atom(col)
  end

  # -- Migrations & test helpers ------------------------------------------------

  @migrations [
    {1,
     """
     CREATE TABLE IF NOT EXISTS capstan_jobs (
       id bigserial PRIMARY KEY,
       kind text NOT NULL,
       queue text NOT NULL,
       state text NOT NULL DEFAULT 'ready',
       input jsonb NOT NULL DEFAULT '{}',
       meta jsonb NOT NULL DEFAULT '{}',
       priority int NOT NULL DEFAULT 0,
       attempt int NOT NULL DEFAULT 0,
       max_attempts int NOT NULL DEFAULT 20,
       partition_key text,
       ready_at timestamptz,
       lease_until timestamptz,
       leased_by text,
       await_scope text,
       await_name text,
       workflow_id text,
       wf_name text,
       wf_deps text[] NOT NULL DEFAULT '{}',
       wf_ignore text[] NOT NULL DEFAULT '{}',
       cron_name text,
       cron_slot timestamptz,
       budget_usd_micros bigint,
       budget_tokens bigint,
       spent_usd_micros bigint NOT NULL DEFAULT 0,
       spent_tokens bigint NOT NULL DEFAULT 0,
       result bytea,
       errors jsonb NOT NULL DEFAULT '[]',
       cancel_requested boolean NOT NULL DEFAULT false,
       inserted_at timestamptz NOT NULL,
       started_at timestamptz,
       finished_at timestamptz
     );
     CREATE INDEX IF NOT EXISTS capstan_jobs_claim_idx
       ON capstan_jobs (queue, priority, ready_at, id) WHERE state = 'ready';
     CREATE INDEX IF NOT EXISTS capstan_jobs_await_due_idx
       ON capstan_jobs (queue, ready_at) WHERE state = 'awaiting' AND ready_at IS NOT NULL;
     CREATE INDEX IF NOT EXISTS capstan_jobs_await_wake_idx
       ON capstan_jobs (await_scope, await_name) WHERE state = 'awaiting';
     CREATE INDEX IF NOT EXISTS capstan_jobs_lease_idx
       ON capstan_jobs (lease_until) WHERE state = 'running';
     CREATE INDEX IF NOT EXISTS capstan_jobs_workflow_idx
       ON capstan_jobs (workflow_id) WHERE workflow_id IS NOT NULL;
     CREATE UNIQUE INDEX IF NOT EXISTS capstan_jobs_cron_slot_idx
       ON capstan_jobs (cron_name, cron_slot) WHERE cron_name IS NOT NULL;
     CREATE TABLE IF NOT EXISTS capstan_steps (
       job_id bigint NOT NULL,
       seq int NOT NULL,
       name text NOT NULL,
       value bytea,
       usd_micros bigint NOT NULL DEFAULT 0,
       tokens bigint NOT NULL DEFAULT 0,
       inserted_at timestamptz,
       PRIMARY KEY (job_id, name)
     );
     CREATE TABLE IF NOT EXISTS capstan_signals (
       scope text NOT NULL,
       name text NOT NULL,
       payload jsonb,
       inserted_at timestamptz,
       PRIMARY KEY (scope, name)
     );
     CREATE TABLE IF NOT EXISTS capstan_rate (
       queue text NOT NULL,
       window_start bigint NOT NULL,
       count int NOT NULL DEFAULT 0,
       PRIMARY KEY (queue, window_start)
     );
     """}
  ]

  @doc "Create or update the schema (idempotent)."
  def migrate!(url) do
    with_conn(url, fn conn ->
      q!(conn, "CREATE TABLE IF NOT EXISTS capstan_meta (version int NOT NULL)", [])

      current =
        case q!(conn, "SELECT max(version) FROM capstan_meta", []) do
          %{rows: [[nil]]} -> 0
          %{rows: [[version]]} -> version
        end

      for {version, ddl} <- @migrations, version > current do
        for statement <- String.split(ddl, ";", trim: true),
            String.trim(statement) != "" do
          q!(conn, statement, [])
        end

        q!(conn, "INSERT INTO capstan_meta (version) VALUES ($1)", [version])
      end
    end)

    :ok
  end

  @doc "Create the database if missing (connects to the postgres db)."
  def ensure_database!(url) do
    opts = parse_url(url)
    database = opts[:database]

    unless database =~ ~r/^[a-zA-Z0-9_]+$/ do
      raise ArgumentError, "unsafe database name: #{inspect(database)}"
    end

    {:ok, conn} =
      opts |> Keyword.put(:database, "postgres") |> Keyword.put(:pool_size, 1)
      |> Postgrex.start_link()

    case Postgrex.query!(conn, "SELECT 1 FROM pg_database WHERE datname = $1", [database]) do
      %{rows: []} -> Postgrex.query!(conn, "CREATE DATABASE #{database}", [])
      _ -> :ok
    end

    GenServer.stop(conn)

    :ok
  end

  @doc "Truncate all Capstan tables (tests)."
  def truncate!(ref) do
    q!(
      ref,
      "TRUNCATE capstan_jobs, capstan_steps, capstan_signals, capstan_rate RESTART IDENTITY",
      []
    )

    :ok
  end

  defp with_conn(url, fun) do
    {:ok, conn} =
      url |> parse_url() |> Keyword.put(:pool_size, 1)
      |> Keyword.put(:types, Capstan.Storage.PostgresTypes)
      |> Postgrex.start_link()

    try do
      fun.(conn)
    after
      GenServer.stop(conn)
    end
  end
end
