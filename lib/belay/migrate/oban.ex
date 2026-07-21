defmodule Belay.Migrate.Oban do
  @moduledoc """
  Move an Oban installation's **pending work** into Belay.

  Converts `available`, `scheduled`, and `retryable` rows from `oban_jobs`
  into `ready` Belay jobs (scheduled/retryable keep their `scheduled_at`
  as `ready_at`). Everything else is deliberately left alone:

    * `executing` rows are flagged, never migrated — they may be mid-run on
      a live Oban node. Stop or drain Oban first.
    * Terminal rows (`completed`, `discarded`, `cancelled`) are history, not
      work. Migrating them would produce journal-less, cost-less rows that
      pollute Belay's metrics while buying nothing. Keep the old table
      read-only until your retention window lapses, then drop it:

          ALTER TABLE oban_jobs RENAME TO oban_jobs_archive;

  Runs are idempotent: each migrated row carries
  `meta.migrated_from_oban_id`, and re-runs skip ids already present.

  Uniqueness cannot be inferred — Oban stores unique *policy* on the worker,
  Belay stores unique *keys* on rows. The analyzer lists workers whose
  pending rows may need `unique:` re-declared at their insert sites.

  Used by `mix belay.migrate_oban`; callable directly:

      {:ok, conn} = Postgrex.start_link(...)
      report = Belay.Migrate.Oban.analyze(conn)
      Belay.Migrate.Oban.execute(conn, report)
  """

  @pending ~w(available scheduled retryable)

  @doc """
  Inspect `oban_jobs` and return a migration report (no writes).

  Options: `:mapping` — `%{"Old.Worker" => "New.Worker"}` kind renames.
  """
  def analyze(conn, opts \\ []) do
    mapping = Keyword.get(opts, :mapping, %{})

    %{rows: state_rows} =
      Postgrex.query!(conn, "SELECT state::text, count(*) FROM oban_jobs GROUP BY 1", [])

    states = Map.new(state_rows, fn [state, n] -> {state, n} end)

    %{rows: worker_rows} =
      Postgrex.query!(
        conn,
        "SELECT worker, queue, count(*) FROM oban_jobs WHERE state::text = ANY($1) GROUP BY 1, 2 ORDER BY 1, 2",
        [@pending]
      )

    # The dry run is exactly the command people run before Belay's schema
    # exists, so a missing belay_jobs table reports "none migrated yet"
    # rather than crashing with a raw undefined_table error.
    already =
      case Postgrex.query(
             conn,
             "SELECT count(*) FROM belay_jobs WHERE meta ? 'migrated_from_oban_id'",
             []
           ) do
        {:ok, %{rows: [[n]]}} -> n
        {:error, %Postgrex.Error{postgres: %{code: :undefined_table}}} -> 0
      end

    workers =
      Enum.map(worker_rows, fn [worker, queue, n] ->
        kind = Map.get(mapping, worker, worker)

        %{
          worker: worker,
          kind: kind,
          queue: queue,
          pending: n,
          ported: ported?(kind)
        }
      end)

    %{
      states: states,
      pending: Enum.sum(for {s, n} <- states, s in @pending, do: n),
      executing: Map.get(states, "executing", 0),
      historical: Enum.sum(for {s, n} <- states, s in ~w(completed discarded cancelled), do: n),
      workers: workers,
      unported: Enum.reject(workers, & &1.ported),
      already_migrated: already,
      mapping: mapping
    }
  end

  @doc """
  Migrate the pending rows described by `analyze/2`'s report. Returns
  `%{migrated: n, skipped_existing: n}`. Only workers that resolve to a
  Belay worker module are migrated unless `force: true`.
  """
  def execute(conn, report, opts \\ []) do
    force? = Keyword.get(opts, :force, false)
    now = DateTime.utc_now()

    kinds =
      for w <- report.workers, w.ported or force?, do: {w.worker, w.kind}, into: %{}

    %{rows: rows} =
      Postgrex.query!(
        conn,
        """
        SELECT id, worker, queue, args, state::text, attempt, max_attempts,
               priority, scheduled_at, errors
        FROM oban_jobs
        WHERE state::text = ANY($1) AND worker = ANY($2)
        ORDER BY id
        """,
        [@pending, Map.keys(kinds)]
      )

    converted = Enum.map(rows, &convert(&1, kinds, now))

    {migrated, skipped} =
      converted
      |> Enum.chunk_every(500)
      |> Enum.reduce({0, 0}, fn chunk, {mig, skip} ->
        inserted = insert_chunk(conn, chunk)

        {mig + inserted, skip + (length(chunk) - inserted)}
      end)

    %{migrated: migrated, skipped_existing: skipped}
  end

  @doc false
  # One Oban row -> Belay insert params. Public-shaped for tests.
  def convert(
        [id, worker, queue, args, state, attempt, max_attempts, priority, scheduled_at, errors],
        kinds,
        now
      ) do
    ready_at =
      case state do
        # Available work is claimable immediately; scheduled/retryable keep
        # their due time (an overdue time is claimable immediately anyway).
        "available" -> now
        _ -> to_utc(scheduled_at)
      end

    %{
      kind: Map.fetch!(kinds, worker),
      queue: queue,
      input: args,
      state: "ready",
      attempt: attempt,
      max_attempts: max_attempts,
      priority: priority,
      ready_at: ready_at,
      errors: convert_errors(errors),
      meta: %{"migrated_from_oban_id" => id},
      inserted_at: now
    }
  end

  # Oban errors are a jsonb[] of %{"at", "attempt", "error"}; Belay errors
  # are a jsonb array of %{"error", "attempt", ...}. Shapes align closely
  # enough to carry history through the retry ceiling correctly.
  defp convert_errors(nil), do: []

  defp convert_errors(errors) when is_list(errors) do
    Enum.map(errors, fn e ->
      %{
        "error" => e["error"] || "unknown",
        "attempt" => e["attempt"],
        "at" => e["at"],
        "migrated" => true
      }
    end)
  end

  defp insert_chunk(_conn, []), do: 0

  defp insert_chunk(conn, chunk) do
    # Idempotency: a NOT EXISTS guard on migrated_from_oban_id makes re-runs
    # skip rows that already landed (cheap at migration scale).
    Enum.reduce(chunk, 0, fn row, acc ->
      %{num_rows: n} =
        Postgrex.query!(
          conn,
          """
          INSERT INTO belay_jobs
            (kind, queue, input, state, attempt, max_attempts, priority,
             ready_at, errors, meta, spent_usd_micros, spent_tokens,
             cancel_requested, inserted_at)
          SELECT $1, $2, $3, 'ready', $4, $5, $6, $7, $8, $9, 0, 0, false, $10
          WHERE NOT EXISTS (
            SELECT 1 FROM belay_jobs
            WHERE meta->>'migrated_from_oban_id' = ($9::jsonb->>'migrated_from_oban_id')
          )
          """,
          [
            row.kind,
            row.queue,
            row.input,
            row.attempt,
            row.max_attempts,
            row.priority,
            row.ready_at,
            row.errors,
            row.meta,
            row.inserted_at
          ]
        )

      acc + n
    end)
  end

  defp ported?(kind) do
    module = Module.concat([kind])

    Code.ensure_loaded?(module) and function_exported?(module, :__belay_defaults__, 0)
  end

  defp to_utc(%NaiveDateTime{} = naive), do: DateTime.from_naive!(naive, "Etc/UTC")
  defp to_utc(%DateTime{} = dt), do: dt
  defp to_utc(nil), do: DateTime.utc_now()
end
