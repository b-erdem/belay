# Integration test for the Oban migrator: fabricates a minimal oban_jobs
# table in the test database, runs analyze/execute, and checks conversion,
# skip rules, and idempotency. Postgres-only by nature.
if Application.get_env(:belay, :test_storage) == :postgres do
  defmodule Belay.MigrateObanTest do
    use Belay.Test.Case, async: false

    alias Belay.Migrate.Oban, as: Migrate

    defmodule PortedWorker do
      use Belay.Worker, queue: :migrated
      @impl Belay.Worker
      def run(_ctx), do: :ok
    end

    setup do
      # Real clock: the migrator stamps ready_at with wall time, which a
      # January-anchored sim clock would see as months in the future.
      context =
        start_belay!(
          sim_clock: false,
          queues: [
            default: [limit: 10, manual: true],
            q1: [limit: 5, manual: true],
            q2: [limit: 5, manual: true]
          ]
        )

      {_mod, ref} = storage(context.name)

      Postgrex.query!(ref, "DROP TABLE IF EXISTS oban_jobs", [])

      Postgrex.query!(
        ref,
        """
        CREATE TABLE oban_jobs (
          id bigserial PRIMARY KEY,
          state text NOT NULL,
          queue text NOT NULL,
          worker text NOT NULL,
          args jsonb NOT NULL DEFAULT '{}',
          errors jsonb[] NOT NULL DEFAULT ARRAY[]::jsonb[],
          attempt integer NOT NULL DEFAULT 0,
          max_attempts integer NOT NULL DEFAULT 20,
          priority integer NOT NULL DEFAULT 0,
          scheduled_at timestamp NOT NULL DEFAULT now(),
          inserted_at timestamp NOT NULL DEFAULT now()
        )
        """,
        []
      )

      ported = "Belay.MigrateObanTest.PortedWorker"

      Postgrex.query!(
        ref,
        """
        INSERT INTO oban_jobs (state, queue, worker, args, attempt, max_attempts, priority, scheduled_at, errors) VALUES
        ('available', 'q1', $1, '{"n": 1}', 0, 5, 0, now(), ARRAY[]::jsonb[]),
        ('scheduled', 'q1', $1, '{"n": 2}', 0, 5, 1, now() + interval '1 hour', ARRAY[]::jsonb[]),
        ('retryable', 'q2', $1, '{"n": 3}', 2, 8, 0, now() + interval '5 minutes',
          ARRAY['{"at": "2026-07-20T00:00:00Z", "attempt": 2, "error": "boom"}'::jsonb]),
        ('executing', 'q1', $1, '{"n": 4}', 1, 5, 0, now(), ARRAY[]::jsonb[]),
        ('completed', 'q1', $1, '{"n": 5}', 1, 5, 0, now(), ARRAY[]::jsonb[]),
        ('available', 'q3', 'Legacy.UnportedWorker', '{}', 0, 3, 0, now(), ARRAY[]::jsonb[])
        """,
        [ported]
      )

      {:ok, Map.put(context, :conn, ref)}
    end

    test "analyze classifies pending, executing, historical, unported", %{conn: conn} do
      report = Migrate.analyze(conn)

      assert report.pending == 4
      assert report.executing == 1
      assert report.historical == 1
      assert [%{worker: "Legacy.UnportedWorker", ported: false}] = report.unported
      assert Enum.count(report.workers, & &1.ported) == 2
    end

    test "execute migrates only ported pending rows, converts faithfully, re-runs skip",
         %{conn: conn, name: name} do
      report = Migrate.analyze(conn)

      assert %{migrated: 3, skipped_existing: 0} = Migrate.execute(conn, report)

      jobs = Belay.list_jobs(name, %{state: "ready", limit: 50})
      migrated = Enum.filter(jobs, &(&1.meta["migrated_from_oban_id"] != nil))
      assert length(migrated) == 3

      retryable = Enum.find(migrated, &(&1.input["n"] == 3))
      assert retryable.attempt == 2
      assert retryable.max_attempts == 8
      assert retryable.queue == "q2"
      assert [%{"error" => "boom", "attempt" => 2, "migrated" => true}] = retryable.errors

      scheduled = Enum.find(migrated, &(&1.input["n"] == 2))
      assert DateTime.compare(scheduled.ready_at, DateTime.utc_now()) == :gt
      assert scheduled.priority == 1

      # Executing (n=4), completed (n=5), unported queue q3: never migrated.
      refute Enum.any?(migrated, &(&1.input["n"] in [4, 5]))
      refute Enum.any?(migrated, &(&1.queue == "q3"))

      # Idempotent re-run.
      assert %{migrated: 0, skipped_existing: 3} = Migrate.execute(conn, Migrate.analyze(conn))

      # And the migrated work actually runs.
      assert %{succeeded: n} = Testing.drain(name, :q1)
      assert n >= 1
    end

    test "mapping renames kinds and counts as ported when the target is", %{conn: conn} do
      report =
        Migrate.analyze(conn,
          mapping: %{"Legacy.UnportedWorker" => "Belay.MigrateObanTest.PortedWorker"}
        )

      assert report.unported == []

      assert %{migrated: 4} = Migrate.execute(conn, report)
    end
  end
end
