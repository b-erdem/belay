defmodule Mix.Tasks.Belay.MigrateOban do
  @shortdoc "Migrate Oban's pending jobs into Belay (dry-run by default)"

  @moduledoc """
  Move pending Oban work into Belay, safely.

      # See what would happen (no writes):
      mix belay.migrate_oban --url postgres://localhost/my_app

      # Do it (stop Oban's producers first):
      mix belay.migrate_oban --url postgres://localhost/my_app --execute

      # Renamed a worker during the port?
      mix belay.migrate_oban --url ... --execute \\
        --map "MyApp.OldWorker=MyApp.NewWorker"

  Migrates `available`/`scheduled`/`retryable` rows only. `executing` rows
  are flagged and skipped (they may be live on an Oban node — drain first).
  Terminal rows are history, not work: keep them where they are
  (`ALTER TABLE oban_jobs RENAME TO oban_jobs_archive`) until retention
  lapses. Re-runs are idempotent via `meta.migrated_from_oban_id`.

  Run this task from your application (`mix belay.migrate_oban` inside
  the app) so worker modules are loadable — the analyzer verifies each
  pending worker is actually ported to `Belay.Worker` and refuses to
  migrate ones that aren't (override with `--force`).

  Uniqueness is the one thing that cannot migrate mechanically: Oban keeps
  unique *policy* on workers, Belay keeps unique *keys* on rows. The
  report lists affected workers; re-declare `unique:` at their insert sites.
  """

  use Mix.Task

  @switches [url: :string, execute: :boolean, force: :boolean, map: :keep]

  @impl Mix.Task
  def run(argv) do
    {opts, _, _} = OptionParser.parse(argv, strict: @switches)

    url = opts[:url] || Mix.raise("--url postgres://... is required")

    mapping =
      opts
      |> Keyword.get_values(:map)
      |> Map.new(fn pair ->
        case String.split(pair, "=", parts: 2) do
          [from, to] -> {from, to}
          _ -> Mix.raise("--map expects Old.Worker=New.Worker, got: #{pair}")
        end
      end)

    Mix.Task.run("app.start")

    {:ok, conn} =
      url
      |> Belay.Storage.Postgres.parse_url()
      |> Keyword.put(:pool_size, 2)
      |> Postgrex.start_link()

    report = Belay.Migrate.Oban.analyze(conn, mapping: mapping)

    print_report(report)

    cond do
      opts[:execute] && report.unported != [] && !opts[:force] ->
        Mix.raise(
          "refusing to migrate with unported workers (see above); port them, --map them, or --force"
        )

      opts[:execute] ->
        result = Belay.Migrate.Oban.execute(conn, report, force: opts[:force] || false)

        Mix.shell().info("""

        Migrated #{result.migrated} jobs (#{result.skipped_existing} already migrated, skipped).

        Next steps:
          1. Keep Oban stopped; deploy with Belay producers on.
          2. When the archive window lapses:
             ALTER TABLE oban_jobs RENAME TO oban_jobs_archive;  -- then eventually DROP
        """)

      true ->
        Mix.shell().info("\nDry run — nothing written. Re-run with --execute to migrate.")
    end
  end

  defp print_report(report) do
    shell = Mix.shell()

    shell.info("== oban_jobs by state")

    for {state, n} <- Enum.sort(report.states) do
      shell.info("  #{String.pad_trailing(state, 10)} #{n}")
    end

    shell.info("\n== pending work (#{report.pending} jobs)")

    for w <- report.workers do
      status = if w.ported, do: "ported", else: "NOT PORTED"
      renamed = if w.kind != w.worker, do: " -> #{w.kind}", else: ""

      shell.info("  #{w.worker}#{renamed}  queue=#{w.queue}  #{w.pending} pending  [#{status}]")
    end

    if report.executing > 0 do
      shell.info("""

      !! #{report.executing} jobs are `executing` — possibly live on an Oban node.
         They are never migrated. Stop/drain Oban, let them finish or become
         retryable, then re-run.
      """)
    end

    if report.historical > 0 do
      shell.info(
        "\n#{report.historical} terminal jobs stay put (history, not work) — see the archive pattern in the task docs."
      )
    end

    if report.already_migrated > 0 do
      shell.info(
        "#{report.already_migrated} rows were migrated by a previous run (will be skipped)."
      )
    end
  end
end
