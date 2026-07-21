# One soak worker node. Killed with -9 and respawned by run.sh throughout the
# soak; everything it was running must be reclaimed by the survivors.
url = System.get_env("SOAK_URL") || "postgres://postgres:belay@localhost:55433/belay_soak"
tag = System.get_env("SOAK_TAG") || "w?"

Code.require_file(Path.join(__DIR__, "soak_workers.exs"))

:persistent_term.put(:soak_tag, tag)

{:ok, _} =
  url
  |> Belay.Storage.Postgres.parse_url()
  |> Keyword.merge(name: SoakDB, pool_size: 5, types: Belay.Storage.PostgresTypes)
  |> Postgrex.start_link()

{:ok, _} =
  Belay.start_link(
    name: SoakNode,
    storage: [adapter: :postgres, url: url],
    queues: [default: 8, children: 8, flow: 4],
    crons: [[name: "soak-cron", expr: "* * * * *", worker: Soak.Cron]],
    poll_interval: 200,
    lease_ttl: 4_000,
    sweep_interval: 1_000,
    cron_interval: 5_000,
    shutdown_grace: 500,
    # Keep every terminal job for post-soak verification.
    retention: [succeeded: :infinity, failed: :infinity, cancelled: :infinity]
  )

IO.puts("SOAK WORKER #{tag} READY os_pid=#{System.pid()}")

Process.sleep(:infinity)
