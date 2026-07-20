# A bench worker node with DEFAULT latency settings (poll_interval 500ms,
# busy_poll 25ms) — the point is to measure what stock configuration delivers
# with and without the Postgres notifier.
url = System.get_env("BENCH_URL") || "postgres://postgres:capstan@localhost:55433/capstan_bench"

notifiers =
  case System.get_env("BENCH_NOTIFIERS", "local") do
    "local" -> [:local]
    "local,postgres" -> [:local, :postgres]
  end

Code.require_file(Path.join(__DIR__, "bench_workers.exs"))

{:ok, _} =
  Capstan.start_link(
    name: BenchNode,
    storage: [adapter: :postgres, url: url],
    queues: [default: 16],
    notifiers: notifiers,
    retention: [succeeded: :infinity, failed: :infinity, cancelled: :infinity]
  )

IO.puts("BENCH WORKER READY notifiers=#{inspect(notifiers)}")

Process.sleep(:infinity)
