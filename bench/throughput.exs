# Drain throughput: how many trivial jobs per second the cluster processes.
url = System.get_env("BENCH_URL") || "postgres://postgres:belay@localhost:55433/belay_bench"
n = String.to_integer(System.get_env("BENCH_JOBS", "3000"))

Code.require_file(Path.join(__DIR__, "bench_workers.exs"))

{:ok, _} =
  Belay.start_link(
    name: BenchDriver,
    storage: [adapter: :postgres, url: url],
    queues: [],
    notifiers: [:local, :postgres],
    retention: [succeeded: :infinity, failed: :infinity, cancelled: :infinity]
  )

Process.sleep(500)

t0 = System.monotonic_time(:millisecond)

for chunk <- Enum.chunk_every(1..n, 500) do
  Belay.insert_all(BenchDriver, Enum.map(chunk, &Bench.Echo.new(%{"i" => &1})))
end

insert_ms = System.monotonic_time(:millisecond) - t0

wait = fn wait ->
  stats = Belay.stats(BenchDriver)
  done = get_in(stats, ["default", "succeeded"]) || 0

  if done >= n do
    :ok
  else
    Process.sleep(100)
    wait.(wait)
  end
end

wait.(wait)

total_ms = System.monotonic_time(:millisecond) - t0

IO.puts(
  "RESULT throughput: #{n} jobs inserted in #{insert_ms}ms, fully processed in #{total_ms}ms " <>
    "=> #{Float.round(n / (total_ms / 1000), 0)} jobs/s end-to-end (3 worker processes)"
)
