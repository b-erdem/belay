# Dispatch-latency benchmark: measures insert → await_result round trips
# (the number an agent dispatcher actually feels) and a 100-job burst drain.
#
# Modes (BENCH_MODE):
#   same_node — queues run in this process; wake-ups are local pokes
#   remote    — jobs run in a separate OS process (bench/worker.exs);
#               wake-ups depend on BENCH_NOTIFIERS (local | local,postgres)
url = System.get_env("BENCH_URL") || "postgres://postgres:capstan@localhost:55433/capstan_bench"
mode = System.get_env("BENCH_MODE", "same_node")
n = String.to_integer(System.get_env("BENCH_N", "200"))
label = System.get_env("BENCH_LABEL", mode)

notifiers =
  case System.get_env("BENCH_NOTIFIERS", "local") do
    "local" -> [:local]
    "local,postgres" -> [:local, :postgres]
  end

Code.require_file(Path.join(__DIR__, "bench_workers.exs"))

queues = if mode == "same_node", do: [default: 16], else: []

{:ok, _} =
  Capstan.start_link(
    name: BenchDriver,
    storage: [adapter: :postgres, url: url],
    queues: queues,
    notifiers: notifiers,
    retention: [succeeded: :infinity, failed: :infinity, cancelled: :infinity]
  )

Process.sleep(500)

measure = fn ->
  t0 = System.monotonic_time(:microsecond)
  {:ok, job} = Capstan.insert(BenchDriver, Bench.Echo.new(%{"t" => t0}))
  {:ok, _} = Capstan.await_result(BenchDriver, job.id, 15_000)

  System.monotonic_time(:microsecond) - t0
end

# Warmup (pools, code paths, adaptive intervals).
for _ <- 1..25, do: measure.()

samples = for _ <- 1..n, do: measure.()

sorted = Enum.sort(samples)
pct = fn p -> Enum.at(sorted, min(round(p / 100 * n), n - 1)) / 1_000 end
mean = Enum.sum(samples) / n / 1_000

# Burst: 100 inserts at once, one await sweep.
burst_t0 = System.monotonic_time(:microsecond)
burst_jobs = Capstan.insert_all(BenchDriver, for(i <- 1..100, do: Bench.Echo.new(%{"i" => i})))

for job <- burst_jobs do
  {:ok, _} = Capstan.await_result(BenchDriver, job.id, 15_000)
end

burst_ms = (System.monotonic_time(:microsecond) - burst_t0) / 1_000

fmt = fn value -> value |> Float.round(1) |> Float.to_string() |> String.pad_leading(6) end

IO.puts(
  "RESULT #{String.pad_trailing(label, 28)} " <>
    "rtt p50=#{fmt.(pct.(50))}ms p90=#{fmt.(pct.(90))}ms p99=#{fmt.(pct.(99))}ms " <>
    "max=#{fmt.(Enum.max(samples) / 1_000)}ms mean=#{fmt.(mean)}ms | " <>
    "100-job burst drained in #{round(burst_ms)}ms"
)
