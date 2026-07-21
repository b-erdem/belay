url = System.get_env("BENCH_URL") || "postgres://postgres:belay@localhost:55433/belay_bench"

Belay.Storage.Postgres.ensure_database!(url)
Belay.Storage.Postgres.reset!(url)

IO.puts("BENCH DB READY #{url}")
