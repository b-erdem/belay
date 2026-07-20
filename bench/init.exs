url = System.get_env("BENCH_URL") || "postgres://postgres:capstan@localhost:55433/capstan_bench"

Capstan.Storage.Postgres.ensure_database!(url)
Capstan.Storage.Postgres.reset!(url)

IO.puts("BENCH DB READY #{url}")
