import Config

if config_env() == :test do
  # BELAY_PG=1 runs the shared suite against Postgres instead of Memory.
  config :belay, :test_storage, if(System.get_env("BELAY_PG"), do: :postgres, else: :memory)

  config :belay, :test_pg_url, "postgres://postgres:belay@localhost:55433/belay_v2_test"

  config :logger, level: :warning
end
