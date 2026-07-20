import Config

if config_env() == :test do
  # CAPSTAN_PG=1 runs the shared suite against Postgres instead of Memory.
  config :capstan, :test_storage, if(System.get_env("CAPSTAN_PG"), do: :postgres, else: :memory)

  config :capstan, :test_pg_url, "postgres://postgres:capstan@localhost:55433/capstan_v2_test"

  config :logger, level: :warning
end
