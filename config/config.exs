import Config

if config_env() == :test do
  # CAPSTAN_PG=1 runs the suite against Postgres instead of SQLite.
  if System.get_env("CAPSTAN_PG") do
    config :capstan, :test_adapter, Ecto.Adapters.Postgres

    config :capstan, Capstan.Test.Repo,
      hostname: "localhost",
      port: 55433,
      username: "postgres",
      password: "capstan",
      database: "capstan_test",
      pool_size: 5,
      stacktrace: true
  else
    config :capstan, :test_adapter, Ecto.Adapters.SQLite3

    config :capstan, Capstan.Test.Repo,
      database: "/tmp/capstan_test.db",
      journal_mode: :wal,
      busy_timeout: 5_000,
      pool_size: 5,
      stacktrace: true
  end

  config :capstan, ecto_repos: [Capstan.Test.Repo]

  config :logger, level: :warning
end
