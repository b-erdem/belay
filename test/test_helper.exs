:ets.new(:capstan_events, [:named_table, :public, :bag])
:ets.new(:capstan_gauge, [:named_table, :public, :set])
:ets.insert(:capstan_gauge, {:running, 0})

if Application.get_env(:capstan, :test_storage) == :postgres do
  url = Application.fetch_env!(:capstan, :test_pg_url)

  Capstan.Storage.Postgres.ensure_database!(url)
  Capstan.Storage.Postgres.reset!(url)
end

ExUnit.start()
