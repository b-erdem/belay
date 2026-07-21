:ets.new(:belay_events, [:named_table, :public, :bag])
:ets.new(:belay_gauge, [:named_table, :public, :set])
:ets.insert(:belay_gauge, {:running, 0})

if Application.get_env(:belay, :test_storage) == :postgres do
  url = Application.fetch_env!(:belay, :test_pg_url)

  Belay.Storage.Postgres.ensure_database!(url)
  Belay.Storage.Postgres.reset!(url)
end

ExUnit.start()
