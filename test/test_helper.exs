repo_config = Application.get_env(:capstan, Capstan.Test.Repo)
adapter = Application.get_env(:capstan, :test_adapter, Ecto.Adapters.SQLite3)

_ = adapter.storage_down(repo_config)
:ok = adapter.storage_up(repo_config)

defmodule Capstan.Test.Migration0 do
  use Ecto.Migration

  def up, do: Oban.Migration.up()
  def down, do: Oban.Migration.down()
end

defmodule Capstan.Test.Migration1 do
  use Ecto.Migration

  def up, do: Capstan.Migration.up()
  def down, do: Capstan.Migration.down()
end

{:ok, _} = Capstan.Test.Repo.start_link()

Ecto.Migrator.up(Capstan.Test.Repo, 0, Capstan.Test.Migration0)
Ecto.Migrator.up(Capstan.Test.Repo, 1, Capstan.Test.Migration1)

:ets.new(:capstan_events, [:named_table, :public, :bag])

ExUnit.start()
