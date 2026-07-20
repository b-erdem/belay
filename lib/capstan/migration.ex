defmodule Capstan.Migration do
  @moduledoc """
  Migrations for Capstan's auxiliary tables and indexes.

  Add to a generated migration after `Oban.Migration`:

      defmodule MyApp.Repo.Migrations.AddCapstan do
        use Ecto.Migration

        def up, do: Capstan.Migration.up()
        def down, do: Capstan.Migration.down()
      end

  Works on Postgres and SQLite3. Creates:

    * `capstan_steps` — memoized durable-step results, keyed by `(job_id, name)`
    * `capstan_signals` — named signals used by `Capstan.await_signal/3`
    * `capstan_rate` — sliding-window rate limiter counters used by `Capstan.Engine`
    * `capstan_crons` — runtime-editable cron entries for `Capstan.Plugins.DynamicCron`
    * expression indexes on `oban_jobs.meta` for workflow/batch/chain/await lookups
  """

  use Ecto.Migration

  @doc "Create Capstan tables and indexes."
  def up(_opts \\ []) do
    create_if_not_exists table(:capstan_steps, primary_key: false) do
      add :job_id, :bigint, null: false
      add :name, :text, null: false
      add :attempt, :integer, null: false, default: 0
      add :result, :binary
      add :inserted_at, :utc_datetime_usec
    end

    create_if_not_exists unique_index(:capstan_steps, [:job_id, :name])

    create_if_not_exists table(:capstan_signals, primary_key: false) do
      add :scope, :text, null: false
      add :name, :text, null: false
      add :payload, :map
      add :inserted_at, :utc_datetime_usec
    end

    create_if_not_exists unique_index(:capstan_signals, [:scope, :name])

    create_if_not_exists table(:capstan_rate, primary_key: false) do
      add :queue, :text, null: false
      add :resource, :text, null: false, default: ""
      add :window_start, :bigint, null: false
      add :count, :integer, null: false, default: 0
    end

    create_if_not_exists unique_index(:capstan_rate, [:queue, :resource, :window_start])

    create_if_not_exists table(:capstan_crons, primary_key: false) do
      add :name, :text, null: false
      add :expression, :text, null: false
      add :worker, :text, null: false
      add :args, :map
      add :opts, :map
      add :timezone, :text
      add :paused, :boolean, null: false, default: false
      add :last_enqueued_at, :utc_datetime_usec
      add :inserted_at, :utc_datetime_usec
      add :updated_at, :utc_datetime_usec
    end

    create_if_not_exists unique_index(:capstan_crons, [:name])

    for {idx, key} <- meta_indexes() do
      execute(meta_index_sql(repo().__adapter__, idx, key))
    end

    :ok
  end

  @doc "Drop Capstan tables and indexes."
  def down(_opts \\ []) do
    for {idx, _key} <- meta_indexes() do
      execute("DROP INDEX IF EXISTS #{idx}")
    end

    drop_if_exists table(:capstan_crons)
    drop_if_exists table(:capstan_rate)
    drop_if_exists table(:capstan_signals)
    drop_if_exists table(:capstan_steps)

    :ok
  end

  defp meta_indexes do
    [
      {"oban_jobs_capstan_workflow_idx", "workflow_id"},
      {"oban_jobs_capstan_batch_idx", "batch_id"},
      {"oban_jobs_capstan_chain_idx", "chain_key"},
      {"oban_jobs_capstan_awaiting_idx", "awaiting_scope"}
    ]
  end

  defp meta_index_sql(Ecto.Adapters.SQLite3, idx, key) do
    """
    CREATE INDEX IF NOT EXISTS #{idx} ON oban_jobs (json_extract(meta, '$.#{key}'))
    WHERE json_extract(meta, '$.#{key}') IS NOT NULL
    """
  end

  defp meta_index_sql(_postgres, idx, key) do
    """
    CREATE INDEX IF NOT EXISTS #{idx} ON oban_jobs ((meta->>'#{key}'))
    WHERE meta->>'#{key}' IS NOT NULL
    """
  end
end
