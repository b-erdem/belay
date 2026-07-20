# Reset the soak database and create the harness's own tables.
url = System.get_env("SOAK_URL") || "postgres://postgres:capstan@localhost:55433/capstan_soak"

Capstan.Storage.Postgres.ensure_database!(url)
Capstan.Storage.Postgres.reset!(url)

{:ok, conn} =
  url
  |> Capstan.Storage.Postgres.parse_url()
  |> Keyword.merge(pool_size: 1, types: Capstan.Storage.PostgresTypes)
  |> Postgrex.start_link()

Postgrex.query!(conn, "DROP TABLE IF EXISTS soak_effects, soak_ledger", [])

Postgrex.query!(
  conn,
  """
  CREATE TABLE soak_effects (
    id bigserial PRIMARY KEY,
    job_id bigint NOT NULL,
    step text NOT NULL,
    attempt int NOT NULL,
    tag text,
    at timestamptz NOT NULL DEFAULT now()
  )
  """,
  []
)

Postgrex.query!(
  conn,
  """
  CREATE TABLE soak_ledger (
    job_id bigint PRIMARY KEY,
    kind text NOT NULL,
    expected jsonb
  )
  """,
  []
)

IO.puts("SOAK DB READY #{url}")
