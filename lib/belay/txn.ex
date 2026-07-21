defmodule Belay.Txn do
  @moduledoc """
  Transactional enqueue: insert jobs inside **your** database transaction, so
  a job exists if and only if the business write committed.

      Postgrex.transaction(MyApp.Pool, fn conn ->
        create_order!(conn, order)

        {:ok, _job} =
          Belay.Txn.insert(conn, MyApp.Belay, MyApp.FulfillOrder.new(%{"id" => order.id}))
      end)

  Works identically with an Ecto repo (anything exporting `query!/2`):

      MyApp.Repo.transaction(fn ->
        order = MyApp.Repo.insert!(changeset)

        {:ok, _job} =
          Belay.Txn.insert(MyApp.Repo, MyApp.Belay, MyApp.FulfillOrder.new(%{"id" => order.id}))
      end)

  Belay itself takes no Ecto dependency — the bridge is duck-typed over
  `query!`, and the SQL is exactly what the Postgres storage adapter runs.

  **Wake-ups are transactional too.** When the instance has the `:postgres`
  notifier configured, the poke is issued with `pg_notify` *inside the same
  transaction* — Postgres delivers notifications only on commit, so workers
  wake at the exact moment the job becomes real, and a rollback wakes nobody.
  Without that notifier, committed jobs are picked up by the adaptive polling
  floor (`busy_poll`..`poll_interval`).

  Requires the named Belay instance to be running in this VM (it supplies
  worker defaults, the clock, and notifier configuration), and Postgres
  storage.
  """

  alias Belay.{Config, Job}
  alias Belay.Notifier
  alias Belay.Storage.Postgres

  @doc "Insert one job in the caller's transaction. Returns `{:ok, job}` (duplicates flagged)."
  def insert(conn_or_repo, name, {Belay.Worker, _worker, _input, _opts} = buildable) do
    case insert_all(conn_or_repo, name, [buildable]) do
      [job] ->
        {:ok, job}

      [] ->
        # Deduped by a unique key — return the existing holder.
        config = Config.fetch!(name)
        row = build_rows(config, [buildable]) |> hd()

        %{rows: rows, columns: columns} =
          exec(
            conn_or_repo,
            "SELECT * FROM belay_jobs WHERE unique_key = $1 ORDER BY id DESC LIMIT 1",
            [row.unique_key]
          )

        [existing] = Postgres.decode_jobs(rows, columns)

        {:ok, %{existing | duplicate?: true}}
    end
  end

  @doc "Insert many jobs in the caller's transaction. Returns inserted jobs (dedupes skipped)."
  def insert_all(conn_or_repo, name, buildables) when is_list(buildables) do
    config = Config.fetch!(name)

    unless elem(config.storage, 0) == Postgres do
      raise ArgumentError, "Belay.Txn requires the :postgres storage adapter"
    end

    rows = build_rows(config, buildables)
    {sql, params} = Postgres.build_insert(rows)

    %{rows: returned, columns: columns} = exec(conn_or_repo, sql, params)

    jobs = Postgres.decode_jobs(returned, columns)

    notify_in_txn(conn_or_repo, config, jobs)

    jobs
  end

  defp build_rows(config, buildables) do
    now = Config.now(config)

    key = Config.encryption_key(config)

    Enum.map(buildables, fn {Belay.Worker, worker, input, opts} ->
      Job.new(
        worker,
        input,
        opts |> Keyword.put(:now, now) |> Keyword.put(:encryption_key, key),
        worker.__belay_defaults__()
      )
    end)
  end

  # pg_notify inside the transaction: delivered on commit, dropped on
  # rollback — the wake-up inherits the transaction's fate.
  defp notify_in_txn(conn_or_repo, config, jobs) do
    if Enum.any?(config.notifiers, &match?({Notifier.Postgres, _}, &1)) do
      channel = Notifier.Postgres.channel(config)

      for queue <- jobs |> Enum.map(& &1.queue) |> Enum.uniq() do
        payload = Jason.encode!(%{"t" => "p", "q" => queue})

        exec(conn_or_repo, "SELECT pg_notify($1, $2)", [channel, payload])
      end
    end

    :ok
  end

  # Ecto repos (and anything repo-shaped) export query!/2; everything else is
  # treated as a Postgrex conn/pool.
  defp exec(conn_or_repo, sql, params) do
    if is_atom(conn_or_repo) and Code.ensure_loaded?(conn_or_repo) and
         function_exported?(conn_or_repo, :query!, 2) do
      conn_or_repo.query!(sql, params)
    else
      Postgrex.query!(conn_or_repo, sql, params)
    end
  end
end
