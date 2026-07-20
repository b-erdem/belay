# Transactional enqueue is Postgres-specific; skipped on the Memory run.
if Application.get_env(:capstan, :test_storage) == :postgres do
  defmodule Capstan.TxnTest do
    use Capstan.Test.Case, async: false

    alias Capstan.Test.Echo
    alias Capstan.Txn

    # An Ecto-repo-shaped module: exports query!/2 backed by the storage pool.
    defmodule FakeRepo do
      def query!(sql, params) do
        Postgrex.query!(:persistent_term.get({__MODULE__, :pool}), sql, params)
      end
    end

    test "a rollback aborts the enqueue; a commit lands it", %{} do
      %{name: name} = start_capstan!()
      pool = storage_ref(name)

      {:error, :nope} =
        Postgrex.transaction(pool, fn conn ->
          [_job] = Txn.insert_all(conn, name, [Echo.new(%{"txn" => "rolled-back"})])

          Postgrex.rollback(conn, :nope)
        end)

      assert Capstan.list_jobs(name) == []

      {:ok, {:ok, job}} =
        Postgrex.transaction(pool, fn conn ->
          Txn.insert(conn, name, Echo.new(%{"txn" => "committed"}))
        end)

      assert %{succeeded: 1} = Testing.drain(name, :default)
      assert {:ok, %{"txn" => "committed"}} = Capstan.await_result(name, job.id, 100)
    end

    test "unique dedup works inside the transaction" do
      %{name: name} = start_capstan!()
      pool = storage_ref(name)

      {:ok, {first, dup}} =
        Postgrex.transaction(pool, fn conn ->
          {:ok, first} = Txn.insert(conn, name, Echo.new(%{}, unique: "txn:1"))
          {:ok, dup} = Txn.insert(conn, name, Echo.new(%{}, unique: "txn:1"))

          {first, dup}
        end)

      refute first.duplicate?
      assert dup.duplicate?
      assert dup.id == first.id
    end

    test "an Ecto-shaped repo module works through the same bridge" do
      %{name: name} = start_capstan!()

      :persistent_term.put({FakeRepo, :pool}, storage_ref(name))

      {:ok, job} = Txn.insert(FakeRepo, name, Echo.new(%{"via" => "repo"}))

      assert %{succeeded: 1} = Testing.drain(name, :default)
      assert {:ok, %{"via" => "repo"}} = Capstan.await_result(name, job.id, 100)
    end

    test "with the :postgres notifier, the wake-up delivers on commit only" do
      # Worker instance with polling disabled: only a committed pg_notify can
      # get the job picked up.
      inserter = start_capstan!(sim_clock: false, queues: [], notifiers: [:local, :postgres])

      worker =
        start_capstan!(
          sim_clock: false,
          notifiers: [:local, :postgres],
          queues: [default: 5],
          poll_interval: 60_000,
          busy_poll: 60_000,
          sweep_interval: 60_000
        )

      Process.sleep(500)

      pool = storage_ref(inserter.name)

      {:ok, {:ok, job}} =
        Postgrex.transaction(pool, fn conn ->
          Txn.insert(conn, inserter.name, Echo.new(%{"woken" => "by-commit"}))
        end)

      assert {:ok, %{"woken" => "by-commit"}} =
               Capstan.await_result(inserter.name, job.id, 5_000)

      _ = worker

      :ok
    end
  end
end
