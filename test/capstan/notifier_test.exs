# The Postgres notifier only means something against real Postgres; the
# Memory run skips this module entirely.
if Application.get_env(:capstan, :test_storage) == :postgres do
  defmodule Capstan.NotifierTest do
    use Capstan.Test.Case, async: false

    alias Capstan.Test.Echo

    # Two Capstan instances in one BEAM share the database but nothing else —
    # no registry, no :pg scope. Only pg_notify can carry the wake-up.
    defp pair!(notifiers) do
      inserter =
        start_capstan!(sim_clock: false, queues: [], notifiers: notifiers)

      worker =
        start_capstan!(
          sim_clock: false,
          notifiers: notifiers,
          queues: [default: 5],
          # Polling effectively disabled: pickup within the test window
          # can only come from a notifier.
          poll_interval: 60_000,
          busy_poll: 60_000,
          sweep_interval: 60_000
        )

      # Let the listener connections establish.
      Process.sleep(500)

      {inserter.name, worker.name}
    end

    test "pg_notify carries pokes and results across unconnected instances" do
      {inserter, _worker} = pair!([:local, :postgres])

      started = System.monotonic_time(:millisecond)

      {:ok, job} = Capstan.insert(inserter, Echo.new(%{"x" => 1}))

      assert {:ok, %{"x" => 1}} = Capstan.await_result(inserter, job.id, 5_000)

      elapsed = System.monotonic_time(:millisecond) - started

      # Far below the 60s polling floor — the NOTIFY path did the work.
      assert elapsed < 2_000
    end

    test "without the postgres notifier, unconnected instances are poll-bound" do
      {inserter, _worker} = pair!([:local])

      {:ok, job} = Capstan.insert(inserter, Echo.new(%{}))

      assert {:error, :timeout} = Capstan.await_result(inserter, job.id, 1_000)
      assert job!(inserter, job.id).state == "ready"
    end
  end
end
