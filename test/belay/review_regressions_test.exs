defmodule Belay.ReviewRegressionsTest do
  # Regressions for issues found by the pre-launch adversarial review.
  use Belay.Test.Case, async: false

  alias Belay.Test.{EmptyFanOut, RaisingTimeout}

  test "a worker that raises under a timeout: retries, it does not crash the executor" do
    %{name: name, clock: clock} = start_belay!()

    {:ok, job} = Belay.insert(name, RaisingTimeout.new(%{}))

    # First attempt: the raise is caught and journaled, not propagated.
    assert %{ready: 1} = Testing.drain(name, :default)
    j = job!(name, job.id)
    assert j.state == "ready"
    assert [%{"error" => err} | _] = j.errors
    assert err =~ "boom under timeout"

    advance(clock, 6)
    assert %{failed: 1} = Testing.drain(name, :default)
    assert job!(name, job.id).state == "failed"
  end

  test "a fan-out of zero children returns [] and the parent completes" do
    %{name: name} = start_belay!(queues: [default: [limit: 5, manual: true]])

    {:ok, job} = Belay.insert(name, EmptyFanOut.new(%{}))

    assert %{succeeded: 1} = Testing.drain(name, :default)
    assert {:ok, %{"children" => 0}} = Belay.await_result(name, job.id, 500)
  end

  test "a cancelled job in a partitioned queue is never claimed (cancel-race guard)" do
    %{name: name} =
      start_belay!(queues: [part: [limit: 5, partition: {:input, "tenant"}, manual: true]])

    {:ok, job} = Belay.insert(name, Belay.Test.Echo.new(%{"tenant" => "t1"}, queue: :part))
    assert {:ok, :cancelled} = Belay.cancel(name, job.id)

    {mod, ref} = storage(name)
    config = Belay.Config.fetch!(name)
    spec = Belay.Queues.resolve_spec!(config, :part)

    {:ok, claimed} = mod.claim(ref, spec, 10, "node", 10_000, Belay.Config.now(config))
    assert claimed == []
    assert job!(name, job.id).state == "cancelled"
  end

  test "queue_stats reports per-queue spend sums", %{} do
    %{name: name} = start_belay!(queues: [default: [limit: 5, manual: true]])

    {:ok, job} = Belay.insert(name, Belay.Test.Budgeted.new(%{"steps" => 3, "usd" => 0.10}))
    Testing.drain(name, :default)

    {mod, ref} = storage(name)
    {:ok, rows} = mod.queue_stats(ref)

    total_usd = rows |> Enum.map(& &1.usd_micros) |> Enum.sum()
    assert total_usd == 300_000
    assert job!(name, job.id).spent_usd_micros == 300_000
  end
end
