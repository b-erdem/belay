defmodule Belay.ProductionTest do
  use Belay.Test.Case, async: false

  alias Belay.Test.{Echo, Emitter, Hanging, Strict}

  setup do
    {:ok, start_belay!()}
  end

  test "retention pruning removes old terminal jobs with their journal", %{
    name: name,
    clock: clock
  } do
    {:ok, job} = Belay.insert(name, Emitter.new(%{}))
    {:ok, keeper} = Belay.insert(name, Echo.new(%{}, schedule_in: 999_999))

    Testing.drain(name, :default)

    assert Belay.events(name, job.id) != []

    advance(clock, 90_000)

    {mod, ref} = storage(name)
    now = Belay.Config.now(config(name))

    {:ok, 1} = mod.prune_jobs(ref, "succeeded", now, 86_400, 100)

    assert Belay.get_job(name, job.id) == :error
    assert Belay.events(name, job.id) == []
    assert {:ok, []} = Belay.steps(name, job.id)

    # Incomplete jobs are untouchable regardless of age.
    assert {:ok, _} = Belay.get_job(name, keeper.id)
  end

  test "signals expire on their ttl", %{name: name, clock: clock} do
    Belay.signal(name, "custom", :old, %{})

    advance(clock, 700_000)

    Belay.signal(name, "custom", :fresh, %{})

    {mod, ref} = storage(name)
    :ok = mod.prune_signals(ref, Belay.Config.now(config(name)), 604_800)

    assert :none = mod.get_signal(ref, ["custom"], "old")
    assert {:ok, _} = mod.get_signal(ref, ["custom"], "fresh")
  end

  test "execution timeouts cut off hung runs and retry", %{name: name, clock: clock} do
    {:ok, job} = Belay.insert(name, Hanging.new(%{"hang" => true}))

    assert %{ready: 1} = Testing.drain(name, :default)

    hung = job!(name, job.id)

    assert hung.state == "ready"
    assert [%{"error" => error}] = hung.errors
    assert error =~ ":timeout"

    advance(clock, 6)

    # Attempt 2 of 2 also times out and exhausts via the normal failure path.
    assert %{failed: 1} = Testing.drain(name, :default)
    assert job!(name, job.id).state == "failed"
    assert Events.count({:hang_attempt, 1}) == 1
    assert Events.count({:hang_attempt, 2}) == 1
  end

  test "input schemas reject bad inserts at the call site", %{name: name} do
    assert_raise Belay.InputError, ~r/url is required/, fn ->
      Belay.insert(name, Strict.new(%{}))
    end

    assert_raise Belay.InputError, ~r/expected \{:enum/, fn ->
      Belay.insert(name, Strict.new(%{"url" => "https://x", "style" => "baggy"}))
    end

    {:ok, job} = Belay.insert(name, Strict.new(%{"url" => "https://x"}))

    Testing.drain(name, :default)

    assert {:ok, %{"url" => "https://x", "style" => "tight"}} =
             Belay.await_result(name, job.id, 100)
  end

  test "stats, list_jobs, and retry_job", %{name: name} do
    {:ok, _ok} = Belay.insert(name, Echo.new(%{}))

    {:ok, bad} =
      Belay.insert(
        name,
        Belay.Test.Tagged.new(%{"tag" => "x", "fail" => true}, max_attempts: 1)
      )

    Testing.drain(name, :default)

    assert %{"default" => %{"succeeded" => 1, "failed" => 1}} = Belay.stats(name)

    assert [%{id: failed_id}] = Belay.list_jobs(name, state: :failed)
    assert failed_id == bad.id

    assert {:error, :not_retryable} =
             Belay.retry_job(name, hd(Belay.list_jobs(name, state: :succeeded)).id)

    {:ok, retried} = Belay.retry_job(name, bad.id)

    assert retried.state == "ready"
    assert retried.max_attempts == 2

    assert %{failed: 1} = Testing.drain(name, :default)
  end
end
