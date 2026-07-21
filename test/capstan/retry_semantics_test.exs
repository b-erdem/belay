defmodule Capstan.RetrySemanticsTest.CancelSelfOnce do
  @moduledoc false

  # First attempt cancels itself between steps (a cooperative cancel honored
  # at the s2 boundary); a retried attempt runs clean.
  use Capstan.Worker, queue: :default

  alias Capstan.Ctx

  @impl Capstan.Worker
  def run(%Ctx{job: job} = ctx) do
    Capstan.step(ctx, "s1", fn -> 1 end)

    if job.attempt == 1 do
      instance = String.to_existing_atom(job.input["instance"])
      {:ok, :requested} = Capstan.cancel(instance, job.id)
    end

    Capstan.step(ctx, "s2", fn -> 2 end)
    {:ok, %{"done" => true}}
  end
end

defmodule Capstan.RetrySemanticsTest do
  # Operator retry is an explicit command that must supersede stale state:
  # a pending cancel_requested from before the retry, and a straight-to-ready
  # transition that would bypass workflow dependencies. Found by extending
  # the TLA+ model with a Retry action (verify/spec/).
  use Capstan.Test.Case, async: false

  alias Capstan.RetrySemanticsTest.CancelSelfOnce
  alias Capstan.Test.{FailN, Tagged}
  alias Capstan.Workflow

  setup do
    {:ok, start_capstan!()}
  end

  test "retry supersedes a stale pending cancel", %{name: name, clock: clock} do
    {:ok, job} = Capstan.insert(name, Tagged.new(%{"tag" => "s1"}, max_attempts: 1))

    # Claim without running the worker, request a cooperative cancel, then
    # crash the attempt: the job fails (attempts exhausted) with the cancel
    # flag still pending — exactly what a late cancel against a dying
    # worker leaves behind.
    {mod, ref} = storage(name)
    config = Capstan.Config.fetch!(name)
    spec = Capstan.Queues.resolve_spec!(config, :default)
    {:ok, [_]} = mod.claim(ref, spec, 1, "test-node", 1_000, Capstan.Config.now(config))

    assert {:ok, :requested} = Capstan.cancel(name, job.id)

    advance(clock, 2)
    now = Capstan.Config.now(config)
    {:ok, _} = mod.reclaim_expired(ref, now, fn _ -> now end)

    assert %{state: "failed", cancel_requested: true} = job!(name, job.id)

    # The retry is issued AFTER the cancel: it wins.
    {:ok, retried} = Capstan.retry_job(name, job.id)
    refute retried.cancel_requested

    assert %{succeeded: 1} = Testing.drain(name, :default)
    assert job!(name, job.id).state == "succeeded"
  end

  test "a cooperatively-cancelled job can be retried at all", %{name: name} do
    # Cooperative cancels keep cancel_requested set through the terminal
    # transition (the contract: transitions never clear it). Retry must
    # clear it, or every cooperatively-cancelled job is un-retryable
    # forever: ready -> claim -> honored cancel -> cancelled, in a loop.
    {:ok, job} =
      Capstan.insert(name, CancelSelfOnce.new(%{"instance" => Atom.to_string(name)}))

    assert %{cancelled: 1} = Testing.drain(name, :default)
    assert %{state: "cancelled", cancel_requested: true} = job!(name, job.id)

    {:ok, _} = Capstan.retry_job(name, job.id)

    assert %{succeeded: 1} = Testing.drain(name, :default)
    assert job!(name, job.id).state == "succeeded"
  end

  test "retrying a cascade-cancelled dependent re-holds it; deps still gate it",
       %{name: name, clock: clock} do
    {:ok, jobs} =
      Workflow.new()
      |> Workflow.add(:a, FailN.new(%{"fail_times" => 1}, max_attempts: 1))
      |> Workflow.add(:b, Tagged.new(%{"tag" => "b"}), deps: [:a])
      |> Workflow.insert(name)

    assert %{failed: 1} = Testing.drain(name, :default)
    assert job!(name, jobs["a"].id).state == "failed"
    assert job!(name, jobs["b"].id).state == "cancelled"

    # Direct retry of the dependent while its dep is failed: it must NOT
    # run. Settlement re-dooms it (its dep is still failed), so the retry
    # is a visible no-op rather than a dependency bypass.
    {:ok, _} = Capstan.retry_job(name, jobs["b"].id)
    Testing.drain(name, :default)
    assert job!(name, jobs["b"].id).state == "cancelled"

    # Root-first recovery: retry a (succeeds on its second attempt), then
    # retry b — it re-holds, settlement releases it, and it runs.
    {:ok, _} = Capstan.retry_job(name, jobs["a"].id)
    advance(clock, 6)
    assert %{succeeded: 1} = Testing.drain(name, :default)
    assert job!(name, jobs["a"].id).state == "succeeded"

    {:ok, _} = Capstan.retry_job(name, jobs["b"].id)
    assert %{succeeded: 1} = Testing.drain(name, :default)
    assert job!(name, jobs["b"].id).state == "succeeded"
  end
end
