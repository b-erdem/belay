defmodule Capstan.LeasesTest do
  use Capstan.Test.Case, async: false

  alias Capstan.Test.{Echo, Tagged}
  alias Capstan.Workflow

  setup do
    {:ok, start_capstan!(lease_ttl: 10_000)}
  end

  defp claim_one!(name) do
    {mod, ref} = storage(name)
    conf = config(name)
    spec = Capstan.Config.queue_spec(conf, :default)

    {:ok, [job]} = mod.claim(ref, spec, 1, "n1", 10_000, Capstan.Config.now(conf))

    job
  end

  defp reclaim!(name) do
    {mod, ref} = storage(name)
    conf = config(name)
    now = Capstan.Config.now(conf)

    {:ok, result} =
      mod.reclaim_expired(ref, now, fn _job -> DateTime.add(now, 5, :second) end)

    result
  end

  test "expired leases are reclaimed for retry with attempts intact", %{
    name: name,
    clock: clock
  } do
    {:ok, inserted} = Capstan.insert(name, Echo.new(%{"n" => 1}))

    claimed = claim_one!(name)
    assert claimed.attempt == 1

    assert reclaim!(name) == %{retried: [], failed: []}

    advance(clock, 11)

    assert %{retried: [id]} = reclaim!(name)
    assert id == inserted.id

    reclaimed = job!(name, inserted.id)

    assert reclaimed.state == "ready"
    assert reclaimed.attempt == 1

    advance(clock, 6)

    assert %{succeeded: 1} = Testing.drain(name, :default)
    assert job!(name, inserted.id).attempt == 2
  end

  test "renewed leases do not expire", %{name: name, clock: clock} do
    {:ok, inserted} = Capstan.insert(name, Echo.new(%{}))

    claim_one!(name)

    advance(clock, 8)

    {mod, ref} = storage(name)
    conf = config(name)
    until = DateTime.add(Capstan.Config.now(conf), 10, :second)

    {:ok, [renewed_id]} = mod.renew_leases(ref, [inserted.id], "n1", until)
    assert renewed_id == inserted.id

    advance(clock, 5)

    assert reclaim!(name) == %{retried: [], failed: []}
  end

  test "stale acks from zombie executors are fenced off", %{name: name, clock: clock} do
    {:ok, _} = Capstan.insert(name, Echo.new(%{}))

    zombie_view = claim_one!(name)

    advance(clock, 11)
    %{retried: [_]} = reclaim!(name)

    advance(clock, 6)
    assert %{succeeded: 1} = Testing.drain(name, :default)

    {mod, ref} = storage(name)
    now = Capstan.Config.now(config(name))

    assert {:error, :stale} = mod.ack(ref, zombie_view, {:succeeded, nil}, now)
  end

  test "lease exhaustion fails the job and cascades its workflow", %{
    name: name,
    clock: clock
  } do
    {:ok, jobs} =
      Workflow.new()
      |> Workflow.add(:a, Tagged.new(%{"tag" => "a"}, max_attempts: 1))
      |> Workflow.add(:b, Tagged.new(%{"tag" => "b"}), deps: [:a])
      |> Workflow.insert(name)

    claim_one!(name)

    advance(clock, 11)

    assert %{failed: [_]} = reclaim!(name)

    states =
      name
      |> Workflow.jobs(jobs["a"].workflow_id)
      |> Map.new(&{&1.wf_name, &1.state})

    assert states == %{"a" => "failed", "b" => "cancelled"}
  end
end
