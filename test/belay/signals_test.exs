defmodule Belay.SignalsTest do
  use Belay.Test.Case, async: false

  alias Belay.Test.Awaiter

  setup do
    {:ok, start_belay!()}
  end

  test "await parks the job; a signal wakes it with the payload", %{name: name} do
    {:ok, job} = Belay.insert(name, Awaiter.new(%{}))

    assert Testing.drain(name, :default) == %{awaiting: 1}

    parked = job!(name, job.id)

    assert parked.state == "awaiting"
    assert parked.await_scope == "job:#{job.id}"
    assert parked.await_name == "approval"
    assert parked.ready_at == nil

    Belay.signal_job(name, job.id, :approval, %{"approved" => true})

    assert job!(name, job.id).state == "ready"
    assert %{succeeded: 1} = Testing.drain(name, :default)
    assert {:ok, %{"approved" => true}} = Belay.await_result(name, job.id, 100)
  end

  test "a pre-delivered signal is found immediately", %{name: name} do
    {:ok, job} = Belay.insert(name, Awaiter.new(%{}))

    Belay.signal_job(name, job.id, :approval, %{"pre" => 1})

    assert %{succeeded: 1} = Testing.drain(name, :default)
    assert {:ok, %{"pre" => 1}} = Belay.await_result(name, job.id, 100)
  end

  test "await deadlines resume with a timeout", %{name: name, clock: clock} do
    {:ok, job} = Belay.insert(name, Awaiter.new(%{"timeout" => 60}))

    Testing.drain(name, :default)

    assert job!(name, job.id).state == "awaiting"

    advance(clock, 61)

    assert %{succeeded: 1} = Testing.drain(name, :default)
    assert {:ok, %{"timeout" => true}} = Belay.await_result(name, job.id, 100)
  end

  test "cleared signals block again", %{name: name} do
    {:ok, job} = Belay.insert(name, Awaiter.new(%{}))

    Belay.signal_job(name, job.id, :approval, %{})
    Belay.clear_signal(name, "job:#{job.id}", :approval)

    Testing.drain(name, :default)

    assert job!(name, job.id).state == "awaiting"
  end

  test "a signal landing while the job runs wakes it at park time", %{name: name} do
    {:ok, job} = Belay.insert(name, Awaiter.new(%{}))

    {mod, ref} = storage(name)
    conf = config(name)
    spec = Belay.Config.queue_spec(conf, :default)
    now = Belay.Config.now(conf)

    {:ok, [claimed]} = mod.claim(ref, spec, 1, "n", 30_000, now)

    # Delivered mid-execution: no awaiting job exists yet, so nothing wakes...
    {:ok, []} = mod.put_signal(ref, "job:#{job.id}", "approval", %{"mid" => 1}, now)

    # ...but the ack that parks the job must see the signal and stay ready.
    {:ok, %{job: parked}} =
      mod.ack(ref, claimed, {:await, "job:#{job.id}", "approval", nil}, now)

    assert parked.state == "ready"

    assert %{succeeded: 1} = Testing.drain(name, :default)
    assert {:ok, %{"mid" => 1}} = Belay.await_result(name, job.id, 100)
  end
end
