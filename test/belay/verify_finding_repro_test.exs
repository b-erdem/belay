defmodule ReproTimeoutRaiseWorker do
  @moduledoc false
  use Belay.Worker, queue: :default, max_attempts: 3, timeout: {5, :second}

  @impl Belay.Worker
  def run(_ctx), do: raise("boom")
end

defmodule ReproPlainRaiseWorker do
  @moduledoc false
  use Belay.Worker, queue: :default, max_attempts: 3

  @impl Belay.Worker
  def run(_ctx), do: raise("boom")
end

defmodule Belay.VerifyFindingReproTest do
  use Belay.Test.Case, async: false

  setup do
    {:ok, start_belay!()}
  end

  defp drain_in_probe(name) do
    {pid, ref} =
      spawn_monitor(fn ->
        result = Belay.Testing.drain(name, :default)
        exit({:drained, result})
      end)

    receive do
      {:DOWN, ^ref, :process, ^pid, reason} -> reason
    after
      5_000 -> :probe_timeout
    end
  end

  test "CONTROL: raising worker WITHOUT timeout drains to retry", %{name: name} do
    {:ok, job} = Belay.insert(name, ReproPlainRaiseWorker.new(%{}))

    reason = drain_in_probe(name)
    IO.inspect(reason, label: "control drain exit reason", limit: 4)

    after_job = job!(name, job.id)

    IO.inspect({after_job.state, after_job.attempt, after_job.errors},
      label: "control job after drain"
    )

    assert {:drained, %{ready: 1}} = reason
    assert after_job.state == "ready"
    assert [%{"error" => err}] = after_job.errors
    assert err =~ "boom"
  end

  test "FINDING: raising worker WITH timeout kills executor, job stuck running", %{name: name} do
    {:ok, job} = Belay.insert(name, ReproTimeoutRaiseWorker.new(%{}))

    reason = drain_in_probe(name)
    IO.inspect(reason, label: "timeout-worker drain exit reason", limit: 4)

    after_job = job!(name, job.id)

    IO.inspect({after_job.state, after_job.attempt, after_job.errors},
      label: "timeout-worker job after drain"
    )

    # If the finding is REAL: the probe dies with the raw RuntimeError (link
    # kill), the job is left leased in "running" with no error journaled.
    # If it is a FALSE POSITIVE: reason == {:drained, %{ready: 1}} like control.
    case reason do
      {:drained, _} ->
        IO.puts("FALSE POSITIVE: drain survived the raise")

      {%RuntimeError{message: "boom"}, _stack} ->
        IO.puts("REAL: probe process killed by linked inner task")

      other ->
        IO.inspect(other, label: "unexpected probe exit")
    end

    IO.puts(
      "job state after: #{after_job.state}, attempt: #{after_job.attempt}, errors: #{length(after_job.errors)}"
    )
  end
end
