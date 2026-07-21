defmodule Capstan.ChunkTest do
  use Capstan.Test.Case, async: false

  alias Capstan.Test.{ChunkBoom, ChunkEcho, ChunkFlaky, ChunkGather, ChunkNoImpl}

  setup do
    {:ok, start_capstan!()}
  end

  test "jobs run in size-bounded chunks with per-job results", %{name: name} do
    jobs =
      for n <- 1..7 do
        {:ok, job} = Capstan.insert(name, ChunkEcho.new(%{"n" => n}))
        job
      end

    assert %{succeeded: 7} = Testing.drain(name, :default)

    # 7 jobs at size 3 → chunks of 3, 3, 1.
    assert Events.count({:chunk, 3}) == 2
    assert Events.count({:chunk, 1}) == 1

    fourth = Enum.at(jobs, 3)
    assert {:ok, 8} = Capstan.await_result(name, fourth.id, 100)
  end

  test "partial failure retries only the failed jobs", %{name: name, clock: clock} do
    for n <- 1..3, do: {:ok, _} = Capstan.insert(name, ChunkFlaky.new(%{"n" => n}))
    {:ok, flaky} = Capstan.insert(name, ChunkFlaky.new(%{"n" => 99, "fail" => true}))

    assert %{succeeded: 3, ready: 1} = Testing.drain(name, :default)
    assert Events.count({:chunk, 4}) == 1

    advance(clock, 6)

    assert %{succeeded: 1} = Testing.drain(name, :default)
    assert Events.count({:chunk, 1}) == 1
    assert {:ok, 99} = Capstan.await_result(name, flaky.id, 100)
  end

  test "a whole-chunk error retries every job on its own backoff", %{name: name, clock: clock} do
    for _ <- 1..4, do: {:ok, _} = Capstan.insert(name, ChunkBoom.new(%{}))

    assert %{ready: 4} = Testing.drain(name, :default)

    advance(clock, 6)

    assert %{succeeded: 4} = Testing.drain(name, :default)
  end

  test "chunk: without run_chunk/1 fails jobs with a config error", %{name: name} do
    {:ok, job} = Capstan.insert(name, ChunkNoImpl.new(%{}))

    assert %{failed: 1} = Testing.drain(name, :default)

    job = job!(name, job.id)
    assert [%{"error" => error} | _] = job.errors
    assert error =~ "run_chunk/1"
  end

  test "live producer gathers up to the window, dispatches full chunks at once" do
    %{name: name} = start_capstan!(sim_clock: false, queues: [default: 10], poll_interval: 40)

    # 2 jobs < size 3: gathered, dispatched together at the ~800ms deadline.
    for n <- 1..2, do: {:ok, _} = Capstan.insert(name, ChunkGather.new(%{"n" => n}))

    wait_until(fn -> Events.count({:gathered, 2}) == 1 end, 3_000)

    # 3 more: a full chunk dispatches without waiting for the window.
    started = System.monotonic_time(:millisecond)
    for n <- 3..5, do: {:ok, _} = Capstan.insert(name, ChunkGather.new(%{"n" => n}))

    wait_until(fn -> Events.count({:gathered, 3}) == 1 end, 3_000)

    # Well under the 800ms gather window proves the full chunk skipped the
    # deadline path — with slack for slow CI runners.
    assert System.monotonic_time(:millisecond) - started < 600
  end

end
