defmodule Belay.Test.Events do
  @moduledoc false

  def record(key) do
    :ets.insert(:belay_events, {System.unique_integer([:positive, :monotonic]), key})
  end

  def all do
    :belay_events |> :ets.tab2list() |> Enum.sort() |> Enum.map(&elem(&1, 1))
  end

  def count(key), do: Enum.count(all(), &(&1 == key))

  def clear do
    :ets.delete_all_objects(:belay_events)
    :ets.insert(:belay_gauge, {:running, 0})
  end

  def gauge_up do
    current = :ets.update_counter(:belay_gauge, :running, 1)
    record({:gauge, current})
    current
  end

  def gauge_down, do: :ets.update_counter(:belay_gauge, :running, -1)

  def peak_gauge do
    all()
    |> Enum.flat_map(fn
      {:gauge, n} -> [n]
      _ -> []
    end)
    |> Enum.max(fn -> 0 end)
  end
end

defmodule Belay.Test.Echo do
  @moduledoc false
  use Belay.Worker, queue: :default, max_attempts: 3

  alias Belay.Ctx

  @impl Belay.Worker
  def run(%Ctx{job: job}), do: {:ok, job.input}
end

defmodule Belay.Test.Tagged do
  @moduledoc false
  use Belay.Worker, queue: :default, max_attempts: 3

  alias Belay.{Ctx, Test.Events}

  @impl Belay.Worker
  def run(%Ctx{job: job}) do
    Events.record({:ran, job.input["tag"]})

    if job.input["fail"], do: {:error, :nope}, else: :ok
  end
end

defmodule Belay.Test.FailN do
  @moduledoc false

  # Raises while attempt <= input["fail_times"], then succeeds.
  use Belay.Worker, queue: :default, max_attempts: 5

  alias Belay.Ctx

  @impl Belay.Worker
  def run(%Ctx{job: job}) do
    if job.attempt <= (job.input["fail_times"] || 0) do
      raise "planned failure #{job.attempt}"
    end

    {:ok, %{"attempt" => job.attempt}}
  end

  @impl Belay.Worker
  def backoff(_attempt), do: 5
end

defmodule Belay.Test.StepFlaky do
  @moduledoc false
  use Belay.Worker, queue: :default, max_attempts: 3

  alias Belay.{Ctx, Test.Events}

  @impl Belay.Worker
  def run(%Ctx{job: job} = ctx) do
    base =
      Belay.step(ctx, :expensive, fn ->
        Events.record(:step_ran)
        41
      end)

    if job.attempt == 1, do: raise("boom after step")

    {:ok, base + 1}
  end

  @impl Belay.Worker
  def backoff(_attempt), do: 5
end

defmodule Belay.Test.Budgeted do
  @moduledoc false

  # Runs input["steps"] steps, each costing input["usd"] dollars and
  # input["tokens"] tokens.
  use Belay.Worker, queue: :default, max_attempts: 1

  alias Belay.{Ctx, Test.Events}

  @impl Belay.Worker
  def run(%Ctx{job: job} = ctx) do
    for i <- 1..job.input["steps"] do
      Belay.step(
        ctx,
        "s#{i}",
        fn ->
          Events.record({:step, i})
          i
        end,
        cost: [usd: job.input["usd"] || 0, tokens: job.input["tokens"] || 0]
      )
    end

    :ok
  end
end

defmodule Belay.Test.Awaiter do
  @moduledoc false
  use Belay.Worker, queue: :default, max_attempts: 10

  alias Belay.Ctx

  @impl Belay.Worker
  def run(%Ctx{job: job} = ctx) do
    opts = if timeout = job.input["timeout"], do: [timeout: timeout], else: []

    case Belay.await(ctx, :approval, opts) do
      {:error, :timeout} -> {:ok, %{"timeout" => true}}
      payload -> {:ok, payload}
    end
  end
end

defmodule Belay.Test.Steered do
  @moduledoc false
  use Belay.Worker, queue: :default, max_attempts: 1

  alias Belay.Ctx

  @impl Belay.Worker
  def run(%Ctx{} = ctx) do
    Belay.step(ctx, :one, fn -> 1 end)

    {:ok, %{"steer" => Belay.steering(ctx)}}
  end
end

defmodule Belay.Test.NapThenDone do
  @moduledoc false
  use Belay.Worker, queue: :default, max_attempts: 5

  alias Belay.{Ctx, Test.Events}

  @impl Belay.Worker
  def run(%Ctx{job: job} = ctx) do
    Belay.step(ctx, :first, fn ->
      Events.record(:first)
      :ok
    end)

    Belay.sleep(ctx, :nap, job.input["seconds"])

    {:ok, %{"woke" => true}}
  end
end

defmodule Belay.Test.StepOnly do
  @moduledoc false
  use Belay.Worker, queue: :default, max_attempts: 3

  alias Belay.Ctx

  @impl Belay.Worker
  def run(%Ctx{} = ctx) do
    {:ok, Belay.step(ctx, :a, fn -> 1 end)}
  end
end

defmodule Belay.Test.SlowLive do
  @moduledoc false
  use Belay.Worker, queue: :limited, max_attempts: 1

  alias Belay.{Ctx, Test.Events}

  @impl Belay.Worker
  def run(%Ctx{}) do
    Events.gauge_up()
    Process.sleep(30)
    Events.gauge_down()

    :ok
  end
end

defmodule Belay.Test.CronJob do
  @moduledoc false
  use Belay.Worker, queue: :default, max_attempts: 1

  alias Belay.{Ctx, Test.Events}

  @impl Belay.Worker
  def run(%Ctx{}) do
    Events.record(:cron_ran)
    :ok
  end
end

defmodule Belay.Test.ChildEcho do
  @moduledoc false
  use Belay.Worker, queue: :default, max_attempts: 3

  alias Belay.Ctx

  @impl Belay.Worker
  def run(%Ctx{job: job}) do
    if job.input["fail"], do: {:error, :child_boom}, else: {:ok, job.input["v"] * 2}
  end
end

defmodule Belay.Test.FanOut do
  @moduledoc false
  use Belay.Worker, queue: :default, max_attempts: 3

  alias Belay.Ctx

  @impl Belay.Worker
  def run(%Ctx{job: job} = ctx) do
    inputs = Enum.map(job.input["values"], &%{"v" => &1})
    children = Belay.map_children(ctx, :fan, Belay.Test.ChildEcho, inputs)

    {:ok, Enum.map(children, &Belay.Job.result/1)}
  end
end

defmodule Belay.Test.SpawnCrash do
  @moduledoc false

  # Spawns two children, crashes once, then collects them on retry — proving
  # spawn memoization prevents duplicate children.
  use Belay.Worker, queue: :default, max_attempts: 3

  alias Belay.Ctx

  @impl Belay.Worker
  def run(%Ctx{job: job} = ctx) do
    _ids =
      Belay.spawn_many(ctx, :kids, [
        Belay.Test.ChildEcho.new(%{"v" => 1}),
        Belay.Test.ChildEcho.new(%{"v" => 2})
      ])

    if job.attempt == 1, do: raise("crash after spawn")

    children = Belay.await_children(ctx)

    {:ok, Enum.map(children, &Belay.Job.result/1)}
  end

  @impl Belay.Worker
  def backoff(_attempt), do: 5
end

defmodule Belay.Test.Hanging do
  @moduledoc false
  use Belay.Worker, queue: :default, max_attempts: 2, timeout: {150, :millisecond}

  alias Belay.{Ctx, Test.Events}

  @impl Belay.Worker
  def run(%Ctx{job: job}) do
    Events.record({:hang_attempt, job.attempt})

    if job.input["hang"] do
      Process.sleep(:timer.seconds(30))
    end

    :ok
  end

  @impl Belay.Worker
  def backoff(_attempt), do: 5
end

defmodule Belay.Test.Emitter do
  @moduledoc false
  use Belay.Worker, queue: :default, max_attempts: 1

  alias Belay.Ctx

  @impl Belay.Worker
  def run(%Ctx{} = ctx) do
    for i <- 1..3 do
      Belay.emit(ctx, %{"chunk" => "token-#{i}"})
    end

    {:ok, :emitted}
  end
end

defmodule Belay.Test.Divergent do
  @moduledoc false

  # The executed path depends on a runtime flag, letting tests simulate a code
  # change between the original run and a replay.
  use Belay.Worker, queue: :default, max_attempts: 1

  alias Belay.Ctx

  @impl Belay.Worker
  def run(%Ctx{} = ctx) do
    a = Belay.step(ctx, :a, fn -> 20 end)

    b =
      case :persistent_term.get({__MODULE__, :path}, :original) do
        :original -> Belay.step(ctx, :b, fn -> 22 end)
        :changed -> Belay.step(ctx, :b_new, fn -> 22 end)
      end

    {:ok, a + b}
  end
end

defmodule Belay.Test.ResourceUser do
  @moduledoc false
  use Belay.Worker, queue: :metered, max_attempts: 1

  alias Belay.Ctx

  @impl Belay.Worker
  def run(%Ctx{job: job} = ctx) do
    Belay.step(ctx, :call, fn -> :called end)
    Belay.debit(ctx, "prov", job.input["actual"])

    :ok
  end
end

defmodule Belay.Test.Strict do
  @moduledoc false
  use Belay.Worker,
    queue: :default,
    max_attempts: 1,
    input_schema: [
      url: [type: :string, required: true],
      style: [type: {:enum, ["tight", "loose"]}, default: "tight"],
      limit: [type: :integer]
    ]

  alias Belay.Ctx

  @impl Belay.Worker
  def run(%Ctx{job: job}), do: {:ok, job.input}
end

defmodule Belay.Test.Secret do
  @moduledoc false
  use Belay.Worker, queue: :default, max_attempts: 1, encrypted: true

  alias Belay.Ctx

  @impl Belay.Worker
  def run(%Ctx{job: job}), do: {:ok, job.input}
end

defmodule Belay.Test.Keys do
  @moduledoc false

  def test_key, do: :binary.copy(<<7>>, 32)
end

defmodule Belay.Test.ChunkEcho do
  @moduledoc false

  # Doubles each job's "n" in one run_chunk call; records observed chunk sizes.
  use Belay.Worker, queue: :default, chunk: [size: 3, gather_ms: 0]

  alias Belay.Test.Events

  @impl Belay.Worker
  def run_chunk(ctxs) do
    Events.record({:chunk, length(ctxs)})

    {:ok, Map.new(ctxs, fn ctx -> {ctx.job.id, ctx.job.input["n"] * 2} end)}
  end
end

defmodule Belay.Test.ChunkFlaky do
  @moduledoc false

  # Per-job outcomes: inputs with "fail" fail on their first attempt only.
  use Belay.Worker, queue: :default, max_attempts: 3, chunk: [size: 10, gather_ms: 0]

  alias Belay.Test.Events

  @impl Belay.Worker
  def run_chunk(ctxs) do
    Events.record({:chunk, length(ctxs)})

    Map.new(ctxs, fn ctx ->
      if ctx.job.input["fail"] && ctx.job.attempt == 1 do
        {ctx.job.id, {:error, "planned chunk failure"}}
      else
        {ctx.job.id, {:ok, ctx.job.input["n"]}}
      end
    end)
  end

  @impl Belay.Worker
  def backoff(_attempt), do: 5
end

defmodule Belay.Test.ChunkBoom do
  @moduledoc false

  # Whole-chunk error on first attempts, success after.
  use Belay.Worker, queue: :default, max_attempts: 3, chunk: [size: 5, gather_ms: 0]

  @impl Belay.Worker
  def run_chunk(ctxs) do
    if hd(ctxs).job.attempt == 1, do: {:error, "chunk boom"}, else: :ok
  end

  @impl Belay.Worker
  def backoff(_attempt), do: 5
end

defmodule Belay.Test.ChunkGather do
  @moduledoc false

  # Live-gathering worker: the window is wide enough that a slow CI runner
  # cannot blur the immediate-dispatch path into the deadline path.
  use Belay.Worker, queue: :default, chunk: [size: 3, gather_ms: 800]

  alias Belay.Test.Events

  @impl Belay.Worker
  def run_chunk(ctxs) do
    Events.record({:gathered, length(ctxs)})

    :ok
  end
end

defmodule Belay.Test.ChunkNoImpl do
  @moduledoc false

  # Declares chunk: but only implements run/1 — a config error the runner
  # must fail loudly instead of retrying into.
  use Belay.Worker, queue: :default, chunk: [size: 2]

  @impl Belay.Worker
  def run(_ctx), do: :ok
end

defmodule Belay.Test.Sleeper do
  @moduledoc false

  # Real wall-clock nap for live adaptive-concurrency tests.
  use Belay.Worker, queue: :default

  @impl Belay.Worker
  def run(ctx) do
    Process.sleep(ctx.job.input["ms"] || 30)

    :ok
  end
end

defmodule Belay.Test.RaisingTimeout do
  @moduledoc false
  # A worker with timeout: set whose body raises — the runner must map the
  # raise to a normal failure/retry, not let it crash the executor.
  use Belay.Worker, queue: :default, max_attempts: 2, timeout: {5, :second}

  @impl Belay.Worker
  def run(_ctx), do: raise("boom under timeout")

  @impl Belay.Worker
  def backoff(_attempt), do: 5
end

defmodule Belay.Test.EmptyFanOut do
  @moduledoc false
  # Fans out zero children — must not park the parent forever.
  use Belay.Worker, queue: :default

  @impl Belay.Worker
  def run(ctx) do
    [] = Belay.map_children(ctx, :none, Belay.Test.Echo, [])
    {:ok, %{"children" => 0}}
  end
end
