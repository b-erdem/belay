defmodule Capstan.Test.Events do
  @moduledoc false

  def record(key) do
    :ets.insert(:capstan_events, {System.unique_integer([:positive, :monotonic]), key})
  end

  def all do
    :capstan_events |> :ets.tab2list() |> Enum.sort() |> Enum.map(&elem(&1, 1))
  end

  def count(key), do: Enum.count(all(), &(&1 == key))

  def clear do
    :ets.delete_all_objects(:capstan_events)
    :ets.insert(:capstan_gauge, {:running, 0})
  end

  def gauge_up do
    current = :ets.update_counter(:capstan_gauge, :running, 1)
    record({:gauge, current})
    current
  end

  def gauge_down, do: :ets.update_counter(:capstan_gauge, :running, -1)

  def peak_gauge do
    all()
    |> Enum.flat_map(fn
      {:gauge, n} -> [n]
      _ -> []
    end)
    |> Enum.max(fn -> 0 end)
  end
end

defmodule Capstan.Test.Echo do
  @moduledoc false
  use Capstan.Worker, queue: :default, max_attempts: 3

  alias Capstan.Ctx

  @impl Capstan.Worker
  def run(%Ctx{job: job}), do: {:ok, job.input}
end

defmodule Capstan.Test.Tagged do
  @moduledoc false
  use Capstan.Worker, queue: :default, max_attempts: 3

  alias Capstan.{Ctx, Test.Events}

  @impl Capstan.Worker
  def run(%Ctx{job: job}) do
    Events.record({:ran, job.input["tag"]})

    if job.input["fail"], do: {:error, :nope}, else: :ok
  end
end

defmodule Capstan.Test.FailN do
  @moduledoc false

  # Raises while attempt <= input["fail_times"], then succeeds.
  use Capstan.Worker, queue: :default, max_attempts: 5

  alias Capstan.Ctx

  @impl Capstan.Worker
  def run(%Ctx{job: job}) do
    if job.attempt <= (job.input["fail_times"] || 0) do
      raise "planned failure #{job.attempt}"
    end

    {:ok, %{"attempt" => job.attempt}}
  end

  @impl Capstan.Worker
  def backoff(_attempt), do: 5
end

defmodule Capstan.Test.StepFlaky do
  @moduledoc false
  use Capstan.Worker, queue: :default, max_attempts: 3

  alias Capstan.{Ctx, Test.Events}

  @impl Capstan.Worker
  def run(%Ctx{job: job} = ctx) do
    base =
      Capstan.step(ctx, :expensive, fn ->
        Events.record(:step_ran)
        41
      end)

    if job.attempt == 1, do: raise("boom after step")

    {:ok, base + 1}
  end

  @impl Capstan.Worker
  def backoff(_attempt), do: 5
end

defmodule Capstan.Test.Budgeted do
  @moduledoc false

  # Runs input["steps"] steps, each costing input["usd"] dollars and
  # input["tokens"] tokens.
  use Capstan.Worker, queue: :default, max_attempts: 1

  alias Capstan.{Ctx, Test.Events}

  @impl Capstan.Worker
  def run(%Ctx{job: job} = ctx) do
    for i <- 1..job.input["steps"] do
      Capstan.step(
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

defmodule Capstan.Test.Awaiter do
  @moduledoc false
  use Capstan.Worker, queue: :default, max_attempts: 10

  alias Capstan.Ctx

  @impl Capstan.Worker
  def run(%Ctx{job: job} = ctx) do
    opts = if timeout = job.input["timeout"], do: [timeout: timeout], else: []

    case Capstan.await(ctx, :approval, opts) do
      {:error, :timeout} -> {:ok, %{"timeout" => true}}
      payload -> {:ok, payload}
    end
  end
end

defmodule Capstan.Test.Steered do
  @moduledoc false
  use Capstan.Worker, queue: :default, max_attempts: 1

  alias Capstan.Ctx

  @impl Capstan.Worker
  def run(%Ctx{} = ctx) do
    Capstan.step(ctx, :one, fn -> 1 end)

    {:ok, %{"steer" => Capstan.steering(ctx)}}
  end
end

defmodule Capstan.Test.NapThenDone do
  @moduledoc false
  use Capstan.Worker, queue: :default, max_attempts: 5

  alias Capstan.{Ctx, Test.Events}

  @impl Capstan.Worker
  def run(%Ctx{job: job} = ctx) do
    Capstan.step(ctx, :first, fn ->
      Events.record(:first)
      :ok
    end)

    Capstan.sleep(ctx, :nap, job.input["seconds"])

    {:ok, %{"woke" => true}}
  end
end

defmodule Capstan.Test.StepOnly do
  @moduledoc false
  use Capstan.Worker, queue: :default, max_attempts: 3

  alias Capstan.Ctx

  @impl Capstan.Worker
  def run(%Ctx{} = ctx) do
    {:ok, Capstan.step(ctx, :a, fn -> 1 end)}
  end
end

defmodule Capstan.Test.SlowLive do
  @moduledoc false
  use Capstan.Worker, queue: :limited, max_attempts: 1

  alias Capstan.{Ctx, Test.Events}

  @impl Capstan.Worker
  def run(%Ctx{}) do
    Events.gauge_up()
    Process.sleep(30)
    Events.gauge_down()

    :ok
  end
end

defmodule Capstan.Test.CronJob do
  @moduledoc false
  use Capstan.Worker, queue: :default, max_attempts: 1

  alias Capstan.{Ctx, Test.Events}

  @impl Capstan.Worker
  def run(%Ctx{}) do
    Events.record(:cron_ran)
    :ok
  end
end

defmodule Capstan.Test.ChildEcho do
  @moduledoc false
  use Capstan.Worker, queue: :default, max_attempts: 3

  alias Capstan.Ctx

  @impl Capstan.Worker
  def run(%Ctx{job: job}) do
    if job.input["fail"], do: {:error, :child_boom}, else: {:ok, job.input["v"] * 2}
  end
end

defmodule Capstan.Test.FanOut do
  @moduledoc false
  use Capstan.Worker, queue: :default, max_attempts: 3

  alias Capstan.Ctx

  @impl Capstan.Worker
  def run(%Ctx{job: job} = ctx) do
    inputs = Enum.map(job.input["values"], &%{"v" => &1})
    children = Capstan.map_children(ctx, :fan, Capstan.Test.ChildEcho, inputs)

    {:ok, Enum.map(children, &Capstan.Job.result/1)}
  end
end

defmodule Capstan.Test.SpawnCrash do
  @moduledoc false

  # Spawns two children, crashes once, then collects them on retry — proving
  # spawn memoization prevents duplicate children.
  use Capstan.Worker, queue: :default, max_attempts: 3

  alias Capstan.Ctx

  @impl Capstan.Worker
  def run(%Ctx{job: job} = ctx) do
    _ids =
      Capstan.spawn_many(ctx, :kids, [
        Capstan.Test.ChildEcho.new(%{"v" => 1}),
        Capstan.Test.ChildEcho.new(%{"v" => 2})
      ])

    if job.attempt == 1, do: raise("crash after spawn")

    children = Capstan.await_children(ctx)

    {:ok, Enum.map(children, &Capstan.Job.result/1)}
  end

  @impl Capstan.Worker
  def backoff(_attempt), do: 5
end

defmodule Capstan.Test.Hanging do
  @moduledoc false
  use Capstan.Worker, queue: :default, max_attempts: 2, timeout: {150, :millisecond}

  alias Capstan.{Ctx, Test.Events}

  @impl Capstan.Worker
  def run(%Ctx{job: job}) do
    Events.record({:hang_attempt, job.attempt})

    if job.input["hang"] do
      Process.sleep(:timer.seconds(30))
    end

    :ok
  end

  @impl Capstan.Worker
  def backoff(_attempt), do: 5
end

defmodule Capstan.Test.Emitter do
  @moduledoc false
  use Capstan.Worker, queue: :default, max_attempts: 1

  alias Capstan.Ctx

  @impl Capstan.Worker
  def run(%Ctx{} = ctx) do
    for i <- 1..3 do
      Capstan.emit(ctx, %{"chunk" => "token-#{i}"})
    end

    {:ok, :emitted}
  end
end

defmodule Capstan.Test.Divergent do
  @moduledoc false

  # The executed path depends on a runtime flag, letting tests simulate a code
  # change between the original run and a replay.
  use Capstan.Worker, queue: :default, max_attempts: 1

  alias Capstan.Ctx

  @impl Capstan.Worker
  def run(%Ctx{} = ctx) do
    a = Capstan.step(ctx, :a, fn -> 20 end)

    b =
      case :persistent_term.get({__MODULE__, :path}, :original) do
        :original -> Capstan.step(ctx, :b, fn -> 22 end)
        :changed -> Capstan.step(ctx, :b_new, fn -> 22 end)
      end

    {:ok, a + b}
  end
end

defmodule Capstan.Test.ResourceUser do
  @moduledoc false
  use Capstan.Worker, queue: :metered, max_attempts: 1

  alias Capstan.Ctx

  @impl Capstan.Worker
  def run(%Ctx{job: job} = ctx) do
    Capstan.step(ctx, :call, fn -> :called end)
    Capstan.debit(ctx, "prov", job.input["actual"])

    :ok
  end
end

defmodule Capstan.Test.Strict do
  @moduledoc false
  use Capstan.Worker,
    queue: :default,
    max_attempts: 1,
    input_schema: [
      url: [type: :string, required: true],
      style: [type: {:enum, ["tight", "loose"]}, default: "tight"],
      limit: [type: :integer]
    ]

  alias Capstan.Ctx

  @impl Capstan.Worker
  def run(%Ctx{job: job}), do: {:ok, job.input}
end

defmodule Capstan.Test.Secret do
  @moduledoc false
  use Capstan.Worker, queue: :default, max_attempts: 1, encrypted: true

  alias Capstan.Ctx

  @impl Capstan.Worker
  def run(%Ctx{job: job}), do: {:ok, job.input}
end

defmodule Capstan.Test.Keys do
  @moduledoc false

  def test_key, do: :binary.copy(<<7>>, 32)
end

defmodule Capstan.Test.ChunkEcho do
  @moduledoc false

  # Doubles each job's "n" in one run_chunk call; records observed chunk sizes.
  use Capstan.Worker, queue: :default, chunk: [size: 3, gather_ms: 0]

  alias Capstan.Test.Events

  @impl Capstan.Worker
  def run_chunk(ctxs) do
    Events.record({:chunk, length(ctxs)})

    {:ok, Map.new(ctxs, fn ctx -> {ctx.job.id, ctx.job.input["n"] * 2} end)}
  end
end

defmodule Capstan.Test.ChunkFlaky do
  @moduledoc false

  # Per-job outcomes: inputs with "fail" fail on their first attempt only.
  use Capstan.Worker, queue: :default, max_attempts: 3, chunk: [size: 10, gather_ms: 0]

  alias Capstan.Test.Events

  @impl Capstan.Worker
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

  @impl Capstan.Worker
  def backoff(_attempt), do: 5
end

defmodule Capstan.Test.ChunkBoom do
  @moduledoc false

  # Whole-chunk error on first attempts, success after.
  use Capstan.Worker, queue: :default, max_attempts: 3, chunk: [size: 5, gather_ms: 0]

  @impl Capstan.Worker
  def run_chunk(ctxs) do
    if hd(ctxs).job.attempt == 1, do: {:error, "chunk boom"}, else: :ok
  end

  @impl Capstan.Worker
  def backoff(_attempt), do: 5
end

defmodule Capstan.Test.ChunkGather do
  @moduledoc false

  # Live-gathering worker: short window so tests can observe the deadline.
  use Capstan.Worker, queue: :default, chunk: [size: 3, gather_ms: 120]

  alias Capstan.Test.Events

  @impl Capstan.Worker
  def run_chunk(ctxs) do
    Events.record({:gathered, length(ctxs)})

    :ok
  end
end

defmodule Capstan.Test.ChunkNoImpl do
  @moduledoc false

  # Declares chunk: but only implements run/1 — a config error the runner
  # must fail loudly instead of retrying into.
  use Capstan.Worker, queue: :default, chunk: [size: 2]

  @impl Capstan.Worker
  def run(_ctx), do: :ok
end

defmodule Capstan.Test.Sleeper do
  @moduledoc false

  # Real wall-clock nap for live adaptive-concurrency tests.
  use Capstan.Worker, queue: :default

  @impl Capstan.Worker
  def run(ctx) do
    Process.sleep(ctx.job.input["ms"] || 30)

    :ok
  end
end
