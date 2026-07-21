# Worker modules shared by every soak process (workers and driver alike).
# Loaded with Code.require_file so job `kind` strings resolve everywhere.
#
# Every step body records a side-effect row FIRST, so the at-least-once
# window (effect done, step not yet journaled, worker killed) is measurable
# in soak_effects rather than invisible.

defmodule Soak.FX do
  def effect!(ctx, step_name) do
    Postgrex.query!(
      SoakDB,
      "INSERT INTO soak_effects (job_id, step, attempt, tag) VALUES ($1, $2, $3, $4)",
      [ctx.job.id, to_string(step_name), ctx.job.attempt, :persistent_term.get(:soak_tag, "?")]
    )
  end
end

defmodule Soak.Step do
  use Belay.Worker, queue: :default, max_attempts: 15

  # expected result: n*k + k*(k+1)/2
  @impl Belay.Worker
  def run(ctx) do
    %{"n" => n, "k" => k} = ctx.job.input
    planned_failures = ctx.job.input["f"] || 0

    total =
      Enum.reduce(1..k, 0, fn i, acc ->
        acc +
          Belay.step(ctx, "s#{i}", fn ->
            Soak.FX.effect!(ctx, "s#{i}")
            Process.sleep(5 + :rand.uniform(25))
            n + i
          end)
      end)

    if ctx.job.attempt <= planned_failures, do: raise("planned failure #{ctx.job.attempt}")

    {:ok, total}
  end

  @impl Belay.Worker
  def backoff(_attempt), do: 1
end

defmodule Soak.Child do
  use Belay.Worker, queue: :children, max_attempts: 15

  @impl Belay.Worker
  def run(ctx) do
    Soak.FX.effect!(ctx, :child)
    Process.sleep(5 + :rand.uniform(15))

    {:ok, ctx.job.input["v"] * 2}
  end

  @impl Belay.Worker
  def backoff(_attempt), do: 1
end

defmodule Soak.Parent do
  use Belay.Worker, queue: :default, max_attempts: 15

  # expected result: Enum.sort(Enum.map(vs, & &1 * 2))
  @impl Belay.Worker
  def run(ctx) do
    inputs = Enum.map(ctx.job.input["vs"], &%{"v" => &1})
    children = Belay.map_children(ctx, :fan, Soak.Child, inputs)

    {:ok, children |> Enum.map(&Belay.Job.result/1) |> Enum.sort()}
  end

  @impl Belay.Worker
  def backoff(_attempt), do: 1
end

defmodule Soak.FlowStep do
  use Belay.Worker, queue: :flow, max_attempts: 15

  @impl Belay.Worker
  def run(ctx) do
    Soak.FX.effect!(ctx, ctx.job.wf_name)
    Process.sleep(5 + :rand.uniform(15))

    :ok
  end

  @impl Belay.Worker
  def backoff(_attempt), do: 1
end

defmodule Soak.Awaiter do
  use Belay.Worker, queue: :default, max_attempts: 50

  @impl Belay.Worker
  def run(ctx) do
    pre = Belay.step(ctx, :pre, fn ->
      Soak.FX.effect!(ctx, :pre)
      1
    end)

    payload = Belay.await(ctx, :go)

    {:ok, Map.put(payload, "pre", pre)}
  end

  @impl Belay.Worker
  def backoff(_attempt), do: 1
end

defmodule Soak.Budget do
  use Belay.Worker, queue: :default, max_attempts: 3

  # With budget usd 0.5 this must fail after recording exactly 3 steps.
  @impl Belay.Worker
  def run(ctx) do
    for i <- 1..5 do
      Belay.step(ctx, "b#{i}", fn ->
        Soak.FX.effect!(ctx, "b#{i}")
        i
      end, cost: [usd: 0.2])
    end

    :ok
  end
end

defmodule Soak.Uni do
  use Belay.Worker, queue: :default, max_attempts: 15

  @impl Belay.Worker
  def run(ctx) do
    Soak.FX.effect!(ctx, :uni)
    Process.sleep(20)

    :ok
  end

  @impl Belay.Worker
  def backoff(_attempt), do: 1
end

defmodule Soak.Cron do
  use Belay.Worker, queue: :default, max_attempts: 5

  @impl Belay.Worker
  def run(ctx) do
    Soak.FX.effect!(ctx, :cron)

    :ok
  end
end
