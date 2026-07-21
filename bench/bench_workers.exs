# Minimal worker shared by the latency bench's driver and worker processes.
defmodule Bench.Echo do
  use Belay.Worker, queue: :default, max_attempts: 3

  @impl Belay.Worker
  def run(ctx), do: {:ok, ctx.job.input}
end
