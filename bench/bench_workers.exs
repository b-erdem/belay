# Minimal worker shared by the latency bench's driver and worker processes.
defmodule Bench.Echo do
  use Capstan.Worker, queue: :default, max_attempts: 3

  @impl Capstan.Worker
  def run(ctx), do: {:ok, ctx.job.input}
end
