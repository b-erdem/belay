defmodule Capstan.Lifecycle do
  @moduledoc false

  # Central dispatch for post-transition side effects, invoked by Capstan.Engine
  # after a job reaches a terminal state. Advancement logic derives everything
  # from database truth, so spurious or repeated invocations are harmless.

  require Logger

  @doc false
  def transitioned(conf, job, state) when state in [:completed, :discarded, :cancelled] do
    meta = job.meta || %{}

    if meta["relay_id"], do: safely(fn -> Capstan.Relay.respond(conf, job, state) end)
    if meta["workflow_id"], do: safely(fn -> Capstan.Workflow.advance(conf, job, state) end)
    if meta["batch_id"], do: safely(fn -> Capstan.Batch.advance(conf, job, state) end)
    if meta["chain_key"], do: safely(fn -> Capstan.Chain.advance(conf, job, state) end)

    :ok
  end

  def transitioned(_conf, _job, _state), do: :ok

  defp safely(fun) do
    fun.()
  rescue
    error ->
      Logger.error(
        "[capstan] lifecycle advancement failed: #{Exception.format(:error, error, __STACKTRACE__)}"
      )

      :error
  end
end
