defmodule Belay.View do
  @moduledoc false

  # JSON-shaped serialization of engine records, shared by the MCP server and
  # the embedded dashboard so both surfaces describe jobs identically.

  alias Belay.Job

  def job_summary(%Job{} = job) do
    %{
      "id" => job.id,
      "worker" => job.kind,
      "queue" => job.queue,
      "state" => job.state,
      "attempt" => job.attempt,
      "priority" => job.priority,
      "parent_id" => job.parent_id,
      "workflow" =>
        job.workflow_id &&
          %{"id" => job.workflow_id, "name" => job.wf_name, "deps" => job.wf_deps},
      "max_attempts" => job.max_attempts,
      "input_preview" => preview(job.input),
      "spent_usd_micros" => job.spent_usd_micros,
      "inserted_at" => iso(job.inserted_at),
      "started_at" => iso(job.started_at),
      "finished_at" => iso(job.finished_at)
    }
  end

  # Compact single-line input preview for list rows (dashboard/MCP); full
  # inputs stay in job_detail. Encrypted inputs render as their envelope.
  defp preview(input) do
    input |> Jason.encode!() |> String.slice(0, 80)
  rescue
    _ -> "{}"
  end

  def job_detail(%Job{} = job) do
    job
    |> job_summary()
    |> Map.merge(%{
      "input" => job.input,
      "meta" => job.meta,
      "errors" => job.errors,
      "ready_at" => iso(job.ready_at),
      "await" => job.await_name && %{"scope" => job.await_scope, "name" => job.await_name},
      "spent" => %{"usd_micros" => job.spent_usd_micros, "tokens" => job.spent_tokens},
      "budget" => %{"usd_micros" => job.budget_usd_micros, "tokens" => job.budget_tokens},
      "result" => job.result && inspect(Job.result(job), limit: 50, printable_limit: 2_000)
    })
  end

  def step_summary(step) do
    %{
      "seq" => step.seq,
      "name" => step.name,
      "usd_micros" => step.usd_micros,
      "tokens" => step.tokens,
      "value" => step.value && inspect(Belay.Codec.decode(step.value), limit: 25)
    }
  end

  def event_summary(event) do
    %{"seq" => event.seq, "payload" => event.payload, "at" => iso(event.inserted_at)}
  end

  def iso(nil), do: nil
  def iso(%DateTime{} = dt), do: DateTime.to_iso8601(dt)
end
