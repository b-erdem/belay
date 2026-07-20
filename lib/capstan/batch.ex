defmodule Capstan.Batch do
  @moduledoc """
  Insert a group of jobs and run a callback when the whole group has finished
  — thin, transparent sugar over `Capstan.Workflow`.

      {:ok, batch_id, jobs} =
        Capstan.Batch.insert(MyCapstan, changeset_list,
          on_complete: MyApp.Notify.new(%{"channel" => "#imports"}))

  The callback job runs once every batch member is terminal, whatever the
  outcomes were, and receives `"batch_id"` merged into its input — read
  `Capstan.Batch.status/2` there for the final counts. Batch members are
  ordinary workflow jobs: cancel them, retry them, and inspect them with the
  normal APIs.
  """

  alias Capstan.Workflow

  @doc """
  Insert `buildables` as a batch. Options:

    * `:on_complete` — a `Worker.new/2` build to run after every member
      finishes (any outcome)
    * `:batch_id` — override the generated id

  Returns `{:ok, batch_id, jobs_by_name}`.
  """
  def insert(name, buildables, opts \\ []) when is_list(buildables) do
    workflow = Workflow.new(workflow_id: Keyword.get_lazy(opts, :batch_id, &random_id/0))

    {workflow, item_names} =
      buildables
      |> Enum.with_index()
      |> Enum.reduce({workflow, []}, fn {buildable, index}, {workflow, names} ->
        item = "item-#{index}"

        {Workflow.add(workflow, item, buildable), [item | names]}
      end)

    workflow =
      case Keyword.get(opts, :on_complete) do
        nil ->
          workflow

        {Capstan.Worker, worker, input, cb_opts} ->
          input = Map.put(input, "batch_id", workflow.id)

          Workflow.add(workflow, "on-complete", {Capstan.Worker, worker, input, cb_opts},
            deps: Enum.reverse(item_names),
            # The callback observes outcomes; it must run regardless of them.
            ignore: [:failed, :cancelled]
          )
      end

    with {:ok, jobs} <- Workflow.insert(workflow, name) do
      {:ok, workflow.id, jobs}
    end
  end

  @doc "Batch status: `%{total:, state_counts:, done?:}` (callback included)."
  def status(name, batch_id), do: Workflow.status(name, batch_id)

  @doc "All jobs in the batch, callback included."
  def jobs(name, batch_id), do: Workflow.jobs(name, batch_id)

  defp random_id, do: :crypto.strong_rand_bytes(16) |> Base.encode16(case: :lower)
end
