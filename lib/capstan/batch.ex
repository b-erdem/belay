defmodule Capstan.Batch do
  @moduledoc """
  Track a group of jobs and run callback jobs when the group reaches milestones.

      changesets = Enum.map(urls, &MyApp.Crawl.new(%{"url" => &1}))

      Capstan.Batch.insert(MyOban, changesets, callback: MyApp.CrawlBatch)

      defmodule MyApp.CrawlBatch do
        use Capstan.Batch.Callback, queue: :default

        def handle_completed(batch_id, _job), do: :ok    # every job completed
        def handle_exhausted(batch_id, _job), do: :ok    # every job reached a final state
      end

  Callbacks run as regular Oban jobs (retried independently) and are inserted
  with uniqueness, so milestones fire once per batch. Requires `Capstan.Engine`.
  """

  import Ecto.Query

  alias Capstan.Query
  alias Ecto.Changeset
  alias Oban.{Job, Repo}

  @doc """
  Stamp changesets as a batch and insert them.

  Options: `:callback` (module using `Capstan.Batch.Callback`, required for
  callbacks), `:callback_queue` (defaults to the callback module's queue),
  `:batch_id` (defaults to a generated UUID).

  Returns `{:ok, batch_id, jobs}`.
  """
  def insert(changesets, opts) when is_list(changesets), do: insert(Oban, changesets, opts)

  def insert(oban, changesets, opts) when is_list(changesets) do
    batch_id = Keyword.get(opts, :batch_id, Ecto.UUID.generate())
    callback = Keyword.get(opts, :callback)

    base = %{"batch_id" => batch_id, "batch_size" => length(changesets)}

    base =
      if callback do
        base
        |> Map.put("batch_callback_worker", Oban.Worker.to_string(callback))
        |> maybe_put_queue(Keyword.get(opts, :callback_queue))
      else
        base
      end

    changesets =
      Enum.map(changesets, fn changeset ->
        meta = Changeset.get_field(changeset, :meta) || %{}

        Changeset.put_change(changeset, :meta, Map.merge(meta, base))
      end)

    jobs = Oban.insert_all(oban, changesets)

    {:ok, batch_id, jobs}
  end

  @doc "All jobs in a batch."
  def all_jobs(batch_id) when is_binary(batch_id), do: all_jobs(Oban, batch_id)

  def all_jobs(oban, batch_id) do
    conf = Oban.config(oban)
    d = Query.dialect(conf)

    Repo.all(conf, Job |> where(^Query.meta_eq(d, "batch_id", batch_id)) |> order_by(:id))
  end

  # -- Engine-driven advancement ------------------------------------------------

  @doc false
  def advance(conf, job, _state) do
    meta = job.meta
    batch_id = meta["batch_id"]
    worker = meta["batch_callback_worker"]

    if is_binary(worker) do
      d = Query.dialect(conf)

      counts =
        conf
        |> Repo.all(
          Job
          |> where(^Query.meta_eq(d, "batch_id", batch_id))
          |> group_by([j], j.state)
          |> select([j], {j.state, count(j.id)})
        )
        |> Map.new()

      size = meta["batch_size"] || Enum.sum(Map.values(counts))
      completed = Map.get(counts, "completed", 0)
      final = completed + Map.get(counts, "cancelled", 0) + Map.get(counts, "discarded", 0)

      cond do
        completed >= size ->
          enqueue_callback(conf, meta, "completed", job.queue)

        final >= size ->
          enqueue_callback(conf, meta, "exhausted", job.queue)

        true ->
          :ok
      end
    end

    :ok
  end

  defp enqueue_callback(conf, meta, event, fallback_queue) do
    changeset =
      Oban.Job.new(
        %{"batch_id" => meta["batch_id"], "event" => event},
        worker: meta["batch_callback_worker"],
        queue: meta["batch_callback_queue"] || fallback_queue,
        unique: [
          fields: [:worker, :args],
          states: Oban.Job.unique_states(:all),
          period: :infinity
        ]
      )

    Oban.Engine.insert_job(conf, changeset, [])

    :ok
  end

  defp maybe_put_queue(base, nil), do: base
  defp maybe_put_queue(base, queue), do: Map.put(base, "batch_callback_queue", to_string(queue))
end

defmodule Capstan.Batch.Callback do
  @moduledoc """
  Define a batch callback worker. `handle_completed/2` fires when every job in
  the batch completed; `handle_exhausted/2` when every job reached a final
  state with at least one cancel/discard. Override the ones you need.
  """

  @callback handle_completed(batch_id :: String.t(), Oban.Job.t()) :: Oban.Worker.result()
  @callback handle_exhausted(batch_id :: String.t(), Oban.Job.t()) :: Oban.Worker.result()

  defmacro __using__(opts) do
    quote location: :keep do
      use Oban.Worker, unquote(opts)

      @behaviour Capstan.Batch.Callback

      @impl Oban.Worker
      def perform(%Oban.Job{args: %{"batch_id" => batch_id, "event" => event}} = job) do
        case event do
          "completed" -> handle_completed(batch_id, job)
          "exhausted" -> handle_exhausted(batch_id, job)
        end
      end

      @impl Capstan.Batch.Callback
      def handle_completed(_batch_id, _job), do: :ok

      @impl Capstan.Batch.Callback
      def handle_exhausted(_batch_id, _job), do: :ok

      defoverridable handle_completed: 2, handle_exhausted: 2
    end
  end
end
