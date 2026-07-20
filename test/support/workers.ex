defmodule Capstan.Test.Events do
  @moduledoc false

  def record(key) do
    :ets.insert(:capstan_events, {System.unique_integer([:positive, :monotonic]), key})
  end

  def all do
    :capstan_events
    |> :ets.tab2list()
    |> Enum.sort()
    |> Enum.map(&elem(&1, 1))
  end

  def count(key), do: Enum.count(all(), &(&1 == key))

  def clear, do: :ets.delete_all_objects(:capstan_events)
end

defmodule Capstan.Test.Echo do
  @moduledoc false
  use Capstan.Worker, queue: :default, recorded: true, max_attempts: 3

  @impl Capstan.Worker
  def process(%Oban.Job{args: args}), do: {:ok, args}
end

defmodule Capstan.Test.Schema do
  @moduledoc false
  use Capstan.Worker,
    queue: :default,
    recorded: true,
    max_attempts: 3,
    args_schema: [
      name: [type: :string, required: true],
      count: [type: :integer, default: 7],
      mode: [type: {:enum, ["fast", "slow"]}]
    ]

  @impl Capstan.Worker
  def process(%Oban.Job{args: args}), do: {:ok, args}
end

defmodule Capstan.Test.Hooked do
  @moduledoc false
  use Capstan.Worker, queue: :default, max_attempts: 1

  alias Capstan.Test.Events

  @impl Capstan.Worker
  def before_process(%Oban.Job{args: %{"veto" => true}}), do: {:cancel, :vetoed}

  def before_process(_job) do
    Events.record(:before)
    :ok
  end

  @impl Capstan.Worker
  def process(_job) do
    Events.record(:process)
    :ok
  end

  @impl Capstan.Worker
  def after_process(_job, result) do
    Events.record({:after, elem(result, 0)})
  rescue
    _ -> Events.record({:after, :plain})
  end
end

defmodule Capstan.Test.StepFlaky do
  @moduledoc false

  # Fails after its first step on attempt one; the memoized step must not
  # re-run on retry.
  use Capstan.Worker, queue: :default, recorded: true, max_attempts: 3

  alias Capstan.Test.Events

  @impl Capstan.Worker
  def process(%Oban.Job{} = job) do
    base =
      Capstan.step(job, :expensive, fn ->
        Events.record(:step_ran)
        41
      end)

    if job.attempt == 1, do: raise("boom after step")

    {:ok, base + 1}
  end
end

defmodule Capstan.Test.Awaiter do
  @moduledoc false
  use Capstan.Worker, queue: :default, recorded: true, max_attempts: 10

  @impl Capstan.Worker
  def process(%Oban.Job{} = job) do
    payload = Capstan.await_signal(job, :approval, resnooze: 300)

    {:ok, payload}
  end
end

defmodule Capstan.Test.Tagged do
  @moduledoc false
  use Capstan.Worker, queue: :default, max_attempts: 3

  alias Capstan.Test.Events

  @impl Capstan.Worker
  def process(%Oban.Job{args: %{"tag" => tag} = args}) do
    Events.record({:ran, tag})

    if args["fail"], do: {:error, :nope}, else: :ok
  end
end

defmodule Capstan.Test.FailOnce do
  @moduledoc false
  use Capstan.Worker, queue: :default, max_attempts: 3

  @impl Capstan.Worker
  def process(%Oban.Job{attempt: 1}), do: raise("first attempt boom")
  def process(_job), do: :ok
end

defmodule Capstan.Test.BatchCb do
  @moduledoc false
  use Capstan.Batch.Callback, queue: :default, max_attempts: 3

  alias Capstan.Test.Events

  def handle_completed(batch_id, _job) do
    Events.record({:batch_completed, batch_id})
    :ok
  end

  def handle_exhausted(batch_id, _job) do
    Events.record({:batch_exhausted, batch_id})
    :ok
  end
end

defmodule Capstan.Test.Sleeper do
  @moduledoc false
  use Oban.Worker, queue: :default, max_attempts: 1

  @impl Oban.Worker
  def perform(_job) do
    Process.sleep(50)
    :ok
  end
end
