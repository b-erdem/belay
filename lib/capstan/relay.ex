defmodule Capstan.Relay do
  @moduledoc """
  Insert a job and await its result — background work with RPC ergonomics.

      relay = Capstan.Relay.async(MyOban, MyApp.Summarize.new(%{"url" => url}))

      case Capstan.Relay.await(relay, 30_000) do
        {:ok, summary} -> summary
        {:error, :timeout} -> ...
        {:error, {:job, state}} -> ...
      end

  The result value requires the worker to use `Capstan.Worker` with
  `recorded: true`; otherwise `await/2` returns `{:ok, nil}` on completion.
  Results ride `Oban.Notifier`, so they work across nodes with any notifier.
  Payloads must fit the notifier's limits (~8kb for Postgres NOTIFY).
  """

  import Ecto.Query

  alias Capstan.Query
  alias Ecto.Changeset
  alias Oban.{Job, Notifier, Repo}

  defstruct [:id, :job, :oban]

  @type t :: %__MODULE__{}

  @doc "Insert the changeset with relay tracking. Returns a relay handle."
  def async(%Changeset{} = changeset), do: async(Oban, changeset)

  def async(oban, %Changeset{} = changeset) do
    relay_id = Ecto.UUID.generate()

    :ok = Notifier.listen(oban, :capstan_relay)

    meta =
      changeset
      |> Changeset.get_field(:meta)
      |> Kernel.||(%{})
      |> Map.put("relay_id", relay_id)

    {:ok, job} = Oban.insert(oban, Changeset.put_change(changeset, :meta, meta))

    %__MODULE__{id: relay_id, job: job, oban: oban}
  end

  @doc """
  Await the relayed result.

  Returns `{:ok, value}` when the job completes, `{:error, {:job, :cancelled | :discarded}}`
  on terminal failure, or `{:error, :timeout}`.
  """
  def await(%__MODULE__{id: id}, timeout \\ 5_000) do
    receive do
      {:notification, :capstan_relay, %{"relay_id" => ^id} = payload} ->
        decode_payload(payload)
    after
      timeout -> {:error, :timeout}
    end
  end

  defp decode_payload(%{"state" => "completed"} = payload) do
    case payload["result"] do
      nil ->
        {:ok, nil}

      bin ->
        case Capstan.Worker.decode_recorded(bin) do
          {:ok, value} -> {:ok, value}
          :error -> {:ok, nil}
        end
    end
  end

  defp decode_payload(%{"state" => state}), do: {:error, {:job, String.to_existing_atom(state)}}

  # -- Engine-driven response ---------------------------------------------------

  @doc false
  def respond(conf, job, state) do
    payload = %{
      "relay_id" => job.meta["relay_id"],
      "state" => to_string(state),
      "result" => if(state == :completed, do: fetch_recorded_bin(conf, job.id))
    }

    Notifier.notify(conf, :capstan_relay, payload)
  end

  defp fetch_recorded_bin(conf, job_id) do
    d = Query.dialect(conf)

    query =
      Job
      |> where([j], j.id == ^job_id)
      |> limit(1)

    query =
      case d do
        :pg -> select(query, [j], fragment("?->>'recorded'", j.meta))
        :sqlite -> select(query, [j], fragment("json_extract(?, '$.recorded')", j.meta))
      end

    case Repo.all(conf, query) do
      [bin | _] -> bin
      [] -> nil
    end
  end
end
