defmodule Capstan.MCP do
  @moduledoc """
  A Model Context Protocol server over stdio, so AI assistants (Claude Code,
  Cursor, or your own agents) can inspect and operate a Capstan installation:

      # .mcp.json / MCP client config
      {"command": "mix", "args": ["capstan.mcp", "--url", "postgres://.../my_app"]}

  Read tools: `stats`, `list_jobs`, `get_job` (with steps, events, children),
  `workflow_status`. Write tools (clearly named as mutations): `retry_job`,
  `cancel_job`, `signal`, `steer_job`.

  The server talks JSON-RPC 2.0 over newline-delimited stdio and needs only
  database access — no running Capstan instance required.
  """

  alias Capstan.Job

  @protocol_version "2025-06-18"

  # -- Serving ------------------------------------------------------------------

  @doc "Blocking stdio serve loop. `storage` is a `{module, ref}` pair."
  def serve(storage) do
    case IO.read(:stdio, :line) do
      :eof ->
        :ok

      {:error, _reason} ->
        :ok

      line ->
        line
        |> String.trim()
        |> handle_line(storage)

        serve(storage)
    end
  end

  defp handle_line("", _storage), do: :ok

  defp handle_line(line, storage) do
    case Jason.decode(line) do
      {:ok, request} ->
        case handle_request(request, storage) do
          nil -> :ok
          response -> IO.write([Jason.encode!(response), "\n"])
        end

      {:error, _} ->
        IO.write([Jason.encode!(error_response(nil, -32700, "parse error")), "\n"])
    end
  end

  # -- Request handling (pure; unit-testable) -----------------------------------

  @doc false
  def handle_request(%{"method" => "initialize", "id" => id}, _storage) do
    result(id, %{
      "protocolVersion" => @protocol_version,
      "capabilities" => %{"tools" => %{}},
      "serverInfo" => %{"name" => "capstan", "version" => version()}
    })
  end

  def handle_request(%{"method" => "notifications/" <> _}, _storage), do: nil

  def handle_request(%{"method" => "ping", "id" => id}, _storage), do: result(id, %{})

  def handle_request(%{"method" => "tools/list", "id" => id}, _storage) do
    result(id, %{"tools" => tools()})
  end

  def handle_request(
        %{"method" => "tools/call", "id" => id, "params" => %{"name" => tool} = params},
        storage
      ) do
    args = Map.get(params, "arguments", %{})

    case call_tool(tool, args, storage) do
      {:ok, data} ->
        result(id, %{
          "content" => [%{"type" => "text", "text" => Jason.encode!(data, pretty: true)}]
        })

      {:error, message} ->
        result(id, %{
          "content" => [%{"type" => "text", "text" => message}],
          "isError" => true
        })
    end
  end

  def handle_request(%{"id" => id}, _storage) do
    error_response(id, -32601, "method not found")
  end

  def handle_request(_request, _storage), do: nil

  # -- Tools --------------------------------------------------------------------

  defp tools do
    [
      tool("stats", "Per-queue, per-state job counts across the installation.", %{}),
      tool("list_jobs", "List jobs newest-first. All filters optional.", %{
        "queue" => %{"type" => "string"},
        "state" => %{
          "type" => "string",
          "enum" => ~w(ready running awaiting held paused succeeded failed cancelled)
        },
        "workflow_id" => %{"type" => "string"},
        "limit" => %{"type" => "integer", "default" => 20}
      }),
      tool(
        "get_job",
        "Full detail for one job: fields, errors, recorded steps with costs, emitted events, and children.",
        %{"id" => %{"type" => "integer"}},
        ["id"]
      ),
      tool("workflow_status", "State counts and completion for a workflow or batch.", %{
        "workflow_id" => %{"type" => "string"}
      }, ["workflow_id"]),
      tool(
        "retry_job",
        "MUTATES: resurrect a failed or cancelled job for another run.",
        %{"id" => %{"type" => "integer"}},
        ["id"]
      ),
      tool(
        "cancel_job",
        "MUTATES: cancel a job (immediate when parked, cooperative when running).",
        %{"id" => %{"type" => "integer"}},
        ["id"]
      ),
      tool(
        "signal",
        "MUTATES: deliver a durable signal to a scope (e.g. approve an awaiting job).",
        %{
          "scope" => %{"type" => "string", "description" => "e.g. job:123 or wf:<id>"},
          "name" => %{"type" => "string"},
          "payload" => %{"type" => "object", "default" => %{}}
        },
        ["scope", "name"]
      ),
      tool(
        "steer_job",
        "MUTATES: inject steering guidance a running job reads at its next step boundary.",
        %{
          "id" => %{"type" => "integer"},
          "payload" => %{"type" => "object"}
        },
        ["id", "payload"]
      )
    ]
  end

  defp tool(name, description, properties, required \\ []) do
    %{
      "name" => name,
      "description" => description,
      "inputSchema" => %{
        "type" => "object",
        "properties" => properties,
        "required" => required
      }
    }
  end

  defp call_tool("stats", _args, {mod, ref}) do
    {:ok, rows} = mod.queue_stats(ref)

    {:ok, rows}
  end

  defp call_tool("list_jobs", args, {mod, ref}) do
    filters =
      args
      |> Map.take(["queue", "state", "workflow_id", "limit"])
      |> Map.new(fn {k, v} -> {String.to_existing_atom(k), v} end)
      |> Map.put_new(:limit, 20)

    {:ok, jobs} = mod.list_jobs(ref, filters)

    {:ok, Enum.map(jobs, &job_summary/1)}
  end

  defp call_tool("get_job", %{"id" => id}, {mod, ref}) do
    with {:ok, job} <- fetch_job(mod, ref, id) do
      {:ok, steps} = mod.list_steps(ref, id)
      {:ok, events} = mod.list_events(ref, id, 0)
      {:ok, children} = mod.children(ref, id)

      {:ok,
       job
       |> job_detail()
       |> Map.put("steps", Enum.map(steps, &step_summary/1))
       |> Map.put("events", Enum.map(events, &event_summary/1))
       |> Map.put("children", Enum.map(children, &job_summary/1))}
    end
  end

  defp call_tool("workflow_status", %{"workflow_id" => workflow_id}, {mod, ref}) do
    {:ok, jobs} = mod.workflow_jobs(ref, workflow_id)

    counts = Enum.frequencies_by(jobs, & &1.state)

    {:ok,
     %{
       "workflow_id" => workflow_id,
       "total" => length(jobs),
       "state_counts" => counts,
       "done" => jobs != [] and Enum.all?(jobs, &Job.terminal?/1),
       "jobs" => Enum.map(jobs, &job_summary/1)
     }}
  end

  defp call_tool("retry_job", %{"id" => id}, {mod, ref}) do
    case mod.retry(ref, id, DateTime.utc_now()) do
      {:ok, job} -> {:ok, job_summary(job)}
      {:error, reason} -> {:error, "cannot retry job #{id}: #{reason}"}
    end
  end

  defp call_tool("cancel_job", %{"id" => id}, {mod, ref}) do
    {:ok, %{status: status}} = mod.request_cancel(ref, id, DateTime.utc_now())

    {:ok, %{"id" => id, "cancel" => to_string(status)}}
  end

  defp call_tool("signal", %{"scope" => scope, "name" => name} = args, {mod, ref}) do
    payload = Map.get(args, "payload", %{})

    {:ok, woken} = mod.put_signal(ref, scope, name, payload, DateTime.utc_now())

    {:ok, %{"delivered" => true, "woke_jobs" => Enum.map(woken, & &1.id)}}
  end

  defp call_tool("steer_job", %{"id" => id, "payload" => payload}, {mod, ref}) do
    {:ok, _woken} = mod.put_signal(ref, "job:#{id}", "$steer", payload, DateTime.utc_now())

    {:ok, %{"steered" => id}}
  end

  defp call_tool(name, _args, _storage), do: {:error, "unknown tool: #{name}"}

  # -- Serialization ------------------------------------------------------------

  defp fetch_job(mod, ref, id) do
    case mod.get_job(ref, id) do
      {:ok, job} -> {:ok, job}
      :error -> {:error, "job #{id} not found"}
    end
  end

  defp job_summary(%Job{} = job) do
    %{
      "id" => job.id,
      "worker" => job.kind,
      "queue" => job.queue,
      "state" => job.state,
      "attempt" => job.attempt,
      "workflow" => job.workflow_id && %{"id" => job.workflow_id, "name" => job.wf_name},
      "inserted_at" => iso(job.inserted_at),
      "finished_at" => iso(job.finished_at)
    }
  end

  defp job_detail(%Job{} = job) do
    job
    |> job_summary()
    |> Map.merge(%{
      "input" => job.input,
      "meta" => job.meta,
      "errors" => job.errors,
      "priority" => job.priority,
      "max_attempts" => job.max_attempts,
      "ready_at" => iso(job.ready_at),
      "await" => job.await_name && %{"scope" => job.await_scope, "name" => job.await_name},
      "spent" => %{"usd_micros" => job.spent_usd_micros, "tokens" => job.spent_tokens},
      "budget" => %{"usd_micros" => job.budget_usd_micros, "tokens" => job.budget_tokens},
      "result" => job.result && inspect(Job.result(job), limit: 50, printable_limit: 2_000)
    })
  end

  defp step_summary(step) do
    %{
      "seq" => step.seq,
      "name" => step.name,
      "usd_micros" => step.usd_micros,
      "tokens" => step.tokens,
      "value" => step.value && inspect(:erlang.binary_to_term(step.value), limit: 25)
    }
  end

  defp event_summary(event) do
    %{"seq" => event.seq, "payload" => event.payload, "at" => iso(event.inserted_at)}
  end

  defp iso(nil), do: nil
  defp iso(%DateTime{} = dt), do: DateTime.to_iso8601(dt)

  defp result(id, payload), do: %{"jsonrpc" => "2.0", "id" => id, "result" => payload}

  defp error_response(id, code, message) do
    %{"jsonrpc" => "2.0", "id" => id, "error" => %{"code" => code, "message" => message}}
  end

  defp version, do: Application.spec(:capstan, :vsn) |> to_string()
end

defmodule Mix.Tasks.Capstan.Mcp do
  @shortdoc "Serve the Capstan MCP server over stdio"

  @moduledoc """
  Serve the Capstan MCP server over stdio against a Postgres database:

      mix capstan.mcp --url postgres://user:pass@host:5432/my_app

  The URL may also come from the `CAPSTAN_URL` environment variable. Add it to
  an MCP client config to let AI assistants inspect queues, jobs, steps, and
  events, and retry/cancel/signal/steer jobs.
  """

  use Mix.Task

  @impl Mix.Task
  def run(argv) do
    {opts, _rest} = OptionParser.parse!(argv, strict: [url: :string])

    url =
      opts[:url] || System.get_env("CAPSTAN_URL") ||
        Mix.raise("pass --url postgres://... or set CAPSTAN_URL")

    Mix.Task.run("app.config")
    {:ok, _} = Application.ensure_all_started(:postgrex)
    {:ok, _} = Application.ensure_all_started(:jason)

    conn_opts =
      url
      |> Capstan.Storage.Postgres.parse_url()
      |> Keyword.merge(
        name: Capstan.MCP.Conn,
        pool_size: 2,
        types: Capstan.Storage.PostgresTypes
      )

    {:ok, _} = Postgrex.start_link(conn_opts)

    Capstan.MCP.serve({Capstan.Storage.Postgres, Capstan.MCP.Conn})
  end
end
