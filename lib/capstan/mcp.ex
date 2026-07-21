defmodule Capstan.MCP do
  @moduledoc """
  A Model Context Protocol server over stdio, so AI assistants (Claude Code,
  Cursor, or your own agents) can inspect and operate a Capstan installation:

      # .mcp.json / MCP client config
      {"command": "mix", "args": ["capstan.mcp", "--url", "postgres://.../my_app"]}

  Read tools: `stats`, `list_jobs`, `get_job` (with steps, events, children),
  `workflow_status`. Write tools (clearly named as mutations): `retry_job`,
  `cancel_job`, `signal`, `steer_job`. Mutations are disabled by default;
  configure an `:authorizer` or explicitly set `allow_mutations: true`.

  The server talks JSON-RPC 2.0 over newline-delimited stdio and needs only
  database access — no running Capstan instance required.
  """

  alias Capstan.Job

  @protocol_version "2025-06-18"

  # -- Serving ------------------------------------------------------------------

  @doc """
  Blocking stdio serve loop. `storage` is a `{module, ref}` pair.

  Options:

    * `:authorizer` — a module exporting `authorize(tool_name, args)` (or a
      2-arity fun) returning `:ok` or `{:error, message}`, consulted before
      every **mutating** tool call (`retry_job`, `cancel_job`, `signal`,
      `steer_job`). Introspection tools are never gated. This is the hook for
      capability systems such as [Legant](https://github.com/legant-dev/legant):
      verify the caller's grant offline and deny anything the token doesn't
      attenuate to.
    * `:allow_mutations` — explicitly enable every mutation without an
      authorizer (default `false`); intended only for a trusted local client.
  """
  def serve(storage, opts \\ []) do
    case IO.read(:stdio, :line) do
      :eof ->
        :ok

      {:error, _reason} ->
        :ok

      line ->
        line
        |> String.trim()
        |> handle_line(storage, opts)

        serve(storage, opts)
    end
  end

  defp handle_line("", _storage, _opts), do: :ok

  defp handle_line(line, storage, opts) do
    case Jason.decode(line) do
      {:ok, request} ->
        case handle_request(request, storage, opts) do
          nil -> :ok
          response -> IO.write([Jason.encode!(response), "\n"])
        end

      {:error, _} ->
        IO.write([Jason.encode!(error_response(nil, -32700, "parse error")), "\n"])
    end
  end

  # -- Request handling (pure; unit-testable) -----------------------------------

  @doc false
  def handle_request(request, storage, opts \\ [])

  def handle_request(%{"method" => "initialize", "id" => id}, _storage, _opts) do
    result(id, %{
      "protocolVersion" => @protocol_version,
      "capabilities" => %{"tools" => %{}},
      "serverInfo" => %{"name" => "capstan", "version" => version()}
    })
  end

  def handle_request(%{"method" => "notifications/" <> _}, _storage, _opts), do: nil

  def handle_request(%{"method" => "ping", "id" => id}, _storage, _opts), do: result(id, %{})

  def handle_request(%{"method" => "tools/list", "id" => id}, _storage, _opts) do
    result(id, %{"tools" => tools()})
  end

  def handle_request(
        %{"method" => "tools/call", "id" => id, "params" => %{"name" => tool} = params},
        storage,
        opts
      ) do
    args = Map.get(params, "arguments", %{})

    with :ok <-
           authorize(
             Keyword.get(opts, :authorizer),
             Keyword.get(opts, :allow_mutations, false),
             tool,
             args
           ),
         {:ok, data} <- call_tool(tool, args, storage) do
      result(id, %{
        "content" => [%{"type" => "text", "text" => Jason.encode!(data, pretty: true)}]
      })
    else
      {:error, message} ->
        result(id, %{
          "content" => [%{"type" => "text", "text" => message}],
          "isError" => true
        })
    end
  end

  def handle_request(%{"id" => id}, _storage, _opts) do
    error_response(id, -32601, "method not found")
  end

  def handle_request(_request, _storage, _opts), do: nil

  @mutating_tools ~w(retry_job cancel_job signal steer_job)

  defp authorize(_authorizer, _allow_mutations, tool, _args) when tool not in @mutating_tools,
    do: :ok

  defp authorize(nil, true, _tool, _args), do: :ok

  defp authorize(nil, false, _tool, _args) do
    {:error, "mutations are disabled; configure an authorizer or pass allow_mutations: true"}
  end

  defp authorize(authorizer, _allow_mutations, tool, args) do
    case run_authorizer(authorizer, tool, args) do
      :ok -> :ok
      {:error, message} -> {:error, "unauthorized: #{message}"}
      other -> {:error, "unauthorized: #{inspect(other)}"}
    end
  end

  defp run_authorizer(fun, tool, args) when is_function(fun, 2), do: fun.(tool, args)
  defp run_authorizer(module, tool, args) when is_atom(module), do: module.authorize(tool, args)

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
      tool(
        "workflow_status",
        "State counts and completion for a workflow or batch.",
        %{
          "workflow_id" => %{"type" => "string"}
        },
        ["workflow_id"]
      ),
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

  @doc false
  defdelegate job_summary(job), to: Capstan.View
  @doc false
  defdelegate job_detail(job), to: Capstan.View
  @doc false
  defdelegate step_summary(step), to: Capstan.View
  @doc false
  defdelegate event_summary(event), to: Capstan.View

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
  events. Mutation tools are read-only by default; pass `--authorizer MyGuard`
  for policy-gated writes or `--allow-mutations` to opt in explicitly.
  """

  use Mix.Task

  @impl Mix.Task
  def run(argv) do
    {opts, _rest} =
      OptionParser.parse!(argv,
        strict: [url: :string, authorizer: :string, allow_mutations: :boolean]
      )

    url =
      opts[:url] || System.get_env("CAPSTAN_URL") ||
        Mix.raise("pass --url postgres://... or set CAPSTAN_URL")

    authorizer =
      case opts[:authorizer] do
        nil -> nil
        name -> name |> String.replace_prefix("Elixir.", "") |> then(&Module.concat([&1]))
      end

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

    Capstan.MCP.serve({Capstan.Storage.Postgres, Capstan.MCP.Conn},
      authorizer: authorizer,
      allow_mutations: opts[:allow_mutations] || false
    )
  end
end
