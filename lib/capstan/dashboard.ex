defmodule Capstan.Dashboard do
  @moduledoc """
  An embedded web dashboard with **zero dependencies** — no Phoenix, no Plug,
  no JS build. One child spec:

      children = [
        {Capstan, name: MyApp.Capstan, ...},
        {Capstan.Dashboard, capstan: MyApp.Capstan, port: 4004}
      ]

  Then open http://localhost:4004 — live queue tiles, a filterable job list,
  a job drawer with the full journal (steps with costs, events, errors,
  children), a rendered **workflow DAG**, and operator actions: retry,
  cancel, signal, and steer.

  Options:

    * `:capstan` (required) — the instance name
    * `:port` — default 4004 (`0` picks an ephemeral port; see `port/1`)
    * `:bind` — default `{127, 0, 0, 1}`; bind `{0, 0, 0, 0}` only behind a
      proxy you trust
    * `:token` — when set, every request must carry it
      (`Authorization: Bearer <token>` or `?token=`)
    * `:authorizer` — gates the mutating endpoints exactly like the MCP
      server's authorizer: `authorize(tool, args) -> :ok | {:error, msg}`

  The server speaks plain HTTP/1.1 over `gen_tcp` with Erlang's built-in
  request parsing; live updates stream over SSE. It is an operator tool:
  no TLS (front it with your proxy for remote access) and one connection
  per request.
  """

  use GenServer

  require Logger

  alias Capstan.{Config, View}

  # -- Lifecycle ----------------------------------------------------------------

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  def child_spec(opts) do
    %{id: {__MODULE__, Keyword.get(opts, :port, 4004)}, start: {__MODULE__, :start_link, [opts]}}
  end

  @doc "The port the dashboard is listening on (useful with `port: 0`)."
  def port(pid), do: GenServer.call(pid, :port)

  @impl GenServer
  def init(opts) do
    capstan = Keyword.fetch!(opts, :capstan)
    port = Keyword.get(opts, :port, 4004)
    bind = Keyword.get(opts, :bind, {127, 0, 0, 1})

    {:ok, listen} =
      :gen_tcp.listen(port, [
        :binary,
        packet: :http_bin,
        active: false,
        reuseaddr: true,
        backlog: 64,
        ip: bind
      ])

    {:ok, actual_port} = :inet.port(listen)

    state = %{
      listen: listen,
      port: actual_port,
      capstan: capstan,
      token: Keyword.get(opts, :token),
      authorizer: Keyword.get(opts, :authorizer)
    }

    server = self()

    acceptor = spawn_link(fn -> accept_loop(listen, server, state) end)

    Logger.info("[capstan] dashboard for #{inspect(capstan)} on http://localhost:#{actual_port}")

    {:ok, Map.put(state, :acceptor, acceptor)}
  end

  @impl GenServer
  def handle_call(:port, _from, state), do: {:reply, state.port, state}

  defp accept_loop(listen, server, state) do
    case :gen_tcp.accept(listen) do
      {:ok, socket} ->
        {:ok, pid} = Task.start(fn -> serve(socket, state) end)

        :gen_tcp.controlling_process(socket, pid)
        send(pid, :go)

        accept_loop(listen, server, state)

      {:error, :closed} ->
        :ok
    end
  end

  # -- Request handling ---------------------------------------------------------

  defp serve(socket, state) do
    receive do
      :go -> :ok
    after
      5_000 -> exit(:normal)
    end

    with {:ok, {:http_request, method, {:abs_path, path}, _version}} <-
           :gen_tcp.recv(socket, 0, 10_000),
         {:ok, headers} <- read_headers(socket, %{}) do
      {route, query} = split_path(to_string(path))
      body = read_body(socket, headers)

      respond(socket, state, method, route, query, headers, body)
    else
      _ -> :ok
    end
  after
    :gen_tcp.close(socket)
  end

  defp read_headers(socket, acc) do
    case :gen_tcp.recv(socket, 0, 5_000) do
      {:ok, {:http_header, _, name, _, value}} ->
        read_headers(socket, Map.put(acc, String.downcase(to_string(name)), to_string(value)))

      {:ok, :http_eoh} ->
        {:ok, acc}

      _ ->
        :error
    end
  end

  defp read_body(socket, headers) do
    case Integer.parse(Map.get(headers, "content-length", "0")) do
      {length, _} when length > 0 ->
        :inet.setopts(socket, packet: :raw)

        case :gen_tcp.recv(socket, length, 5_000) do
          {:ok, body} -> body
          _ -> ""
        end

      _ ->
        ""
    end
  end

  defp split_path(path) do
    case String.split(path, "?", parts: 2) do
      [route] -> {route, %{}}
      [route, query] -> {route, URI.decode_query(query)}
    end
  end

  defp respond(socket, state, method, route, query, headers, body) do
    cond do
      not authorized_request?(state, query, headers) ->
        send_json(socket, 401, %{"error" => "unauthorized"})

      route == "/api/sse" ->
        sse_loop(socket, state)

      true ->
        {status, payload} = handle(method, route, query, body, state)

        case payload do
          {:html, html} -> send_response(socket, status, "text/html; charset=utf-8", html)
          json -> send_json(socket, status, json)
        end
    end
  rescue
    error ->
      Logger.warning("[capstan] dashboard request failed: #{Exception.message(error)}")

      send_json(socket, 500, %{"error" => Exception.message(error)})
  end

  defp authorized_request?(%{token: nil}, _query, _headers), do: true

  defp authorized_request?(%{token: token}, query, headers) do
    query["token"] == token or Map.get(headers, "authorization") == "Bearer " <> token
  end

  # -- Routes -------------------------------------------------------------------

  defp handle(:GET, "/", _query, _body, _state) do
    {200, {:html, Capstan.Dashboard.Page.html()}}
  end

  defp handle(:GET, "/api/overview", _query, _body, state) do
    jobs = Capstan.list_jobs(state.capstan, limit: 30)

    {200,
     %{
       "instance" => inspect(state.capstan),
       "stats" => Capstan.stats(state.capstan),
       "queues" => queue_specs(state),
       "recent" => Enum.map(jobs, &View.job_summary/1)
     }}
  end

  defp handle(:GET, "/api/jobs", query, _body, state) do
    filters =
      query
      |> Map.take(["queue", "state", "workflow_id", "limit", "before_id", "parent_id"])
      |> Map.new(fn
        {key, value} when key in ["limit", "before_id", "parent_id"] ->
          {String.to_existing_atom(key), String.to_integer(value)}

        {key, value} ->
          {String.to_existing_atom(key), value}
      end)

    {200, %{"jobs" => state.capstan |> Capstan.list_jobs(filters) |> Enum.map(&View.job_summary/1)}}
  end

  defp handle(:GET, "/api/jobs/" <> id, _query, _body, state) do
    id = String.to_integer(id)

    case Capstan.get_job(state.capstan, id) do
      {:ok, job} ->
        {:ok, steps} = Capstan.steps(state.capstan, id)
        events = Capstan.events(state.capstan, id)
        children = Capstan.list_jobs(state.capstan, parent_id: id)

        {200,
         job
         |> View.job_detail()
         |> Map.put("steps", Enum.map(steps, &View.step_summary/1))
         |> Map.put("events", Enum.map(events, &View.event_summary/1))
         |> Map.put("children", Enum.map(children, &View.job_summary/1))}

      :error ->
        {404, %{"error" => "job #{id} not found"}}
    end
  end

  defp handle(:GET, "/api/workflows/" <> workflow_id, _query, _body, state) do
    jobs = Capstan.Workflow.jobs(state.capstan, workflow_id)

    {200,
     %{
       "workflow_id" => workflow_id,
       "jobs" => Enum.map(jobs, &View.job_summary/1),
       "done" => jobs != [] and Enum.all?(jobs, &Capstan.Job.terminal?/1)
     }}
  end

  defp handle(:POST, "/api/jobs/" <> rest, _query, body, state) do
    case String.split(rest, "/", parts: 2) do
      [id, action] when action in ["retry", "cancel", "steer"] ->
        mutate(state, action, String.to_integer(id), decode_body(body))

      _ ->
        {404, %{"error" => "unknown action"}}
    end
  end

  defp handle(:POST, "/api/signals", _query, body, state) do
    %{"scope" => scope, "name" => name} = params = decode_body(body)

    with :ok <- authorize_mutation(state, "signal", params) do
      :ok = Capstan.signal(state.capstan, scope, name, Map.get(params, "payload", %{}))

      {200, %{"delivered" => true}}
    end
  end

  defp handle(_method, _route, _query, _body, _state) do
    {404, %{"error" => "not found"}}
  end

  defp mutate(state, "retry", id, params) do
    with :ok <- authorize_mutation(state, "retry_job", Map.put(params, "id", id)) do
      case Capstan.retry_job(state.capstan, id) do
        {:ok, job} -> {200, View.job_summary(job)}
        {:error, reason} -> {409, %{"error" => to_string(reason)}}
      end
    end
  end

  defp mutate(state, "cancel", id, params) do
    with :ok <- authorize_mutation(state, "cancel_job", Map.put(params, "id", id)) do
      {:ok, status} = Capstan.cancel(state.capstan, id)

      {200, %{"cancel" => to_string(status)}}
    end
  end

  defp mutate(state, "steer", id, params) do
    with :ok <- authorize_mutation(state, "steer_job", Map.put(params, "id", id)) do
      :ok = Capstan.steer(state.capstan, id, Map.get(params, "payload", %{}))

      {200, %{"steered" => id}}
    end
  end

  defp authorize_mutation(%{authorizer: nil}, _tool, _args), do: :ok

  defp authorize_mutation(%{authorizer: authorizer}, tool, args) do
    case run_authorizer(authorizer, tool, args) do
      :ok -> :ok
      {:error, message} -> {403, %{"error" => "unauthorized: #{message}"}}
      other -> {403, %{"error" => "unauthorized: #{inspect(other)}"}}
    end
  end

  defp run_authorizer(fun, tool, args) when is_function(fun, 2), do: fun.(tool, args)
  defp run_authorizer(module, tool, args) when is_atom(module), do: module.authorize(tool, args)

  defp decode_body(""), do: %{}

  defp decode_body(body) do
    case Jason.decode(body) do
      {:ok, map} when is_map(map) -> map
      _ -> %{}
    end
  end

  defp queue_specs(state) do
    config = Config.fetch!(state.capstan)

    static = Map.new(config.queues, fn {queue, spec} -> {queue, spec_view(spec)} end)

    dynamic =
      try do
        Map.new(Capstan.Queues.dynamic_specs(config), fn {queue, spec} ->
          {queue, spec |> spec_view() |> Map.put("dynamic", true)}
        end)
      rescue
        _ -> %{}
      end

    Map.merge(static, dynamic)
  end

  defp spec_view(spec) do
    %{
      "limit" => spec.local_limit,
      "global_limit" => spec.global_limit,
      "rate" => spec.rate && Map.new(spec.rate, fn {k, v} -> {to_string(k), v} end),
      "partition" => spec.partition && Tuple.to_list(spec.partition) |> Enum.map(&to_string/1),
      "manual" => spec.manual
    }
  end

  # -- SSE ----------------------------------------------------------------------

  defp sse_loop(socket, state) do
    :inet.setopts(socket, packet: :raw)

    :gen_tcp.send(socket, [
      "HTTP/1.1 200 OK\r\n",
      "content-type: text/event-stream\r\ncache-control: no-cache\r\nconnection: keep-alive\r\n\r\n"
    ])

    sse_push(socket, state)
  end

  defp sse_push(socket, state) do
    {_status, payload} = handle(:GET, "/api/overview", %{}, "", state)

    case :gen_tcp.send(socket, ["data: ", Jason.encode!(payload), "\n\n"]) do
      :ok ->
        Process.sleep(1_000)
        sse_push(socket, state)

      {:error, _} ->
        :ok
    end
  end

  # -- Response plumbing --------------------------------------------------------

  defp send_json(socket, status, payload) do
    send_response(socket, status, "application/json", Jason.encode!(payload))
  end

  defp send_response(socket, status, content_type, body) do
    :inet.setopts(socket, packet: :raw)

    :gen_tcp.send(socket, [
      "HTTP/1.1 #{status} #{reason(status)}\r\n",
      "content-type: #{content_type}\r\n",
      "content-length: #{IO.iodata_length(body)}\r\n",
      "connection: close\r\n\r\n",
      body
    ])
  end

  defp reason(200), do: "OK"
  defp reason(401), do: "Unauthorized"
  defp reason(403), do: "Forbidden"
  defp reason(404), do: "Not Found"
  defp reason(409), do: "Conflict"
  defp reason(_), do: "Error"
end
