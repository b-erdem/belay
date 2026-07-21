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
      (`Authorization: Bearer <token>`; `?token=` is accepted only for GET
      requests so browser `EventSource` can authenticate)
    * `:authorizer` — gates the mutating endpoints exactly like the MCP
      server's authorizer: `authorize(tool, args) -> :ok | {:error, msg}`

  Mutations are disabled by default. Configure either `:token` or
  `:authorizer` to enable them. Mutating requests must use JSON and, when an
  `Origin` header is present, it must match the request's `Host` header.

  The server speaks plain HTTP/1.1 over `gen_tcp` with Erlang's built-in
  request parsing; live updates stream over SSE. It is an operator tool:
  no TLS (front it with your proxy for remote access) and one connection
  per request.
  """

  use GenServer

  require Logger

  alias Capstan.{Config, View}

  @max_body_bytes 1_048_576
  @max_header_bytes 65_536
  @max_headers 100

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
         {:ok, headers} <- read_headers(socket, %{}, 0, 0) do
      {route, query} = split_path(to_string(path))

      case read_body(socket, headers) do
        {:ok, body} ->
          respond(socket, state, method, route, query, headers, body)

        {:error, :too_large} ->
          send_json(socket, 413, %{"error" => "request body too large"})

        {:error, :bad_length} ->
          send_json(socket, 400, %{"error" => "invalid content-length"})

        {:error, :incomplete} ->
          send_json(socket, 400, %{"error" => "incomplete request body"})

        {:error, :unsupported_transfer} ->
          send_json(socket, 400, %{"error" => "transfer-encoding is not supported"})
      end
    else
      {:error, :headers_too_large} ->
        send_json(socket, 431, %{"error" => "request headers too large"})

      {:error, :duplicate_content_length} ->
        send_json(socket, 400, %{"error" => "duplicate content-length"})

      _ ->
        :ok
    end
  after
    :gen_tcp.close(socket)
  end

  defp read_headers(_socket, _acc, count, _bytes) when count >= @max_headers,
    do: {:error, :headers_too_large}

  defp read_headers(_socket, _acc, _count, bytes) when bytes >= @max_header_bytes,
    do: {:error, :headers_too_large}

  defp read_headers(socket, acc, count, bytes) do
    case :gen_tcp.recv(socket, 0, 5_000) do
      {:ok, {:http_header, _, name, _, value}} ->
        name = String.downcase(to_string(name))
        value = to_string(value)

        cond do
          name == "content-length" and Map.has_key?(acc, name) ->
            {:error, :duplicate_content_length}

          bytes + byte_size(name) + byte_size(value) > @max_header_bytes ->
            {:error, :headers_too_large}

          true ->
            read_headers(
              socket,
              Map.put(acc, name, value),
              count + 1,
              bytes + byte_size(name) + byte_size(value)
            )
        end

      {:ok, :http_eoh} ->
        {:ok, acc}

      _ ->
        :error
    end
  end

  defp read_body(_socket, %{"transfer-encoding" => _}), do: {:error, :unsupported_transfer}

  defp read_body(socket, headers) do
    case Integer.parse(Map.get(headers, "content-length", "0")) do
      {length, ""} when length > @max_body_bytes ->
        {:error, :too_large}

      {length, ""} when length > 0 ->
        :inet.setopts(socket, packet: :raw)

        case :gen_tcp.recv(socket, length, 5_000) do
          {:ok, body} -> {:ok, body}
          _ -> {:error, :incomplete}
        end

      {0, ""} ->
        {:ok, ""}

      _ ->
        {:error, :bad_length}
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
      not authorized_request?(state, method, query, headers) ->
        send_json(socket, 401, %{"error" => "unauthorized"})

      method == :POST and not json_request?(headers) ->
        send_json(socket, 415, %{"error" => "content-type must be application/json"})

      method == :POST and not same_origin?(headers) ->
        send_json(socket, 403, %{"error" => "cross-origin mutation denied"})

      method == :POST and not json_object?(body) ->
        send_json(socket, 400, %{"error" => "request body must be a JSON object"})

      method == :GET and route == "/api/sse" ->
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

  defp authorized_request?(%{token: nil}, _method, _query, _headers), do: true

  defp authorized_request?(%{token: token}, method, query, headers) do
    presented = strip_bearer(Map.get(headers, "authorization")) || get_query_token(method, query)
    is_binary(presented) and secure_equal?(presented, token)
  end

  defp get_query_token(:GET, query), do: query["token"]
  defp get_query_token(_method, _query), do: nil

  defp strip_bearer("Bearer " <> rest), do: rest
  defp strip_bearer(_), do: nil

  # Hashing hides token length; hash_equals/2 compares the fixed-size digests
  # in constant time.
  defp secure_equal?(a, b) when is_binary(a) and is_binary(b) do
    :crypto.hash_equals(:crypto.hash(:sha256, a), :crypto.hash(:sha256, b))
  end

  defp secure_equal?(_a, _b), do: false

  defp json_request?(headers) do
    headers
    |> Map.get("content-type", "")
    |> String.downcase()
    |> String.split(";", parts: 2)
    |> hd()
    |> String.trim()
    |> Kernel.==("application/json")
  end

  defp json_object?(body) do
    case Jason.decode(body) do
      {:ok, value} when is_map(value) -> true
      _ -> false
    end
  end

  defp same_origin?(headers) do
    case Map.get(headers, "origin") do
      nil -> true
      "null" -> false
      origin -> same_authority?(URI.parse(origin), Map.get(headers, "host"))
    end
  end

  defp same_authority?(%URI{scheme: scheme, host: host, port: port}, request_host)
       when scheme in ["http", "https"] and is_binary(host) and is_binary(request_host) do
    origin_port = port || if(scheme == "https", do: 443, else: 80)
    request = URI.parse("#{scheme}://#{request_host}")
    request_port = request.port || if(scheme == "https", do: 443, else: 80)

    String.downcase(host) == String.downcase(request.host || "") and origin_port == request_port
  end

  defp same_authority?(_origin, _request_host), do: false

  # -- Routes -------------------------------------------------------------------

  defp handle(:GET, "/", _query, _body, _state) do
    {200, {:html, Capstan.Dashboard.Page.html()}}
  end

  defp handle(:GET, "/api/overview", _query, _body, state) do
    jobs = Capstan.list_jobs(state.capstan, limit: 30)
    {storage, ref} = Capstan.Config.fetch!(state.capstan).storage_ref
    {:ok, rows} = storage.queue_stats(ref)

    stats =
      Enum.reduce(rows, %{}, fn %{queue: q, state: s, count: c}, acc ->
        Map.update(acc, q, %{s => c}, &Map.put(&1, s, c))
      end)

    spend = %{
      "usd_micros" => rows |> Enum.map(& &1.usd_micros) |> Enum.sum(),
      "tokens" => rows |> Enum.map(& &1.tokens) |> Enum.sum()
    }

    {200,
     %{
       "instance" => inspect(state.capstan),
       "stats" => stats,
       "spend" => spend,
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

    {200,
     %{"jobs" => state.capstan |> Capstan.list_jobs(filters) |> Enum.map(&View.job_summary/1)}}
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

  defp authorize_mutation(%{authorizer: nil, token: nil}, _tool, _args) do
    {403, %{"error" => "dashboard mutations are disabled; configure :token or :authorizer"}}
  end

  defp authorize_mutation(%{authorizer: nil, token: token}, _tool, _args) when is_binary(token),
    do: :ok

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
      "limit_min" => spec.limit_min,
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
    # The instance can go away mid-stream (shutdown, test teardown); end the
    # stream quietly instead of crashing the connection task.
    {_status, payload} =
      try do
        handle(:GET, "/api/overview", %{}, "", state)
      catch
        _, _ -> {:halt, nil}
      end

    with false <- payload == nil,
         :ok <- :gen_tcp.send(socket, ["data: ", Jason.encode!(payload), "\n\n"]) do
      Process.sleep(1_000)
      sse_push(socket, state)
    else
      _ -> :ok
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
  defp reason(400), do: "Bad Request"
  defp reason(401), do: "Unauthorized"
  defp reason(403), do: "Forbidden"
  defp reason(404), do: "Not Found"
  defp reason(409), do: "Conflict"
  defp reason(413), do: "Content Too Large"
  defp reason(415), do: "Unsupported Media Type"
  defp reason(431), do: "Request Header Fields Too Large"
  defp reason(_), do: "Error"
end
