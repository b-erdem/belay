defmodule Capstan.DashboardTest do
  use Capstan.Test.Case, async: false

  alias Capstan.Test.{Emitter, Tagged}
  alias Capstan.Workflow

  setup do
    context = start_capstan!()

    {:ok, dash} =
      Capstan.Dashboard.start_link(
        capstan: context.name,
        port: 0,
        token: "s3cret",
        authorizer: fn
          "cancel_job", _args -> {:error, "no cancels today"}
          _tool, _args -> :ok
        end
      )

    port = Capstan.Dashboard.port(dash)

    {:ok, Map.merge(context, %{base: "http://localhost:#{port}", dash: dash})}
  end

  # Raw HTTP client over gen_tcp — the dashboard speaks plain HTTP/1.1 and
  # closes per request, so reading to socket close is the whole protocol.
  defp request!(method, url, body \\ "", headers \\ []) do
    %{port: port, path: path, query: query} = URI.parse(url)
    target = path <> if(query, do: "?" <> query, else: "")

    {:ok, socket} = :gen_tcp.connect(~c"localhost", port, [:binary, active: false])

    :ok =
      :gen_tcp.send(socket, [
        "#{method} #{target} HTTP/1.1\r\nhost: localhost\r\n",
        Enum.map(headers, fn {name, value} -> "#{name}: #{value}\r\n" end),
        "content-length: #{byte_size(body)}\r\n\r\n",
        body
      ])

    response = read_all(socket, [])
    :gen_tcp.close(socket)

    [head, body] = String.split(response, "\r\n\r\n", parts: 2)
    ["HTTP/1.1", status | _] = head |> String.split("\r\n") |> hd() |> String.split(" ")

    {String.to_integer(status), body}
  end

  defp read_all(socket, acc) do
    case :gen_tcp.recv(socket, 0, 5_000) do
      {:ok, chunk} -> read_all(socket, [acc | chunk])
      {:error, :closed} -> IO.iodata_to_binary(acc)
    end
  end

  defp get!(url), do: request!("GET", url)

  defp post!(url, payload) do
    {status, body} =
      request!("POST", url, Jason.encode!(payload), [
        {"content-type", "application/json"},
        {"authorization", "Bearer s3cret"}
      ])

    {status, Jason.decode!(body)}
  end

  test "serves the app and enforces the token", %{base: base} do
    assert {401, _} = get!(base <> "/api/overview")

    {200, html} = get!(base <> "/?token=s3cret")
    assert html =~ "<!doctype html"
    assert html =~ "Capstan"
  end

  test "overview, job detail, and workflow endpoints return real data", %{
    base: base,
    name: name
  } do
    {:ok, emitted} = Capstan.insert(name, Emitter.new(%{}))

    {:ok, wf_jobs} =
      Workflow.new()
      |> Workflow.add(:a, Tagged.new(%{"tag" => "a"}))
      |> Workflow.add(:b, Tagged.new(%{"tag" => "b"}), deps: [:a])
      |> Workflow.insert(name)

    Testing.drain(name, :default)

    {200, body} = get!(base <> "/api/overview?token=s3cret")
    overview = Jason.decode!(body)

    assert overview["stats"]["default"]["succeeded"] == 3
    assert overview["queues"]["default"]["manual"] == true

    {200, body} = get!(base <> "/api/jobs/#{emitted.id}?token=s3cret")
    detail = Jason.decode!(body)

    assert detail["state"] == "succeeded"
    assert length(detail["events"]) == 3
    assert detail["result"] =~ ":emitted"

    wf_id = wf_jobs["a"].workflow_id

    {200, body} = get!(base <> "/api/workflows/#{wf_id}?token=s3cret")
    dag = Jason.decode!(body)

    assert dag["done"] == true
    assert [%{"workflow" => %{"deps" => []}}, %{"workflow" => %{"deps" => ["a"]}}] = dag["jobs"]
  end

  test "mutations work and respect the authorizer", %{base: base, name: name} do
    {:ok, failed} =
      Capstan.insert(name, Tagged.new(%{"tag" => "x", "fail" => true}, max_attempts: 1))

    Testing.drain(name, :default)

    {200, retried} = post!(base <> "/api/jobs/#{failed.id}/retry", %{})
    assert retried["state"] == "ready"

    {403, denied} = post!(base <> "/api/jobs/#{failed.id}/cancel", %{})
    assert denied["error"] =~ "no cancels today"

    {200, _} =
      post!(base <> "/api/signals", %{
        "scope" => "custom",
        "name" => "ping",
        "payload" => %{"n" => 1}
      })

    {mod, ref} = storage(name)
    assert {:ok, %{"n" => 1}} = mod.get_signal(ref, ["custom"], "ping")
  end

  test "SSE stream authenticates and pushes an overview frame immediately",
       %{base: base, name: name} do
    {:ok, _} = Capstan.insert(name, Tagged.new(%{"tag" => "sse"}))
    Testing.drain(name, :default)

    %{port: port} = URI.parse(base)
    {:ok, socket} = :gen_tcp.connect(~c"localhost", port, [:binary, active: false])

    :ok =
      :gen_tcp.send(socket, "GET /api/sse?token=s3cret HTTP/1.1\r\nhost: localhost\r\n\r\n")

    raw = read_first_frame(socket, "")
    :gen_tcp.close(socket)

    assert raw =~ "HTTP/1.1 200 OK"
    assert raw =~ "content-type: text/event-stream"

    # Frames are `data: <json>\n\n`; headers use \r\n so the bare \n\n
    # terminator is unambiguous.
    [_headers, frame] = String.split(raw, "data: ", parts: 2)
    [json | _] = String.split(frame, "\n\n", parts: 2)
    overview = Jason.decode!(json)

    assert overview["stats"]["default"]["succeeded"] == 1
  end

  test "SSE stream rejects bad tokens", %{base: base} do
    assert {401, _} = get!(base <> "/api/sse?token=wrong")
  end

  test "query tokens cannot authorize mutations", %{base: base} do
    {status, _body} =
      request!("POST", base <> "/api/signals?token=s3cret", ~s({}), [
        {"content-type", "application/json"}
      ])

    assert status == 401
  end

  test "mutations require JSON and reject cross-origin requests", %{base: base} do
    auth = [{"authorization", "Bearer s3cret"}]
    assert {415, _} = request!("POST", base <> "/api/signals", "scope=x", auth)

    assert {415, _} =
             request!("POST", base <> "/api/signals", ~s({}), [
               {"authorization", "Bearer s3cret"},
               {"content-type", "application/jsonp"}
             ])

    assert {400, _} =
             request!("POST", base <> "/api/signals", "{", [
               {"authorization", "Bearer s3cret"},
               {"content-type", "application/json"}
             ])

    assert {400, _} =
             request!("POST", base <> "/api/signals", ~s([]), [
               {"authorization", "Bearer s3cret"},
               {"content-type", "application/json"}
             ])

    assert {403, _} =
             request!("POST", base <> "/api/signals", ~s({}), [
               {"authorization", "Bearer s3cret"},
               {"content-type", "application/json"},
               {"origin", "https://evil.example"}
             ])
  end

  test "rejects unsupported or ambiguous request framing", %{base: base} do
    %{port: port} = URI.parse(base)

    {:ok, chunked} = :gen_tcp.connect(~c"localhost", port, [:binary, active: false])

    :ok =
      :gen_tcp.send(
        chunked,
        "POST /api/signals HTTP/1.1\r\nhost: localhost\r\n" <>
          "content-type: application/json\r\nauthorization: Bearer s3cret\r\n" <>
          "transfer-encoding: chunked\r\n\r\n0\r\n\r\n"
      )

    chunked_response = read_all(chunked, [])
    :gen_tcp.close(chunked)
    assert chunked_response =~ "HTTP/1.1 400"

    {:ok, duplicate} = :gen_tcp.connect(~c"localhost", port, [:binary, active: false])

    :ok =
      :gen_tcp.send(
        duplicate,
        "POST /api/signals HTTP/1.1\r\nhost: localhost\r\n" <>
          "content-type: application/json\r\nauthorization: Bearer s3cret\r\n" <>
          "content-length: 2\r\ncontent-length: 2\r\n\r\n{}"
      )

    duplicate_response = read_all(duplicate, [])
    :gen_tcp.close(duplicate)
    assert duplicate_response =~ "HTTP/1.1 400"
  end

  test "tokenless dashboards are read-only by default", %{name: name} do
    {:ok, read_only} = Capstan.Dashboard.start_link(capstan: name, port: 0)
    read_only_base = "http://localhost:#{Capstan.Dashboard.port(read_only)}"

    assert {200, _} = get!(read_only_base <> "/api/overview")

    {403, body} =
      request!("POST", read_only_base <> "/api/signals", ~s({"scope":"x","name":"y"}), [
        {"content-type", "application/json"}
      ])

    assert Jason.decode!(body)["error"] =~ "mutations are disabled"
  end

  test "rejects bodies over one MiB before reading them", %{base: base} do
    %{port: port} = URI.parse(base)
    {:ok, socket} = :gen_tcp.connect(~c"localhost", port, [:binary, active: false])

    :ok =
      :gen_tcp.send(
        socket,
        "POST /api/signals HTTP/1.1\r\nhost: localhost\r\ncontent-type: application/json\r\n" <>
          "authorization: Bearer s3cret\r\ncontent-length: 1048577\r\n\r\n"
      )

    response = read_all(socket, [])
    assert response =~ "HTTP/1.1 413"
  end

  # Read until one complete SSE frame has arrived (the stream never closes
  # on its own, so `read_all/2` would hang here).
  defp read_first_frame(socket, acc) do
    case String.split(acc, "data: ", parts: 2) do
      [_, frame] when frame != "" ->
        if String.contains?(frame, "\n\n"), do: acc, else: recv_more(socket, acc)

      _ ->
        recv_more(socket, acc)
    end
  end

  defp recv_more(socket, acc) do
    case :gen_tcp.recv(socket, 0, 5_000) do
      {:ok, chunk} -> read_first_frame(socket, acc <> chunk)
      {:error, _} -> acc
    end
  end
end
