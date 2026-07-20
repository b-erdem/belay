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
  defp request!(method, url, body \\ "") do
    %{port: port, path: path, query: query} = URI.parse(url)
    target = path <> if(query, do: "?" <> query, else: "")

    {:ok, socket} = :gen_tcp.connect(~c"localhost", port, [:binary, active: false])

    :ok =
      :gen_tcp.send(socket, [
        "#{method} #{target} HTTP/1.1\r\nhost: localhost\r\n",
        "content-type: application/json\r\ncontent-length: #{byte_size(body)}\r\n\r\n",
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
    {status, body} = request!("POST", url, Jason.encode!(payload))

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

    {200, retried} = post!(base <> "/api/jobs/#{failed.id}/retry?token=s3cret", %{})
    assert retried["state"] == "ready"

    {403, denied} = post!(base <> "/api/jobs/#{failed.id}/cancel?token=s3cret", %{})
    assert denied["error"] =~ "no cancels today"

    {200, _} =
      post!(base <> "/api/signals?token=s3cret", %{
        "scope" => "custom",
        "name" => "ping",
        "payload" => %{"n" => 1}
      })

    {mod, ref} = storage(name)
    assert {:ok, %{"n" => 1}} = mod.get_signal(ref, ["custom"], "ping")
  end
end
