defmodule Capstan.MCPTest do
  use Capstan.Test.Case, async: false

  alias Capstan.MCP
  alias Capstan.Test.{Awaiter, Emitter, Tagged}

  setup do
    {:ok, start_capstan!()}
  end

  defp call(name, tool, args \\ %{}) do
    request = %{
      "jsonrpc" => "2.0",
      "id" => 1,
      "method" => "tools/call",
      "params" => %{"name" => tool, "arguments" => args}
    }

    %{"result" => result} = MCP.handle_request(request, storage(name))

    case result do
      %{"isError" => true, "content" => [%{"text" => text}]} -> {:error, text}
      %{"content" => [%{"text" => text}]} -> {:ok, Jason.decode!(text)}
    end
  end

  test "initialize and tools/list speak MCP", %{name: name} do
    assert %{"result" => %{"protocolVersion" => _, "serverInfo" => %{"name" => "capstan"}}} =
             MCP.handle_request(
               %{"jsonrpc" => "2.0", "id" => 1, "method" => "initialize"},
               storage(name)
             )

    assert MCP.handle_request(%{"method" => "notifications/initialized"}, storage(name)) == nil

    %{"result" => %{"tools" => tools}} =
      MCP.handle_request(%{"jsonrpc" => "2.0", "id" => 2, "method" => "tools/list"}, storage(name))

    names = Enum.map(tools, & &1["name"])

    assert "stats" in names
    assert "get_job" in names
    assert "steer_job" in names
  end

  test "introspection tools return real data", %{name: name} do
    {:ok, job} = Capstan.insert(name, Emitter.new(%{}))

    Testing.drain(name, :default)

    {:ok, stats} = call(name, "stats")
    assert [%{"queue" => "default", "state" => "succeeded", "count" => 1}] = stats

    {:ok, listed} = call(name, "list_jobs", %{"state" => "succeeded"})
    assert [%{"id" => id}] = listed
    assert id == job.id

    {:ok, detail} = call(name, "get_job", %{"id" => job.id})

    assert detail["state"] == "succeeded"
    assert length(detail["events"]) == 3
    assert detail["result"] =~ ":emitted"
  end

  test "mutation tools operate jobs", %{name: name} do
    {:ok, failed} = Capstan.insert(name, Tagged.new(%{"tag" => "f", "fail" => true}, max_attempts: 1))
    {:ok, waiting} = Capstan.insert(name, Awaiter.new(%{}))

    Testing.drain(name, :default)

    {:ok, retried} = call(name, "retry_job", %{"id" => failed.id})
    assert retried["state"] == "ready"

    {:ok, signal_result} =
      call(name, "signal", %{
        "scope" => "job:#{waiting.id}",
        "name" => "approval",
        "payload" => %{"approved" => true}
      })

    assert signal_result["woke_jobs"] == [waiting.id]

    {:ok, %{"cancel" => "cancelled"}} = call(name, "cancel_job", %{"id" => retried["id"]})

    assert {:error, text} = call(name, "get_job", %{"id" => 999_999})
    assert text =~ "not found"
  end
end
