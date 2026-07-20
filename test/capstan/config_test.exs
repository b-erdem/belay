defmodule Capstan.ConfigTest do
  use ExUnit.Case, async: true

  alias Capstan.Config

  defp new(overrides) do
    Config.new(Keyword.merge([name: CfgTest, storage: [adapter: :memory]], overrides))
  end

  test "the postgres notifier requires postgres storage" do
    assert_raise ArgumentError, ~r/postgres notifier requires/, fn ->
      new(notifiers: [:local, :postgres])
    end
  end

  test "rate specs require both allowed and period" do
    assert_raise KeyError, fn ->
      new(queues: [q: [limit: 5, rate: [allowed: 10]]])
    end
  end

  test "partition sources are validated" do
    assert_raise FunctionClauseError, fn ->
      new(queues: [q: [limit: 5, partition: {:nope, "k"}]])
    end
  end

  test "retention overrides merge over safe defaults" do
    config = new(retention: [succeeded: 60, failed: :infinity])

    assert config.retention["succeeded"] == 60
    assert config.retention["failed"] == :infinity
    assert config.retention["cancelled"] == 604_800
  end

  test "encryption keys must be exactly 32 bytes" do
    config = new(encryption: [key: {__MODULE__, :short_key, []}])

    assert_raise ArgumentError, ~r/32-byte/, fn -> Config.encryption_key(config) end
  end

  def short_key, do: "too-short"

  test "cron expressions are validated at boot" do
    assert_raise ArgumentError, ~r/invalid cron/, fn ->
      new(crons: [[name: "x", expr: "99 * * * *", worker: SomeWorker]])
    end
  end
end
