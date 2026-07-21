defmodule Capstan.AdaptiveLimitTest do
  use Capstan.Test.Case, async: false

  alias Capstan.Test.Sleeper

  test "limit bounds are validated" do
    assert_raise ArgumentError, ~r/1 <= min <= max/, fn ->
      Capstan.Config.new(
        name: AdaptCfg,
        storage: [adapter: :memory],
        queues: [q: [limit: [min: 5, max: 2]]]
      )
    end
  end

  test "producer scales up under sustained load and decays when idle" do
    handler_id = "adaptive-test-#{System.unique_integer([:positive])}"
    parent = self()

    :telemetry.attach(
      handler_id,
      [:capstan, :queue, :scale],
      fn _event, %{limit: limit}, %{from: from}, _cfg ->
        send(parent, {:scaled, from, limit})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    %{name: name} =
      start_capstan!(
        sim_clock: false,
        queues: [default: [limit: [min: 1, max: 8]]],
        poll_interval: 60
      )

    for _ <- 1..12 do
      {:ok, _} = Capstan.insert(name, Sleeper.new(%{"ms" => 40}))
    end

    # Saturated claim rounds double the limit toward the max...
    assert_receive {:scaled, 1, 2}, 2_000
    assert_receive {:scaled, 2, 4}, 2_000

    # ...and the backlog drains under the raised limit.
    wait_until(fn -> Capstan.stats(name)["default"]["succeeded"] == 12 end, 5_000)

    # Idle polls decay back toward the floor.
    assert_receive {:scaled, from, to} when to < from, 3_000
  end
end
