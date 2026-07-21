defmodule Belay.CodecTest do
  # The cross-language value envelope: rows written by a foreign SDK (JSON)
  # must be readable by the Elixir engine exactly like ETF rows.
  use Belay.Test.Case, async: false

  alias Belay.Test.StepOnly

  test "a JSON step value written by a foreign SDK is replayed transparently", %{} do
    %{name: name} = start_belay!()

    {:ok, job} = Belay.insert(name, StepOnly.new(%{}))

    # Simulate a Python/TS worker having journaled step :a as JSON.
    {mod, ref} = storage(name)
    now = Belay.Config.now(config(name))

    {:ok, _} =
      mod.put_step(
        ref,
        job.id,
        "a",
        ~s({"lang":"python","n":7}),
        %{usd_micros: 0, tokens: 0},
        now
      )

    assert %{succeeded: 1} = Testing.drain(name, :default)

    # StepOnly returns its step value: the memoized JSON row, decoded.
    assert {:ok, %{"lang" => "python", "n" => 7}} = Belay.await_result(name, job.id, 100)
  end

  test "results decode from either encoding" do
    %{name: name} = start_belay!()

    {:ok, job} = Belay.insert(name, StepOnly.new(%{}))

    {mod, ref} = storage(name)
    conf = config(name)
    spec = Belay.Config.queue_spec(conf, :default)
    now = Belay.Config.now(conf)

    {:ok, [claimed]} = mod.claim(ref, spec, 1, "n", 30_000, now)

    # A foreign worker acking with a JSON result.
    {:ok, _} = mod.ack(ref, claimed, {:succeeded, ~s(["done", 42])}, now)

    assert {:ok, ["done", 42]} = Belay.await_result(name, job.id, 100)
  end

  test "codec discriminator is exact" do
    assert Belay.Codec.decode(Belay.Codec.encode(%{a: 1})) == %{a: 1}
    assert Belay.Codec.decode(~s({"a": 1})) == %{"a" => 1}
    assert Belay.Codec.decode("42") == 42
    assert Belay.Codec.decode(nil) == nil
  end
end
