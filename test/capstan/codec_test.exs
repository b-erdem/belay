defmodule Capstan.CodecTest do
  # The cross-language value envelope: rows written by a foreign SDK (JSON)
  # must be readable by the Elixir engine exactly like ETF rows.
  use Capstan.Test.Case, async: false

  alias Capstan.Test.StepOnly

  test "a JSON step value written by a foreign SDK is replayed transparently", %{} do
    %{name: name} = start_capstan!()

    {:ok, job} = Capstan.insert(name, StepOnly.new(%{}))

    # Simulate a Python/TS worker having journaled step :a as JSON.
    {mod, ref} = storage(name)
    now = Capstan.Config.now(config(name))

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
    assert {:ok, %{"lang" => "python", "n" => 7}} = Capstan.await_result(name, job.id, 100)
  end

  test "results decode from either encoding" do
    %{name: name} = start_capstan!()

    {:ok, job} = Capstan.insert(name, StepOnly.new(%{}))

    {mod, ref} = storage(name)
    conf = config(name)
    spec = Capstan.Config.queue_spec(conf, :default)
    now = Capstan.Config.now(conf)

    {:ok, [claimed]} = mod.claim(ref, spec, 1, "n", 30_000, now)

    # A foreign worker acking with a JSON result.
    {:ok, _} = mod.ack(ref, claimed, {:succeeded, ~s(["done", 42])}, now)

    assert {:ok, ["done", 42]} = Capstan.await_result(name, job.id, 100)
  end

  test "codec discriminator is exact" do
    assert Capstan.Codec.decode(Capstan.Codec.encode(%{a: 1})) == %{a: 1}
    assert Capstan.Codec.decode(~s({"a": 1})) == %{"a" => 1}
    assert Capstan.Codec.decode("42") == 42
    assert Capstan.Codec.decode(nil) == nil
  end
end
