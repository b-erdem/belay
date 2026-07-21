defmodule Belay.EncryptionTest do
  use Belay.Test.Case, async: false

  alias Belay.Test.Secret

  test "inputs are ciphertext at rest and plaintext in the worker" do
    %{name: name} =
      start_belay!(encryption: [key: {Belay.Test.Keys, :test_key, []}])

    {:ok, job} = Belay.insert(name, Secret.new(%{"ssn" => "123-45-6789"}))

    # At rest: only the envelope, never the payload.
    stored = job!(name, job.id)

    assert %{"$enc" => ciphertext} = stored.input
    refute ciphertext =~ "123-45"

    assert %{succeeded: 1} = Testing.drain(name, :default)

    # The worker saw plaintext; the row still holds ciphertext.
    assert {:ok, %{"ssn" => "123-45-6789"}} = Belay.await_result(name, job.id, 100)
    assert %{"$enc" => _} = job!(name, job.id).input
  end

  test "an encrypted worker without a configured key fails at the call site" do
    %{name: name} = start_belay!()

    assert_raise ArgumentError, ~r/no.*encryption.*configured/s, fn ->
      Belay.insert(name, Secret.new(%{"x" => 1}))
    end
  end
end
