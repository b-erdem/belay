defmodule Capstan.EncryptionTest do
  use Capstan.Test.Case, async: false

  alias Capstan.Test.Secret

  test "inputs are ciphertext at rest and plaintext in the worker" do
    %{name: name} =
      start_capstan!(encryption: [key: {Capstan.Test.Keys, :test_key, []}])

    {:ok, job} = Capstan.insert(name, Secret.new(%{"ssn" => "123-45-6789"}))

    # At rest: only the envelope, never the payload.
    stored = job!(name, job.id)

    assert %{"$enc" => ciphertext} = stored.input
    refute ciphertext =~ "123-45"

    assert %{succeeded: 1} = Testing.drain(name, :default)

    # The worker saw plaintext; the row still holds ciphertext.
    assert {:ok, %{"ssn" => "123-45-6789"}} = Capstan.await_result(name, job.id, 100)
    assert %{"$enc" => _} = job!(name, job.id).input
  end

  test "an encrypted worker without a configured key fails at the call site" do
    %{name: name} = start_capstan!()

    assert_raise ArgumentError, ~r/no.*encryption.*configured/s, fn ->
      Capstan.insert(name, Secret.new(%{"x" => 1}))
    end
  end
end
