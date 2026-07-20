defmodule Capstan.UniqueTest do
  use Capstan.Test.Case, async: false

  alias Capstan.Test.Echo

  setup do
    {:ok, start_capstan!()}
  end

  test "an incomplete unique key blocks duplicates and frees on completion", %{name: name} do
    {:ok, first} = Capstan.insert(name, Echo.new(%{"n" => 1}, unique: "sync:42"))

    refute first.duplicate?

    {:ok, dup} = Capstan.insert(name, Echo.new(%{"n" => 2}, unique: "sync:42"))

    assert dup.duplicate?
    assert dup.id == first.id
    assert length(Capstan.list_jobs(name)) == 1

    assert %{succeeded: 1} = Testing.drain(name, :default)

    {:ok, second} = Capstan.insert(name, Echo.new(%{"n" => 3}, unique: "sync:42"))

    refute second.duplicate?
    assert second.id != first.id
  end

  test "windowed uniqueness dedupes across outcomes until the window turns", %{
    name: name,
    clock: clock
  } do
    unique = [key: "hourly-report", within: 3_600]

    {:ok, first} = Capstan.insert(name, Echo.new(%{}, unique: unique))

    assert %{succeeded: 1} = Testing.drain(name, :default)

    # Even completed, the same window still dedupes.
    {:ok, dup} = Capstan.insert(name, Echo.new(%{}, unique: unique))

    assert dup.duplicate?
    assert dup.id == first.id

    advance(clock, 3_601)

    {:ok, fresh} = Capstan.insert(name, Echo.new(%{}, unique: unique))

    refute fresh.duplicate?
    assert fresh.id != first.id
  end

  test "different keys never collide", %{name: name} do
    {:ok, a} = Capstan.insert(name, Echo.new(%{}, unique: "k:a"))
    {:ok, b} = Capstan.insert(name, Echo.new(%{}, unique: "k:b"))

    refute a.duplicate?
    refute b.duplicate?
    assert a.id != b.id
  end
end
