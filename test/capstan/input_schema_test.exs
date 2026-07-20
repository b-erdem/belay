defmodule Capstan.InputSchemaTest do
  use ExUnit.Case, async: true

  alias Capstan.InputSchema

  @schema [
    url: [type: :string, required: true],
    count: [type: :integer, default: 3],
    ratio: [type: :float],
    flag: [type: :boolean],
    opts: [type: :map],
    tags: [type: :list],
    mode: [type: {:enum, ["a", "b"]}]
  ]

  test "applies defaults, passes unknown keys through untouched" do
    input = InputSchema.validate!(W, @schema, %{"url" => "u", "extra" => 1})

    assert input == %{"url" => "u", "count" => 3, "extra" => 1}
  end

  test "integers satisfy :float but not vice versa" do
    assert %{"ratio" => 2} = InputSchema.validate!(W, @schema, %{"url" => "u", "ratio" => 2})

    assert_raise Capstan.InputError, ~r/count expected :integer/, fn ->
      InputSchema.validate!(W, @schema, %{"url" => "u", "count" => 2.5})
    end
  end

  test "collects every violation into one precise error" do
    error =
      assert_raise Capstan.InputError, fn ->
        InputSchema.validate!(W, @schema, %{"flag" => "yes", "mode" => "z"})
      end

    message = Exception.message(error)

    assert message =~ "url is required"
    assert message =~ "flag expected :boolean"
    assert message =~ ~s(mode expected {:enum, ["a", "b"]})
  end

  test "non-map input fails with a clear message" do
    assert_raise Capstan.InputError, ~r/must be a map/, fn ->
      InputSchema.validate!(W, @schema, "nope")
    end
  end

  test "nil schema is a no-op" do
    assert InputSchema.validate!(W, nil, %{"anything" => true}) == %{"anything" => true}
  end
end
