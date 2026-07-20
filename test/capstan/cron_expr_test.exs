defmodule Capstan.CronExprTest do
  use ExUnit.Case, async: true

  alias Capstan.CronExpr

  test "parses steps, ranges, and lists" do
    {:ok, expr} = CronExpr.parse("*/15 8-10 1,15 * 1-5")

    assert MapSet.to_list(expr.minutes) == [0, 15, 30, 45]
    assert MapSet.to_list(expr.hours) == [8, 9, 10]
    assert MapSet.to_list(expr.days) == [1, 15]
    assert MapSet.to_list(expr.weekdays) == [1, 2, 3, 4, 5]
  end

  test "matches weekday business hours" do
    expr = CronExpr.parse!("0 8 * * 1-5")

    assert CronExpr.matches?(expr, ~U[2026-01-05 08:00:30Z])
    refute CronExpr.matches?(expr, ~U[2026-01-04 08:00:00Z])
    refute CronExpr.matches?(expr, ~U[2026-01-05 09:00:00Z])
  end

  test "nicknames and sunday-as-7" do
    assert CronExpr.parse!("@daily") |> CronExpr.matches?(~U[2026-01-05 00:00:00Z])
    assert CronExpr.parse!("0 0 * * 7") |> CronExpr.matches?(~U[2026-01-04 00:00:00Z])
  end

  test "rejects garbage" do
    assert {:error, _} = CronExpr.parse("61 * * * *")
    assert {:error, _} = CronExpr.parse("* * *")
    assert {:error, _} = CronExpr.parse("*/0 * * * *")
  end
end
