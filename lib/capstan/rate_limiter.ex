defmodule Capstan.RateLimiter do
  @moduledoc false

  # Sliding-window rate limiting over the `capstan_rate` table: the current
  # fixed window's count plus the previous window's count weighted by overlap.
  # Counters are keyed by (queue, resource, window_start) so future weighted
  # resources (e.g. "anthropic:tokens") share the same substrate.

  import Ecto.Query

  alias Capstan.RateWindow
  alias Oban.Repo

  @doc "How many more units may be admitted right now."
  def allowance(conf, queue, %{allowed: allowed, period: period} = limit, now \\ nil) do
    now = now || System.system_time(:second)
    resource = Map.get(limit, :resource, "")
    win = div(now, period) * period
    prev = win - period

    counts =
      conf
      |> Repo.all(
        from(r in RateWindow,
          where: r.queue == ^queue and r.resource == ^resource,
          where: r.window_start in ^[prev, win],
          select: {r.window_start, r.count}
        )
      )
      |> Map.new()

    curr_count = Map.get(counts, win, 0)
    prev_count = Map.get(counts, prev, 0)

    elapsed_frac = (now - win) / period
    used = curr_count + round(prev_count * (1.0 - elapsed_frac))

    max(allowed - used, 0)
  end

  @doc "Record `units` of consumption in the current window."
  def debit(conf, queue, limit, units, now \\ nil)

  def debit(_conf, _queue, _limit, units, _now) when units <= 0, do: :ok

  def debit(conf, queue, %{period: period} = limit, units, now) do
    now = now || System.system_time(:second)
    resource = Map.get(limit, :resource, "")
    win = div(now, period) * period

    row = %{queue: queue, resource: resource, window_start: win, count: units}

    Repo.insert_all(conf, RateWindow, [row],
      on_conflict: [inc: [count: units]],
      conflict_target: [:queue, :resource, :window_start]
    )

    prune(conf, queue, resource, win - period)

    :ok
  end

  defp prune(conf, queue, resource, before_window) do
    query =
      from(r in RateWindow,
        where: r.queue == ^queue and r.resource == ^resource,
        where: r.window_start < ^before_window
      )

    Repo.delete_all(conf, query)
  end
end
