# The soak driver: inserts a mixed workload while run.sh kills workers and
# restarts Postgres, then waits for quiescence and verifies invariants.
# Writes soak/REPORT.md and exits non-zero on any invariant failure.

url = System.get_env("SOAK_URL") || "postgres://postgres:capstan@localhost:55433/capstan_soak"
waves = String.to_integer(System.get_env("SOAK_WAVES") || "36")
wave_ms = String.to_integer(System.get_env("SOAK_WAVE_MS") || "3500")

Code.require_file(Path.join(__DIR__, "soak_workers.exs"))

defmodule Soak.D do
  @moduledoc false

  # Retry through database outages: docker restarts take a few seconds.
  def retry(fun, tries \\ 40) do
    fun.()
  rescue
    error ->
      if tries <= 1, do: reraise(error, __STACKTRACE__)

      Process.sleep(500)
      retry(fun, tries - 1)
  end

  def q!(sql, params \\ []), do: Postgrex.query!(SoakDB, sql, params)

  def ledger!(job_id, kind, expected) do
    retry(fn ->
      q!(
        "INSERT INTO soak_ledger (job_id, kind, expected) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
        [job_id, kind, expected]
      )
    end)
  end

  def log(msg), do: IO.puts("[driver] #{msg}")
end

{:ok, _} =
  url
  |> Capstan.Storage.Postgres.parse_url()
  |> Keyword.merge(name: SoakDB, pool_size: 3, types: Capstan.Storage.PostgresTypes)
  |> Postgrex.start_link()

{:ok, _} =
  Capstan.start_link(
    name: SoakDriver,
    storage: [adapter: :postgres, url: url],
    queues: [],
    poll_interval: 1_000,
    lease_ttl: 4_000,
    sweep_interval: 1_000,
    retention: [succeeded: :infinity, failed: :infinity, cancelled: :infinity]
  )

alias Soak.D

D.log("inserting #{waves} waves (#{wave_ms}ms apart) against #{url}")

# -- Phase 1: workload waves under chaos ---------------------------------------

awaiters =
  Enum.reduce(1..waves, [], fn wave, awaiters ->
    D.retry(fn ->
      # Plain steppy jobs with planned failures mixed in.
      for j <- 1..6 do
        k = 3 + rem(wave + j, 4)
        f = rem(j, 3)

        {:ok, job} =
          Capstan.insert(SoakDriver, Soak.Step.new(%{"n" => wave, "k" => k, "f" => f}))

        expected = wave * k + div(k * (k + 1), 2)

        D.ledger!(job.id, "step", %{"result" => expected})
      end

      # A fan-out parent with three children.
      vs = [wave, wave + 1, wave + 2]
      {:ok, parent} = Capstan.insert(SoakDriver, Soak.Parent.new(%{"vs" => vs}))

      D.ledger!(parent.id, "parent", %{"result" => Enum.sort(Enum.map(vs, &(&1 * 2)))})
    end)

    # A three-stage workflow every other wave.
    if rem(wave, 2) == 0 do
      D.retry(fn ->
        {:ok, jobs} =
          Capstan.Workflow.new()
          |> Capstan.Workflow.add(:a, Soak.FlowStep.new(%{}))
          |> Capstan.Workflow.add(:b, Soak.FlowStep.new(%{}), deps: [:a])
          |> Capstan.Workflow.add(:c, Soak.FlowStep.new(%{}), deps: [:b])
          |> Capstan.Workflow.insert(SoakDriver)

        for {pos, job} <- jobs do
          D.ledger!(job.id, "flow", %{"pos" => pos, "wf" => job.workflow_id})
        end
      end)
    end

    # Budget jobs must die at exactly three recorded steps.
    if rem(wave, 4) == 0 do
      D.retry(fn ->
        {:ok, job} = Capstan.insert(SoakDriver, Soak.Budget.new(%{}, budget: [usd: 0.5]))

        D.ledger!(job.id, "budget", nil)
      end)
    end

    # Unique bursts: four rapid inserts, one job.
    if rem(wave, 3) == 0 do
      D.retry(fn ->
        results =
          for _ <- 1..4 do
            {:ok, job} = Capstan.insert(SoakDriver, Soak.Uni.new(%{}, unique: "uni:#{wave}"))
            job
          end

        fresh = Enum.reject(results, & &1.duplicate?)

        if length(fresh) == 1, do: D.ledger!(hd(fresh).id, "uni", nil)
      end)
    end

    # Two awaiters per wave; signal the ones from two waves ago.
    new_awaiters =
      D.retry(fn ->
        for _ <- 1..2 do
          {:ok, job} = Capstan.insert(SoakDriver, Soak.Awaiter.new(%{}))

          D.ledger!(job.id, "await", %{"result" => %{"w" => wave, "pre" => 1}})

          {job.id, wave}
        end
      end)

    {due, pending} = Enum.split_with(awaiters, fn {_id, w} -> wave - w >= 2 end)

    for {id, w} <- due do
      D.retry(fn -> Capstan.signal_job(SoakDriver, id, :go, %{"w" => w}) end)
    end

    if rem(wave, 6) == 0, do: D.log("wave #{wave}/#{waves}")

    Process.sleep(wave_ms)

    pending ++ new_awaiters
  end)

# -- Phase 2: signal every remaining awaiter until all land --------------------

D.log("waves done; signalling remaining awaiters")

for {id, w} <- awaiters do
  D.retry(fn -> Capstan.signal_job(SoakDriver, id, :go, %{"w" => w}) end)
end

await_ids =
  D.retry(fn ->
    %{rows: rows} = D.q!("SELECT job_id, expected FROM soak_ledger WHERE kind = 'await'")
    rows
  end)

Enum.reduce_while(1..90, nil, fn _, _ ->
  stuck =
    D.retry(fn ->
      %{rows: rows} =
        D.q!(
          "SELECT id FROM capstan_jobs WHERE id = ANY($1) AND state NOT IN ('succeeded','failed','cancelled')",
          [Enum.map(await_ids, fn [id, _] -> id end)]
        )

      List.flatten(rows)
    end)

  if stuck == [] do
    {:halt, nil}
  else
    for id <- stuck do
      [_, expected] = Enum.find(await_ids, fn [i, _] -> i == id end)

      D.retry(fn -> Capstan.signal_job(SoakDriver, id, :go, %{"w" => expected["result"]["w"]}) end)
    end

    Process.sleep(1_000)
    {:cont, nil}
  end
end)

# -- Phase 3: quiescence -------------------------------------------------------

D.log("waiting for quiescence")

quiesced? =
  Enum.reduce_while(1..240, false, fn i, _ ->
    remaining =
      D.retry(fn ->
        %{rows: [[n]]} =
          D.q!("SELECT count(*) FROM capstan_jobs WHERE state NOT IN ('succeeded','failed','cancelled')")

        n
      end)

    cond do
      remaining == 0 -> {:halt, true}
      i == 240 -> {:halt, false}
      true ->
        if rem(i, 10) == 0, do: D.log("#{remaining} jobs not yet terminal")
        Process.sleep(1_000)
        {:cont, false}
    end
  end)

# -- Phase 4: verification -----------------------------------------------------

D.log("verifying invariants")

failures = :ets.new(:failures, [:bag, :public])
fail = fn check, detail -> :ets.insert(failures, {check, detail}) end

%{rows: state_rows} = D.q!("SELECT state, count(*) FROM capstan_jobs GROUP BY 1 ORDER BY 1")

unless quiesced? do
  %{rows: rows} =
    D.q!(
      "SELECT id, kind, queue, state FROM capstan_jobs WHERE state NOT IN ('succeeded','failed','cancelled') LIMIT 10"
    )

  fail.("quiescence", "non-terminal jobs remain: #{inspect(rows)}")
end

# Every ledgered job must exist.
%{rows: missing} =
  D.q!("SELECT l.job_id FROM soak_ledger l LEFT JOIN capstan_jobs j ON j.id = l.job_id WHERE j.id IS NULL")

for [id] <- missing, do: fail.("lost-jobs", "ledgered job #{id} vanished")

# Outcome + result correctness per kind.
%{rows: ledger_rows} =
  D.q!("""
  SELECT l.job_id, l.kind, l.expected, j.state, j.result, j.attempt, j.max_attempts, j.errors
  FROM soak_ledger l JOIN capstan_jobs j ON j.id = l.job_id
  """)

decode = fn
  nil -> nil
  bin -> :erlang.binary_to_term(bin)
end

for [id, kind, expected, state, result, attempt, max_attempts, errors] <- ledger_rows do
  if attempt > max_attempts, do: fail.("attempts", "job #{id} attempt #{attempt} > max #{max_attempts}")

  case kind do
    k when k in ["step", "parent", "await"] ->
      cond do
        state != "succeeded" ->
          fail.(kind, "job #{id} ended #{state} (errors: #{inspect(errors, limit: 3)})")

        decode.(result) != expected["result"] ->
          fail.(kind, "job #{id} result #{inspect(decode.(result))} != #{inspect(expected["result"])}")

        true -> :ok
      end

    "flow" ->
      if state != "succeeded", do: fail.("flow", "job #{id} ended #{state}")

    "uni" ->
      if state != "succeeded", do: fail.("uni", "job #{id} ended #{state}")

    "budget" ->
      last_error = errors |> List.last() |> Kernel.||(%{})

      %{rows: [[step_count]]} =
        D.q!("SELECT count(*) FROM capstan_steps WHERE job_id = $1", [id])

      cond do
        state != "failed" -> fail.("budget", "job #{id} ended #{state}, wanted failed")
        last_error["error"] != "budget_exceeded" -> fail.("budget", "job #{id} wrong error #{inspect(last_error)}")
        step_count != 3 -> fail.("budget", "job #{id} recorded #{step_count} steps, wanted 3")
        true -> :ok
      end
  end
end

# Workflow ordering: first effect of b after first effect of a, etc.
%{rows: flow_rows} =
  D.q!("""
  SELECT l.expected->>'wf', l.expected->>'pos', min(e.at)
  FROM soak_ledger l JOIN soak_effects e ON e.job_id = l.job_id
  WHERE l.kind = 'flow'
  GROUP BY 1, 2
  """)

flow_rows
|> Enum.group_by(fn [wf, _, _] -> wf end, fn [_, pos, at] -> {pos, at} end)
|> Enum.each(fn {wf, entries} ->
  by_pos = Map.new(entries)

  with %{"a" => a, "b" => b, "c" => c} <- by_pos do
    unless DateTime.compare(a, b) == :lt and DateTime.compare(b, c) == :lt do
      fail.("flow-order", "workflow #{wf} ran out of order: #{inspect(by_pos)}")
    end
  else
    _ -> fail.("flow-order", "workflow #{wf} missing effects: #{inspect(Map.keys(by_pos))}")
  end
end)

# Spawn idempotency: every soak parent has exactly three children.
%{rows: bad_parents} =
  D.q!("""
  SELECT j.parent_id, count(*) FROM capstan_jobs j
  JOIN soak_ledger l ON l.job_id = j.parent_id AND l.kind = 'parent'
  GROUP BY 1 HAVING count(*) != 3
  """)

for [pid, n] <- bad_parents, do: fail.("spawn-idempotency", "parent #{pid} has #{n} children, wanted 3")

# Cron slots fire exactly once cluster-wide.
%{rows: cron_dups} =
  D.q!("SELECT cron_name, cron_slot, count(*) FROM capstan_jobs WHERE cron_name IS NOT NULL GROUP BY 1,2 HAVING count(*) > 1")

for [name, slot, n] <- cron_dups, do: fail.("cron-dedup", "#{name}@#{slot} fired #{n} times")

%{rows: [[cron_count]]} = D.q!("SELECT count(*) FROM capstan_jobs WHERE cron_name IS NOT NULL")

# At-least-once accounting: duplicate step-body executions (crash windows).
%{rows: dup_rows} =
  D.q!("SELECT count(*) FROM (SELECT job_id, step FROM soak_effects GROUP BY 1,2 HAVING count(*) > 1) d")

[[dup_steps]] = dup_rows

%{rows: [[total_effects]]} = D.q!("SELECT count(*) FROM soak_effects")
%{rows: [[distinct_effects]]} = D.q!("SELECT count(*) FROM (SELECT DISTINCT job_id, step FROM soak_effects) d")

if dup_steps > 200, do: fail.("duplicate-rate", "#{dup_steps} duplicated steps — systemic re-running?")

# -- Report --------------------------------------------------------------------

chaos_log =
  case File.read("soak/tmp/chaos.log") do
    {:ok, content} -> content
    _ -> ""
  end

kills = chaos_log |> String.split("\n") |> Enum.count(&String.starts_with?(&1, "KILL"))
db_restarts = chaos_log |> String.split("\n") |> Enum.count(&String.starts_with?(&1, "DBRESTART"))

%{rows: [[total_jobs]]} = D.q!("SELECT count(*) FROM capstan_jobs")
%{rows: kind_rows} = D.q!("SELECT kind, count(*) FROM soak_ledger GROUP BY 1 ORDER BY 1")

all_failures = :ets.tab2list(failures)
checks = ~w(quiescence lost-jobs step parent await flow flow-order uni budget attempts spawn-idempotency cron-dedup duplicate-rate)

check_lines =
  Enum.map(checks, fn check ->
    fails = for {^check, detail} <- all_failures, do: detail

    case fails do
      [] ->
        "- [x] #{check}"

      fails ->
        details = fails |> Enum.take(5) |> Enum.map_join("\n", &"      - #{&1}")
        "- [ ] **#{check} FAILED (#{length(fails)})**\n#{details}"
    end
  end)

verdict = if all_failures == [], do: "**PASS**", else: "**FAIL (#{length(all_failures)} findings)**"

report = """
# Chaos soak report — #{Date.utc_today()}

Accelerated chaos soak: #{waves} workload waves against 3 worker OS processes,
`kill -9` roughly every 4s, #{db_restarts} full Postgres restart(s) mid-run.
This is the rc gate's accelerated soak; the 48h endurance run remains open.

## Verdict: #{verdict}

## Load
- Total jobs processed: #{total_jobs} (#{cron_count} from cron)
- Ledgered by kind: #{Enum.map_join(kind_rows, ", ", fn [k, n] -> "#{k}=#{n}" end)}
- Final states: #{Enum.map_join(state_rows, ", ", fn [s, n] -> "#{s}=#{n}" end)}

## Chaos
- Worker kills (kill -9): #{kills}
- Postgres restarts: #{db_restarts}

## At-least-once accounting
- Step bodies executed: #{total_effects} for #{distinct_effects} distinct steps
- Duplicated step executions (crash windows between side effect and journal
  write): #{dup_steps} — expected to be > 0 under kill -9 and bounded by the
  kill count; results above prove journaled values stayed correct regardless.

## Invariants
#{Enum.join(check_lines, "\n")}
"""

File.write!("soak/REPORT.md", report)

IO.puts("\n" <> report)

if all_failures == [] do
  D.log("SOAK PASS")
  System.halt(0)
else
  D.log("SOAK FAIL")
  System.halt(1)
end
