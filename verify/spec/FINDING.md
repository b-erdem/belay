# No violation found: all four durable-execution properties hold

## Verdict

**No violation found.** TLC exhaustively model-checked `attest_out/Spec.tla`
(9,026,179 states generated, 1,525,104 distinct states, search depth 34,
0 states left on queue — a complete, not depth-bounded, search) and found
no counterexample for any of the four properties:

- `NoExecutionPastBudget`
- `CancelRequestSurvivesUntilTerminal`
- `StepsJournaledAtMostOnce`
- `TerminalStatesAreFinal`

There is no trace to reproduce and nothing to root-cause. This report
instead documents *why* the spec models each property as holding, tying
every guard back to the concrete source lines it abstracts, so the
"no violation" result is falsifiable rather than a black box.

## Reproduction

Not applicable — no counterexample exists. To re-run the check:

```
cd attest_out && tlc Spec.tla
```

(or via whatever TLC invocation the harness used; the tail of the raw
output confirms `Model checking completed. No error has been found.`)

## What happens

No counterexample trace exists (0 steps). TLC's complete state-space
search (fingerprint collision probability ~1.6e-7, i.e. not a bound
that leaves meaningful unexplored territory) confirms that from every
reachable combination of `state`, `attempt`, `journaled`, `spent`,
`cancelReq`, `live`, `execCount`, and `clock` across the 4-job fixture,
each of the four properties continues to hold under every enabled
action (`Insert`, `Claim`/`Drain`, `Exec`, `Crash`, `Reclaim`,
`CancelRunning`/`CancelParked`, `Succeed`, `FailBudget`, `HonorCancel`,
`Raise`, `Settle`, `Advance`).

## Root cause

Not applicable (no bug). For traceability, here is how each property's
guard in the spec maps to the actual guard in the source it models:

1. **`NoExecutionPastBudget`** (Spec.tla:313-314) — models the
   pre-flight budget check that `runner.ex:step/4` performs *before*
   running a step body:
   - `runner.ex:182` `fresh = check_cancel!(config, job)` (re-reads the
     row for fresh durable state before the budget check)
   - `runner.ex:188` `check_budget!(job, fresh)` — pre-flight, against
     durably-committed spend, before `fun.()` runs
   - `runner.ex:202` `check_budget!(job, spent)` — post-write check,
     after `put_step`, catches the crossing step itself
   - `runner.ex:184-187` comment documents the historical bug this
     guards against: "Found by the 7h endurance soak: 6 of 1750 budget
     jobs ran a 4th step" — i.e. this is the regression test target,
     and per the TLC result the spec's `Exec` action (Spec.tla:175-185,
     guarded by `spent[j] <= Budget[j]` at line 180) cannot re-derive
     that bug in this model.
   - `check_budget!` itself: `runner.ex:414-424`, comparing
     `spent.spent_usd_micros > job.budget_usd_micros`.

2. **`StepsJournaledAtMostOnce`** (Spec.tla:318-319) — models the
   memoized-replay path at `runner.ex:174-176`: `storage.get_step`
   returning `{:ok, bin}` short-circuits before `fun.()` is ever
   called, so a journaled step's body cannot execute twice. The PK
   `(job_id, name)` with `ON CONFLICT DO NOTHING` referenced in
   Spec.tla:13-14 is the durable-storage half of this guarantee (not
   directly visible in `runner.ex`; enforced at the storage-adapter
   layer, e.g. `lib/capstan/storage/memory.ex` / `postgres.ex`
   `put_step`).

3. **`CancelRequestSurvivesUntilTerminal`** (Spec.tla:324-326) — models
   that no non-terminal transition clears `cancel_requested`.
   Confirmed directly in `lib/capstan/storage.ex`:
   - `apply_outcome/3` (storage.ex:182-214) pattern-matches on every
     ack outcome (`:succeeded`, `:retry`, `:failed`, `:cancelled`,
     `:snooze`, `:await`) and none of the six branches touch
     `cancel_requested`.
   - `clear_execution/1` (storage.ex:217-219) only resets
     `lease_until`/`leased_by` — it does not touch `cancel_requested`
     either, despite the name suggesting a broader reset.
   - This is exactly what the spec's `Reclaim` (Spec.tla:204-209) and
     `Raise` (Spec.tla:252-257) actions encode: `cancelReq` is in the
     `UNCHANGED` tuple for every transition except `CancelRunning`
     (sets it true) and the terminal ack actions, none of which clear
     it.

4. **`TerminalStatesAreFinal`** (Spec.tla:330-331) — every modelled
   action's guard requires `state[j] \notin TerminalStates` (e.g.
   `Exec` requires `state[j] = "running"`, `Claim` requires
   `state[j] = "ready"`); the only action enabled once a job is
   terminal is the explicit `Terminating` self-loop
   (Spec.tla:272-274). No source-level "retry after terminal" or
   "un-cancel" path is modelled or exists in the reviewed code paths
   (`runner.ex`, `storage.ex`).

## Severity

**Not applicable — no bug.** The properties as specified are consistent
with the reviewed implementation.

Caveat worth stating honestly: this is a *model* check, not a proof
about the Elixir source. The spec's own header (Spec.tla:35-48) lists
real abstractions that narrow what "no violation" covers:
- Real time (lease TTLs, backoff delays) is abstracted to a 3-value
  cosmetic clock (`clock \in 0..MaxClock`, Spec.tla:59,196-198) and a
  boolean `live` flag — actual lease-expiry races, clock skew, and
  concurrent-node reclaim contention are not modelled.
- `drain` and `claim` collapse to one `ClaimJob` transition
  (Spec.tla:159-169); the model does not distinguish a worker process
  crash mid-step-body from a crash between `put_step` and ack — it
  only distinguishes crash-before-journal (no `journaled'` update)
  from crash-after-journal (via separate `Exec`/`Crash` interleavings).
- Signals, `:awaiting`/paused states, dynamic children, rate limiting,
  and encryption are explicitly out of scope (Spec.tla:44-45).
- The fixture is 4 jobs with fixed programs (Spec.tla:56-79), not an
  unbounded number of concurrently racing jobs/nodes.

So: within the modelled abstraction, TLC's exhaustive search rules out
violations of these four properties. It does not certify the absence
of bugs in code paths the abstraction elides (real lease timing,
multi-node races, `:awaiting`/signals, adapter-specific SQL semantics
in `postgres.ex` vs. the modelled `memory.ex`/`storage.ex` behavior).

## Mitigations

Not applicable — no defect to mitigate. If tightening confidence
further is desired, the highest-leverage next step is broadening the
abstraction rather than re-running this spec: model real lease
deadlines/backoff numerically instead of the boolean `live` flag, and
add a second concurrent claimant per job to exercise multi-node
reclaim races that the current single-attempt-at-a-time model cannot
represent.

## Related context

- `runner.ex:184-187` documents the source incident the
  `NoExecutionPastBudget` property was written to regression-test
  ("Found by the 7h endurance soak: 6 of 1750 budget jobs ran a 4th
  step"), corroborated by the repo's own recent commit history
  (`git log`: "Fix budget crash window found by the 7h endurance soak",
  "Archive endurance FAIL report + 30-min revalidation PASS") — i.e.
  this property models a previously-real, now-fixed bug, and TLC
  confirms the fix holds in the model.
- `verify/traces.exs` scenario `budget_crash_window` (lines 273-288)
  is the concrete-execution analogue of the same regression: it drives
  a real crash between journaling the crossing step (`b3`) and acking
  the budget failure, then asserts the replayed attempt is refused with
  no fourth `exec` event.
- SCHEMA.md sec 7 (cited at Spec.tla:23) and sec 8.6 (cited at
  Spec.tla:211) are the documentation source for the cancel-survives
  and cooperative-cancel-while-running semantics respectively; not
  independently re-verified in this pass beyond the source citations
  above.
