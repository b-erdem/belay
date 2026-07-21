---------------------------- MODULE PreFixCancelClear ----------------------------
(*****************************************************************************)
(* A TLA+ model of the Capstan job engine's DURABLE execution mechanics,    *)
(* drawn from the reference storage implementation                          *)
(* (lib/capstan/storage/memory.ex + lib/capstan/storage.ex Logic) and the   *)
(* runner (lib/capstan/runner.ex), grounded in verify/traces.exs.           *)
(*                                                                          *)
(* The point of the model is the four durable safety mechanisms Capstan     *)
(* relies on, all of which must survive a kill -9 (crash) + lease-expiry     *)
(* reclaim:                                                                  *)
(*                                                                          *)
(*   * Journaled step results replay without re-executing that step name.    *)
(*     Exec+journal is atomic in this abstraction; body-before-journal       *)
(*     duplicate execution is outside the modeled property.                 *)
(*   * The BUDGET PRE-FLIGHT check: a step body must NOT run when the        *)
(*     durable spend already exceeds the cap (runner.ex:188 check_budget!    *)
(*     BEFORE fun.()).  This is the fix the 7h endurance soak forced         *)
(*     (comment at runner.ex:185-188: "6 of 1750 budget jobs ran a 4th      *)
(*     step").                                                              *)
(*   * cancel_requested is NEVER cleared by a transition: apply_outcome /    *)
(*     clear_execution (storage.ex:182-219) leave it untouched, so a         *)
(*     cooperative cancel survives crash + reclaim + retry until a step      *)
(*     boundary honors it or the job goes terminal (SCHEMA.md sec 7).        *)
(*   * Terminal states (succeeded/failed/cancelled) are absorbing for the    *)
(*     engine actions modelled here (claim/exec/ack/reclaim/settle/cancel).  *)
(*                                                                          *)
(* Action names mirror the trace vocabulary in verify/traces.exs            *)
(* (event=exec ; action.kind in insert|drain|claim|crash|reclaim|cancel|    *)
(* advance).  The engine-internal ack transitions a `drain` performs after   *)
(* claiming (succeed / budget-fail / honor-cancel / worker-raise) have no    *)
(* trace `kind` of their own -- they are observed only through the `state`   *)
(* snapshot that follows -- so they are modelled as their own actions:       *)
(* Succeed, FailBudget, HonorCancel, Raise, Settle.                          *)
(*                                                                          *)
(* ABSTRACTIONS (see structured report):                                    *)
(*   * `drain` and `claim` both reduce to the same ready->running claim       *)
(*     transition; a real drain then runs the worker, which this model       *)
(*     decomposes into Exec* + one ack action.                              *)
(*   * The wall clock, lease TTLs and retry backoff are abstracted: `ready`   *)
(*     is always claimable and Reclaim is gated on a crashed (dead) lease,    *)
(*     not on a numeric deadline.  `Advance` is a cosmetic bounded clock.     *)
(*   * Per-step money is scaled to small integers (0.2 USD -> 2, 0.5 cap ->  *)
(*     5) preserving the crossing arithmetic.                               *)
(*   * Signals, awaiting/paused states, dynamic children, rate limiting and  *)
(*     encryption are out of scope for these four properties.               *)
(*   * A specific 4-job fixture stands in for the trace workers: a 5-step    *)
(*     budget job, an always-raising job, its held dependent, and a 2-step   *)
(*     job used for happy/retry/cancel behaviours.                          *)
(*****************************************************************************)

EXTENDS Integers, Sequences, FiniteSets

--------------------------------------------------------------------------
(* --- Fixture: the concrete jobs the traces exercise --------------------- *)

Jobs == {1, 2, 3, 4}

BIG == 100          \* an "uncapped" budget; larger than any reachable spend
MaxClock == 2

\* Job 1: the FiveStepBudget worker (verify/traces.exs V.FiveStepBudget),
\*        0.2 USD/step against a 0.5 cap -> scaled to cost 2, budget 5.
\*        Program kept at 4 steps so a step PAST the crossing (b4) exists to
\*        test that it never executes.
\* Job 2: the AlwaysFails worker (max_attempts 1); no steps, raises.
\* Job 3: the Trivial dependent of job 2 in a workflow; starts `held`.
\* Job 4: the TwoStep / FlakyBetweenSteps / SelfCancel / Trivial worker.
Program == [ j \in Jobs |->
              CASE j = 1 -> << "b1", "b2", "b3", "b4" >>
                [] j = 2 -> << >>
                [] j = 3 -> << "t1" >>
                [] OTHER -> << "s1", "s2" >> ]

Cost        == [ j \in Jobs |-> IF j = 1 THEN 2 ELSE 0 ]
MaxCost     == [ j \in Jobs |-> IF j = 1 THEN 2 ELSE 0 ]
Budget      == [ j \in Jobs |-> IF j = 1 THEN 5 ELSE BIG ]
MaxAttempts == [ j \in Jobs |-> IF j = 2 THEN 1 ELSE 2 ]
Deps        == [ j \in Jobs |-> IF j = 3 THEN {2} ELSE {} ]
WfIgnore    == [ j \in Jobs |-> {} ]

StepNames == { "b1", "b2", "b3", "b4", "t1", "s1", "s2" }

TerminalStates == { "succeeded", "failed", "cancelled" }
AllStates ==
  { "absent", "ready", "running", "held" } \cup TerminalStates

\* held iff it has unmet workflow deps at insert time (SCHEMA sec 5.1).
InitState(j) == IF Deps[j] # {} THEN "held" ELSE "ready"

--------------------------------------------------------------------------
VARIABLES
  state,       \* [Jobs -> AllStates]
  attempt,     \* [Jobs -> Nat]        incremented by claim only
  journaled,   \* [Jobs -> SUBSET StepNames]  durable step rows
  spent,       \* [Jobs -> Nat]        durable accumulated usd (scaled)
  cancelReq,   \* [Jobs -> BOOLEAN]    cooperative-cancel flag (durable)
  live,        \* [Jobs -> BOOLEAN]    is the running attempt's lease alive
  execCount,   \* [Jobs -> [StepNames -> Nat]]  times a step BODY ran
  clock        \* cosmetic bounded clock advanced by Advance

vars == << state, attempt, journaled, spent, cancelReq, live, execCount, clock >>

--------------------------------------------------------------------------
(* --- Helpers ------------------------------------------------------------ *)

\* [helper] terminal-state test (used by several actions and properties)
Terminal(s) == s \in TerminalStates

\* Index of the first program step of j not yet journaled (0 if all done).
\* Steps journal in order, so journaled[j] is always a prefix set.
NextIdx(j) ==
  IF \E k \in 1..Len(Program[j]) : Program[j][k] \notin journaled[j]
  THEN CHOOSE k \in 1..Len(Program[j]) :
          /\ Program[j][k] \notin journaled[j]
          /\ \A m \in 1..(k-1) : Program[j][m] \in journaled[j]
  ELSE 0

\* [helper] every program step is journaled (worker would return {:ok, _})
AllJournaled(j) == NextIdx(j) = 0

\* [helper] workflow settlement predicates (storage.ex Logic.settle,
\* doomed?/satisfied?).  Doomed jobs count as cancelled for dependents;
\* here chains are one deep so a single pass suffices.
Doomed(j) ==
  \E d \in Deps[j] :
     \/ (state[d] = "failed"    /\ "failed"    \notin WfIgnore[j])
     \/ (state[d] = "cancelled" /\ "cancelled" \notin WfIgnore[j])

Satisfied(j) ==
  \A d \in Deps[j] :
     \/ state[d] = "succeeded"
     \/ (state[d] = "failed"    /\ "failed"    \in WfIgnore[j])
     \/ (state[d] = "cancelled" /\ "cancelled" \in WfIgnore[j])

--------------------------------------------------------------------------
(* --- Init --------------------------------------------------------------- *)

Init ==
  /\ state     = [ j \in Jobs |-> "absent" ]
  /\ attempt   = [ j \in Jobs |-> 0 ]
  /\ journaled = [ j \in Jobs |-> {} ]
  /\ spent     = [ j \in Jobs |-> 0 ]
  /\ cancelReq = [ j \in Jobs |-> FALSE ]
  /\ live      = [ j \in Jobs |-> FALSE ]
  /\ execCount = [ j \in Jobs |-> [ n \in StepNames |-> 0 ] ]
  /\ clock     = 0

--------------------------------------------------------------------------
(* --- Actions ------------------------------------------------------------ *)

\* insert (SCHEMA sec 5.1): a new row lands `ready`, or `held` if it carries
\* unmet workflow deps.
Insert(j) ==
  /\ state[j] = "absent"
  /\ state' = [state EXCEPT ![j] = InitState(j)]
  /\ UNCHANGED << attempt, journaled, spent, cancelReq, live, execCount, clock >>

\* claim (SCHEMA sec 6): ready -> running, attempt := attempt + 1, take lease.
ClaimJob(j) ==
  /\ state[j] = "ready"
  /\ state'   = [state   EXCEPT ![j] = "running"]
  /\ attempt' = [attempt EXCEPT ![j] = @ + 1]
  /\ live'    = [live    EXCEPT ![j] = TRUE]
  /\ UNCHANGED << journaled, spent, cancelReq, execCount, clock >>

\* Both trace kinds `claim` and `drain` perform this same claim; a real drain
\* additionally runs the worker, which the model decomposes into Exec* + ack.
Claim(j) == ClaimJob(j)
Drain(j) == ClaimJob(j)

\* exec (runner.ex step/4): run + journal the next un-journaled step.
\* Guards encode the two runner.ex pre-flight checks that run BEFORE fun.():
\*   check_cancel!  -> ~cancelReq[j]        (honor a pending cancel instead)
\*   check_budget!  -> spent[j] <= Budget[j] (refuse once durably over cap)
Exec(j) ==
  /\ state[j] = "running"
  /\ live[j]
  /\ ~cancelReq[j]
  /\ NextIdx(j) # 0
  /\ spent[j] <= Budget[j]
  /\ LET nm == Program[j][NextIdx(j)] IN
       /\ journaled' = [journaled EXCEPT ![j] = @ \cup {nm}]
       /\ spent'     = [spent     EXCEPT ![j] = @ + Cost[j]]
       /\ execCount' = [execCount EXCEPT ![j][nm] = @ + 1]
  /\ UNCHANGED << state, attempt, cancelReq, live, clock >>

\* crash (kill -9): the worker dies; the lease is abandoned and will expire.
Crash(j) ==
  /\ state[j] = "running"
  /\ live[j]
  /\ live' = [live EXCEPT ![j] = FALSE]
  /\ UNCHANGED << state, attempt, journaled, spent, cancelReq, execCount, clock >>

\* advance: cosmetic clock tick (leases/backoff are abstracted).
Advance ==
  /\ clock < MaxClock
  /\ clock' = clock + 1
  /\ UNCHANGED << state, attempt, journaled, spent, cancelReq, live, execCount >>

\* reclaim (SCHEMA sec 6): any node reclaims an expired (crashed) running
\* lease -> retry (ready) if attempt < max, else failed.  cancel_requested,
\* journaled steps and spend are ALL preserved (apply_outcome never touches
\* them) -- this is what cancel_across_crash / budget_crash_window test.
Reclaim(j) ==
  /\ state[j] = "running"
  /\ ~live[j]
  /\ state' = [state EXCEPT ![j] =
                 IF attempt[j] >= MaxAttempts[j] THEN "failed" ELSE "ready"]
  /\ cancelReq' = [cancelReq EXCEPT ![j] = FALSE]
  /\ UNCHANGED << attempt, journaled, spent, live, execCount, clock >>

\* cancel while running (SCHEMA sec 8.6): cooperative -- only set the flag.
CancelRunning(j) ==
  /\ state[j] = "running"
  /\ cancelReq' = [cancelReq EXCEPT ![j] = TRUE]
  /\ UNCHANGED << state, attempt, journaled, spent, live, execCount, clock >>

\* cancel while parked (ready/held): immediate terminal cancel.
CancelParked(j) ==
  /\ state[j] \in { "ready", "held" }
  /\ state' = [state EXCEPT ![j] = "cancelled"]
  /\ UNCHANGED << attempt, journaled, spent, cancelReq, live, execCount, clock >>

\* ack {:succeeded}: all steps journaled, within budget, no pending cancel.
Succeed(j) ==
  /\ state[j] = "running"
  /\ live[j]
  /\ ~cancelReq[j]
  /\ AllJournaled(j)
  /\ spent[j] <= Budget[j]
  /\ state' = [state EXCEPT ![j] = "succeeded"]
  /\ UNCHANGED << attempt, journaled, spent, cancelReq, live, execCount, clock >>

\* ack {:failed, budget_exceeded}: the recorded spend crossed the cap
\* (runner.ex check_budget! post-write, or the replay pre-flight refusal).
FailBudget(j) ==
  /\ state[j] = "running"
  /\ live[j]
  /\ spent[j] > Budget[j]
  /\ state' = [state EXCEPT ![j] = "failed"]
  /\ UNCHANGED << attempt, journaled, spent, cancelReq, live, execCount, clock >>

\* ack {:cancelled}: the step-boundary honor of a pending cancel_requested.
HonorCancel(j) ==
  /\ state[j] = "running"
  /\ live[j]
  /\ cancelReq[j]
  /\ state' = [state EXCEPT ![j] = "cancelled"]
  /\ UNCHANGED << attempt, journaled, spent, cancelReq, live, execCount, clock >>

\* A worker raise (map_return error -> retry_or_fail): retry if attempts
\* remain, else fail.  Models FlakyBetweenSteps (retry) and AlwaysFails (fail).
Raise(j) ==
  /\ state[j] = "running"
  /\ live[j]
  /\ state' = [state EXCEPT ![j] =
                 IF attempt[j] >= MaxAttempts[j] THEN "failed" ELSE "ready"]
  /\ UNCHANGED << attempt, journaled, spent, cancelReq, live, execCount, clock >>

\* settle (SCHEMA sec 8.3): a held job releases when deps are satisfied and
\* dooms (cancels) when a dep failed without an ignore flag.  Guarded on
\* state = held, so it never touches a terminal job.
Settle(j) ==
  /\ state[j] = "held"
  /\ Deps[j] # {}
  /\ (Doomed(j) \/ Satisfied(j))
  /\ state' = [state EXCEPT ![j] =
                 IF Doomed(j) THEN "cancelled" ELSE "ready"]
  /\ UNCHANGED << attempt, journaled, spent, cancelReq, live, execCount, clock >>

\* Self-loop at the fully-settled end state so a finished run is not a
\* deadlock (all jobs terminal, nothing left to do).
Terminating ==
  /\ \A j \in Jobs : Terminal(state[j])
  /\ UNCHANGED vars

--------------------------------------------------------------------------
Next ==
  \/ \E j \in Jobs :
        \/ Insert(j)
        \/ Claim(j) \/ Drain(j)
        \/ Exec(j)
        \/ Crash(j)
        \/ Reclaim(j)
        \/ CancelRunning(j) \/ CancelParked(j)
        \/ Succeed(j)
        \/ FailBudget(j)
        \/ HonorCancel(j)
        \/ Raise(j)
        \/ Settle(j)
  \/ Advance
  \/ Terminating

Spec == Init /\ [][Next]_vars

--------------------------------------------------------------------------
(* --- Properties --------------------------------------------------------- *)

TypeOK ==
  /\ state     \in [Jobs -> AllStates]
  /\ attempt   \in [Jobs -> 0..3]
  /\ journaled \in [Jobs -> SUBSET StepNames]
  /\ spent     \in [Jobs -> 0..(BIG + 10)]
  /\ cancelReq \in [Jobs -> BOOLEAN]
  /\ live      \in [Jobs -> BOOLEAN]
  /\ execCount \in [Jobs -> [StepNames -> 0..3]]
  /\ clock     \in 0..MaxClock

\* Property 1 (INVARIANT #4).  No step body runs once durable spend is over
\* budget: at most the single CROSSING step can push spend past the cap, so
\* the recorded spend never overshoots by more than one step's cost.  A
\* second over-budget execution (the soak bug) would drive spent past
\* Budget + MaxCost.
NoExecutionPastBudget ==
  \A j \in Jobs : spent[j] <= Budget[j] + MaxCost[j]

\* Property 3. Memoized replay must not re-run a journaled step within the
\* atomic Exec+journal abstraction.
JournaledStepsNotReexecuted ==
  \A j \in Jobs : \A n \in StepNames : execCount[j][n] <= 1

\* Property 2 (SCHEMA sec 7).  A pending cancel_requested is never cleared by
\* any transition while the job is non-terminal -- it survives crash/reclaim/
\* retry until a step boundary honors it or the job goes terminal.
CancelRequestSurvivesUntilTerminal ==
  [][ \A j \in Jobs :
        (cancelReq[j] /\ ~Terminal(state[j])) => cancelReq'[j] ]_vars

\* Property 4.  Terminal states are absorbing under the modelled engine
\* actions (no operator `retry` is modelled).
TerminalStatesAreFinal ==
  [][ \A j \in Jobs : Terminal(state[j]) => state'[j] = state[j] ]_vars

=============================================================================
