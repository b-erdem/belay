# Formal verification and trace evidence

Belay uses several deliberately small verification artifacts instead of one
model that pretends to cover the whole engine:

- `spec/` models durable step replay, fail-after-crossing budgets,
  cancellation across reclaim, terminal retry, and workflow dependency gates.
- `attempt_fence/` isolates the zombie-worker ack race.
- `wake_protocol/` explores the parent/child wake race under controlled
  concurrent schedules.
- `traces/` are deterministic executions of the real Memory adapter and
  runner. Attest mechanically replays every event through `Spec.TraceMatch`.

The models support specific claims. They are not a proof that the Elixir or SQL
implementation is bug-free.

## Durable-core model

The canonical five-job fixture was completely checked by TLC:

**187,975,659 distinct states (1,245,753,531 generated), depth 49, zero
violations** (10 workers, 45m42s). The exact command, tool fingerprint, final
output, and TLC fingerprint-collision estimate are preserved in
[`verify/spec/RESULT.md`](https://github.com/b-erdem/belay/blob/main/verify/spec/RESULT.md).

| Checked property | Claim inside the abstraction |
|---|---|
| `NoExecutionPastBudget` | Once durable spend is over the limit, another unfinished step cannot execute; recorded overshoot is at most one declared step cost. |
| `CancelRequestSurvivesUntilTerminal` | Crash/reclaim and ordinary outcomes cannot clear a pending cooperative cancellation. |
| `JournaledStepsNotReexecuted` | After the atomic model action journals a step, replay does not execute that name again. |
| `TerminalStatesAreFinal` | Only explicit operator retry exits failed/cancelled states, re-holding workflow members and clearing prior cancel intent. |
| `DepGate` | A running workflow job cannot have an unignored failed or cancelled dependency. |
| `TypeOK` | The bounded state stays within its declared domains. |

`JournaledStepsNotReexecuted` is intentionally narrower than “step bodies run
at most once.” The model treats body execution and journal commit as one action.
The real body can run again after a crash between an external effect and the
journal write; Belay documents that API as at-least-once until commit.

## Real-trace admission

`Spec.tla` defines `TraceMatch(e)` over event payloads, including job state,
attempt, cancellation, durable spend, and the sequence of journaled step names.
Attest generated an independent TLC replay harness for each checked trace:

| Trace | Events admitted |
|---|---:|
| happy path | 7/7 |
| memoized retry | 9/9 |
| exact budget crossing | 7/7 |
| budget crash/reclaim window | 12/12 |
| cancel mid-run | 6/6 |
| cancel across crash/reclaim | 11/11 |
| workflow cascade settlement | 8/8 |

Overall: **7/7 traces, 60/60 events admitted without truncation**. This checks
that the model can reproduce the observed executions; it does not establish
that the traces cover every implementation behavior.

## Mutation validation

A passing model matters only if relevant broken semantics fail:

- `spec/mutations/PreFixBudget.tla` removes the budget pre-flight guard and
  admits a fourth paid step after crash/reclaim.
- `spec/mutations/PreFixCancelClear.tla` clears a cancel request during
  reclaim.
- `spec/mutations/PreFixRetry.tla` retries a workflow member directly to ready
  and preserves stale cancel intent.
- `attempt_fence/mutations/PreFixAttemptFence.tla` removes the attempt equality
  from ack. TLC finds `Claim(1) → Expire(1) → Claim(2) → Ack(1)`.

All four mutants produce the expected counterexample. The focused attempt-
fence canonical model completely explores 10 distinct states to depth 6 with
no violation; the corresponding adapter regression test runs against both
Memory and PostgreSQL.

## Re-running

```bash
# Focused canonical model + all four required failing mutants
TLA2TOOLS_JAR=/path/to/tla2tools.jar verify/check.sh

# Add mechanical admission of all seven traces
TLA2TOOLS_JAR=/path/to/tla2tools.jar \
ATTEST_BIN=/path/to/attest \
verify/check.sh

# Full durable-core state space (long-running)
cd verify/spec
java -XX:+UseParallelGC -cp /path/to/tla2tools.jar \
  tlc2.TLC -workers 10 -fp 0 -config Spec.cfg Spec
```

CI parse-checks the canonical model, completely checks the focused attempt-
fence model, and requires the attempt, budget, cancellation, and retry mutants
to fail in their expected ways. The 188M-state run is intentionally not a per-
commit CI job.

## Explicit limits

The canonical model abstracts wall-clock lease timing to a small clock and a
live/dead flag; it does not model SQL isolation, notifier delivery, signals,
awaiting/paused states, uniqueness, encryption, rate limiting, chunk workers,
or concurrent body execution before a journal commit. The attempt-fence and
wake models cover two concurrency mechanisms separately. PostgreSQL/Memory
adapter equivalence, chaos soak, and integration tests cover different gaps;
none turns model checking into source-code proof.
