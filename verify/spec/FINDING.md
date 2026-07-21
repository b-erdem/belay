# Durable-core model verification record

## Verdict

TLC completely explored the bounded `Spec.tla` state space with no violation:

- 1,245,753,531 states generated
- 187,975,659 distinct states
- 0 states left on queue
- depth 49
- six configured checks (`TypeOK`, four durable-execution properties, and
  `DepGate`)

The exact release run, including TLC's `1.3E-4` actual-fingerprint collision
estimate, is preserved in `RESULT.md`.

The run used ten workers and fingerprint polynomial 0. `TraceMatch` is harness
glue and does not participate in the canonical `Next` relation.

## What was checked

The fixture contains five jobs: one budgeted four-step job, one permanently
failing workflow root, a two-level dependency chain, and a two-step job used
for success/retry/cancel behavior. Operator retries are globally bounded to two
so every single-retry interaction and the root-then-dependent recovery pair are
covered while the state space remains finite.

The configured properties are:

- `NoExecutionPastBudget`
- `CancelRequestSurvivesUntilTerminal`
- `JournaledStepsNotReexecuted`
- `TerminalStatesAreFinal`
- `DepGate`
- `TypeOK`

## Grounding and falsification

Attest's TLC harness admitted all seven deterministic engine traces (60/60
events, no truncation), binding event payloads to attempt, state, cancellation,
spend, and journaled-step snapshots.

The three mutants in `mutations/` each restore a known broken semantic and each
fails with the expected counterexample. Attempt fencing is verified in the
separate focused model under `../attempt_fence/`, whose mutant proves that a
zombie ack is accepted if the attempt equality is removed.

## Scope boundary

This is an exhaustive result for the model, not the source code. In particular,
`Exec` combines body execution and journal commit atomically. Therefore
`JournaledStepsNotReexecuted` says that a committed step is not replayed; it
does not claim exactly-once or at-most-once external effects in the real
crash-before-journal window. See `../README.md` for the rest of the explicit
abstractions and reproduction commands.
