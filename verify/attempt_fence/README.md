# Attempt-fence model

This focused TLA+ model covers the concurrency window that the larger durable-
execution model intentionally abstracts away: attempt 1 expires but its worker
remains alive, attempt 2 claims the same job, and attempt 1 later tries to ack.

`AttemptFence.tla` requires an ack's attempt to equal the row's current attempt.
TLC completely explores the bounded model and checks
`StaleAttemptCannotCommit`. The matching adapter regression test is
`test/belay/leases_test.exs` and runs against both Memory and PostgreSQL.

`mutations/PreFixAttemptFence.tla` removes that equality guard. TLC then finds
the expected four-transition counterexample:

`Claim(1) → Expire(1) → Claim(2) → Ack(1)`.

Run both with `TLA2TOOLS_JAR=/path/to/tla2tools.jar ../check.sh`.
