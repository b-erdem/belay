# Complete TLC run — 2026-07-21

This is the release record for the bounded durable-core model in `Spec.tla`.
The checked spec/config are the files beside this record.

This full run (188M states, ~46 min) is recorded here rather than run in CI.
The `formal-smoke` CI job instead does a fast check on every push — a SANY
parse of this model plus the small attempt-fence model and all four
mutation counterexamples (`verify/check.sh`) — using a pinned stable
`tla2tools.jar`. The two TLA+ builds differ; SANY syntax and the tiny
mutant models are version-stable, so the smoke is a valid regression gate
without re-running the 46-minute exhaustive check.

## Tool and command

- TLC: `2026.05.04.141011`
- `tla2tools.jar` SHA-256:
  `e47073579d0ff27989bd3789ce0cc8ab42ca6e2c4374c0e95c3dfa0bff9f0113`
- Workers: 10
- Fingerprint polynomial: 0

```bash
java -XX:+UseParallelGC -cp "$TLA2TOOLS_JAR" \
  tlc2.TLC -workers 10 -fp 0 -config Spec.cfg Spec
```

## Result

```text
Model checking completed. No error has been found.
  Estimates of the probability that TLC did not check all reachable states
  because two distinct states had the same fingerprint:
  calculated (optimistic):  val = .011
  based on the actual fingerprints:  val = 1.3E-4
1245753531 states generated, 187975659 distinct states found, 0 states left on queue.
The depth of the complete state graph search is 49.
The average outdegree of the complete state graph is 1 (minimum is 0, the maximum 11 and the 95th percentile is 3).
Finished in 45min 42s at (2026-07-21 21:43:09)
```

“Complete” here means TLC emptied the bounded model's state queue. As TLC's
output states, fingerprint-based exploration retains a small collision risk;
the estimate based on this run's actual fingerprints was `1.3E-4`. This is a
model result, not a proof of the Elixir or SQL implementation. See
`../README.md` for the abstraction boundary and the separate trace, mutation,
schedule, adapter, and chaos evidence.
