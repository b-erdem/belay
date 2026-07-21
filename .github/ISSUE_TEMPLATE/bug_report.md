---
name: Bug report
about: Something behaved incorrectly
labels: bug
---

**Versions**: Belay / Elixir / OTP / Postgres:

**Storage adapter**: postgres | memory

**What happened, and what did you expect?**

**Smallest reproduction** — ideally a test using `Belay.Testing.drain/2`
and a `Belay.Clock.Sim` (see guides/testing.md); for engine races, note
whether it reproduces under `soak/run.sh`:

```elixir
```

**Relevant logs** (look for `[belay]` lines — stale acks, skipped claims,
and resettles are diagnostic gold):
