---
name: Bug report
about: Something behaved incorrectly
labels: bug
---

**Versions**: Capstan / Elixir / OTP / Postgres:

**Storage adapter**: postgres | memory

**What happened, and what did you expect?**

**Smallest reproduction** — ideally a test using `Capstan.Testing.drain/2`
and a `Capstan.Clock.Sim` (see guides/testing.md); for engine races, note
whether it reproduces under `soak/run.sh`:

```elixir
```

**Relevant logs** (look for `[capstan]` lines — stale acks, skipped claims,
and resettles are diagnostic gold):
