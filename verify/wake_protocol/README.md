# Wake-protocol schedule exploration (Lockstep)

A controlled-concurrency model of the parent-wake protocol (SCHEMA.md race
R1), explored with [Lockstep](https://hex.pm/packages/lockstep) — every
interesting interleaving scheduled deterministically instead of sampled by
chaos.

Two tests share one model (a serialized "database" process with explicit
READ COMMITTED transaction visibility; signal delivery and parking
serialized in-process, playing the role of the `capstan_sig` advisory
lock):

- **`count_gated`** — the rc.1 protocol (signal only when your ack's
  transaction sees every sibling done). Lockstep finds the lost wake as a
  deadlock **at iteration 1** under PCT and saves the schedule to
  `traces/`: both children count each other incomplete before either
  commits, both skip the signal, the parent parks forever. This is the
  bug the rc.2 chaos soak caught statistically; here it reproduces
  deterministically with a replayable trace.
- **`unconditional`** — the shipped protocol (every terminal ack signals;
  parent re-verifies on wake). No failing schedule in 400 explored
  interleavings — deliberately WITHOUT modeling the sweeper reconciler,
  demonstrating the wake protocol is sound on its own and the reconciler
  is a backstop, not a load-bearing layer.

Run:

```bash
mix deps.get
mix test
```

Replay the saved failing schedule:

```bash
mix lockstep.replay traces/<file>.lockstep
```

This is a standalone project on purpose: it models the protocol
abstractly and never imports Capstan code, so Capstan's dependency tree
stays untouched.
