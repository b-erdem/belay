# How Capstan compares

An honest map. Every project here is good software; the question is fit.

## TL;DR table

| | Capstan | Oban | Oban Pro | Temporal | Sidekiq Pro | River Pro |
|---|---|---|---|---|---|---|
| Model | Postgres library | Postgres library | + paid extension | server + SDKs | Redis library | Postgres library |
| Retry granularity | **step replay** | whole job | whole job | step replay | whole job | whole job |
| Human-in-the-loop waits | ✓ (`await`/signals) | ✗ | ✓ (1.7 signals) | ✓ | ✗ | ✗ |
| Dynamic runtime fan-out | ✓ (`spawn`/children) | ✗ | grafts | ✓ (child workflows) | ✗ | ✗ |
| Cost budgets that kill | ✓ | ✗ | ✗ | ✗ | ✗ | ✗ |
| Token-aware rate limits | ✓ (true-up) | ✗ | weights (static) | ✗ | ✗ | ✗ |
| Event streams / replay debug | ✓ / ✓ | ✗ / ✗ | ✗ / ✗ | ✓ / ✓ | ✗ / ✗ | ✗ / ✗ |
| MCP / agent-operable | ✓ | ✗ | ✗ | ✓ | ✗ | ✗ |
| Leaderless | ✓ | leader-elected plugins | leader-elected | server-managed | n/a | leader-elected |
| Web UI | not yet | ✓ (free) | ✓ | ✓ | ✓ | ✓ |
| Price | free, Apache-2.0 | free | $150/mo/app | free + cloud | $995+/yr | $125/mo |
| Battle-tested years | **0** | 6+ | 5+ | 8+ | 12+ | 2+ |

## Against Oban / Oban Pro

Oban is the Elixir ecosystem's default for good reason, and Oban Pro is
excellent commercial software whose revenue funds Oban itself. Choose them
when you want years of production hardening, the free Web dashboard, and the
ecosystem of integrations — for classic job workloads that's the boring
correct choice.

Capstan exists for what that stack structurally doesn't sell: retries at
step granularity (a retried Pro workflow re-runs whole jobs), enforceable
spend budgets, post-hoc token accounting, durable event streams, journal
replay debugging, dynamic child DAGs, and agent operability — open, with the
base thing as the best version. Capstan also makes different architectural
bets: no leader election anywhere, no LISTEN/NOTIFY dependency, leases
instead of hour-scale rescue heuristics, and first-class columns instead of
`meta` blobs.

## Against Temporal (and Restate, Inngest, Trigger.dev)

Temporal is the durable-execution heavyweight: deterministic replay,
multi-language, enormous scale, $5B of momentum. If you need cross-language
workflows or its operational ceiling, use it.

Capstan's position: Temporal-class *semantics* (steps, signals, waits,
children, streams) at Oban-class *packaging* — one library in your app, one
Postgres database, no separate server cluster, no workflow-determinism
sandbox to learn, and no per-action cloud metering. The trade: no
deterministic replay of arbitrary code (Capstan replays the journal, not the
event loop), single-language, and none of Temporal's decade of hardening.

## Against Sidekiq / River / Solid Queue

Different language ecosystems (Ruby/Go), same generation: job-granular
retries and paid tiers for composition features. Capstan's design brief was
explicitly the next generation — if you're in Elixir and your workload looks
like 2026 (agents, LLM calls, human gates, spend caps), that's the gap it
fills.

## When you should not use Capstan

- You need a UI today (ours is on the roadmap; Oban Web is excellent).
- You need SQLite/MySQL storage, or enqueueing inside your Ecto
  transactions (both planned; not in rc.1).
- You need sub-100ms dispatch latency at massive scale — poll-first tops out
  at `poll_interval` pickup latency off-node.
- You need something proven over years. Capstan is honest about being new:
  the suite is strong and runs identically against both storage adapters,
  but production miles are the one feature that can't be implemented in a
  sprint.
