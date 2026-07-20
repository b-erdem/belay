# Operations

What actually happens in production, and the knobs you own.

## Delivery guarantees, stated plainly

Capstan is **at-least-once**. A worker that dies after doing the work but
before acking will have the job re-run — with the crucial mitigation that
memoized steps make the re-run skip everything the first run recorded.
Two windows to know about:

- **Crash orphans**: a killed node's running jobs sit unreachable until their
  lease expires (`lease_ttl`, default 30s), then any node's sweeper reclaims
  them for retry. That's the worst-case added latency for a crash.
- **Zombie acks**: a worker cut off from the database long enough to lose its
  lease might come back and try to ack; acks are fenced by attempt number and
  rejected. You'll see a `stale ack` warning — that's the fence working.

## Shutdown

On SIGTERM, producers stop claiming first, running jobs get
`shutdown_grace` (default 15s) to finish, and whatever survives is killed —
its lease expires and another node picks it up. Set `shutdown_grace` just
under your platform's kill timeout.

## Dispatch latency

Agent workloads are bursts of short tasks, so dispatch overhead is the
product. Capstan layers three mechanisms — each optional layer only buys
latency; polling remains the correctness floor:

1. **Local pokes** (always on): inserts, releases, and completions poke
   producers on the same node instantly, and across BEAM nodes when you run
   distributed Erlang.
2. **Adaptive polling** (always on): producers poll at `busy_poll` (25ms)
   while work flows and decay to `poll_interval` (500ms) when idle — burst
   latency without idle database load. Poke storms coalesce into single
   claim rounds.
3. **`pg_notify` accelerator** (opt-in, `notifiers: [:local, :postgres]`):
   wake-ups ride the database itself, for fleets that share Postgres but not
   an Erlang cluster. One dedicated listen connection per node
   (auto-reconnecting; point `listen_url:` past any transaction pooler);
   payloads are a queue name or job id, far under NOTIFY's limits.

Measured insert→result round trips (`bench/run.sh`, stock settings, local
Postgres 16, M1):

| Topology | p50 | p90 | p99 | 100-job burst |
|---|---|---|---|---|
| same node (pokes) | 8.6ms | 10.4ms | 13.0ms | 336ms |
| cross-process, polling only | 49.4ms | 179ms | 202ms | 349ms |
| cross-process + `pg_notify` | 11.0ms | 12.8ms | 24.5ms | 371ms |

Honest caveats: Postgres serializes NOTIFY-issuing transactions on a global
queue at commit, so at very high sustained insert rates the accelerator
itself becomes a contention point — Capstan already coalesces to one poke
per queue per insert call, and if you ever see NOTIFY contention you can
drop back to `[:local]` and keep the adaptive-polling latencies above.
`await_result` wakes on result notifications and otherwise re-checks on a
5ms→200ms backoff.

## Sizing the loop

| Knob | Default | Meaning |
|---|---|---|
| `poll_interval` | 500ms | idle polling ceiling (worst-case cold pickup) |
| `busy_poll` | 25ms | polling cadence while a queue is hot |
| `notifiers` | `[:local]` | add `:postgres` for cross-fleet NOTIFY wake-ups |
| `lease_ttl` | 30s | crash-orphan window; renewed at ttl/3 |
| `sweep_interval` | 5s | reclaim + retention cadence |
| `shutdown_grace` | 15s | time running jobs get on shutdown |
| queue `limit` | — | per-node concurrency |
| queue `global_limit` | — | cluster-wide concurrency (live-leased count) |
| queue `rate` | — | sliding-window admission, optionally resource-scoped |
| queue `partition` | — | per-key fairness (`{:input, "tenant_id"}`) |

## Retention

Terminal jobs are pruned with their steps and events by the sweeper:

```elixir
{Capstan,
 ...,
 retention: [succeeded: 86_400, failed: 7 * 86_400, cancelled: 7 * 86_400],
 signal_ttl: 7 * 86_400}
```

Set a state to `:infinity` to keep it forever (and own the table growth).
Incomplete jobs are never pruned.

## Day-2 tooling

```elixir
Capstan.stats(MyApp.Capstan)
#=> %{"ai" => %{"ready" => 12, "running" => 2}, "default" => %{"succeeded" => 1043}}

Capstan.list_jobs(MyApp.Capstan, state: :failed, limit: 20)
Capstan.retry_job(MyApp.Capstan, job_id)
Capstan.cancel(MyApp.Capstan, job_id)      # immediate when parked, cooperative when running
Capstan.pause_queue(MyApp.Capstan, :ai)    # local producer stops claiming
Capstan.steps(MyApp.Capstan, job_id)       # the journal, costs included
Capstan.events(MyApp.Capstan, job_id)      # the emitted stream
Capstan.Replay.dry_run(MyApp.Capstan, job_id)  # what did it actually do?
```

The same surface is exposed to AI assistants and scripts through
`mix capstan.mcp`.

## Telemetry

Events: `[:capstan, :job, :start | :stop | :exception]` with durations and
job metadata. For one-line structured logs:

```elixir
Capstan.Telemetry.attach_default_logger(:info)
# capstan job=812 worker=MyApp.Agent queue=ai state=succeeded attempt=1 duration=8143ms
```

## Postgres notes

- The claim path is `FOR UPDATE SKIP LOCKED` over partial indexes; the
  migration creates every index the engine relies on.
- **LISTEN/NOTIFY is not used.** PgBouncer transaction pooling, RDS Proxy,
  and serverless Postgres are all fine.
- Retention is a `DELETE ... LIMIT` batch per sweep; autovacuum handles the
  rest at moderate scale. At very high throughput, shorten retention before
  reaching for anything exotic.
- Known limit: partitioned claims over-fetch candidates by a bounded
  heuristic (take×4+16); pathological single-key skew can under-fill a claim
  round. Documented, poll picks it up next round.
