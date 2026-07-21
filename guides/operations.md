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

Honest caveats, with receipts: Postgres
serializes NOTIFY-issuing commits on a global lock (the Recall.ai outage
class; ~190k vs ~350k TPS with/without it in pgsql-hackers benchmarks —
present through PG 18, and PG 19's fix covers only the listener-wake side).
Capstan stays far from that zone by coalescing to one `pg_notify` per queue
per insert batch/completion, and the accelerator is droppable at any time —
`[:local]` keeps the adaptive-polling latencies above. The pooler rule:
`pg_notify` through PgBouncer is fine; only the **listen** connection needs
a direct line (`listen_url:`). Notifications are at-most-once by design,
which is exactly why the polling floor is non-negotiable. `await_result`
wakes on result notifications and otherwise re-checks on a 5ms→200ms
backoff. One structural advantage worth naming: workers are BEAM processes
in your running app — there is no sandbox cold-start tier between dispatch
and execution at all.

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

## Throughput levers: chunks and adaptive limits

**Chunk workers** turn N claimed jobs into one worker invocation — one bulk
INSERT, one batch-priced embeddings call:

```elixir
use Capstan.Worker, queue: :embeddings, chunk: [size: 100, gather_ms: 500]
```

The producer gathers claimed jobs per worker up to `size`, waiting at most
`gather_ms` for stragglers (full chunks dispatch immediately). Gathered jobs
are already leased, so a crash mid-gather reclaims them like any other
crash; the gather window is clamped to half the lease TTL. See
`Capstan.Worker` for the `run_chunk/1` contract including per-job partial
failure.

**Adaptive concurrency** scales a queue's per-node limit with load:

```elixir
queues: [ingest: [limit: [min: 2, max: 50]]]
```

Saturated claim rounds double the limit toward `max`; an idle queue decays
back to `min`. Leaderless — each node adapts its own limit, and
`global_limit`, rate limits, and partition fairness still bound the fleet
exactly. Scale changes emit `[:capstan, :queue, :scale]`.

For policy-driven scaling (queue depth thresholds, business hours, an
operating agent watching costs), drive `Capstan.Queues.put/3` from your own
logic — it is runtime CRUD reconciled by every node. The bundled MCP server
does not currently expose queue-definition CRUD.

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

## The dashboard

One child spec, zero dependencies:

```elixir
{Capstan.Dashboard, capstan: MyApp.Capstan, port: 4004, token: System.fetch_env!("DASH_TOKEN")}
```

Live queue tiles (with each queue's limits and rates), a filterable job
list, a drawer with the full journal - steps with costs, events, errors,
children - a rendered workflow DAG, and retry/cancel/signal/steer actions
(mutations require either a dashboard token or the same `authorizer:`
contract as the MCP server; tokenless dashboards are read-only). It binds
127.0.0.1 by default and speaks plain HTTP; front it with your proxy for
remote access.

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

The same inspection surface is exposed to AI assistants and scripts through
`mix capstan.mcp`. Mutations are disabled unless you pass
`--authorizer MyGuard` or explicitly opt in with `--allow-mutations`.

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
- **LISTEN/NOTIFY is never load-bearing.** The optional accelerator uses
  it; correctness never does. PgBouncer transaction pooling, RDS Proxy, and
  serverless Postgres are all fine (see Dispatch latency for the one
  direct-connection rule when you enable the accelerator).
- Retention is a `DELETE ... LIMIT` batch per sweep; autovacuum handles the
  rest at moderate scale. At very high throughput, shorten retention before
  reaching for anything exotic.
- Partitioned claims are exact: per-key allowances are computed with a
  window-function ranking inside the claim transaction, under the queue's
  advisory lock — heavy key skew cannot starve minority keys.
- Measured throughput (`bench/throughput.exs`): ~416 trivial jobs/s
  end-to-end on a laptop with 3 worker processes and unbatched acks; each
  job costs a claim and an ack transaction. Batched acking is the known
  post-1.0 lever for multiples beyond that.
