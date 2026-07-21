# The Capstan Wire Contract

**Contract v1 · schema migration 2 · status: draft-stable (frozen at 1.0.0)**

Capstan's protocol is its Postgres schema. Everything an SDK in any language
needs — enqueueing, claiming, acking, steps, signals, children, workflows —
is specified here as tables plus SQL semantics. The Elixir implementation in
`lib/capstan/storage/postgres.ex` is the reference; where prose and reference
disagree, the reference wins and the prose has a bug.

Keywords MUST / SHOULD / MAY are used in the RFC sense.

## 1. Compatibility rules

- The contract version maps to `capstan_meta.version` (currently 2). SDKs
  MUST check it at startup and refuse to run against a higher major schema
  than they know.
- Within a contract major version, changes are **additive only**: new
  columns (nullable or defaulted), new tables, new index variants. SDKs MUST
  therefore never `SELECT *`-depend on column order and MUST tolerate unknown
  columns.
- Two SDK conformance levels:
  - **Client** (§10): enqueue, read, signal, steer, retry, cancel, await
    results. Cannot lose or corrupt work by construction — safe to build
    first.
  - **Worker**: additionally claim, execute, journal steps, and ack. Every
    normative rule in §§5–9 applies.

## 2. Value encoding (the envelope)

`capstan_steps.value` and `capstan_jobs.result` are `bytea`, in one of two
encodings discriminated by the first byte:

| First byte | Encoding | Written by |
|---|---|---|
| `131` (0x83) | Erlang external term format | Elixir SDK |
| anything else | UTF-8 JSON | every other SDK (MUST) |

JSON never begins with byte 131, so the discriminator is exact. All SDKs
MUST decode both (foreign ETF MAY be surfaced as opaque bytes). Values are
limited to 1 MiB. Reserved step values (`$sleep:*`, `$spawn:*`, §9) are
SDK-internal: never interpret a foreign SDK's reserved values.

`input`, `meta`, `errors`, signal/event payloads are `jsonb`. Timestamps are
`timestamptz` in UTC. Money is integer micro-dollars (`*_usd_micros`).

**Clock rule:** every operation takes "now" as a bind parameter supplied by
the SDK; SQL MUST NOT call `now()`. Keep worker clocks within a few seconds
of true time (NTP); leases and windows tolerate small skew.

## 3. Tables

Authoritative DDL: the `@migrations` list in the reference. Summary:

### capstan_jobs

| Column | Type | Semantics |
|---|---|---|
| `id` | bigserial PK | job identity; also the attempt-fencing anchor |
| `kind` | text | worker name (Elixir module string; foreign SDKs use any stable registry name their runtime resolves) |
| `queue` | text | queue name |
| `state` | text | §4 |
| `input` | jsonb | job input; encrypted inputs are `{"$enc": base64(iv‖tag‖ct)}` (AES-256-GCM, AAD `capstan.input.v1`) |
| `meta` | jsonb | free-form; the engine never branches on it |
| `priority` | int | 0 = highest; claim order key |
| `attempt` | int | incremented by claim; decremented by snooze/await parks |
| `max_attempts` | int | retry ceiling |
| `partition_key` | text | optional explicit fairness key |
| `ready_at` | timestamptz | when `ready` becomes claimable; the await deadline when `awaiting`; NULL = now / no deadline |
| `lease_until`, `leased_by` | timestamptz, text | live lease (§6) |
| `await_scope`, `await_name` | text | set while `awaiting` |
| `workflow_id`, `wf_name`, `wf_deps` (text[]), `wf_ignore` (text[]) | | workflow membership (§8.3) |
| `cron_name`, `cron_slot` | text, timestamptz | cron provenance + dedup key |
| `unique_key`, `unique_mode` | text | `incomplete` \| `window` \| `always` (§5.2) |
| `parent_id` | bigint | dynamic-children link (§8.4) |
| `budget_usd_micros`, `budget_tokens` | bigint | caps; NULL = uncapped |
| `spent_usd_micros`, `spent_tokens` | bigint | accumulated by `put_step` |
| `result` | bytea | envelope-encoded on success |
| `errors` | jsonb | append-only array of `{"error": str, "attempt": int, ...}` |
| `cancel_requested` | boolean | cooperative-cancel flag (§8.6) |
| `inserted_at`, `started_at`, `finished_at` | timestamptz | lifecycle stamps |

Required indexes (names normative — migrations are shared):
`capstan_jobs_claim_idx` (queue, priority, ready_at, id) WHERE state='ready';
`capstan_jobs_await_due_idx`; `capstan_jobs_await_wake_idx` (await_scope,
await_name) WHERE state='awaiting'; `capstan_jobs_lease_idx` (lease_until)
WHERE state='running'; `capstan_jobs_workflow_idx`; `capstan_jobs_parent_idx`;
`capstan_jobs_prune_idx`; unique `capstan_jobs_cron_slot_idx` (cron_name,
cron_slot) WHERE cron_name IS NOT NULL; unique
`capstan_jobs_unique_incomplete_idx` (unique_key) WHERE unique_mode =
'incomplete' AND state IN (incomplete set); unique
`capstan_jobs_unique_window_idx` (unique_key) WHERE unique_mode IN
('window','always').

### The rest

- `capstan_steps (job_id, seq, name, value, usd_micros, tokens, inserted_at)`
  — PK `(job_id, name)`; `seq` = per-job insertion order.
- `capstan_events (job_id, seq, payload, inserted_at)` — PK `(job_id, seq)`;
  append-only stream.
- `capstan_signals (scope, name, payload, inserted_at)` — PK `(scope, name)`;
  upsert-latest, persistent until cleared or TTL-pruned.
- `capstan_rate (bucket, window_start, count)` — sliding-window counters;
  bucket = `queue:<q>` or `resource:<name>`.
- `capstan_queues (name, opts, updated_at)` / `capstan_crons (name,
  expression, worker, input, opts, paused, updated_at)` — runtime CRUD rows,
  reconciled by every node.
- `capstan_meta (version)` — migration bookkeeping.

## 4. State machine

States: `ready` `running` `awaiting` `held` `paused` · terminal: `succeeded`
`failed` `cancelled`.

- **ready** — claimable when `ready_at IS NULL OR ready_at <= now`.
  Scheduled work, retry backoff, and snoozes are all `ready` with a future
  `ready_at`; there is no staging step.
- **awaiting** — parked on `(await_scope, await_name)`; claimable only when
  `ready_at` (the deadline) is due, which resumes the job so its own code
  observes the timeout.
- **held** — created ineligible (unmet workflow deps, operator pause of a
  parked job); leaves only via settlement (§8.3) or cancel.
- **running** — under a live lease.
- Legal writers: claim (`ready|awaiting-due → running`); ack (`running → any
  except held/paused`); wake (`awaiting → ready`); settle (`held →
  ready|cancelled`); reclaim (`running → ready|failed`); retry (`failed|
  cancelled → ready`); cancel (`any non-terminal, non-running → cancelled`;
  `running → cancel_requested=true`).

## 5. Enqueueing

### 5.1 Insert

Multi-row `INSERT INTO capstan_jobs (…) VALUES (…) ON CONFLICT DO NOTHING
RETURNING *`. The targetless conflict clause absorbs both cron-slot and
unique-key dedup; skipped rows are not returned. New rows MUST set:
`attempt=0`, `errors='[]'`, `spent_*=0`, `cancel_requested=false`,
`state='ready'` (or `'held'` for dep-bearing workflow rows), `ready_at=now`
(or the scheduled time), `inserted_at=now`.

Transactional enqueue is the same statement executed on the caller's
connection inside their transaction. The wake-up MAY be issued
transactionally as `SELECT pg_notify($channel, $payload)` in the same
transaction (§9.2) — Postgres delivers it only on commit.

### 5.2 Uniqueness

`unique_mode`:
- `incomplete` — at most one job with the key in a non-terminal state
  (enforced by the partial index). On conflict, fetch the holder:
  `SELECT … WHERE unique_key=$1 ORDER BY id DESC LIMIT 1` and report it as a
  duplicate.
- `window` — key MUST embed the window bucket:
  `<key>@<floor(epoch_seconds / window)>`; dedupes regardless of outcome.
- `always` — dedupes forever; used for spawn idempotency (§8.4).

## 6. Claiming and leases

All claim work happens in **one transaction**. When the queue has
`global_limit`, `rate`, or `partition`, first take
`pg_advisory_xact_lock(hashtext('capstan:' || queue))` — it serializes
claimers so admission math cannot race.

Admission (computed before selecting candidates):
- `global_limit`: allowed = `limit - count(*) WHERE queue=$q AND
  state='running' AND lease_until > $now` (live-leased only).
- `rate` `{allowed, period, estimate}` over bucket `queue:<q>` or
  `resource:<r>`: with current window `w = floor(epoch/period)*period`,
  `used = count(w) + round(count(w-period) * (1 - (epoch-w)/period))`,
  `admit_jobs = min(demand, floor(max(min(allowed - used, allowed), 0) /
  max(estimate,1)))`. After claiming N jobs, debit `N * estimate` into
  window `w` via upsert-increment.

Candidate selection (claimability predicate `C` = the ready/awaiting-due
disjunction from §4; order `priority ASC, ready_at ASC NULLS FIRST, id ASC`):
- Plain: `SELECT id … WHERE queue=$q AND C ORDER … LIMIT take FOR UPDATE
  SKIP LOCKED`.
- Partitioned (exact; runs under the advisory lock, so no `FOR UPDATE`
  needed on the ranking pass): rank per key with `row_number() OVER
  (PARTITION BY COALESCE(field->>$key,'') ORDER …)`, join per-key live-
  running counts, keep rows with `rn <= greatest(per_key_limit - running_k,
  0)`, order globally, `LIMIT take`. `per_key_limit` = `global_limit`
  (else `local_limit`).

Claim update: `SET state='running', attempt=attempt+1, lease_until=$now+ttl,
leased_by=$node, started_at=COALESCE(started_at,$now) WHERE id=ANY($ids)
RETURNING *`.

**Leases.** Renew all local leases in one statement each `ttl/3`:
`SET lease_until=$until WHERE id=ANY($ids) AND state='running' AND
leased_by=$node RETURNING id` — ids not returned are lost; the SDK SHOULD
kill those local executions. Any node reclaims expirees:
select `running AND lease_until <= $now FOR UPDATE SKIP LOCKED`, then per
row apply outcome `retry` (attempt < max) or `failed` with error
`{"error":"lease_expired"}` — including full §7 settlement.

## 7. Acking

One transaction per ack. The terminal/park UPDATE is **fenced**:
`WHERE id=$id AND state='running' AND attempt=$claimed_attempt`. Zero rows
⇒ the ack is stale (job was reclaimed); the SDK MUST discard it silently.

Outcome column effects (all also `SET lease_until=NULL, leased_by=NULL`;
`cancel_requested` is **never cleared by transitions** — a pending cancel
request survives crashes and retries until a step boundary honors it or the
job goes terminal, where it becomes moot):

| Outcome | state | other columns |
|---|---|---|
| succeeded | `succeeded` | `result`, `finished_at=$now`, clear await fields |
| retry | `ready` | `ready_at=$now+backoff`, append error to `errors` |
| failed | `failed` | append error, `finished_at` |
| cancelled | `cancelled` | append reason, `finished_at` |
| snooze | `ready` | `ready_at=$target`, **`attempt=attempt-1`** |
| await | `awaiting` | `await_scope/name`, `ready_at=deadline\|NULL`, **`attempt=attempt-1`** |

Terminal outcomes additionally clear `unique_key` semantics implicitly (the
partial `incomplete` index stops covering the row — no write needed).

**Await park-and-wake (normative).** Before writing an `await` park, take
`pg_advisory_xact_lock(hashtext('capstan_sig:' || scope))`, then check
`SELECT 1 FROM capstan_signals WHERE scope=$s AND name=$n` — if present,
park as `ready` (`ready_at=$now`, await fields NULL) instead. Signal
delivery (§8.1) takes the same lock; this serialization plus the persistent
signal row makes lost wake-ups impossible by construction.

**After any terminal write** (ack, reclaim-fail, cancel):
1. If `workflow_id`: settle (§8.3) under
   `pg_advisory_xact_lock(hashtext('capstan_wf:' || workflow_id))`.
2. For the job **and each settlement-cancelled job** with a `parent_id`:
   deliver the `$children` signal to `job:<parent_id>` (§8.4) —
   **unconditionally**, never gated on a sibling count (§11, race R1).

Lock ordering MUST be workflow-lock before signal-scope-lock; never the
reverse.

## 8. Semantics built on the above

### 8.1 Signals
Deliver = under the scope lock: upsert `capstan_signals`, then
`UPDATE capstan_jobs SET state='ready', ready_at=$now, await_scope=NULL,
await_name=NULL WHERE state='awaiting' AND await_scope=$s AND await_name=$n
RETURNING *` (poke the returned queues). Read = `scope = ANY($scopes)`;
a job's default scopes are `job:<id>` plus `wf:<workflow_id>` when present.
Signals persist until explicitly cleared or TTL-pruned — awaiters that race
delivery still find them.

### 8.2 Steps
Read `SELECT value WHERE job_id=$1 AND name=$2`; hit ⇒ decode envelope and
skip execution. Miss ⇒ the SDK MUST first re-read the job row and fail with
`budget_exceeded` if durable `spent_*` already exceeds a budget column —
without this pre-flight check, a crash between journaling the over-budget
step and acking the failure lets the next attempt replay past the journal
and execute one more paid step. Then run the function, then
`INSERT (job_id, seq=(SELECT COALESCE(MAX(seq),0)+1 …), name, value, costs,
$now) ON CONFLICT (job_id, name) DO NOTHING`, then
`UPDATE capstan_jobs SET spent_usd_micros = spent_usd_micros + $u,
spent_tokens = spent_tokens + $t WHERE id=$1 RETURNING spent_*` (one
transaction). If a returned spent exceeds its budget column, the SDK MUST
fail the job with error `budget_exceeded`. Returned values are memoized —
including error-shaped ones; only raising leaves a step unrecorded.

### 8.3 Workflow settlement (pure function, shared by all writers)
Over all jobs of the workflow: a `held` job with `wf_deps` **releases** when
every dep name's state is `succeeded`, or is terminal-failed with the
matching flag in `wf_ignore` (`"failed"`/`"cancelled"`); it **dooms** when
any dep is terminal-failed without its flag. Iterate to fixpoint, doomed
jobs counting as `cancelled` for their own dependents. Apply: releases →
`ready` (guard `WHERE state='held'`), dooms → `cancelled` + `finished_at`
(same guard). Reference implementation: settle/1 in lib/capstan/storage.ex.

### 8.4 Dynamic children
Spawn MUST be doubly idempotent: (a) the spawn is a memoized step named
`$spawn:<name>` whose value is the child-id list; (b) each child carries
`unique_mode='always'` key `$spawn:<parent_id>:<name>:<index>`, and the id
list is rebuilt by fetching those keys — never trusted from a possibly-
partial insert. Await-children = re-verify `SELECT … WHERE parent_id=$1`;
if any non-terminal, clear the stale `$children` signal, re-verify once
more, then park on (`job:<parent_id>`, `$children`). Sweepers MUST run the
reconciler backstop: any `awaiting` job on `$children` whose children are
all terminal (and exist) → `ready` (§11, R2).

### 8.5 Cron
For each due minute slot (UTC-truncated), insert with `(cron_name,
cron_slot)`; the unique index makes firing exactly-once cluster-wide with no
leader. Dynamic `capstan_crons` rows merge over static config by name;
`paused` rows are skipped.

### 8.6 Cancel, retry, pause
Cancel: parked/held → terminal `cancelled` + settlement + parent notify;
running → `SET cancel_requested=true`; workers MUST check the flag at step
boundaries and self-cancel. Retry: `failed|cancelled → ready`,
`max_attempts = GREATEST(max_attempts, attempt+1)`, clear lease/finished.
Pause is SDK-local (stop claiming); the `paused` state is reserved for
operator freezes of parked jobs.

### 8.7 Retention
Sweepers delete terminal jobs older than per-state retention **together
with** their `capstan_steps` and `capstan_events` rows (batched, e.g.
LIMIT 500), prune signals past TTL and rate windows older than a day.
Non-terminal jobs are never pruned.

## 9. Wake-ups (accelerator, never load-bearing)

Polling is the correctness floor; every wake mechanism is lossy-by-assumption.

- **9.1 Polling**: SDKs SHOULD poll adaptively — a hot cadence while claims
  return work (reference: 25ms), decaying to an idle ceiling (500ms).
- **9.2 pg_notify** (optional): channel `capstan_<database-name sanitized to
  [A-Za-z0-9_]>`. Payloads: poke `{"t":"p","q":"<queue>"}`, result
  `{"t":"r","id":<job_id>}`. Emit at most one poke per queue per
  insert-batch/completion (the NOTIFY commit lock punishes chattiness).
  LISTEN requires a direct (non-transaction-pooled) connection; `pg_notify`
  through a pooler is fine.

## 10. Conformance

**Client SDK checklist**: schema-version check; enqueue single/batch with
uniqueness + duplicate reporting; transactional enqueue on a caller
connection; read job/steps/events/workflow; deliver signals (with scope
lock) and steering (`$steer`); cancel/retry; await results (poll or result
notifications); envelope decoding both ways.

**Worker SDK**: all of §§5–9. Verify with the language-agnostic harness in
`soak/`: point `soak/run.sh` at your worker binary instead of
`soak/worker.exs` (implement the eight soak worker kinds), and the driver —
which only reads the database — checks the thirteen invariants (§11) under
`kill -9` and database restarts. A worker SDK is conformant when the soak
passes.

## 11. Invariants (normative, soak-verified)

1. No inserted job is ever lost; all reach a terminal state given workers.
2. A fenced ack from a stale attempt never mutates the row.
3. At-least-once execution; step *values*, once journaled, are immutable and
   exactly one row per (job, name).
4. Budgets fail the job on the first step whose recorded spend crosses a cap.
5. Workflow releases occur only with satisfied deps; dooms only with a
   doomed dep; settlement is a fixpoint (re-settling is a no-op).
6. Cron slots insert exactly once cluster-wide.
7. Unique keys admit at most one live holder (`incomplete`) / one ever
   (`window` per bucket, `always`).
8. A parent has exactly the children its spawn declared, regardless of
   crashes between insert and journal.
9. Awaiting jobs with a delivered signal eventually run (lock-serialized
   wake + persistent signals + reconciler backstop).

Race lessons encoded above, learned the hard way (soak reports in-repo):
**R1** — never count-gate sibling completion signals under READ COMMITTED
(concurrent last-two-children both see the other incomplete); signal
unconditionally and re-verify on wake. **R2** — pair every wake protocol
with an idempotent reconciler; unknown orderings then cost latency, not
liveness.

## 12. Out of contract

Dashboard and MCP server (consumers of the contract, not part of it);
polling cadences and executor runtime choices (QoS, SDK-local); the Memory
adapter (an in-process test double); Elixir worker-module resolution
(`kind` is a registry name — each SDK maps it to code its own way).
