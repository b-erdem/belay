# Migrating from Oban

The whole migration is four moves: port the workers, swap the supervision
child, move pending jobs with one command, archive the old table.

## 1. Port the workers

Mechanical, worker by worker:

| Oban | Belay |
|---|---|
| `use Oban.Worker, queue: :q, max_attempts: n` | `use Belay.Worker, queue: :q, max_attempts: n` |
| `perform(%Oban.Job{args: args})` | `run(%Belay.Ctx{job: %{input: input}})` |
| `backoff(%Oban.Job{attempt: a})` | `backoff(attempt)` |
| `timeout(_job)` callback | `timeout:` option on the `use` line |
| worker-level `unique:` policy | per-insert `unique:` **keys** (see below) |
| `{:ok, _} \| :ok \| {:error, _} \| {:cancel, _} \| {:snooze, _}` | identical |

The plugins you delete are engine features you configure:

| Oban plugin | Belay |
|---|---|
| `Pruner` | `retention: [succeeded: ..., failed: ..., cancelled: ...]` |
| `Lifeline` | nothing — renewed leases reclaim dead nodes' jobs in ~2× `lease_ttl`, and a *live* long job keeps renewing, so it is never double-run |
| `Cron` | `crons: [[name: ..., expr: ..., worker: ...]]` (per-slot dedup built in) |

Uniqueness is the one non-mechanical piece: Oban stores unique *policy* on
the worker; Belay stores unique *keys* on rows. Move the intent to the
insert site:

```elixir
# Oban: unique: [keys: [:run_id], states: :incomplete] on the worker
# Belay: a key at insert
Belay.insert(MyApp.Belay, MyWorker.new(%{"run_id" => id}, unique: "ingest:#{id}"))
```

## 2. Swap the child spec

```elixir
# out: {Oban, Application.fetch_env!(:my_app, Oban)}
# in:
{Belay,
 name: MyApp.Belay,
 storage: [adapter: :postgres, url: db_url],   # same database is fine
 queues: [...], crons: [...], retention: [...]}
```

Run `Belay.Storage.Postgres.migrate!(db_url)` at deploy (idempotent).

## 3. Move the pending jobs

Stop Oban's producers (scale the old release down, or set its queues to
`false`), then:

```bash
mix belay.migrate_oban --url postgres://.../my_app          # dry run: a report
mix belay.migrate_oban --url postgres://.../my_app --execute
```

The migrator moves `available`/`scheduled`/`retryable` rows (schedules and
retry state preserved), refuses workers that aren't ported (`--map
Old=New` for renames), never touches `executing` rows (they may be live on
an Oban node — the analyzer flags them), and is idempotent
(`meta.migrated_from_oban_id`).

## 4. Archive, don't import, history

Terminal Oban rows are history, not work. Imported, they'd be journal-less
rows polluting Belay's metrics; where they are, they audit fine:

```sql
ALTER TABLE oban_jobs RENAME TO oban_jobs_archive;
-- and once your retention window lapses:
DROP TABLE oban_jobs_archive;
```

Domain columns holding old job ids keep working: liveness probes against
Belay treat unknown ids as dead, which is the correct answer for them.

## Field notes from the first real port

A production-shaped Phoenix app (4 workers, webhooks, deletion flows,
hourly cron) ported in an afternoon: 222/222 tests green on the first full
run, zero engine changes needed. Two things to know from it:

- **Tests**: run Belay queues as `manual: true` in the test env and
  drive them with `Belay.Testing.drain` — it executes workers in the
  test process, so Ecto sandbox access just works. Truncate `belay_*`
  tables between tests through Belay's own pool (it is outside the
  sandbox by design).
- **Really stop Oban.** A forgotten dev node's producers will happily
  claim jobs you are mid-migration on — the analyzer's `executing` warning
  exists because this genuinely happens.
