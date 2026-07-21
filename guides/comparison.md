# How Capstan compares

Capstan is not “every queue and workflow system, but better.” Its narrower
bet is more useful: **Elixir-native durable steps and agent-workload controls,
packaged as one Postgres-backed library**. This page says where that
combination wins and where mature alternatives still do.

Feature sets and commercial terms move. This comparison describes product
shape rather than volatile price/version details; follow the linked official
docs before making a purchasing decision.

## The short map

| | Capstan | Oban / Oban Pro | Temporal | DBOS / `pg_durable` | Cloud durable runtimes |
|---|---|---|---|---|---|
| Deploy shape | Elixir library + your Postgres | Elixir library + your Postgres | Separate service + SDK | Application library or Postgres extension | Provider-managed runtime |
| Recovery unit | journaled step result | whole job | event-history replay / activities | checkpointed step | platform checkpoint |
| Elixir-native | **yes** | **yes** | no official Elixir SDK | no | no |
| Queue operations | built in | class-leading | activities/task queues | not the primary product | platform-specific |
| Human waits / signals | built in | Pro has workflow signals | built in | product-specific | built in or platform-specific |
| Cost-aware controls | declared step budgets + estimate/true-up rate admission | weighted rate limits in Pro | build it | build it | build it |
| Extra service | no | no | yes | no for DBOS; extension for `pg_durable` | the cloud platform |
| Production maturity | **new** | years of Elixir production | the mature distributed-workflow benchmark | newer | provider scale, provider coupling |

“Step result” does not mean exactly-once external effects. Capstan, like other
checkpoint systems, has a crash-before-checkpoint window; use idempotency keys
for external effects. See [Durable steps](durable-steps.md).

## Oban and Oban Pro

[Oban](https://hexdocs.pm/oban/) is the Elixir ecosystem default for good
reason. Choose it when the problem is dependable background jobs and you value
its production history, integrations, mature operational knowledge, and free
web UI. [Oban Pro](https://oban.pro/docs/pro/overview.html) adds polished
workflows, advanced engines, chunking, rate controls, and commercial support.

Choose Capstan when the unit you need to resume is smaller than a job, or when
the workload specifically needs journal replay, declared per-step spend,
token-usage true-up, dynamic children, embedded event streams, or operator
steering. Capstan also makes different architectural bets: no leader election,
no correctness dependency on LISTEN/NOTIFY, and an injectable clock with the
same behavioral suite against Memory and PostgreSQL.

The trade is blunt: Capstan does not have Oban's production miles. Replacing a
healthy Oban installation should be justified by one of those semantic gaps,
not by a longer feature checklist.

## Temporal, Restate, Inngest, and Trigger.dev

[Temporal](https://docs.temporal.io/) is the mature distributed-workflow
benchmark: multi-language SDKs, deterministic workflow replay, a dedicated
service, and a much larger operational ceiling. Restate, Inngest, and
Trigger.dev offer related durable-execution shapes with different hosting and
programming models.

Capstan's advantage is packaging for an Elixir/Postgres team: no workflow
cluster, no second control plane, and no deterministic-code sandbox. Its replay
is a journal of named steps, not deterministic replay of arbitrary workflow
code. If you need cross-language orchestration, global-scale workflow hosting,
or the ecosystem around a mature workflow server, use the dedicated system.

## DBOS and `pg_durable`

[DBOS](https://docs.dbos.dev/architecture) and Microsoft's
[`pg_durable`](https://microsoft.github.io/pg_durable/) validate the same broad
idea: durable execution can be checkpointed in Postgres without operating a
Temporal-style service. They are the closest architectural neighbors, not
classic job queues.

Capstan differentiates on the BEAM-native queue around that kernel: OTP
supervision, transactional enqueue, leases and fencing, cron, uniqueness,
cluster/partition/rate admission, workflow DAGs, deterministic ExUnit helpers,
dashboard, and MCP inspection. Choose DBOS or `pg_durable` when their supported
languages/runtime integration fit better, or when you want a narrower durable-
function layer rather than an Elixir job engine.

## AWS Lambda, Cloudflare, and Vercel durable workflows

[AWS Lambda durable functions](https://docs.aws.amazon.com/lambda/latest/dg/durable-functions.html),
[Cloudflare Workflows](https://developers.cloudflare.com/workflows/), and
[Vercel's durable workflow model](https://vercel.com/blog/a-new-programming-model-for-durable-execution)
offer checkpointed execution as part of a cloud runtime. They can remove more
operations than Capstan because the provider owns the workers and control
plane.

Capstan is the fit when Elixir is the runtime, data must stay in Postgres you
operate, local development should not emulate a cloud service, or provider
portability matters. The cloud runtimes win when their deployment platform is
already the constraint and managed scaling is more valuable than BEAM/Postgres
ownership.

## Sidekiq, River, and Solid Queue

These are strong queue systems in Ruby or Go ecosystems. They are relevant as
design references, but usually not a direct choice for an Elixir application.
Their center of gravity is job-granular retry; Capstan's is checkpointed work
inside a job plus the surrounding queue.

## When not to use Capstan

- A mature queue already meets the workload and step recovery would add no
  material value.
- You need years of production history or a commercial support contract more
  than you need Capstan's semantics.
- You need an official cross-language workflow SDK, globally managed control
  plane, SQLite/MySQL storage, or cloud-managed autoscaling.
- External effects cannot be made idempotent across an at-least-once
  crash-before-journal window.

That is the honest launch position: Capstan is a compelling new option for a
specific Elixir workload shape, not a universal replacement on maturity alone.
