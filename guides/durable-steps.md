# Durable steps

A plain background job retries *from the top*. That was fine when jobs were
"send an email"; it's ruinous when attempt one spent ninety seconds and $0.40
of LLM tokens before a network blip. Belay's core primitive fixes the unit
of retry:

```elixir
def run(ctx) do
  text    = Belay.step(ctx, :transcribe, fn -> whisper!(ctx.job.input["url"]) end)
  summary = Belay.step(ctx, :summarize, fn -> llm!(text) end, cost: [usd: 0.02, tokens: 1200])

  Belay.step(ctx, :store, fn -> MyApp.Store.put!(summary) end)

  {:ok, summary}
end
```

A step's **committed result is memoized once per job and name**. Its return
value is serialized (`:erlang.term_to_binary/1`) into the journal; every later
attempt that sees that row returns the stored value without executing the
function.

The body itself is at-least-once until that write commits. A crash after an
external side effect but before the journal write can execute the body again.
Use a provider idempotency key derived from `{job_id, step_name}`, or make the
effect naturally idempotent. If the job crashes after `:summarize` has
committed, the retry replays `:transcribe` and `:summarize` in microseconds and
resumes at `:store`.

## Semantics worth internalizing

- **Returned values are memoized — including `{:error, _}` tuples.** If a
  step should be re-run on retry, raise (or let the failing call raise)
  instead of returning an error value.
- **Step names are the identity.** Loop iterations need distinct names:
  `Belay.step(ctx, "iteration-#{n}", ...)`.
- **Values must be term-serializable.** No pids, refs, or functions; there's
  a 1 MB per-value limit to keep the journal honest (store large artifacts
  elsewhere and memoize the reference).
- **Steps are the cancellation and budget checkpoints.** Cooperative
  cancellation and budget limits take effect at step boundaries — long-running
  step *functions* should be as small as the work allows.

## Durable sleep

```elixir
Belay.step(ctx, :send_offer, fn -> send_offer!(user) end)
Belay.sleep(ctx, :cooling_off, 3 * 86_400)
Belay.step(ctx, :follow_up, fn -> follow_up!(user) end)
```

`sleep/3` memoizes its wake time under the given name and parks the job —
no process, no connection, no slot. Deploys and crashes during the three days
don't matter: on wake the replay skips `:send_offer`, passes the recorded
sleep, and runs `:follow_up`.

## Costs and budgets

Steps carry declared cost columns (`usd`, `tokens`). Budgets provide a
fail-after-crossing limit:

```elixir
Belay.insert(MyApp.Belay,
  MyApp.ResearchAgent.new(%{"topic" => topic}, budget: [usd: 5.00, tokens: 500_000]))
```

The engine checks accumulated spend both *before* each unfinished step and
*after* recording its declared cost. The crossing step runs, commits, and then
fails the job with `:budget_exceeded`; later unfinished steps are refused. In
the modeled single-attempt path, overshoot is bounded by one step's declared
cost. The failed job keeps its journal, so `Belay.steps/2` shows exactly what
Belay recorded.

This is not a payment-provider hard stop. Actual usage can differ from the
declared estimate, and a provider charge made in the crash-before-journal
window can repeat. Reconcile actuals with `Belay.debit/3` and use provider
idempotency keys when a true external-spend guarantee matters.

## The journal is a debugging asset

Because every step's value is recorded, `Belay.Replay.dry_run/2` can re-run
a job's *code* against its *journal*: memoized reads return recorded values,
nothing side-effectful executes, and the first thing the recording never saw
is reported precisely. See the moduledoc of `Belay.Replay` — it turns
"what did production actually do?" from archaeology into a function call.
