# Durable steps

A plain background job retries *from the top*. That was fine when jobs were
"send an email"; it's ruinous when attempt one spent ninety seconds and $0.40
of LLM tokens before a network blip. Capstan's core primitive fixes the unit
of retry:

```elixir
def run(ctx) do
  text    = Capstan.step(ctx, :transcribe, fn -> whisper!(ctx.job.input["url"]) end)
  summary = Capstan.step(ctx, :summarize, fn -> llm!(text) end, cost: [usd: 0.02, tokens: 1200])

  Capstan.step(ctx, :store, fn -> MyApp.Store.put!(summary) end)

  {:ok, summary}
end
```

A step runs **at most once per job**. Its return value is serialized
(`:erlang.term_to_binary/1`) into the job's journal keyed by name; every
later attempt returns the stored value without executing the function. If
the job crashes after `:summarize`, the retry replays `:transcribe` and
`:summarize` from the journal in microseconds and resumes at `:store`.

## Semantics worth internalizing

- **Returned values are memoized — including `{:error, _}` tuples.** If a
  step should be re-run on retry, raise (or let the failing call raise)
  instead of returning an error value.
- **Step names are the identity.** Loop iterations need distinct names:
  `Capstan.step(ctx, "iteration-#{n}", ...)`.
- **Values must be term-serializable.** No pids, refs, or functions; there's
  a 1 MB per-value limit to keep the journal honest (store large artifacts
  elsewhere and memoize the reference).
- **Steps are the cancellation and budget checkpoints.** Cooperative
  cancellation and budget caps take effect at step boundaries — long-running
  step *functions* should be as small as the work allows.

## Durable sleep

```elixir
Capstan.step(ctx, :send_offer, fn -> send_offer!(user) end)
Capstan.sleep(ctx, :cooling_off, 3 * 86_400)
Capstan.step(ctx, :follow_up, fn -> follow_up!(user) end)
```

`sleep/3` memoizes its wake time under the given name and parks the job —
no process, no connection, no slot. Deploys and crashes during the three days
don't matter: on wake the replay skips `:send_offer`, passes the recorded
sleep, and runs `:follow_up`.

## Costs and budgets

Steps carry cost columns (`usd`, `tokens`). Budgets turn them into a
guarantee:

```elixir
Capstan.insert(MyApp.Capstan,
  MyApp.ResearchAgent.new(%{"topic" => topic}, budget: [usd: 5.00, tokens: 500_000]))
```

The engine checks accumulated spend against the budget both *before* running
each step body (against durable spend, so not even a crash mid-failure can
buy an extra step on retry) and *after* recording its cost, failing the job
with `:budget_exceeded` the moment either cap is crossed. The failed job
keeps its journal — you can inspect exactly which steps spent what with
`Capstan.steps/2`.

## The journal is a debugging asset

Because every step's value is recorded, `Capstan.Replay.dry_run/2` can re-run
a job's *code* against its *journal*: memoized reads return recorded values,
nothing side-effectful executes, and the first thing the recording never saw
is reported precisely. See the moduledoc of `Capstan.Replay` — it turns
"what did production actually do?" from archaeology into a function call.
