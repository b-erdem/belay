# Building agents on Capstan

Agent workloads break classic job-queue assumptions: they run for minutes to
days, every retry re-buys tokens, they need human decisions mid-flight, they
fan out unpredictably, and they can spend real money fast. Capstan's
primitives were designed against exactly that list.

## The durable agent loop

```elixir
defmodule MyApp.ResearchAgent do
  use Capstan.Worker, queue: :ai, max_attempts: 10, timeout: {10, :second}

  @impl Capstan.Worker
  def run(ctx) do
    goal = ctx.job.input["goal"]

    Enum.reduce_while(1..20, %{messages: [system_prompt(goal)]}, fn n, state ->
      # One LLM turn = one step: a crash re-buys zero tokens.
      turn =
        Capstan.step(ctx, "turn-#{n}", fn -> llm_turn!(state.messages) end,
          cost: [usd: turn_cost(), tokens: turn_tokens()])

      Capstan.emit(ctx, %{"turn" => n, "summary" => turn.summary})

      case Capstan.steering(ctx) do
        %{"instruction" => extra} -> {:cont, apply_steering(state, turn, extra)}
        nil -> if turn.done?, do: {:halt, finish(ctx, turn)}, else: {:cont, advance(state, turn)}
      end
    end)
  end
end
```

Everything in that loop is durable: the turns are journaled, progress streams
out through `emit/2`, an operator can `Capstan.steer/3` new instructions that
the next boundary picks up, and a `budget:` on insert kills the run at the
spend cap — with the journal intact for the post-mortem.

## Human in the loop

```elixir
draft = Capstan.step(ctx, :draft, fn -> compose!(ctx) end)

case Capstan.await(ctx, :approval, timeout: 86_400) do
  %{"approved" => true} -> Capstan.step(ctx, :publish, fn -> publish!(draft) end)
  %{"approved" => false, "reason" => why} -> {:cancel, {:rejected, why}}
  {:error, :timeout} -> {:cancel, :approval_expired}
end
```

`await/3` parks the job — zero processes, zero cost — until
`Capstan.signal_job(name, job_id, :approval, %{"approved" => true})` arrives
from your review UI (or from the MCP `signal` tool). Signals are durable:
delivered before the job asks, they're found immediately; races cost latency,
never correctness.

## Fan-out / fan-in (map-reduce inside one job)

```elixir
results =
  ctx
  |> Capstan.map_children(:chunks, MyApp.SummarizeChunk,
       Enum.map(chunks, &%{"text" => &1}))
  |> Enum.map(&Capstan.Job.result/1)

merged = Capstan.step(ctx, :merge, fn -> merge!(results) end)
```

Children are real jobs — they parallelize across the cluster, respect their
queue's limits, retry independently, and show up in `list_jobs`. The parent
parks until the last child lands. Spawning is memoized, so a parent crash
after spawning **cannot** duplicate children. For irregular shapes, use
`Capstan.spawn/3` + `Capstan.await_children/1` directly and grow the DAG at
runtime — the agent authors its own workflow.

## Provider limits in tokens, not requests

LLM providers meter tokens per minute; queues traditionally count requests.
Capstan's rate limiter does both:

```elixir
queues: [
  ai: [limit: 10,
       rate: [allowed: 100_000, period: 60, resource: "anthropic", estimate: 2_000]]
]
```

Admission divides the window's remaining tokens by the per-job `estimate`;
inside the job you correct the record with reality:

```elixir
response = Capstan.step(ctx, :call, fn -> claude!(prompt) end)
Capstan.debit(ctx, "anthropic", response.usage.total_tokens)
```

The debit replaces the estimate with actual usage, so the window converges on
true consumption. Name the same `resource:` from several queues and they
share one provider budget.

## Streaming progress out

`Capstan.emit/2` appends to a durable per-job event stream.
`Capstan.subscribe_events/2` delivers live `{:capstan_event, id, seq, payload}`
messages (a LiveView showing agent progress is a few lines);
`Capstan.events/3` replays from any offset — reconnecting clients and
crashed subscribers lose nothing.

## Let agents operate the queue

`mix capstan.mcp --url postgres://...` serves an MCP server any assistant can
use to check `stats`, read a job's steps and events, `retry_job`,
`cancel_job`, deliver a `signal`, or `steer_job` — the infrastructure your
agents run on becomes infrastructure your agents can also inspect and fix.
