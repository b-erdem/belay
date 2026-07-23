# Building agents on Belay

Agent workloads break classic job-queue assumptions: they run for minutes to
days, every retry re-buys tokens, they need human decisions mid-flight, they
fan out unpredictably, and they can spend real money fast. Belay's
primitives were designed against exactly that list.

## The durable agent loop

```elixir
defmodule MyApp.ResearchAgent do
  use Belay.Worker, queue: :ai, max_attempts: 10, timeout: {10, :second}

  @impl Belay.Worker
  def run(ctx) do
    goal = ctx.job.input["goal"]

    Enum.reduce_while(1..20, %{messages: [system_prompt(goal)]}, fn n, state ->
      # Once a turn commits, later attempts replay it without another call.
      turn =
        Belay.step(ctx, "turn-#{n}", fn -> llm_turn!(state.messages) end,
          cost: [usd: turn_cost(), tokens: turn_tokens()])

      Belay.emit(ctx, %{"turn" => n, "summary" => turn.summary})

      case Belay.steering(ctx) do
        %{"instruction" => extra} -> {:cont, apply_steering(state, turn, extra)}
        nil -> if turn.done?, do: {:halt, finish(ctx, turn)}, else: {:cont, advance(state, turn)}
      end
    end)
  end
end
```

The loop's committed checkpoints are durable: turns are journaled, progress streams
out through `emit/2`, an operator can `Belay.steer_job/3` new instructions that
the next boundary picks up, and a `budget:` on insert fails the run after a
step crosses the configured limit — with the journal intact for the
post-mortem. A provider call can still repeat in the crash-before-journal
window; pass `{job_id, step_name}` as its idempotency key when supported.

## Human in the loop

```elixir
draft = Belay.step(ctx, :draft, fn -> compose!(ctx) end)

case Belay.await(ctx, :approval, timeout: 86_400) do
  %{"approved" => true} -> Belay.step(ctx, :publish, fn -> publish!(draft) end)
  %{"approved" => false, "reason" => why} -> {:cancel, {:rejected, why}}
  {:error, :timeout} -> {:cancel, :approval_expired}
end
```

`await/3` parks the job — zero processes, zero cost — until
`Belay.signal_job(name, job_id, :approval, %{"approved" => true})` arrives
from your review UI (or from the MCP `signal` tool after enabling authorized
mutations). Signals are durable:
delivered before the job asks, they're found immediately; races cost latency,
never correctness.

## Gate the irreversible step, cap the run

Two failure modes hit tool-using agents in production. One is an agent that
takes an irreversible action, sends the email, executes the migration, places
the order, without a person seeing it first. The other is an agent that loops
and keeps calling a paid API until someone notices the bill. Belay handles
both with the primitives above, in one worker, because the job is where the
wait and the spend both live.

```elixir
defmodule MyApp.OutreachAgent do
  use Belay.Worker, queue: :ai, max_attempts: 5

  @impl Belay.Worker
  def run(ctx) do
    # Paid research steps. Each commits to the journal, so a retry, or a
    # resume after approval, does not run them (or pay for them) again.
    contacts =
      Belay.step(ctx, :research, fn -> find_contacts!(ctx.job.input) end,
        cost: [usd: 0.30, tokens: 18_000])

    drafts =
      Belay.step(ctx, :draft, fn -> draft_messages!(contacts) end,
        cost: [usd: 0.10, tokens: 6_000])

    # The irreversible action waits for a person. The job parks at zero cost
    # until your review UI (or the MCP `signal` tool) delivers the decision.
    case Belay.await(ctx, :approval, timeout: 3 * 86_400) do
      %{"approved" => true} ->
        Belay.step(ctx, :send, fn -> send_all!(drafts) end)
        {:ok, %{"sent" => length(drafts)}}

      _ ->
        {:cancel, :not_approved}
    end
  end
end

# The whole run is bounded. Checked before each step against durable spend,
# so a loop or a retry cannot walk past the cap.
Belay.insert(MyApp.Belay,
  MyApp.OutreachAgent.new(%{"segment" => "trial-expired"}, budget: [usd: 2.00]))
```

Two properties are worth naming, because they are where general-purpose LLM
gateways and agent frameworks stop short:

- **The budget is per run, not per API key.** A gateway quota caps a whole
  key or team; it cannot say "this one outreach job may spend two dollars."
  Belay owns that axis because the budget rides on the job.
- **The cap is enforced, not just observed.** The check reads durable spend
  before each step, so the run stops before the next paid call instead of
  surfacing on a dashboard after the invoice. And because the work after
  approval resumes from committed steps, the research and drafting you
  already paid for are not repeated when the human approves an hour later.

## Fan-out / fan-in (map-reduce inside one job)

```elixir
results =
  ctx
  |> Belay.map_children(:chunks, MyApp.SummarizeChunk,
       Enum.map(chunks, &%{"text" => &1}))
  |> Enum.map(&Belay.Job.result/1)

merged = Belay.step(ctx, :merge, fn -> merge!(results) end)
```

Children are real jobs — they parallelize across the cluster, respect their
queue's limits, retry independently, and show up in `list_jobs`. The parent
parks until the last child lands. Spawning is memoized, so a parent crash
after spawning **cannot** duplicate children. For irregular shapes, use
`Belay.spawn/3` + `Belay.await_children/1` directly and grow the DAG at
runtime — the agent authors its own workflow.

## Provider limits in tokens, not requests

LLM providers meter tokens per minute; queues traditionally count requests.
Belay's rate limiter does both:

```elixir
queues: [
  ai: [limit: 10,
       rate: [allowed: 100_000, period: 60, resource: "anthropic", estimate: 2_000]]
]
```

Admission divides the window's remaining tokens by the per-job `estimate`;
inside the job you correct the record with reality:

```elixir
response = Belay.step(ctx, :call, fn -> claude!(prompt) end)
Belay.debit(ctx, "anthropic", response.usage.total_tokens)
```

The debit replaces the estimate with actual usage, so the window converges on
true consumption. Name the same `resource:` from several queues and they
share one provider budget.

## Streaming progress out

`Belay.emit/2` appends to a durable per-job event stream.
`Belay.subscribe_events/2` delivers live `{:belay_event, id, seq, payload}`
messages (a LiveView showing agent progress is a few lines);
`Belay.events/3` replays from any offset — reconnecting clients and
crashed subscribers lose nothing.

## Dispatch fast enough for tool-call fan-outs

Sub-agent dispatch is on the critical path of every agent turn. With the
opt-in Postgres notifier (`notifiers: [:local, :postgres]`), insert→result
round trips measure ~11ms p50 even across unconnected worker fleets — see
[operations](operations.md#dispatch-latency) for the full table. A
`map_children` fan-out of quick tool calls costs milliseconds of overhead
per child, not poll intervals.

## Let agents operate the queue

`mix belay.mcp --url postgres://...` serves an MCP server any assistant can
use to check `stats` and read jobs, steps, events, and workflows. Mutation
tools (`retry_job`, `cancel_job`, `signal`, `steer_job`) are advertised but
disabled by default.

**Authority for operating agents.** Enable writes with a pluggable
authorizer (`mix belay.mcp --authorizer MyGuard`, or `authorizer:` on
`Belay.MCP.serve/2`): a module deciding `authorize(tool, args) → :ok |
{:error, msg}` before any retry/cancel/signal/steer executes. This is the
natural mount point for capability-token systems like
[Legant](https://github.com/legant-dev/legant) — verify the agent's grant
offline and let a supervising agent steer only the jobs its token attenuates
to. The same idea extends to spawn chains: carry a grant in job `meta` and
attenuate it in `spawn/3` inputs, so a child agent can never hold more
authority than its parent.

For a trusted local client you may instead pass `--allow-mutations`, but an
authorizer is the safer default whenever another agent or user can reach the
MCP process.
