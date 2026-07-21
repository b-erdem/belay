defmodule WakeProtocolTest do
  @moduledoc """
  Controlled-concurrency exploration of Belay's parent-wake protocol
  (SCHEMA.md race R1), using Lockstep to schedule every interleaving that
  matters instead of hoping a chaos soak samples the bad one.

  The real bug (found by the rc.2 soak, fixed in layers): a parent job parks
  awaiting a `$children` signal; each child's ack transaction counted its
  incomplete siblings and only signalled when the count hit zero. Under
  READ COMMITTED, the last two children acking concurrently each read the
  other's row *before* the other committed, both counted one incomplete
  sibling, both skipped the signal — parent parked forever.

  The model: one process is the database (a single serialized authority,
  like Postgres). Transaction visibility is explicit — writes buffer
  per-transaction, reads see committed state plus the reader's own buffer,
  commit merges. Signal delivery and parking serialize inside the database
  process, which is exactly the role of the `belay_sig:<scope>` advisory
  lock in the real system. The parent re-verifies completeness on every
  wake and parks with the signal count it last saw (no lost-wakeup window).

  Two variants share the parent; only the children differ:

    * `:count_gated` — the rc.1 protocol. Lockstep must FIND the lost wake
      (as a deadlock) within a bounded schedule budget: the bug-zoo-style
      assertion proves the model reproduces R1 deterministically, with a
      saved schedule.
    * `:unconditional` — the shipped protocol (signal on every terminal
      ack). Lockstep must find NO failing schedule across the budget.

  Abstraction notes: the real ack inserts the signal inside the ack
  transaction; the model signals after commit. The loss mechanism — the
  in-transaction sibling count — is unchanged. The rc.2 sweeper reconciler
  (third fix layer) is deliberately NOT modeled: the point of the
  `:unconditional` run is that the wake protocol alone is sound, so the
  reconciler is a belt over braces, not load-bearing.
  """

  use ExUnit.Case, async: false

  @children [:c1, :c2]

  # ---- The database process -------------------------------------------------

  defp db_start do
    Lockstep.spawn(fn ->
      db_loop(%{committed: %{}, txs: %{}, signals: 0, parked: nil})
    end)
  end

  defp db_loop(state) do
    case Lockstep.recv() do
      # Buffered transactional write (UPDATE ... inside BEGIN).
      {:write, tx, key, val, from} ->
        Lockstep.send(from, :ok)
        txs = Map.update(state.txs, tx, %{key => val}, &Map.put(&1, key, val))
        db_loop(%{state | txs: txs})

      # READ COMMITTED: committed rows plus this transaction's own writes.
      # A sibling's uncommitted ack is invisible — the heart of R1.
      {:count_incomplete, tx, me, from} ->
        visible = Map.merge(state.committed, Map.get(state.txs, tx, %{}))
        n = Enum.count(@children -- [me], fn c -> Map.get(visible, c) != :succeeded end)
        Lockstep.send(from, {:count, n})
        db_loop(state)

      {:commit, tx, from} ->
        committed = Map.merge(state.committed, Map.get(state.txs, tx, %{}))
        Lockstep.send(from, :ok)
        db_loop(%{state | committed: committed, txs: Map.delete(state.txs, tx)})

      {:read_committed, from} ->
        Lockstep.send(from, {:snapshot, state.committed})
        db_loop(state)

      # Signal delivery — serialized against parking by virtue of running
      # in this single process (the advisory lock's job in production).
      {:signal, from} ->
        Lockstep.send(from, :ok)
        state = %{state | signals: state.signals + 1}

        case state.parked do
          nil ->
            db_loop(state)

          parent ->
            Lockstep.send(parent, {:wake, state.signals})
            db_loop(%{state | parked: nil})
        end

      # Parking with the caller's last-seen signal count: if anything was
      # delivered since, refuse the park and make the parent re-verify.
      {:park, seen, parent} ->
        if state.signals > seen do
          Lockstep.send(parent, {:recheck, state.signals})
          db_loop(state)
        else
          db_loop(%{state | parked: parent})
        end
    end
  end

  defp call(db, msg) do
    Lockstep.send(db, msg)
    Lockstep.recv()
  end

  # ---- Children -------------------------------------------------------------

  # rc.1: signal only when this ack's transaction sees every sibling done.
  defp child_count_gated(db, me) do
    tx = {:tx, me}
    :ok = call(db, {:write, tx, me, :succeeded, self()})
    {:count, n} = call(db, {:count_incomplete, tx, me, self()})
    :ok = call(db, {:commit, tx, self()})

    if n == 0 do
      :ok = call(db, {:signal, self()})
    end
  end

  # Shipped protocol: every terminal ack signals, no counting.
  defp child_unconditional(db, me) do
    tx = {:tx, me}
    :ok = call(db, {:write, tx, me, :succeeded, self()})
    :ok = call(db, {:commit, tx, self()})
    :ok = call(db, {:signal, self()})
  end

  # ---- Parent ---------------------------------------------------------------

  # Re-verify on every wake; park carrying the signal count last seen.
  defp parent_loop(db, main, seen) do
    {:snapshot, committed} = call(db, {:read_committed, self()})

    if Enum.all?(@children, fn c -> Map.get(committed, c) == :succeeded end) do
      Lockstep.send(main, :parent_done)
    else
      Lockstep.send(db, {:park, seen, self()})

      case Lockstep.recv() do
        {:recheck, n} -> parent_loop(db, main, n)
        {:wake, n} -> parent_loop(db, main, n)
      end
    end
  end

  # ---- Harness --------------------------------------------------------------

  defp scenario(child_fun) do
    main = self()
    db = db_start()

    _parent = Lockstep.spawn(fn -> parent_loop(db, main, 0) end)

    for c <- @children do
      Lockstep.spawn(fn -> child_fun.(db, c) end)
    end

    # A lost wake leaves the parent parked and this recv blocked with no
    # runnable process anywhere: Lockstep reports the deadlock schedule.
    assert :parent_done == Lockstep.recv()
  end

  test "count-gated signalling loses the wake — Lockstep rediscovers R1" do
    bug =
      assert_raise Lockstep.BugFound, fn ->
        Lockstep.Runner.run(
          fn -> scenario(&child_count_gated/2) end,
          iterations: 200,
          strategy: :pct,
          max_steps: 300,
          seed: 0xCA9057A9,
          suite: "wake_protocol_count_gated"
        )
      end

    assert bug.iteration <= 50,
           "R1 should surface fast under PCT; took #{bug.iteration} iterations"

    IO.puts(:stderr, "  [wake] R1 rediscovered at iteration #{bug.iteration}")
  end

  test "unconditional signalling survives every explored schedule" do
    assert :ok ==
             Lockstep.Runner.run(
               fn -> scenario(&child_unconditional/2) end,
               iterations: 400,
               strategy: :pct,
               max_steps: 300,
               seed: 0xCA9057A9,
               suite: "wake_protocol_unconditional"
             )
  end
end
