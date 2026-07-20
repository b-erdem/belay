#!/bin/bash
# Accelerated chaos soak: 3 worker OS processes, kill -9 every ~4s, periodic
# full Postgres restarts, then invariant verification (soak/driver.exs).
# Usage: soak/run.sh   (env: SOAK_CHAOS_SECS, SOAK_WAVES, SOAK_WAVE_MS)
set -u
cd "$(dirname "$0")/.."

export SOAK_URL="${SOAK_URL:-postgres://postgres:capstan@localhost:55433/capstan_soak}"
DBCONT="${SOAK_DB_CONTAINER:-capstan-postgres}"
CHAOS_SECS="${SOAK_CHAOS_SECS:-150}"
export SOAK_WAVES="${SOAK_WAVES:-36}"
export SOAK_WAVE_MS="${SOAK_WAVE_MS:-3500}"

mkdir -p soak/tmp
rm -f soak/tmp/*.log soak/REPORT.md

echo "== compiling =="
mix compile --warnings-as-errors || exit 2

echo "== resetting soak database =="
mix run --no-compile soak/init.exs || exit 2

declare -a PIDS

start_worker() {
  local i=$1
  SOAK_TAG="w$i" nohup mix run --no-compile soak/worker.exs >> "soak/tmp/worker$i.log" 2>&1 &
  PIDS[$i]=$!
}

echo "== starting 3 workers =="
for i in 1 2 3; do start_worker "$i"; done
sleep 6

echo "== chaos for ${CHAOS_SECS}s (kill -9 every ~4s, DB restarts at 1/3 and 2/3) =="
(
  end=$((SECONDS + CHAOS_SECS))
  n=0
  third=$((CHAOS_SECS / 3))
  db1=$((SECONDS + third))
  db2=$((SECONDS + 2 * third))
  while [ $SECONDS -lt $end ]; do
    sleep 4
    i=$(((RANDOM % 3) + 1))
    if kill -9 "${PIDS[$i]}" 2>/dev/null; then
      echo "KILL w$i $(date +%s)" >> soak/tmp/chaos.log
    fi
    sleep 1
    start_worker "$i"
    n=$((n + 1))
    if [ $SECONDS -ge $db1 ]; then
      db1=$((end + 999999))
      echo "DBRESTART $(date +%s)" >> soak/tmp/chaos.log
      docker restart "$DBCONT" > /dev/null 2>&1
    fi
    if [ $SECONDS -ge $db2 ]; then
      db2=$((end + 999999))
      echo "DBRESTART $(date +%s)" >> soak/tmp/chaos.log
      docker restart "$DBCONT" > /dev/null 2>&1
    fi
  done
  echo "CHAOS DONE $(date +%s)" >> soak/tmp/chaos.log
) &
CHAOS_PID=$!

echo "== driver: workload + verification =="
mix run --no-compile soak/driver.exs
RC=$?

kill "$CHAOS_PID" 2>/dev/null
wait "$CHAOS_PID" 2>/dev/null
# The chaos subshell respawns workers under its own PID bookkeeping, so kill
# by pattern — otherwise respawned workers outlive the run holding DB
# connections.
pkill -9 -f "soak/worker.exs" 2>/dev/null

STALE=$(grep -h -c "stale ack" soak/tmp/worker*.log 2>/dev/null | paste -sd+ - | bc)
SKIPPED=$(grep -h -c "claim skipped" soak/tmp/worker*.log 2>/dev/null | paste -sd+ - | bc)
{
  echo ""
  echo "## Worker-side observations (from logs)"
  echo "- Fenced stale acks: ${STALE:-0}"
  echo "- Claim rounds skipped during outages: ${SKIPPED:-0}"
} >> soak/REPORT.md

echo "== done (rc=$RC) =="
exit $RC
