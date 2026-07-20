#!/bin/bash
# Dispatch-latency benchmark across three topologies. Requires the
# capstan-postgres container (port 55433).
set -u
cd "$(dirname "$0")/.."

export BENCH_URL="${BENCH_URL:-postgres://postgres:capstan@localhost:55433/capstan_bench}"

mix compile --warnings-as-errors || exit 2

run_remote() {
  local notifiers=$1 label=$2 n=$3
  mix run --no-compile bench/init.exs > /dev/null
  BENCH_NOTIFIERS="$notifiers" nohup mix run --no-compile bench/worker.exs > /tmp/capstan_bench_worker.log 2>&1 &
  local wpid=$!
  sleep 5
  BENCH_MODE=remote BENCH_NOTIFIERS="$notifiers" BENCH_N="$n" BENCH_LABEL="$label" \
    mix run --no-compile bench/latency.exs
  kill -9 $wpid 2>/dev/null
  pkill -9 -f "bench/worker.exs" 2>/dev/null
  sleep 1
}

echo "== dispatch latency benchmark =="
mix run --no-compile bench/init.exs > /dev/null
BENCH_MODE=same_node BENCH_N=200 BENCH_LABEL="same-node (pokes)" mix run --no-compile bench/latency.exs

run_remote "local"          "cross-process (poll only)"    40
run_remote "local,postgres" "cross-process (pg_notify)"    200
