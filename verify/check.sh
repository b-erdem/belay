#!/bin/sh
set -eu

: "${TLA2TOOLS_JAR:?set TLA2TOOLS_JAR to tla2tools.jar}"

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
WORK=${TMPDIR:-/tmp}/capstan-formal-$$
trap 'rm -rf "$WORK"' EXIT INT TERM
mkdir -p "$WORK/canonical" "$WORK/mutants/attempt" \
  "$WORK/mutants/budget" "$WORK/mutants/cancel" "$WORK/mutants/retry"
cp "$ROOT/attempt_fence/AttemptFence.tla" "$ROOT/attempt_fence/AttemptFence.cfg" "$WORK/canonical/"
cp "$ROOT/attempt_fence/mutations/PreFixAttemptFence.tla" \
  "$ROOT/attempt_fence/mutations/PreFixAttemptFence.cfg" "$WORK/mutants/attempt/"
cp "$ROOT/spec/mutations/PreFixBudget.tla" \
  "$ROOT/spec/mutations/PreFixBudget.cfg" "$WORK/mutants/budget/"
cp "$ROOT/spec/mutations/PreFixCancelClear.tla" \
  "$ROOT/spec/mutations/PreFixCancelClear.cfg" "$WORK/mutants/cancel/"
cp "$ROOT/spec/mutations/PreFixRetry.tla" \
  "$ROOT/spec/mutations/PreFixRetry.cfg" "$WORK/mutants/retry/"

run_tlc() {
  dir=$1
  module=$2
  (
    cd "$dir"
    java -cp "$TLA2TOOLS_JAR" tlc2.TLC -workers 1 -config "$module.cfg" "$module"
  )
}

run_tlc "$WORK/canonical" AttemptFence

expect_violation() {
  dir=$1
  module=$2
  expected_status=$3
  log="$WORK/$module.log"

  set +e
  run_tlc "$dir" "$module" >"$log" 2>&1
  actual_status=$?
  set -e

  if [ "$actual_status" -ne "$expected_status" ]; then
    cat "$log" >&2
    echo "expected $module to fail with TLC exit $expected_status; got $actual_status" >&2
    exit 1
  fi

  reason=$(grep -m 1 -E '^Error: (Invariant|Action property)' "$log" || true)
  echo "$module: expected counterexample (${reason:-TLC exit $actual_status})"
}

expect_violation "$WORK/mutants/attempt" PreFixAttemptFence 12
expect_violation "$WORK/mutants/budget" PreFixBudget 12
expect_violation "$WORK/mutants/cancel" PreFixCancelClear 13
expect_violation "$WORK/mutants/retry" PreFixRetry 13

if [ -n "${ATTEST_BIN:-}" ]; then
  "$ATTEST_BIN" trace-check "$ROOT/spec/Spec.tla" "$ROOT/traces"
else
  echo "ATTEST_BIN is unset; skipped mechanical trace admission" >&2
fi
