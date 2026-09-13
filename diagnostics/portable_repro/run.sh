#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUT="${PORTABLE_REPRO_OUTPUT:-/evidence}"
MANIFEST="${PORTABLE_REPRO_MANIFEST:-/fixtures/fidelity.json}"
PORT="${PORT:-10000}"
MAX_SECONDS="${PORTABLE_REPRO_MAX_SECONDS:-900}"
CANONICAL_SHA="92c0c1620f78116e7ecbeade039e9aedaf3a51a9"

mkdir -p "$OUT"

if [[ "${PORTABLE_REPRO_SOURCE_SHA:-}" != "$CANONICAL_SHA" ]]; then
  echo "refusing run: PORTABLE_REPRO_SOURCE_SHA does not match canonical SHA" >&2
  exit 2
fi

# Everything below occurs before importing solana_roi.production.
python "$ROOT/diagnostics/portable_repro/preflight.py" --output "$OUT/preflight.json"
python "$ROOT/diagnostics/portable_repro/fidelity.py" "$MANIFEST" | tee "$OUT/fidelity-validation.json"

if command -v git >/dev/null 2>&1 && git -C "$ROOT" rev-parse HEAD >/dev/null 2>&1; then
  ACTUAL_SHA="$(git -C "$ROOT" rev-parse HEAD)"
  if [[ "$ACTUAL_SHA" != "$CANONICAL_SHA" ]]; then
    echo "refusing run: checkout $ACTUAL_SHA != canonical $CANONICAL_SHA" >&2
    exit 2
  fi
fi

export PAPER_ONLY=true
export PORT
export PYTHONPATH="$ROOT/diagnostics/portable_repro/network_guard${PYTHONPATH:+:$PYTHONPATH}"

python "$ROOT/diagnostics/portable_repro/collector.py" \
  --output "$OUT/memory.jsonl" \
  --interval "${PORTABLE_REPRO_SAMPLE_SECONDS:-5}" \
  --samples "${PORTABLE_REPRO_MAX_SAMPLES:-240}" &
COLLECTOR_PID=$!

python "$ROOT/diagnostics/portable_repro/observer.py" \
  --base-url "http://127.0.0.1:$PORT" \
  --output "$OUT/http-observations.jsonl" \
  --interval "${PORTABLE_REPRO_OBSERVER_SECONDS:-10}" \
  --samples "${PORTABLE_REPRO_OBSERVER_SAMPLES:-120}" &
OBSERVER_PID=$!

cleanup() {
  status=$?
  trap - EXIT INT TERM
  kill "$COLLECTOR_PID" "$OBSERVER_PID" 2>/dev/null || true
  wait "$COLLECTOR_PID" 2>/dev/null || true
  wait "$OBSERVER_PID" 2>/dev/null || true
  cp /sys/fs/cgroup/memory.events "$OUT/memory.events.final" 2>/dev/null || true
  cp /sys/fs/cgroup/memory.stat "$OUT/memory.stat.final" 2>/dev/null || true
  cp /sys/fs/cgroup/memory.current "$OUT/memory.current.final" 2>/dev/null || true
  printf '{"exit_status":%s,"finished_at":%(%s)T}\n' "$status" -1 > "$OUT/termination.json"
  exit "$status"
}
trap cleanup EXIT INT TERM

# GNU coreutils timeout returns 124 on wall-clock expiry. A whole-container OOM may
# bypass this trap, so run_docker.sh also preserves Docker state and host samples.
timeout --signal=TERM --kill-after=15s "${MAX_SECONDS}s" \
  uvicorn solana_roi.production:app --host 0.0.0.0 --port "$PORT" \
  2>&1 | tee "$OUT/application.log"
