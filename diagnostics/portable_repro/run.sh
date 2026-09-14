#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUT="${PORTABLE_REPRO_OUTPUT:-/evidence}"
MANIFEST="${PORTABLE_REPRO_MANIFEST:-/fixtures/fidelity.json}"
PORT="${PORT:-10000}"
MAX_SECONDS="${PORTABLE_REPRO_MAX_SECONDS:-900}"
CANONICAL_SHA="92c0c1620f78116e7ecbeade039e9aedaf3a51a9"
ROBINHOOD_SCENARIO="${PORTABLE_REPRO_ROBINHOOD_SCENARIO:-alchemy-429-then-drpc}"
ROBINHOOD_FAULT_DELAY="${PORTABLE_REPRO_ROBINHOOD_FAULT_DELAY_SECONDS:-15}"

case "$ROBINHOOD_SCENARIO" in
  drpc-503-then-alchemy|drpc-timeout-then-alchemy) DEFAULT_ROBINHOOD_PRIMARY="drpc" ;;
  *) DEFAULT_ROBINHOOD_PRIMARY="alchemy" ;;
esac
ROBINHOOD_PRIMARY="${PORTABLE_REPRO_ROBINHOOD_PRIMARY:-$DEFAULT_ROBINHOOD_PRIMARY}"

mkdir -p "$OUT"
if [[ "${PORTABLE_REPRO_SOURCE_SHA:-}" != "$CANONICAL_SHA" ]]; then
  echo "refusing run: PORTABLE_REPRO_SOURCE_SHA does not match canonical SHA" >&2; exit 2
fi
if [[ "${EXPECTED_SOURCE_SHA:-$CANONICAL_SHA}" != "$CANONICAL_SHA" ]]; then
  echo "refusing run: EXPECTED_SOURCE_SHA does not match canonical SHA" >&2; exit 2
fi
if [[ ! -s "$ROOT/.canonical_source_sha" || ! -s "$ROOT/.harness_source_sha" || ! -s "$ROOT/.canonical_source_tree_sha256" ]]; then
  echo "refusing run: source provenance markers are missing" >&2; exit 2
fi
ACTUAL_MARKER_SHA="$(tr -d '\r\n' < "$ROOT/.canonical_source_sha")"
HARNESS_SHA="$(tr -d '\r\n' < "$ROOT/.harness_source_sha")"
EXPECTED_TREE_HASH="$(tr -d '\r\n' < "$ROOT/.canonical_source_tree_sha256")"
[[ "$ACTUAL_MARKER_SHA" == "$CANONICAL_SHA" ]] || { echo "refusing run: canonical source marker mismatch: $ACTUAL_MARKER_SHA" >&2; exit 2; }
ACTUAL_TREE_HASH="$(python - "$ROOT" <<'PY'
from __future__ import annotations
import hashlib, pathlib, sys
root = pathlib.Path(sys.argv[1])
paths = [root / "pyproject.toml"]
paths.extend(sorted(p for p in (root / "src").rglob("*") if p.is_file()))
h = hashlib.sha256()
for path in paths:
    h.update(path.relative_to(root).as_posix().encode()); h.update(b"\0"); h.update(path.read_bytes()); h.update(b"\0")
print(h.hexdigest())
PY
)"
[[ "$ACTUAL_TREE_HASH" == "$EXPECTED_TREE_HASH" ]] || { echo "refusing run: production source-tree hash mismatch" >&2; exit 2; }
printf '{"canonical_source_sha":"%s","harness_source_sha":"%s","canonical_source_tree_sha256":"%s","verified":true}\n' \
  "$CANONICAL_SHA" "$HARNESS_SHA" "$ACTUAL_TREE_HASH" > "$OUT/source-provenance-runtime.json"

# All gates and deterministic replay services are established before production import.
python "$ROOT/diagnostics/portable_repro/preflight.py" --output "$OUT/preflight.json"
python "$ROOT/diagnostics/portable_repro/fidelity.py" "$MANIFEST" | tee "$OUT/fidelity-validation.json"
"$ROOT/diagnostics/portable_repro/prepare_replay_tls.sh" "$OUT" >/dev/null
CERT="$OUT/replay-cert.pem"; KEY="$OUT/replay-key.pem"

python "$ROOT/diagnostics/portable_repro/solana_replay.py" \
  --cert "$CERT" --key "$KEY" \
  --evidence "$OUT/solana-replay.jsonl" --ready "$OUT/solana-replay-ready.json" \
  --notification-interval "${PORTABLE_REPRO_SOLANA_NOTIFICATION_SECONDS:-1.0}" &
SOLANA_REPLAY_PID=$!
python "$ROOT/diagnostics/portable_repro/robinhood_replay.py" \
  --cert "$CERT" --key "$KEY" \
  --evidence "$OUT/robinhood-replay.jsonl" --ready "$OUT/robinhood-replay-ready.json" \
  --scenario "$ROBINHOOD_SCENARIO" --fault-delay "$ROBINHOOD_FAULT_DELAY" &
ROBINHOOD_REPLAY_PID=$!

for ready in "$OUT/solana-replay-ready.json" "$OUT/robinhood-replay-ready.json"; do
  for _ in $(seq 1 100); do [[ -s "$ready" ]] && break; sleep 0.1; done
  [[ -s "$ready" ]] || { echo "replay endpoint failed readiness: $ready" >&2; exit 2; }
done

export PAPER_ONLY=true SOLANA_ROI_TRADE_MODE=paper SOLANA_ROI_PAPER_ONLY=1
export SOLANA_ROI_FAIL_CLOSED_NO_LIVE_TRADES=1 SOLANA_ROI_DISABLE_LIVE_ORDER_ENTRY=1
export PORT SSL_CERT_FILE="$CERT" REQUESTS_CA_BUNDLE="$CERT" CURL_CA_BUNDLE="$CERT"
export SOLANA_ROI_RPC_ENDPOINTS_JSON='[{"name":"replay-a","http":"https://127.0.0.1:20443","ws":"wss://127.0.0.1:20444"},{"name":"replay-b","http":"https://127.0.0.1:21443","ws":"wss://127.0.0.1:21444"}]'
export SOLANA_ROI_ALCHEMY_API_KEY=''
export ROBINHOOD_CHAIN_ENABLED=true
export ROBINHOOD_RPC_URL='https://127.0.0.1:18443' ROBINHOOD_WS_URL='wss://127.0.0.1:18444'
export ROBINHOOD_BACKUP_RPC_URL='https://127.0.0.1:19443' ROBINHOOD_BACKUP_WS_URL='wss://127.0.0.1:19444'
export ROBINHOOD_PROVIDER_PRIMARY="$ROBINHOOD_PRIMARY"
export ROBINHOOD_PROVIDER_FAILOVER_ERROR_THRESHOLD="${ROBINHOOD_PROVIDER_FAILOVER_ERROR_THRESHOLD:-1}"
export PYTHONPATH="$ROOT/diagnostics/portable_repro/network_guard${PYTHONPATH:+:$PYTHONPATH}"

# Prove the Python guard blocks DNS and direct IP independently of Docker --network=none.
python - <<'PY' > "$OUT/network-guard-negative-control.json"
import json, socket
result = {}
for name, fn in {
    "dns": lambda: socket.getaddrinfo("example.com", 443),
    "literal_connect": lambda: socket.create_connection(("8.8.8.8", 53), timeout=0.1),
}.items():
    try: fn()
    except OSError as exc: result[name] = {"blocked": True, "error": str(exc)}
    else: raise SystemExit(f"network negative control unexpectedly escaped: {name}")
print(json.dumps(result, sort_keys=True))
PY

python "$ROOT/diagnostics/portable_repro/collector.py" --output "$OUT/memory.jsonl" \
  --interval "${PORTABLE_REPRO_SAMPLE_SECONDS:-5}" --samples "${PORTABLE_REPRO_MAX_SAMPLES:-240}" &
COLLECTOR_PID=$!
python "$ROOT/diagnostics/portable_repro/observer.py" --base-url "http://127.0.0.1:$PORT" \
  --output "$OUT/http-observations.jsonl" --interval "${PORTABLE_REPRO_OBSERVER_SECONDS:-10}" \
  --samples "${PORTABLE_REPRO_OBSERVER_SAMPLES:-120}" &
OBSERVER_PID=$!

cleanup() {
  status=$?; trap - EXIT INT TERM
  kill "$COLLECTOR_PID" "$OBSERVER_PID" "$SOLANA_REPLAY_PID" "$ROBINHOOD_REPLAY_PID" 2>/dev/null || true
  wait "$COLLECTOR_PID" 2>/dev/null || true; wait "$OBSERVER_PID" 2>/dev/null || true
  wait "$SOLANA_REPLAY_PID" 2>/dev/null || true; wait "$ROBINHOOD_REPLAY_PID" 2>/dev/null || true
  cp /sys/fs/cgroup/memory.events "$OUT/memory.events.final" 2>/dev/null || true
  cp /sys/fs/cgroup/memory.stat "$OUT/memory.stat.final" 2>/dev/null || true
  cp /sys/fs/cgroup/memory.current "$OUT/memory.current.final" 2>/dev/null || true
  printf '{"exit_status":%s,"finished_at":%(%s)T}\n' "$status" -1 > "$OUT/termination.json"
  exit "$status"
}
trap cleanup EXIT INT TERM

timeout --signal=TERM --kill-after=15s "${MAX_SECONDS}s" \
  uvicorn solana_roi.production:app --host 0.0.0.0 --port "$PORT" 2>&1 | tee "$OUT/application.log"
