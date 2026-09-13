#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CANONICAL_SHA="92c0c1620f78116e7ecbeade039e9aedaf3a51a9"
FIXTURES="${PORTABLE_REPRO_FIXTURES:-$ROOT/fixtures}"
EVIDENCE="${PORTABLE_REPRO_EVIDENCE:-$ROOT/evidence/portable-repro-$(date -u +%Y%m%dT%H%M%SZ)}"
IMAGE="${PORTABLE_REPRO_IMAGE:-solana-roi-portable-repro:$CANONICAL_SHA}"
NAME="portable-repro-${CANONICAL_SHA:0:8}-$$"

command -v docker >/dev/null 2>&1 || { echo "docker is required" >&2; exit 2; }
[[ -f "$FIXTURES/fidelity.json" ]] || { echo "missing $FIXTURES/fidelity.json" >&2; exit 2; }
mkdir -p "$EVIDENCE"

cleanup() {
  trap - EXIT INT TERM
  docker rm -f "$NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

docker build \
  --build-arg "SOURCE_SHA=$CANONICAL_SHA" \
  -f "$ROOT/diagnostics/portable_repro/Dockerfile" \
  -t "$IMAGE" "$ROOT" \
  2>&1 | tee "$EVIDENCE/docker-build.log"

CID="$(docker create \
  --name "$NAME" \
  --memory=2g --memory-swap=2g \
  --network=none \
  -v "$FIXTURES:/fixtures:ro" \
  -v "$EVIDENCE:/evidence" \
  -e PORTABLE_REPRO_MANIFEST=/fixtures/fidelity.json \
  "$IMAGE")"
printf '%s\n' "$CID" > "$EVIDENCE/container-id.txt"

docker start "$NAME" >/dev/null
PID="$(docker inspect -f '{{.State.Pid}}' "$NAME")"
printf '%s\n' "$PID" > "$EVIDENCE/container-init-pid.txt"

python "$ROOT/diagnostics/portable_repro/host_collect.py" \
  --pid "$PID" --output "$EVIDENCE/host-memory.jsonl" \
  --interval "${PORTABLE_REPRO_HOST_SAMPLE_SECONDS:-2}" &
HOST_COLLECTOR_PID=$!

set +e
EXIT_CODE="$(docker wait "$NAME")"
WAIT_STATUS=$?
set -e

kill "$HOST_COLLECTOR_PID" 2>/dev/null || true
wait "$HOST_COLLECTOR_PID" 2>/dev/null || true

docker logs "$NAME" > "$EVIDENCE/docker-stdout-stderr.log" 2>&1 || true
docker inspect "$NAME" > "$EVIDENCE/docker-inspect.json"
printf '{"docker_wait_status":%s,"container_exit_code":%s}\n' "$WAIT_STATUS" "$EXIT_CODE" > "$EVIDENCE/host-termination.json"

echo "evidence: $EVIDENCE"
exit "$WAIT_STATUS"
