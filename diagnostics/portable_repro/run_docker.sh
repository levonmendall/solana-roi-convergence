#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CANONICAL_SHA="${CANONICAL_SHA:-92c0c1620f78116e7ecbeade039e9aedaf3a51a9}"
FIXTURES="${PORTABLE_REPRO_FIXTURES:-$ROOT/fixtures}"
EVIDENCE="${PORTABLE_REPRO_EVIDENCE:-$ROOT/evidence/portable-repro-$(date -u +%Y%m%dT%H%M%SZ)}"
IMAGE="${PORTABLE_REPRO_IMAGE:-solana-roi-portable-repro:$CANONICAL_SHA}"
NAME="portable-repro-${CANONICAL_SHA:0:8}-$$"

command -v docker >/dev/null 2>&1 || { echo "docker is required" >&2; exit 2; }
command -v git >/dev/null 2>&1 || { echo "git is required" >&2; exit 2; }
command -v tar >/dev/null 2>&1 || { echo "tar is required" >&2; exit 2; }
[[ -f "$FIXTURES/fidelity.json" ]] || { echo "missing $FIXTURES/fidelity.json" >&2; exit 2; }
git -C "$ROOT" cat-file -e "${CANONICAL_SHA}^{commit}" 2>/dev/null || {
  echo "canonical SHA is not present in this checkout: $CANONICAL_SHA" >&2
  exit 2
}
mkdir -p "$EVIDENCE"

BUILD_CONTEXT="$(mktemp -d)"
cleanup() {
  status=$?
  trap - EXIT INT TERM
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  rm -rf "$BUILD_CONTEXT"
  exit "$status"
}
trap cleanup EXIT INT TERM

# Production source is reconstructed from the immutable canonical commit. The only
# PR-head overlay permitted into the image is the diagnostic harness itself.
git -C "$ROOT" archive "$CANONICAL_SHA" | tar -x -C "$BUILD_CONTEXT"
mkdir -p "$BUILD_CONTEXT/diagnostics"
rm -rf "$BUILD_CONTEXT/diagnostics/portable_repro"
cp -a "$ROOT/diagnostics/portable_repro" "$BUILD_CONTEXT/diagnostics/portable_repro"

HARNESS_SHA="$(git -C "$ROOT" rev-parse HEAD)"
printf '%s\n' "$CANONICAL_SHA" > "$BUILD_CONTEXT/.canonical_source_sha"
printf '%s\n' "$HARNESS_SHA" > "$BUILD_CONTEXT/.harness_source_sha"
python - "$BUILD_CONTEXT" > "$BUILD_CONTEXT/.canonical_source_tree_sha256" <<'PY'
from __future__ import annotations
import hashlib
import pathlib
import sys
root = pathlib.Path(sys.argv[1])
paths = [root / "pyproject.toml"]
paths.extend(sorted(p for p in (root / "src").rglob("*") if p.is_file()))
h = hashlib.sha256()
for path in paths:
    rel = path.relative_to(root).as_posix().encode()
    h.update(rel); h.update(b"\0"); h.update(path.read_bytes()); h.update(b"\0")
print(h.hexdigest())
PY

cat > "$EVIDENCE/source-provenance.json" <<EOF
{"canonical_source_sha":"$CANONICAL_SHA","harness_source_sha":"$HARNESS_SHA","canonical_source_tree_sha256":"$(cat "$BUILD_CONTEXT/.canonical_source_tree_sha256")"}
EOF

docker build \
  --build-arg "SOURCE_SHA=$CANONICAL_SHA" \
  -f "$BUILD_CONTEXT/diagnostics/portable_repro/Dockerfile" \
  -t "$IMAGE" "$BUILD_CONTEXT" \
  2>&1 | tee "$EVIDENCE/docker-build.log"

CID="$(docker create \
  --name "$NAME" \
  --memory=2g --memory-swap=2g \
  --network=none \
  -v "$FIXTURES:/fixtures:ro" \
  -v "$EVIDENCE:/evidence" \
  -e PORTABLE_REPRO_MANIFEST=/fixtures/fidelity.json \
  -e EXPECTED_SOURCE_SHA="$CANONICAL_SHA" \
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
