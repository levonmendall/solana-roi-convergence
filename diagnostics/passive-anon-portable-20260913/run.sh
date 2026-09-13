#!/usr/bin/env bash
# Run ONLY on a separately authorized disposable Linux Docker host with cgroup v2.
set -euo pipefail
if [[ $# != 6 ]]; then
  echo 'Usage: run.sh CANONICAL_REPO FIXTURES ENV_FILE PYTHON_IMAGE_DIGEST REPLAY_IMAGE_DIGEST OUTPUT_DIR' >&2
  exit 2
fi
REPO=$(realpath "$1")
FIXTURES=$(realpath "$2")
ENV_FILE=$(realpath "$3")
PYTHON_IMAGE=$4
REPLAY_IMAGE=$5
OUTPUT=$(realpath -m "$6")
HARNESS=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
SHA=92c0c1620f78116e7ecbeade039e9aedaf3a51a9
[[ "$PYTHON_IMAGE" == *@sha256:* && "$REPLAY_IMAGE" == *@sha256:* ]] || { echo 'Verified immutable image digests required' >&2; exit 2; }
[[ ! -e "$OUTPUT" ]] || { echo 'Output directory must be new' >&2; exit 2; }
[[ $(docker info --format '{{.CgroupVersion}}') == 2 ]] || { echo 'Native Linux cgroup v2 required' >&2; exit 2; }
git -C "$REPO" cat-file -e "$SHA^{commit}"
python3 "$HARNESS/validate_inputs.py" "$FIXTURES"
python3 - "$ENV_FILE" "$SHA" <<'PY'
import sys
lines=open(sys.argv[1]).read().splitlines()
env=dict(line.split('=',1) for line in lines if line and not line.startswith('#'))
required={'PAPER_ONLY':'true','SOLANA_ROI_PRODUCTION_CLEANUP_ENABLED':'false',
          'SOLANA_ROI_DB_PATH':'/state/solana-roi.sqlite3','SOLANA_ROI_RELEASE_COMMIT':sys.argv[2]}
for key,value in required.items():
    if env.get(key)!=value:raise SystemExit('Required disposable setting missing: '+key)
PY
mkdir -p "$OUTPUT/context/source" "$OUTPUT/context/harness" "$OUTPUT/control"
git -C "$REPO" archive "$SHA" | tar -x -C "$OUTPUT/context/source"
cp "$HARNESS/Dockerfile" "$OUTPUT/context/Dockerfile"
cp "$HARNESS/gate.py" "$OUTPUT/context/harness/gate.py"
RUN="roi-anon-repro-$(date -u +%Y%m%dT%H%M%SZ)-$$"
docker build --build-arg "PYTHON_IMAGE=$PYTHON_IMAGE" -t "$RUN" "$OUTPUT/context" >"$OUTPUT/build.log" 2>&1
docker image inspect "$RUN" >"$OUTPUT/image.json"
docker network create --internal "$RUN-net" >"$OUTPUT/network-id.txt"
docker volume create "$RUN-state" >"$OUTPUT/volume-name.txt"
# The replay adapter contract is specified in README.md. No live egress exists.
docker run -d --name "$RUN-replay" --network "$RUN-net" --network-alias replay \
  --mount "type=bind,src=$FIXTURES,dst=/fixtures,readonly" "$REPLAY_IMAGE" \
  --manifest /fixtures/manifest.json >"$OUTPUT/replay-container-id.txt"
docker run -d --name "$RUN-app" --memory=2147483648 --memory-swap=2147483648 \
  --cpus=1 --cgroupns=private --network "$RUN-net" --network-alias app \
  --env-file "$ENV_FILE" -p 127.0.0.1:18768:10000 \
  --mount "type=volume,src=$RUN-state,dst=/state" \
  --mount "type=bind,src=$OUTPUT/control,dst=/control" "$RUN" >"$OUTPUT/app-container-id.txt"
for attempt in $(seq 1 60); do
  [[ -f "$OUTPUT/control/isolation.json" ]] && break
  sleep 1
done
[[ -f "$OUTPUT/control/isolation.json" ]] || { echo 'Isolation NOT proven; state not loaded' >&2; exit 1; }
docker inspect "$RUN-app" >"$OUTPUT/container.json"
cat "$OUTPUT/control/isolation.json"
# Isolation gate passed before representative state is copied or app imported.
docker cp "$FIXTURES/state/." "$RUN-app:/state/"
touch "$OUTPUT/control/launch"
python3 "$HARNESS/collect.py" "$RUN-app" --output "$OUTPUT/measurements.jsonl" --seconds 900 --interval 60
docker logs "$RUN-app" >"$OUTPUT/application.log" 2>&1
echo "Observation complete; review fidelity before attribution. Disposable containers remain: $RUN-app $RUN-replay"
