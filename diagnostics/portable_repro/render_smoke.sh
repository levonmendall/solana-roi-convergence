#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CANONICAL_SHA="92c0c1620f78116e7ecbeade039e9aedaf3a51a9"
OUT="${PORTABLE_REPRO_OUTPUT:-/tmp/portable-repro-evidence}"
FIXTURES="${PORTABLE_REPRO_FIXTURES:-/tmp/portable-repro-fixtures}"
mkdir -p "$OUT" "$FIXTURES"

command -v git >/dev/null 2>&1 || { echo "git required for Render source proof" >&2; exit 2; }
git -C "$ROOT" cat-file -e "${CANONICAL_SHA}^{commit}" 2>/dev/null || {
  echo "canonical commit unavailable in Render checkout" >&2; exit 2;
}
# Production-bearing files must be byte-identical to the canonical release. Harness
# files are intentionally allowed to differ because this is the draft diagnostic PR.
if ! git -C "$ROOT" diff --quiet "$CANONICAL_SHA" HEAD -- src pyproject.toml requirements.txt; then
  echo "production tree differs from canonical release; refusing smoke run" >&2
  git -C "$ROOT" diff --name-only "$CANONICAL_SHA" HEAD -- src pyproject.toml requirements.txt >&2 || true
  exit 2
fi

printf '%s\n' "$CANONICAL_SHA" > "$ROOT/.canonical_source_sha"
printf '%s\n' "$(git -C "$ROOT" rev-parse HEAD)" > "$ROOT/.harness_source_sha"
python - "$ROOT" > "$ROOT/.canonical_source_tree_sha256" <<'PY'
from __future__ import annotations
import hashlib, pathlib, sys
root = pathlib.Path(sys.argv[1])
paths = [root / "pyproject.toml"]
paths.extend(sorted(p for p in (root / "src").rglob("*") if p.is_file()))
h = hashlib.sha256()
for path in paths:
    h.update(path.relative_to(root).as_posix().encode()); h.update(b"\0")
    h.update(path.read_bytes()); h.update(b"\0")
print(h.hexdigest())
PY

SMOKE_DB="$FIXTURES/smoke.sqlite3"
python "$ROOT/diagnostics/portable_repro/generate_smoke_fixture.py" \
  --output "$SMOKE_DB" --manifest-output "$FIXTURES/smoke-fixture.json"

cat > "$FIXTURES/fidelity.json" <<EOF
{
  "canonical_sha": "$CANONICAL_SHA",
  "network_mode": "replay-only",
  "production_entrypoint": "uvicorn solana_roi.production:app --host 0.0.0.0 --port \$PORT",
  "state_families": {
    "wallet_history": {"fixture_path": "$SMOKE_DB", "provenance": "CANONICAL SCHEMA/CONFIG"},
    "direct_solana": {"fixture_path": "$SMOKE_DB", "provenance": "CANONICAL SCHEMA/CONFIG"},
    "hydration": {"fixture_path": "$SMOKE_DB", "provenance": "UNKNOWN"},
    "forward_evidence": {"fixture_path": "$SMOKE_DB", "provenance": "UNKNOWN"},
    "events_checkpoints": {"fixture_path": "$SMOKE_DB", "provenance": "UNKNOWN"},
    "lifecycle_portfolio_control": {"fixture_path": "$SMOKE_DB", "provenance": "UNKNOWN"},
    "certification_replication": {"fixture_path": "$SMOKE_DB", "provenance": "UNKNOWN"},
    "shadow_price_tokens": {"fixture_path": "$SMOKE_DB", "provenance": "UNKNOWN"}
  },
  "provider_paths": {
    "solana_http": {"classification": "SYNTHETIC STRUCTURAL REPLAY", "live_network_allowed": false},
    "solana_websocket": {"classification": "SYNTHETIC STRUCTURAL REPLAY", "live_network_allowed": false},
    "jupiter": {"classification": "SYNTHETIC STRUCTURAL REPLAY", "live_network_allowed": false},
    "robinhood_rpc": {"classification": "SYNTHETIC STRUCTURAL REPLAY", "live_network_allowed": false},
    "blockscout": {"classification": "SYNTHETIC STRUCTURAL REPLAY", "live_network_allowed": false}
  },
  "production_scale_claimed": false,
  "purpose": "REAL_CGROUP_HARNESS_SMOKE_ONLY_NOT_CAUSAL_REPRODUCTION"
}
EOF

export PORTABLE_REPRO_SOURCE_SHA="$CANONICAL_SHA"
export EXPECTED_SOURCE_SHA="$CANONICAL_SHA"
export PORTABLE_REPRO_OUTPUT="$OUT"
export PORTABLE_REPRO_MANIFEST="$FIXTURES/fidelity.json"
export SOLANA_ROI_DB_PATH="$SMOKE_DB"
export PORTABLE_REPRO_MAX_SECONDS="${PORTABLE_REPRO_MAX_SECONDS:-600}"

echo "PORTABLE_REPRO_RENDER_SMOKE head=$(git -C "$ROOT" rev-parse HEAD) canonical=$CANONICAL_SHA scale=SMOKE_ONLY"
exec "$ROOT/diagnostics/portable_repro/run.sh"
