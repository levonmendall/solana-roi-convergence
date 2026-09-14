#!/usr/bin/env python3
"""Validate portable-reproduction fidelity manifests before app startup."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

CANONICAL_SHA = "92c0c1620f78116e7ecbeade039e9aedaf3a51a9"
REQUIRED_STATE_FAMILIES = {
    "wallet_history",
    "direct_solana",
    "hydration",
    "forward_evidence",
    "events_checkpoints",
    "lifecycle_portfolio_control",
    "certification_replication",
    "shadow_price_tokens",
}
REQUIRED_PROVIDER_PATHS = {
    "solana_http",
    "solana_websocket",
    "jupiter",
    "robinhood_rpc",
    "blockscout",
}
ALLOWED_REPLAY_CLASSES = {
    "RECORDED HIGH-FIDELITY REPLAY",
    "SYNTHETIC BEHAVIORALLY REPRESENTATIVE REPLAY",
    "SYNTHETIC STRUCTURAL REPLAY",
}


class FidelityError(RuntimeError):
    pass


def _load(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise FidelityError(f"cannot load fidelity manifest {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise FidelityError("fidelity manifest root must be an object")
    return data


def validate_manifest(data: dict[str, Any]) -> None:
    if data.get("canonical_sha") != CANONICAL_SHA:
        raise FidelityError("canonical_sha mismatch")
    if data.get("network_mode") != "replay-only":
        raise FidelityError("network_mode must be 'replay-only'")
    if data.get("production_entrypoint") != "uvicorn solana_roi.production:app --host 0.0.0.0 --port $PORT":
        raise FidelityError("production_entrypoint mismatch")

    states = data.get("state_families")
    if not isinstance(states, dict):
        raise FidelityError("state_families must be an object")
    missing_states = sorted(REQUIRED_STATE_FAMILIES - states.keys())
    if missing_states:
        raise FidelityError("missing required state families: " + ",".join(missing_states))
    for name in REQUIRED_STATE_FAMILIES:
        item = states[name]
        if not isinstance(item, dict) or not item.get("fixture_path"):
            raise FidelityError(f"state family {name} lacks fixture_path")
        if item.get("provenance") not in {
            "OBSERVED PRODUCTION METRIC",
            "CANONICAL SCHEMA/CONFIG",
            "EXISTING PRODUCTION LOG/DIAGNOSTIC",
            "DERIVED FROM OBSERVED EVIDENCE",
            "UNKNOWN",
        }:
            raise FidelityError(f"state family {name} has invalid provenance")

    providers = data.get("provider_paths")
    if not isinstance(providers, dict):
        raise FidelityError("provider_paths must be an object")
    missing_providers = sorted(REQUIRED_PROVIDER_PATHS - providers.keys())
    if missing_providers:
        raise FidelityError("missing required provider paths: " + ",".join(missing_providers))
    for name in REQUIRED_PROVIDER_PATHS:
        item = providers[name]
        if not isinstance(item, dict):
            raise FidelityError(f"provider path {name} must be an object")
        if item.get("classification") not in ALLOWED_REPLAY_CLASSES:
            raise FidelityError(f"provider path {name} is not represented by replay")
        if item.get("live_network_allowed") is not False:
            raise FidelityError(f"provider path {name} permits live network")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("manifest")
    args = parser.parse_args()
    try:
        data = _load(Path(args.manifest))
        validate_manifest(data)
    except FidelityError as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, sort_keys=True))
        return 2
    print(json.dumps({"ok": True, "canonical_sha": CANONICAL_SHA}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
