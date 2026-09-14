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
EVIDENCE_PROVENANCE = {
    "OBSERVED PRODUCTION METRIC",
    "EXISTING PRODUCTION LOG/DIAGNOSTIC",
    "DERIVED FROM OBSERVED EVIDENCE",
}
STATE_PROVENANCE = EVIDENCE_PROVENANCE | {"CANONICAL SCHEMA/CONFIG", "UNKNOWN"}


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


def _exact_nonnegative_int(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise FidelityError(f"{label} must be an exact non-negative integer")
    return value


def _validate_production_scale(data: dict[str, Any]) -> None:
    if data.get("production_scale_claimed") is not True:
        return

    shape = data.get("production_shape")
    if not isinstance(shape, dict):
        raise FidelityError("production-scale claim lacks production_shape evidence")
    db_bytes = _exact_nonnegative_int(shape.get("sqlite_db_bytes"), "production_shape.sqlite_db_bytes")
    page_size = _exact_nonnegative_int(shape.get("sqlite_page_size"), "production_shape.sqlite_page_size")
    page_count = _exact_nonnegative_int(shape.get("sqlite_page_count"), "production_shape.sqlite_page_count")
    if page_size <= 0 or page_count <= 0 or db_bytes != page_size * page_count:
        raise FidelityError("production_shape SQLite geometry is inconsistent")
    if shape.get("provenance") not in EVIDENCE_PROVENANCE:
        raise FidelityError("production_shape lacks production evidence provenance")

    targets = data.get("state_cardinality_targets")
    inventory = data.get("fixture_inventory")
    if not isinstance(targets, dict):
        raise FidelityError("production-scale claim lacks state_cardinality_targets")
    if not isinstance(inventory, dict):
        raise FidelityError("production-scale claim lacks fixture_inventory")

    fixture_db_bytes = _exact_nonnegative_int(inventory.get("sqlite_db_bytes"), "fixture_inventory.sqlite_db_bytes")
    fixture_page_size = _exact_nonnegative_int(inventory.get("sqlite_page_size"), "fixture_inventory.sqlite_page_size")
    fixture_page_count = _exact_nonnegative_int(inventory.get("sqlite_page_count"), "fixture_inventory.sqlite_page_count")
    if (fixture_db_bytes, fixture_page_size, fixture_page_count) != (db_bytes, page_size, page_count):
        raise FidelityError("fixture SQLite geometry does not exactly match production evidence")

    fixture_states = inventory.get("state_families")
    if not isinstance(fixture_states, dict):
        raise FidelityError("fixture_inventory lacks state_families")
    missing_targets = sorted(REQUIRED_STATE_FAMILIES - targets.keys())
    missing_fixture_states = sorted(REQUIRED_STATE_FAMILIES - fixture_states.keys())
    if missing_targets:
        raise FidelityError("production-scale claim has unknown state cardinalities: " + ",".join(missing_targets))
    if missing_fixture_states:
        raise FidelityError("fixture inventory lacks state cardinalities: " + ",".join(missing_fixture_states))

    for name in REQUIRED_STATE_FAMILIES:
        target = targets[name]
        actual = fixture_states[name]
        if not isinstance(target, dict):
            raise FidelityError(f"state cardinality target {name} must be an object")
        if target.get("known") is not True:
            raise FidelityError(f"state cardinality target {name} is not proven known")
        expected_rows = _exact_nonnegative_int(target.get("rows"), f"state_cardinality_targets.{name}.rows")
        if target.get("provenance") not in EVIDENCE_PROVENANCE or not target.get("evidence"):
            raise FidelityError(f"state cardinality target {name} lacks production evidence")
        actual_rows = _exact_nonnegative_int(actual, f"fixture_inventory.state_families.{name}")
        if actual_rows != expected_rows:
            raise FidelityError(
                f"fixture state cardinality mismatch for {name}: expected {expected_rows}, got {actual_rows}"
            )


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
        if item.get("provenance") not in STATE_PROVENANCE:
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

    _validate_production_scale(data)


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
