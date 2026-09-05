from __future__ import annotations

import json
import urllib.request
import warnings
from typing import Any


STATUS_URL = "https://solana-roi-convergence.onrender.com/v1/ingestion/status"
EXPECTED_REPAIR_VERSION = "robinhood-wallet-selection-authority-boundary-v1"


def _find_key(value: Any, key: str) -> Any:
    if isinstance(value, dict):
        if key in value:
            return value[key]
        for child in value.values():
            found = _find_key(child, key)
            if found is not None:
                return found
    elif isinstance(value, list):
        for child in value:
            found = _find_key(child, key)
            if found is not None:
                return found
    return None


def test_live_robinhood_wallet_integrity_audit() -> None:
    request = urllib.request.Request(
        STATUS_URL,
        headers={"User-Agent": "solana-roi-wallet-integrity-verifier/1.0"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        payload = json.load(response)

    boundary = _find_key(payload, "robinhood_wallet_selection_authority_boundary")
    assert isinstance(boundary, dict), "live status is missing Robinhood wallet authority boundary telemetry"
    assert boundary.get("repair_version") == EXPECTED_REPAIR_VERSION
    assert boundary.get("wallet_selection_authority") == "wallet_quality_and_forward_followthrough_only"
    assert boundary.get("copyability_intelligence_mode") == "diagnostic_only"
    assert boundary.get("copyability_has_wallet_selection_authority") is False
    assert boundary.get("copyability_has_candidate_demotion_authority") is False
    assert boundary.get("copyability_has_paper_entry_authority") is False
    assert boundary.get("chase_has_wallet_selection_authority") is False
    assert boundary.get("latency_has_wallet_selection_authority") is False

    repair = boundary.get("state_integrity_repair")
    assert isinstance(repair, dict), "live state-integrity migration has not completed"
    assert repair.get("repair_version") == EXPECTED_REPAIR_VERSION
    assert repair.get("row_counts_preserved") is True
    assert repair.get("candidate_identity_set_preserved") is True
    assert repair.get("active_tracking_set_not_reduced_by_repair") is True

    active = list(boundary.get("currently_active_tracking_candidates") or [])
    preserved = list(repair.get("preserved_active_tracking_actors") or [])
    restored = list(repair.get("restored_misplaced_intelligence_rejections") or [])
    counts = dict(repair.get("durable_row_counts") or {})

    assert set(preserved).issubset(set(active)), (
        "a wallet recorded as active before the authority-boundary repair is no longer active"
    )

    warnings.warn(
        "LIVE_ROBINHOOD_WALLET_INTEGRITY="
        + json.dumps(
            {
                "repair_version": repair.get("repair_version"),
                "row_counts_preserved": repair.get("row_counts_preserved"),
                "candidate_identity_set_preserved": repair.get("candidate_identity_set_preserved"),
                "active_tracking_set_not_reduced_by_repair": repair.get("active_tracking_set_not_reduced_by_repair"),
                "preserved_active_tracking_count": repair.get("preserved_active_tracking_count"),
                "preserved_active_tracking_actors": preserved,
                "currently_active_tracking_candidates": active,
                "restored_misplaced_intelligence_rejection_count": repair.get("restored_misplaced_intelligence_rejection_count"),
                "restored_misplaced_intelligence_rejections": restored,
                "durable_row_counts": counts,
            },
            sort_keys=True,
        ),
        RuntimeWarning,
    )
