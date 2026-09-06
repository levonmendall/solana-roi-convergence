from __future__ import annotations

from typing import Any

from .strategy_v51_authority import AUTHORITY_ID, ECONOMIC_FREEZE_EPOCH
from .v51_seeded_e2e import run_seeded_equivalence_case
from .v51_synthetic_provenance import SYNTHETIC_SURFACE


CAPABILITY_VERSION = "v51-five-lane-isolated-capability-e2e-v1"

LANE_DESCRIPTORS: dict[str, dict[str, Any]] = {
    "pump_fun": {
        "economic_surface": "SOLANA",
        "venue": "PUMP_FUN",
        "lifecycle": "bonding_curve",
        "lane": "elite_wallet_continuation",
        "strategy_family": "PUMP_FUN",
        "negative_patch": {"entry_executable": False},
        "negative_reason": "exact_entry_or_exit_execution_evidence_unavailable",
    },
    "pump_amm": {
        "economic_surface": "SOLANA",
        "venue": "PUMP_AMM",
        "lifecycle": "early_post_graduation",
        "lane": "graduation_continuation",
        "strategy_family": "PUMP_AMM",
        "negative_patch": {"stale_candidate": True},
        "negative_reason": "stale_candidate",
    },
    "raydium": {
        "economic_surface": "SOLANA",
        "venue": "RAYDIUM",
        "lifecycle": "post_migration",
        "lane": "migration_continuation",
        "strategy_family": "RAYDIUM",
        "negative_patch": {"structurally_tradeable": False},
        "negative_reason": "mechanical_hard_stop",
    },
    "fomo": {
        "economic_surface": "FOMO",
        "venue": "PUMP_AMM",
        "lifecycle": "early_post_graduation",
        "lane": "fomo_continuation",
        "strategy_family": "FOMO_CLEAN",
        "negative_patch": {"hazard_evidence_sufficient": False},
        "negative_reason": "hazard_insufficient_evidence",
    },
    "robinhood": {
        "economic_surface": "ROBINHOOD_CHAIN",
        "venue": "UNISWAP_V3",
        "lifecycle": "new_weth_pool",
        "lane": "robinhood_entity_continuation",
        "strategy_family": "ROBINHOOD_ENTITY",
        "negative_patch": {"exposure_available": False},
        "negative_reason": "portfolio_exposure_exhausted",
    },
}


def _base_case(lane_name: str, *, qualifying: bool) -> dict[str, Any]:
    if lane_name not in LANE_DESCRIPTORS:
        raise ValueError(f"unknown Batch 3 lane: {lane_name}")
    descriptor = LANE_DESCRIPTORS[lane_name]
    suffix = "positive" if qualifying else "negative"
    case: dict[str, Any] = {
        "candidate_id": f"batch3-{lane_name}-{suffix}",
        "token": f"synthetic-token-{lane_name}",
        "surface": SYNTHETIC_SURFACE,
        "strict_synthetic_isolation": True,
        "economic_surface": descriptor["economic_surface"],
        "venue": descriptor["venue"],
        "lifecycle": descriptor["lifecycle"],
        "lane": descriptor["lane"],
        "strategy_family": descriptor["strategy_family"],
        "risk_signature": "clean",
        "risk_severity": 0.0,
        "structurally_tradeable": True,
        "entry_executable": True,
        "exit_executable": True,
        "hazard_evidence_sufficient": True,
        "exposure_available": True,
        "latency_seconds": 3.0,
        "chase_fraction": 0.08,
        "round_trip_cost_fraction": 0.03,
        "base_position_fraction": 0.01,
        "settled_net_return": 0.20,
        "release_commit": "batch3-isolated-capability",
        "synthetic_origin": f"batch3_lane_capability:{lane_name}:{suffix}",
    }
    if not qualifying:
        case.update(dict(descriptor["negative_patch"]))
    return case


def run_lane_capability_case(
    store: Any,
    lane_name: str,
    *,
    qualifying: bool,
) -> dict[str, Any]:
    case = _base_case(lane_name, qualifying=qualifying)
    result = run_seeded_equivalence_case(store, case)
    descriptor = LANE_DESCRIPTORS[lane_name]
    return {
        "capability_version": CAPABILITY_VERSION,
        "lane": lane_name,
        "strategy_family": descriptor["strategy_family"],
        "economic_surface": descriptor["economic_surface"],
        "venue": descriptor["venue"],
        "lifecycle": descriptor["lifecycle"],
        "qualifying_case": qualifying,
        "expected_negative_reason": None if qualifying else descriptor["negative_reason"],
        "result": result,
        "synthetic": True,
        "certification_eligible": False,
        "promotion_eligible": False,
        "authority_id": AUTHORITY_ID,
        "economic_freeze_epoch": ECONOMIC_FREEZE_EPOCH,
        "paper_only": True,
        "live_money_authority": False,
        "changes_strategy_authority": False,
        "changes_economic_thresholds": False,
    }


def run_five_lane_capability_matrix(store: Any) -> dict[str, Any]:
    lanes: dict[str, Any] = {}
    for lane_name in LANE_DESCRIPTORS:
        positive = run_lane_capability_case(store, lane_name, qualifying=True)
        negative = run_lane_capability_case(store, lane_name, qualifying=False)
        lanes[lane_name] = {"positive": positive, "negative": negative}
    return {
        "capability_version": CAPABILITY_VERSION,
        "lanes": lanes,
        "positive_case_count": len(lanes),
        "negative_case_count": len(lanes),
        "all_cases_synthetic": True,
        "certification_eligible_case_count": 0,
        "promotion_eligible_case_count": 0,
        "paper_only": True,
        "live_money_authority": False,
        "changes_strategy_authority": False,
        "changes_economic_thresholds": False,
    }


__all__ = [
    "CAPABILITY_VERSION",
    "LANE_DESCRIPTORS",
    "run_five_lane_capability_matrix",
    "run_lane_capability_case",
]
