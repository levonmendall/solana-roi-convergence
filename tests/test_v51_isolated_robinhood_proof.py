from __future__ import annotations

from solana_roi.strategy_v51_authority import AUTHORITY_ID, ECONOMIC_FREEZE_EPOCH
from solana_roi.v51_proof_merge import merge_candidate_coverages, merge_economic_certifications
from solana_roi.v51_strategy_api import _isolated_robinhood_proof


def test_api_accepts_only_live_nonblocking_robinhood_proof() -> None:
    proof = {
        "available": True,
        "authority_id": AUTHORITY_ID,
        "economic_freeze_epoch": ECONOMIC_FREEZE_EPOCH,
    }
    assert _isolated_robinhood_proof(
        lambda: {"runtime_ready": True, "failed_closed": False, "v51_proof": proof}
    ) == proof
    assert _isolated_robinhood_proof(
        lambda: {"runtime_ready": False, "failed_closed": True, "v51_proof": proof}
    ) is None
    assert _isolated_robinhood_proof(
        lambda: {"runtime_ready": True, "failed_closed": False, "v51_proof": {"available": False}}
    ) is None


def test_economic_proof_merge_adds_robinhood_without_sqlite_access() -> None:
    primary = {
        "authority_id": AUTHORITY_ID,
        "economic_freeze_epoch": ECONOMIC_FREEZE_EPOCH,
        "families": {
            "PUMP_AMM": {
                "closed_outcome_count": 20,
                "capital_efficiency_score": 0.02,
            }
        },
        "incremental_alpha": {
            "attributable_outcome_count": 0,
            "entity_family_attribution": {},
        },
        "closed_outcome_count": 20,
        "research_family_ranking": ["PUMP_AMM"],
        "paper_allocation_weights": {"PUMP_AMM": 0.25},
        "paper_cash_weight": 0.75,
    }
    secondary = {
        "authority_id": AUTHORITY_ID,
        "economic_freeze_epoch": ECONOMIC_FREEZE_EPOCH,
        "families": {
            "ROBINHOOD_CHAIN": {
                "closed_outcome_count": 12,
                "capital_efficiency_score": 0.01,
            }
        },
        "incremental_alpha": {
            "attributable_outcome_count": 5,
            "entity_family_attribution": {
                "ROBINHOOD_CHAIN|entity-a": {
                    "family": "ROBINHOOD_CHAIN",
                    "entity": "entity-a",
                    "matched_residual_sample_count": 5,
                }
            },
        },
    }
    merged = merge_economic_certifications(primary, secondary)
    assert merged["robinhood_proof_available"] is True
    assert merged["robinhood_proof_transport"] == "nonblocking_worker_status_cache"
    assert merged["closed_outcome_count"] == 32
    assert "ROBINHOOD_CHAIN" in merged["families"]
    assert "ROBINHOOD_CHAIN|entity-a" in merged["incremental_alpha"]["entity_family_attribution"]
    assert merged["paper_allocation_weights"]["ROBINHOOD_CHAIN"] <= 0.25


def test_candidate_coverage_merge_fails_closed_when_proof_missing() -> None:
    primary = {
        "authority_id": AUTHORITY_ID,
        "economic_freeze_epoch": ECONOMIC_FREEZE_EPOCH,
        "source_candidates_seen": {"solana": 4, "fomo": 2},
        "stage_summary": {},
        "coverage_debt_count": 0,
        "coverage_complete": True,
    }
    missing = merge_candidate_coverages(primary, None)
    assert missing["coverage_complete"] is False
    assert missing["robinhood_proof_available"] is False

    robinhood = {
        "authority_id": AUTHORITY_ID,
        "economic_freeze_epoch": ECONOMIC_FREEZE_EPOCH,
        "canonical_candidate_count": 3,
        "coverage_debt_count": 0,
        "coverage_complete": True,
        "stage_summary": {"decision": {"complete": 3}},
    }
    merged = merge_candidate_coverages(primary, robinhood)
    assert merged["coverage_complete"] is True
    assert merged["source_candidates_seen"]["robinhood"] == 3
    assert merged["stage_summary"]["ROBINHOOD_CHAIN"]["decision"]["complete"] == 3


def test_production_worker_status_producer_is_v51_proof_wrapped() -> None:
    # Existing isolation regressions intentionally replace _ORIGINAL_STATUS while
    # exercising worker internals. Re-run the idempotent final hook to model the
    # production/reload recovery contract rather than depending on suite order.
    from solana_roi import production  # noqa: F401
    from solana_roi import robinhood_worker_isolation_repair as isolation
    from solana_roi.v51_final_production_install import install_v51_final_production_hook

    install_v51_final_production_hook()

    assert isolation._ORIGINAL_STATUS is not None
    assert bool(getattr(isolation._ORIGINAL_STATUS, "_roi_v51_isolated_proof", False)) is True
