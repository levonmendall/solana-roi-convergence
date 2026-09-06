from __future__ import annotations

from typing import Any

from .strategy_v51_authority import AUTHORITY_ID, ECONOMIC_FREEZE_EPOCH
from .v51_candidate_pipeline import _record as _record_stage
from .v51_economic_core import bootstrap_execution_multiplier
from .v51_synthetic_provenance import (
    SYNTHETIC_SURFACE,
    attach_synthetic_provenance,
    register_synthetic_provenance,
)

HARNESS_VERSION = "v51-seeded-e2e-equivalence-v3-synthetic-provenance"


def _stage(
    store: Any,
    *,
    candidate_id: str,
    release: str,
    provenance: dict[str, Any],
    stage: str,
    status: str,
    reason: str,
    payload: dict[str, Any] | None = None,
) -> None:
    _record_stage(
        store,
        surface=SYNTHETIC_SURFACE,
        candidate_id=candidate_id,
        release_commit=release,
        stage=stage,
        status=status,
        reason=reason,
        payload=attach_synthetic_provenance(payload, provenance),
    )


def _reject(
    store: Any,
    *,
    candidate_id: str,
    release: str,
    provenance: dict[str, Any],
    reason: str,
    execution_status: str = "not_requested",
) -> dict[str, Any]:
    _stage(
        store,
        candidate_id=candidate_id,
        release=release,
        provenance=provenance,
        stage="execution_evidence",
        status=execution_status,
        reason=reason,
    )
    _stage(
        store,
        candidate_id=candidate_id,
        release=release,
        provenance=provenance,
        stage="decision",
        status="complete",
        reason=reason,
        payload={"decision": "paper_reject"},
    )
    _stage(
        store,
        candidate_id=candidate_id,
        release=release,
        provenance=provenance,
        stage="position",
        status="not_opened",
        reason=reason,
    )
    return {
        "decision": "paper_reject",
        "reason": reason,
        "authority_id": AUTHORITY_ID,
        "economic_freeze_epoch": ECONOMIC_FREEZE_EPOCH,
        "synthetic": True,
        "certification_eligible": False,
        "promotion_eligible": False,
    }


def run_seeded_equivalence_case(store: Any, case: dict[str, Any]) -> dict[str, Any]:
    """Exercise the final eight-stage economic contract without provider network I/O.

    Every seeded case is structurally isolated on SEEDED_E2E and receives immutable
    synthetic provenance before the first stage is persisted. Lane/economic-surface
    identity lives in provenance and payloads; the harness can therefore prove the
    production economic contract without ever writing a canonical SOLANA/FOMO/
    ROBINHOOD_CHAIN candidate or economic-outcome surface.
    """
    requested_surface = str(case.get("surface") or SYNTHETIC_SURFACE)
    if requested_surface != SYNTHETIC_SURFACE:
        raise ValueError("seeded_e2e_must_use_isolated_synthetic_surface")

    candidate_id = str(case["candidate_id"])
    release = str(case.get("release_commit") or "seeded-equivalence")
    lane = str(case.get("lane") or "unclassified")
    economic_surface = str(case.get("economic_surface") or "SOLANA")
    venue = str(case.get("venue") or "UNKNOWN")
    provenance = register_synthetic_provenance(
        store,
        candidate_id=candidate_id,
        origin=str(case.get("synthetic_origin") or "seeded_equivalence"),
        lane=lane,
        economic_surface=economic_surface,
        venue=venue,
        release_commit=release,
    )

    _stage(
        store,
        candidate_id=candidate_id,
        release=release,
        provenance=provenance,
        stage="ingestion",
        status="complete",
        reason="seeded_raw_observation_persisted",
        payload={
            "venue": venue,
            "token": case.get("token"),
            "economic_surface": economic_surface,
        },
    )
    _stage(
        store,
        candidate_id=candidate_id,
        release=release,
        provenance=provenance,
        stage="candidate",
        status="complete",
        reason="seeded_candidate_classified",
        payload={
            "lane": lane,
            "lifecycle": case.get("lifecycle"),
            "economic_surface": economic_surface,
        },
    )

    if bool(case.get("stale_candidate", False)):
        _stage(
            store,
            candidate_id=candidate_id,
            release=release,
            provenance=provenance,
            stage="context",
            status="failed_closed",
            reason="stale_candidate",
        )
        return _reject(
            store,
            candidate_id=candidate_id,
            release=release,
            provenance=provenance,
            reason="stale_candidate",
        )
    if bool(case.get("stale_risk", False)):
        _stage(
            store,
            candidate_id=candidate_id,
            release=release,
            provenance=provenance,
            stage="context",
            status="failed_closed",
            reason="stale_risk",
        )
        return _reject(
            store,
            candidate_id=candidate_id,
            release=release,
            provenance=provenance,
            reason="stale_risk",
        )
    if not bool(case.get("hazard_evidence_sufficient", True)):
        _stage(
            store,
            candidate_id=candidate_id,
            release=release,
            provenance=provenance,
            stage="context",
            status="failed_closed",
            reason="hazard_insufficient_evidence",
        )
        return _reject(
            store,
            candidate_id=candidate_id,
            release=release,
            provenance=provenance,
            reason="hazard_insufficient_evidence",
        )

    _stage(
        store,
        candidate_id=candidate_id,
        release=release,
        provenance=provenance,
        stage="context",
        status="complete",
        reason="seeded_risk_and_execution_context_built",
        payload={
            "risk_signature": case.get("risk_signature", "clean"),
            "risk_severity": case.get("risk_severity", 0.0),
        },
    )

    if not bool(case.get("structurally_tradeable", True)):
        return _reject(
            store,
            candidate_id=candidate_id,
            release=release,
            provenance=provenance,
            reason="mechanical_hard_stop",
        )
    if not bool(case.get("entry_executable", True)) or not bool(case.get("exit_executable", True)):
        return _reject(
            store,
            candidate_id=candidate_id,
            release=release,
            provenance=provenance,
            reason="exact_entry_or_exit_execution_evidence_unavailable",
            execution_status="failed_closed",
        )
    if not bool(case.get("exposure_available", True)):
        return _reject(
            store,
            candidate_id=candidate_id,
            release=release,
            provenance=provenance,
            reason="portfolio_exposure_exhausted",
            execution_status="complete",
        )

    multiplier = bootstrap_execution_multiplier(
        latency_seconds=float(case.get("latency_seconds", 0.0)),
        chase_fraction=float(case.get("chase_fraction", 0.0)),
        round_trip_cost_fraction=float(case.get("round_trip_cost_fraction", 0.0)),
        risk_severity=float(case.get("risk_severity", 0.0)),
        risk_signature=str(case.get("risk_signature") or "clean"),
    )
    _stage(
        store,
        candidate_id=candidate_id,
        release=release,
        provenance=provenance,
        stage="execution_evidence",
        status="complete" if multiplier > 0 else "failed_closed",
        reason="exact_execution_context_scored",
        payload={"economic_multiplier": multiplier},
    )
    requested = max(0.0, float(case.get("base_position_fraction", 0.01))) * multiplier
    if requested <= 0.0:
        reason = "latency_chase_cost_or_hazard_context_has_no_accessible_bootstrap_size"
        _stage(
            store,
            candidate_id=candidate_id,
            release=release,
            provenance=provenance,
            stage="decision",
            status="complete",
            reason=reason,
            payload={"decision": "paper_reject"},
        )
        _stage(
            store,
            candidate_id=candidate_id,
            release=release,
            provenance=provenance,
            stage="position",
            status="not_opened",
            reason=reason,
        )
        return {
            "decision": "paper_reject",
            "reason": reason,
            "authority_id": AUTHORITY_ID,
            "economic_freeze_epoch": ECONOMIC_FREEZE_EPOCH,
            "synthetic": True,
            "certification_eligible": False,
            "promotion_eligible": False,
        }

    _stage(
        store,
        candidate_id=candidate_id,
        release=release,
        provenance=provenance,
        stage="decision",
        status="complete",
        reason="seeded_forward_economic_gate_passed",
        payload={"decision": "paper_enter", "position_fraction": requested},
    )
    _stage(
        store,
        candidate_id=candidate_id,
        release=release,
        provenance=provenance,
        stage="position",
        status="paper_position_authorized",
        reason="seeded_paper_position_opened",
        payload={"position_fraction": requested},
    )
    net_return = float(case.get("settled_net_return", 0.0))
    _stage(
        store,
        candidate_id=candidate_id,
        release=release,
        provenance=provenance,
        stage="settlement",
        status="complete",
        reason="seeded_paper_settlement",
        payload={"net_return": net_return},
    )
    _stage(
        store,
        candidate_id=candidate_id,
        release=release,
        provenance=provenance,
        stage="learning",
        status="complete",
        reason="seeded_outcome_available_to_learning",
        payload={"net_return": net_return},
    )
    return {
        "decision": "paper_enter",
        "position_fraction": requested,
        "settled_net_return": net_return,
        "authority_id": AUTHORITY_ID,
        "economic_freeze_epoch": ECONOMIC_FREEZE_EPOCH,
        "synthetic": True,
        "certification_eligible": False,
        "promotion_eligible": False,
    }


__all__ = ["HARNESS_VERSION", "run_seeded_equivalence_case"]
