from __future__ import annotations

from typing import Any

from .strategy_v51_authority import AUTHORITY_ID, ECONOMIC_FREEZE_EPOCH
from .v51_candidate_pipeline import _record as _record_stage
from .v51_economic_core import bootstrap_execution_multiplier

HARNESS_VERSION = "v51-seeded-e2e-equivalence-v1"


def run_seeded_equivalence_case(store: Any, case: dict[str, Any]) -> dict[str, Any]:
    """Exercise the canonical eight-stage economic contract without network I/O.

    This is deliberately not a backtest. It is a deterministic architecture
    regression proving that a raw forward-like observation is transformed into
    context, exact execution evidence, a decision, paper position state,
    settlement and learning with explicit rejection reasons when it fails.
    """
    candidate_id = str(case["candidate_id"])
    release = str(case.get("release_commit") or "seeded-equivalence")
    surface = "SEEDED_E2E"
    _record_stage(store, surface=surface, candidate_id=candidate_id, release_commit=release, stage="ingestion", status="complete", reason="seeded_raw_observation_persisted", payload={"venue": case.get("venue"), "token": case.get("token")})
    _record_stage(store, surface=surface, candidate_id=candidate_id, release_commit=release, stage="candidate", status="complete", reason="seeded_candidate_classified", payload={"lane": case.get("lane"), "lifecycle": case.get("lifecycle")})
    _record_stage(store, surface=surface, candidate_id=candidate_id, release_commit=release, stage="context", status="complete", reason="seeded_risk_and_execution_context_built", payload={"risk_signature": case.get("risk_signature", "clean"), "risk_severity": case.get("risk_severity", 0.0)})

    if not bool(case.get("structurally_tradeable", True)):
        reason = "mechanical_hard_stop"
        _record_stage(store, surface=surface, candidate_id=candidate_id, release_commit=release, stage="execution_evidence", status="not_requested", reason=reason)
        _record_stage(store, surface=surface, candidate_id=candidate_id, release_commit=release, stage="decision", status="complete", reason=reason, payload={"decision": "paper_reject"})
        _record_stage(store, surface=surface, candidate_id=candidate_id, release_commit=release, stage="position", status="not_opened", reason=reason)
        return {"decision": "paper_reject", "reason": reason, "authority_id": AUTHORITY_ID, "economic_freeze_epoch": ECONOMIC_FREEZE_EPOCH}

    if not bool(case.get("entry_executable", True)) or not bool(case.get("exit_executable", True)):
        reason = "exact_entry_or_exit_execution_evidence_unavailable"
        _record_stage(store, surface=surface, candidate_id=candidate_id, release_commit=release, stage="execution_evidence", status="failed_closed", reason=reason)
        _record_stage(store, surface=surface, candidate_id=candidate_id, release_commit=release, stage="decision", status="complete", reason=reason, payload={"decision": "paper_reject"})
        _record_stage(store, surface=surface, candidate_id=candidate_id, release_commit=release, stage="position", status="not_opened", reason=reason)
        return {"decision": "paper_reject", "reason": reason, "authority_id": AUTHORITY_ID, "economic_freeze_epoch": ECONOMIC_FREEZE_EPOCH}

    multiplier = bootstrap_execution_multiplier(
        latency_seconds=float(case.get("latency_seconds", 0.0)),
        chase_fraction=float(case.get("chase_fraction", 0.0)),
        round_trip_cost_fraction=float(case.get("round_trip_cost_fraction", 0.0)),
        risk_severity=float(case.get("risk_severity", 0.0)),
        risk_signature=str(case.get("risk_signature") or "clean"),
    )
    _record_stage(store, surface=surface, candidate_id=candidate_id, release_commit=release, stage="execution_evidence", status="complete" if multiplier > 0 else "failed_closed", reason="exact_execution_context_scored", payload={"economic_multiplier": multiplier})
    requested = max(0.0, float(case.get("base_position_fraction", 0.01))) * multiplier
    if requested <= 0.0:
        reason = "latency_chase_cost_or_hazard_context_has_no_accessible_bootstrap_size"
        _record_stage(store, surface=surface, candidate_id=candidate_id, release_commit=release, stage="decision", status="complete", reason=reason, payload={"decision": "paper_reject"})
        _record_stage(store, surface=surface, candidate_id=candidate_id, release_commit=release, stage="position", status="not_opened", reason=reason)
        return {"decision": "paper_reject", "reason": reason, "authority_id": AUTHORITY_ID, "economic_freeze_epoch": ECONOMIC_FREEZE_EPOCH}

    _record_stage(store, surface=surface, candidate_id=candidate_id, release_commit=release, stage="decision", status="complete", reason="seeded_forward_economic_gate_passed", payload={"decision": "paper_enter", "position_fraction": requested})
    _record_stage(store, surface=surface, candidate_id=candidate_id, release_commit=release, stage="position", status="paper_position_authorized", reason="seeded_paper_position_opened", payload={"position_fraction": requested})
    net_return = float(case.get("settled_net_return", 0.0))
    _record_stage(store, surface=surface, candidate_id=candidate_id, release_commit=release, stage="settlement", status="complete", reason="seeded_paper_settlement", payload={"net_return": net_return})
    _record_stage(store, surface=surface, candidate_id=candidate_id, release_commit=release, stage="learning", status="complete", reason="seeded_outcome_available_to_learning", payload={"net_return": net_return})
    return {"decision": "paper_enter", "position_fraction": requested, "settled_net_return": net_return, "authority_id": AUTHORITY_ID, "economic_freeze_epoch": ECONOMIC_FREEZE_EPOCH}


__all__ = ["HARNESS_VERSION", "run_seeded_equivalence_case"]
