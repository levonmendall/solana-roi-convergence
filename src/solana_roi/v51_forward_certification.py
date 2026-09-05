"""Live forward-proof certification for frozen v5.1 paper economics.

This module composes existing production transport telemetry and evidence-validity
products. It is deliberately read-only: it cannot change strategy economics,
allocation caps, entry/exit rules, signing, submission, or live-money authority.
"""
from __future__ import annotations

import os
from typing import Any, Callable

from .strategy_v51_authority import authority
from .v51_cross_surface_proof import build_cross_surface_evidence_bundle
from .v51_measurement_integrity import cached_proof_state

CERTIFICATION_VERSION = "v51-live-forward-certification-v2-cross-surface"


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _eligible_families(promotion: dict[str, Any]) -> list[str]:
    families = _dict(promotion.get("families"))
    return sorted(
        str(name)
        for name, proof in families.items()
        if isinstance(proof, dict) and bool(proof.get("promotion_claim_valid"))
    )


def _future_mature_families(maturity: dict[str, Any]) -> list[str]:
    families = _dict(maturity.get("families"))
    return sorted(
        str(name)
        for name, proof in families.items()
        if isinstance(proof, dict) and bool(proof.get("future_50pct_eligibility"))
    )


def _hazard_observations(hazard: dict[str, Any]) -> int:
    if hazard.get("observation_count") is not None:
        return int(hazard.get("observation_count") or 0)
    total = 0
    for row in _dict(hazard.get("bins")).values():
        if not isinstance(row, dict):
            continue
        total += int(row.get("settled_entered_count") or 0)
        total += int(row.get("rejected_resolved_count") or 0)
    return total


def _mature_correlation_pairs(correlation: dict[str, Any]) -> int:
    return sum(
        1
        for row in _dict(correlation.get("pairs")).values()
        if isinstance(row, dict) and bool(row.get("mature"))
    )


def _one_capital_base(evidence: dict[str, Any]) -> bool:
    portfolio = _dict(evidence.get("portfolio_reconciliation"))
    audit_portfolio = _dict(portfolio.get("audit_epoch_portfolio"))
    promotion_portfolio = _dict(portfolio.get("promotion_compatible_portfolio"))
    robinhood_portfolio = _dict(portfolio.get("robinhood_audit_portfolio"))
    local_ok = bool(
        portfolio.get("family_navs_are_not_summed_as_independent_capital")
        and audit_portfolio.get("overlapping_positions_share_one_capital_base")
        and promotion_portfolio.get("overlapping_positions_share_one_capital_base")
    )
    if robinhood_portfolio:
        local_ok = local_ok and bool(
            _dict(robinhood_portfolio.get("audit_epoch_portfolio")).get(
                "overlapping_positions_share_one_capital_base"
            )
            and _dict(robinhood_portfolio.get("promotion_compatible_portfolio")).get(
                "overlapping_positions_share_one_capital_base"
            )
        )
    return local_ok


def _cached_robinhood_proof(
    status_provider: Callable[[], dict[str, Any]] | None,
) -> tuple[dict[str, Any] | None, str]:
    """Read cached proof independently from current transport readiness.

    Transport readiness is gate 39. Historical/current-release evidence already
    published by the isolated worker remains auditable even if the transport later
    degrades; stale/incompatible proof still cannot satisfy attestation or SLO gates.
    """
    if status_provider is None:
        return None, "unavailable"
    try:
        status = status_provider()
    except Exception:
        return None, "unavailable"
    proof = status.get("v51_proof") if isinstance(status, dict) else None
    if not isinstance(proof, dict) or not bool(proof.get("available")):
        return None, "unavailable"
    state = cached_proof_state(proof)
    return proof, state


def compose_forward_certification(
    *,
    unified_status: dict[str, Any],
    evidence: dict[str, Any],
    expected_release_commit: str | None = None,
) -> dict[str, Any]:
    """Compose items 35-46 without creating new economic authority."""
    spec = authority()
    overall = _dict(unified_status.get("overall"))
    release_commit = str(unified_status.get("release_commit") or "").strip()
    expected = str(expected_release_commit or "").strip()
    release_bound = bool(release_commit) and (not expected or release_commit == expected)

    safety_ok = bool(
        overall.get("paper_only") is True
        and overall.get("live_money_authority") is False
        and overall.get("signing_available") is False
        and overall.get("transaction_submission_available") is False
    )

    transport_checks: dict[str, dict[str, Any]] = {}
    for surface in ("solana", "fomo", "robinhood"):
        payload = _dict(unified_status.get(surface))
        blockers = [str(item) for item in (payload.get("blockers") or [])]
        ready = bool(payload.get("runtime_ready")) and not blockers
        transport_checks[surface] = {
            "ready": ready,
            "runtime_ready": bool(payload.get("runtime_ready")),
            "blockers": blockers,
            "all_regimes_e2e_achievable": bool(payload.get("all_regimes_e2e_achievable")),
            "all_regimes_e2e_proven": bool(payload.get("all_regimes_e2e_proven")),
        }
    transports_ready = all(row["ready"] for row in transport_checks.values())

    slo = _dict(evidence.get("forward_proof_slo"))
    proof_state = str(slo.get("proof_state") or "unavailable")
    measurement_slo_ok = proof_state == "confirmed"
    recent_stage_events = int(slo.get("stage_events_last_60m") or 0)
    recent_forward_flow = measurement_slo_ok and recent_stage_events > 0

    attestation = _dict(evidence.get("release_attestation"))
    attested_release = str(attestation.get("release_commit") or "").strip()
    release_attested = bool(attestation.get("attested")) and bool(release_commit) and attested_release == release_commit

    promotion = _dict(evidence.get("promotion_certification"))
    eligible_families = _eligible_families(promotion)
    family_promotion_evidence_ready = bool(eligible_families)

    counterfactual = _dict(evidence.get("rejected_counterfactuals"))
    rejected_count = int(counterfactual.get("rejected_candidate_count") or 0)
    rejected_pending = int(counterfactual.get("pending_count") or 0)
    rejected_resolved = int(counterfactual.get("resolved_count") or 0)
    rejected_positive = int(counterfactual.get("resolved_positive_count") or 0)
    counterfactual_complete = rejected_pending == 0

    hazard = _dict(evidence.get("hazard_calibration"))
    hazard_observation_count = _hazard_observations(hazard)
    hazard_read_only = hazard.get("changes_current_hazard_multipliers") is False

    correlation = _dict(evidence.get("cross_family_correlation"))
    mature_correlation_pairs = _mature_correlation_pairs(correlation)
    maturity = _dict(evidence.get("maturity_allocation_proof"))
    future_mature_families = _future_mature_families(maturity)
    one_capital_base = _one_capital_base(evidence)

    hard_operational_gates_ok = bool(
        release_bound and safety_ok and transports_ready and measurement_slo_ok and one_capital_base
    )
    evidence_maturity_ok = bool(
        recent_forward_flow and release_attested and family_promotion_evidence_ready and counterfactual_complete
    )
    system_forward_certified = bool(hard_operational_gates_ok and evidence_maturity_ok)

    blockers: list[str] = []
    if not release_bound:
        blockers.append("exact_release_not_bound")
    if not safety_ok:
        blockers.append("paper_only_safety_boundary_not_proven")
    for surface, row in transport_checks.items():
        if not row["ready"]:
            blockers.append(f"{surface}_transport_not_ready")
    if not measurement_slo_ok:
        blockers.append(f"forward_measurement_{proof_state}")
    if not one_capital_base:
        blockers.append("one_capital_base_reconciliation_not_proven")
    if measurement_slo_ok and not recent_forward_flow:
        blockers.append("no_recent_forward_candidate_stage_events")
    if not release_attested:
        blockers.append("current_release_live_attestation_pending")
    if not family_promotion_evidence_ready:
        blockers.append("no_family_has_valid_frozen_v51_promotion_claim")
    if not counterfactual_complete:
        blockers.append("rejected_opportunity_counterfactuals_pending")

    if not release_bound:
        state = "release_mismatch"
    elif not safety_ok:
        state = "safety_boundary_failed"
    elif not transports_ready:
        state = "transport_degraded"
    elif not measurement_slo_ok or not one_capital_base:
        state = "measurement_degraded"
    elif not evidence_maturity_ok:
        state = "collecting_forward_evidence"
    else:
        state = "certified"

    current_cap = float(spec["allocation"]["immature_family_max_weight"])
    permanent_cap = float(spec["allocation"]["permanent_family_max_weight"])

    return {
        "certification_version": CERTIFICATION_VERSION,
        "authority_id": spec["authority_id"],
        "strategy_version": spec["strategy_version"],
        "economic_freeze_epoch": spec["economic_freeze_epoch"],
        "release_commit": release_commit or None,
        "expected_release_commit": expected or None,
        "state": state,
        "system_forward_certified": system_forward_certified,
        "hard_operational_gates_ok": hard_operational_gates_ok,
        "evidence_maturity_ok": evidence_maturity_ok,
        "blockers": list(dict.fromkeys(blockers)),
        "robinhood_proof_state": evidence.get("robinhood_proof_state"),
        "checks": {
            "35_exact_live_release": {
                "pass": release_bound,
                "release_commit": release_commit or None,
                "expected_release_commit": expected or None,
            },
            "36_paper_only_safety_boundary": {
                "pass": safety_ok,
                "paper_only": overall.get("paper_only"),
                "live_money_authority": overall.get("live_money_authority"),
                "signing_available": overall.get("signing_available"),
                "transaction_submission_available": overall.get("transaction_submission_available"),
            },
            "37_solana_transport": transport_checks["solana"],
            "38_fomo_transport": transport_checks["fomo"],
            "39_robinhood_transport": transport_checks["robinhood"],
            "40_real_forward_candidate_flow": {
                "pass": recent_forward_flow,
                "proof_state": proof_state,
                "stage_events_last_60m": recent_stage_events,
                "coverage_debt_count": int(slo.get("coverage_debt_count") or 0),
                "local_proof_state": slo.get("local_proof_state"),
                "robinhood_forward_proof_state": slo.get("robinhood_forward_proof_state"),
            },
            "41_current_release_attestation": {
                "pass": release_attested,
                "attested": bool(attestation.get("attested")),
                "attested_release_commit": attested_release or None,
                "surfaces": attestation.get("surfaces"),
                "robinhood_proof_state": attestation.get("robinhood_proof_state"),
            },
            "42_validation_holdout_family_economics": {
                "pass": family_promotion_evidence_ready,
                "promotion_eligible_families": eligible_families,
                "family_count": len(_dict(promotion.get("families"))),
                "active_frozen_family_cap": current_cap,
            },
            "43_rejected_opportunity_counterfactuals": {
                "pass": counterfactual_complete,
                "rejected_candidate_count": rejected_count,
                "resolved_count": rejected_resolved,
                "pending_count": rejected_pending,
                "resolved_positive_count": rejected_positive,
                "robinhood_proof_state": counterfactual.get("robinhood_proof_state"),
                "retrospective_entry_authority": False,
            },
            "44_hazard_calibration": {
                "pass": hazard_read_only,
                "observation_count": hazard_observation_count,
                "changes_current_hazard_multipliers": hazard.get("changes_current_hazard_multipliers"),
                "diagnostic_only": True,
            },
            "45_correlation_and_one_capital_base": {
                "pass": one_capital_base,
                "one_capital_base": one_capital_base,
                "mature_correlation_pair_count": mature_correlation_pairs,
                "future_mature_scaling_eligible_families": future_mature_families,
                "active_frozen_family_cap": current_cap,
                "permanent_authority_ceiling": permanent_cap,
                "active_cap_changed": False,
            },
            "46_final_forward_certification": {
                "pass": system_forward_certified,
                "promotion_eligible_families_under_existing_v51_claims": eligible_families,
                "future_mature_scaling_eligible_families": future_mature_families,
                "changes_strategy_authority": False,
            },
        },
        "promotion_eligible_families_under_existing_v51_claims": eligible_families,
        "future_mature_scaling_eligible_families": future_mature_families,
        "active_frozen_family_cap": current_cap,
        "permanent_family_cap_ceiling": permanent_cap,
        "correlation_maturity_is_not_required_for_current_frozen_cap": True,
        "hazard_calibration_is_diagnostic_only": True,
        "changes_strategy_authority": False,
        "changes_economic_thresholds": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


def build_forward_certification(
    store: Any,
    *,
    unified_status: dict[str, Any],
    expected_release_commit: str | None = None,
    robinhood_proof: dict[str, Any] | None = None,
    robinhood_proof_state: str = "unavailable",
) -> dict[str, Any]:
    evidence = build_cross_surface_evidence_bundle(
        store,
        robinhood_proof,
        robinhood_proof_state=robinhood_proof_state,
    )
    return compose_forward_certification(
        unified_status=unified_status,
        evidence=evidence,
        expected_release_commit=expected_release_commit,
    )


def _e2e_status(app: Any) -> dict[str, Any]:
    for route in app.routes:
        if getattr(route, "path", None) == "/v1/strategy/e2e-status":
            payload = route.endpoint()
            if isinstance(payload, dict):
                return payload
            raise RuntimeError("strategy e2e status returned non-dict payload")
    raise RuntimeError("strategy e2e status route not found")


def install_forward_certification(
    app: Any,
    *,
    runtime_provider: Callable[[], Any],
    robinhood_status_provider: Callable[[], dict[str, Any]] | None = None,
) -> None:
    if bool(getattr(app.state, "roi_v51_forward_certification", False)):
        return
    existing_paths = {getattr(route, "path", None) for route in app.routes}
    if "/v1/strategy/forward-certification" not in existing_paths:
        def forward_certification_status() -> dict[str, Any]:
            runtime = runtime_provider()
            expected_release = os.getenv("RENDER_GIT_COMMIT", "").strip() or None
            proof, proof_state = _cached_robinhood_proof(robinhood_status_provider)
            return build_forward_certification(
                runtime.store,
                unified_status=_e2e_status(app),
                expected_release_commit=expected_release,
                robinhood_proof=proof,
                robinhood_proof_state=proof_state,
            )

        app.add_api_route(
            "/v1/strategy/forward-certification",
            forward_certification_status,
            methods=["GET"],
            name="v51_forward_certification",
        )
    app.state.roi_v51_forward_certification = True


__all__ = [
    "CERTIFICATION_VERSION",
    "_cached_robinhood_proof",
    "build_forward_certification",
    "compose_forward_certification",
    "install_forward_certification",
]
