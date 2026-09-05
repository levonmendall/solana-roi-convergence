from __future__ import annotations

from typing import Any

from .strategy_v51_authority import authority
from .v51_evidence_analytics import (
    FUTURE_MATURE_CORRELATION_MAX_ABS,
    _audit_records,
    _portfolio_reconcile,
    _promotion_certification_from_records,
    build_cross_family_correlation,
    build_evidence_validity_bundle,
    promotion_records,
)


CROSS_SURFACE_VERSION = "v51-cross-surface-proof-v2-forward-certification"


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _secondary_records(robinhood_proof: dict[str, Any] | None, key: str) -> list[dict[str, Any]]:
    if not isinstance(robinhood_proof, dict):
        return []
    rows = robinhood_proof.get(key)
    if not isinstance(rows, list):
        return []
    return [dict(row) for row in rows if isinstance(row, dict)]


def combined_promotion_records(store: Any, robinhood_proof: dict[str, Any] | None) -> list[dict[str, Any]]:
    primary = promotion_records(store)
    secondary = _secondary_records(robinhood_proof, "promotion_records")
    # Surface isolation prevents source ID collision from creating duplicate economic
    # authority. Keep the raw rows separate; event clustering later uses family+token+lifecycle.
    return [*primary, *secondary]


def build_cross_surface_promotion_certification(
    store: Any,
    robinhood_proof: dict[str, Any] | None,
) -> dict[str, Any]:
    rows = combined_promotion_records(store, robinhood_proof)
    result = _promotion_certification_from_records(rows)
    result["cross_surface_version"] = CROSS_SURFACE_VERSION
    result["robinhood_promotion_records_available"] = bool(
        _secondary_records(robinhood_proof, "promotion_records")
    )
    result["proof_transport"] = "canonical_store_plus_isolated_robinhood_cached_records"
    return result


def build_cross_surface_correlation(
    store: Any,
    robinhood_proof: dict[str, Any] | None,
) -> dict[str, Any]:
    return build_cross_family_correlation(
        store,
        rows=combined_promotion_records(store, robinhood_proof),
    )


def build_cross_surface_maturity_allocation(
    store: Any,
    robinhood_proof: dict[str, Any] | None,
) -> dict[str, Any]:
    certification = build_cross_surface_promotion_certification(store, robinhood_proof)
    correlation = build_cross_surface_correlation(store, robinhood_proof)
    by_family: dict[str, list[dict[str, Any]]] = {}
    for pair in correlation.get("pairs", {}).values():
        by_family.setdefault(str(pair.get("left")), []).append(pair)
        by_family.setdefault(str(pair.get("right")), []).append(pair)
    current_cap = float(authority()["allocation"]["immature_family_max_weight"])
    permanent_cap = float(authority()["allocation"]["permanent_family_max_weight"])
    families: dict[str, Any] = {}
    for family, proof in certification.get("families", {}).items():
        mature_pairs = [pair for pair in by_family.get(family, []) if bool(pair.get("mature"))]
        correlations = [
            abs(float(pair["pearson_correlation"]))
            for pair in mature_pairs
            if pair.get("pearson_correlation") is not None
        ]
        max_abs = max(correlations) if correlations else None
        material = (proof.get("execution_stress") or {}).get("material") or {}
        stressed_growth = material.get("best_expected_log_growth")
        stressed_positive = stressed_growth is not None and float(stressed_growth) > 0.0
        future_eligible = bool(
            proof.get("promotion_claim_valid")
            and max_abs is not None
            and max_abs <= FUTURE_MATURE_CORRELATION_MAX_ABS
            and stressed_positive
        )
        families[family] = {
            "current_frozen_cap": current_cap,
            "permanent_authority_ceiling": permanent_cap,
            "future_50pct_eligibility": future_eligible,
            "correlation_evidence_mature": bool(mature_pairs),
            "max_abs_mature_pair_correlation": max_abs,
            "material_stress_positive_expected_log_growth": stressed_positive,
            "promotion_claim_valid": bool(proof.get("promotion_claim_valid")),
            "active_cap_changed_by_this_proof": False,
        }
    return {
        "cross_surface_version": CROSS_SURFACE_VERSION,
        "active_allocation_cap_remains_frozen": current_cap,
        "future_permanent_ceiling": permanent_cap,
        "families": families,
        "paper_only": True,
        "live_money_authority": False,
    }


def build_cross_surface_portfolio(
    store: Any,
    robinhood_proof: dict[str, Any] | None,
) -> dict[str, Any]:
    # Main-thread code cannot inspect the private Robinhood trial store for entry times.
    # Cached Robinhood rows therefore fall back to settlement-time ordering when no
    # entry timestamp was already published. That degradation is counted explicitly by
    # _portfolio_reconcile rather than inventing concurrency precision.
    promotion = combined_promotion_records(store, robinhood_proof)
    audit = _audit_records(store)
    return {
        "cross_surface_version": CROSS_SURFACE_VERSION,
        "promotion_compatible_one_capital_base": _portfolio_reconcile(promotion),
        "canonical_solana_fomo_audit_one_capital_base": _portfolio_reconcile(audit),
        "robinhood_audit_portfolio": (
            robinhood_proof.get("portfolio_reconciliation") if isinstance(robinhood_proof, dict) else None
        ),
        "family_navs_are_not_summed_as_independent_capital": True,
        "cross_store_entry_time_precision": "explicit_fallback_count_reported_when_private_robinhood_entry_time_not_cached",
        "paper_only": True,
        "live_money_authority": False,
    }


def combine_release_attestation(
    local: dict[str, Any],
    robinhood: dict[str, Any] | None,
    *,
    robinhood_proof_state: str,
) -> dict[str, Any]:
    local = _dict(local)
    rh = _dict(robinhood)
    release = str(local.get("release_commit") or rh.get("release_commit") or "") or None
    local_surfaces = dict(_dict(local.get("surfaces")))
    rh_surfaces = dict(_dict(rh.get("surfaces")))
    surfaces = {**local_surfaces, **rh_surfaces}
    local_required = bool(local_surfaces) and all(bool(_dict(row).get("attested")) for row in local_surfaces.values())
    robinhood_required = bool(_dict(rh_surfaces.get("ROBINHOOD_CHAIN")).get("attested"))
    rh_usable = robinhood_proof_state == "confirmed"
    return {
        "release_commit": release,
        "measurement_epoch": local.get("measurement_epoch") or rh.get("measurement_epoch"),
        "attested": bool(local_required and robinhood_required and rh_usable),
        "surfaces": surfaces,
        "local_attested": local_required,
        "robinhood_attested": robinhood_required,
        "robinhood_proof_state": robinhood_proof_state,
        "requires_all_detected_local_surfaces_and_robinhood": True,
        "paper_only": True,
        "live_money_authority": False,
    }


def combine_rejected_counterfactuals(
    local: dict[str, Any],
    robinhood: dict[str, Any] | None,
    *,
    robinhood_proof_state: str,
) -> dict[str, Any]:
    local = _dict(local)
    rh = _dict(robinhood)
    total = int(local.get("rejected_candidate_count") or 0) + int(rh.get("rejected_candidate_count") or 0)
    resolved = int(local.get("resolved_count") or 0) + int(rh.get("resolved_count") or 0)
    pending = int(local.get("pending_count") or 0) + int(rh.get("pending_count") or 0)
    positive = int(local.get("resolved_positive_count") or 0) + int(rh.get("resolved_positive_count") or 0)
    return {
        "cross_surface_version": CROSS_SURFACE_VERSION,
        "rejected_candidate_count": total,
        "resolved_count": resolved,
        "pending_count": pending,
        "resolved_positive_count": positive,
        "local_solana_fomo": local,
        "isolated_robinhood": rh if rh else None,
        "robinhood_proof_state": robinhood_proof_state,
        "counterfactual_complete": pending == 0,
        "retrospective_entry_authority": False,
        "paper_only": True,
        "live_money_authority": False,
    }


def combine_forward_proof_slo(
    local: dict[str, Any],
    robinhood: dict[str, Any] | None,
    *,
    robinhood_proof_state: str,
) -> dict[str, Any]:
    local = _dict(local)
    rh = _dict(robinhood)
    local_state = str(local.get("proof_state") or "unavailable")
    rh_state = str(rh.get("proof_state") or "unavailable") if rh else "unavailable"
    if "degraded" in {local_state, rh_state}:
        state = "degraded"
    elif local_state == "confirmed" and rh_state == "confirmed" and robinhood_proof_state == "confirmed":
        state = "confirmed"
    elif local_state == "confirmed" and not rh:
        state = "partial"
    else:
        state = "unavailable"
    return {
        "cross_surface_version": CROSS_SURFACE_VERSION,
        "proof_state": state,
        "local_proof_state": local_state,
        "robinhood_forward_proof_state": rh_state,
        "robinhood_proof_state": robinhood_proof_state,
        "stage_events_last_5m": int(local.get("stage_events_last_5m") or 0) + int(rh.get("stage_events_last_5m") or 0),
        "stage_events_last_60m": int(local.get("stage_events_last_60m") or 0) + int(rh.get("stage_events_last_60m") or 0),
        "coverage_debt_count": int(local.get("coverage_debt_count") or 0) + int(rh.get("coverage_debt_count") or 0),
        "local_solana_fomo": local,
        "isolated_robinhood": rh if rh else None,
        "paper_only": True,
        "live_money_authority": False,
    }


def combine_hazard_calibration(
    local: dict[str, Any],
    robinhood: dict[str, Any] | None,
    *,
    robinhood_proof_state: str,
) -> dict[str, Any]:
    local = _dict(local)
    rh = _dict(robinhood)
    observation_count = 0
    for payload in (local, rh):
        for row in _dict(payload.get("bins")).values():
            if isinstance(row, dict):
                observation_count += int(row.get("settled_entered_count") or 0)
                observation_count += int(row.get("rejected_resolved_count") or 0)
    return {
        "cross_surface_version": CROSS_SURFACE_VERSION,
        "observation_count": observation_count,
        "local_solana_fomo": local,
        "isolated_robinhood": rh if rh else None,
        "robinhood_proof_state": robinhood_proof_state,
        "changes_current_hazard_multipliers": False,
        "diagnostic_only": True,
        "paper_only": True,
        "live_money_authority": False,
    }


def build_cross_surface_evidence_bundle(
    store: Any,
    robinhood_proof: dict[str, Any] | None,
    *,
    robinhood_proof_state: str,
) -> dict[str, Any]:
    """Compose the 35-46 proof plane without reading the private Robinhood SQLite file."""
    local = build_evidence_validity_bundle(store)
    rh = _dict(robinhood_proof)
    portfolio = build_cross_surface_portfolio(store, robinhood_proof)
    rh_portfolio = _dict(portfolio.get("robinhood_audit_portfolio"))
    return {
        "analytics_version": local.get("analytics_version"),
        "cross_surface_version": CROSS_SURFACE_VERSION,
        "release_attestation": combine_release_attestation(
            _dict(local.get("release_attestation")),
            _dict(rh.get("release_attestation")) if rh else None,
            robinhood_proof_state=robinhood_proof_state,
        ),
        "execution_cost_ledger": {
            "local_solana_fomo": local.get("execution_cost_ledger"),
            "isolated_robinhood": rh.get("execution_cost_ledger") if rh else None,
            "robinhood_proof_state": robinhood_proof_state,
        },
        "promotion_certification": build_cross_surface_promotion_certification(store, robinhood_proof),
        "rejected_counterfactuals": combine_rejected_counterfactuals(
            _dict(local.get("rejected_counterfactuals")),
            _dict(rh.get("rejected_counterfactuals")) if rh else None,
            robinhood_proof_state=robinhood_proof_state,
        ),
        "hazard_calibration": combine_hazard_calibration(
            _dict(local.get("hazard_calibration")),
            _dict(rh.get("hazard_calibration")) if rh else None,
            robinhood_proof_state=robinhood_proof_state,
        ),
        "cross_family_correlation": build_cross_surface_correlation(store, robinhood_proof),
        "maturity_allocation_proof": build_cross_surface_maturity_allocation(store, robinhood_proof),
        "portfolio_reconciliation": {
            "cross_surface_version": CROSS_SURFACE_VERSION,
            "family_navs_are_not_summed_as_independent_capital": bool(
                portfolio.get("family_navs_are_not_summed_as_independent_capital")
            ),
            "audit_epoch_portfolio": portfolio.get("canonical_solana_fomo_audit_one_capital_base"),
            "promotion_compatible_portfolio": portfolio.get("promotion_compatible_one_capital_base"),
            "robinhood_audit_portfolio": rh_portfolio if rh_portfolio else None,
            "paper_only": True,
            "live_money_authority": False,
        },
        "forward_proof_slo": combine_forward_proof_slo(
            _dict(local.get("forward_proof_slo")),
            _dict(rh.get("forward_proof_slo")) if rh else None,
            robinhood_proof_state=robinhood_proof_state,
        ),
        "robinhood_proof_state": robinhood_proof_state,
        "paper_only": True,
        "live_money_authority": False,
    }


__all__ = [
    "CROSS_SURFACE_VERSION",
    "build_cross_surface_correlation",
    "build_cross_surface_evidence_bundle",
    "build_cross_surface_maturity_allocation",
    "build_cross_surface_portfolio",
    "build_cross_surface_promotion_certification",
    "combine_forward_proof_slo",
    "combine_hazard_calibration",
    "combine_rejected_counterfactuals",
    "combine_release_attestation",
    "combined_promotion_records",
]
