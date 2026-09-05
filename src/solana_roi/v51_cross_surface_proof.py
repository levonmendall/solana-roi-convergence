from __future__ import annotations

from typing import Any

from .strategy_v51_authority import authority
from .v51_evidence_analytics import (
    FUTURE_MATURE_CORRELATION_MAX_ABS,
    _audit_records,
    _portfolio_reconcile,
    _promotion_certification_from_records,
    build_cross_family_correlation,
    promotion_records,
)


CROSS_SURFACE_VERSION = "v51-cross-surface-proof-v1"


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


__all__ = [
    "CROSS_SURFACE_VERSION",
    "build_cross_surface_correlation",
    "build_cross_surface_maturity_allocation",
    "build_cross_surface_portfolio",
    "build_cross_surface_promotion_certification",
    "combined_promotion_records",
]
