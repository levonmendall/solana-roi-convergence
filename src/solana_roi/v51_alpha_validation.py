"""Prospective alpha validation for frozen ROI Convergence v5.1.

Items 47-58 answer whether the already-frozen strategy has earned trustworthy,
after-cost forward alpha.  This module is read-only with respect to strategy
authority: it cannot change thresholds, allocation caps, signing, submission, or
live-money capability.
"""
from __future__ import annotations

import os
from collections import defaultdict
from typing import Any, Callable

from .strategy_v51_authority import authority
from .v51_cross_surface_proof import (
    build_cross_surface_evidence_bundle,
    combined_promotion_records,
)
from .v51_economic_certification import _records as local_audit_records
from .v51_economic_certification import incremental_alpha_attribution
from .v51_evidence_analytics import _portfolio_reconcile
from .v51_forward_certification import _cached_robinhood_proof

ALPHA_CERTIFICATE_VERSION = "v51-prospective-alpha-validation-47-58-v1"


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _table_exists(store: Any, table: str) -> bool:
    try:
        with store._lock:
            return store.db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                (table,),
            ).fetchone() is not None
    except Exception:
        return False


def _family_states(promotion: dict[str, Any]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for family, raw in _dict(promotion.get("families")).items():
        proof = _dict(raw)
        independent_n = int(proof.get("independent_event_cluster_count") or 0)
        minimum_n = int(proof.get("minimum_independent_outcomes") or 0)
        validation_n = int(proof.get("validation_cluster_count") or 0)
        holdout_n = int(proof.get("holdout_cluster_count") or 0)
        kill = _dict(proof.get("promotion_kill_profile"))
        robust = _dict(proof.get("robust_profile"))
        stress = _dict(_dict(proof.get("execution_stress")).get("material"))
        killed = bool(kill.get("killed"))
        promoted = bool(proof.get("promotion_claim_valid"))
        growth = robust.get("best_expected_log_growth")
        stressed_growth = stress.get("best_expected_log_growth")
        enough_n = minimum_n > 0 and independent_n >= minimum_n
        if killed:
            state = "killed"
        elif promoted:
            state = "promoted"
        elif enough_n and growth is not None and float(growth) <= 0.0:
            state = "degrading"
        elif validation_n > 0 and holdout_n > 0:
            state = "holdout"
        elif validation_n > 0:
            state = "validation"
        else:
            state = "discovery"
        result[str(family)] = {
            "state": state,
            "promotion_claim_valid": promoted,
            "killed": killed,
            "raw_outcome_count": int(proof.get("raw_outcome_count") or 0),
            "independent_event_cluster_count": independent_n,
            "minimum_independent_outcomes": minimum_n,
            "validation_cluster_count": validation_n,
            "holdout_cluster_count": holdout_n,
            "fdr_accepted": bool(proof.get("fdr_accepted")),
            "best_expected_log_growth": growth,
            "material_stress_expected_log_growth": stressed_growth,
            "capital_efficiency_score": proof.get("capital_efficiency_score"),
            "changes_strategy_authority": False,
        }
    return result


def _candidate_flow(store: Any, robinhood_proof: dict[str, Any] | None, evidence: dict[str, Any]) -> dict[str, Any]:
    by_surface: dict[str, dict[str, int]] = {}
    if _table_exists(store, "v51_candidates"):
        with store._lock:
            for row in store.db.execute(
                "SELECT surface,COUNT(*) AS count FROM v51_candidates GROUP BY surface"
            ).fetchall():
                by_surface[str(row["surface"])] = {"canonical_candidates": int(row["count"] or 0)}
    if _table_exists(store, "v51_candidate_current_state"):
        with store._lock:
            rows = store.db.execute(
                "SELECT surface,stage,status,COUNT(*) AS count FROM v51_candidate_current_state "
                "GROUP BY surface,stage,status"
            ).fetchall()
        for row in rows:
            surface = str(row["surface"])
            target = by_surface.setdefault(surface, {"canonical_candidates": 0})
            key = f"{row['stage']}:{row['status']}"
            target[key] = int(row["count"] or 0)
    rh_coverage = _dict(_dict(robinhood_proof or {}).get("candidate_coverage"))
    if rh_coverage:
        by_surface["ROBINHOOD_CHAIN"] = {
            "canonical_candidates": int(rh_coverage.get("canonical_candidate_count") or 0),
            "decision_complete": int(rh_coverage.get("paper_entry_count") or 0)
            + int(rh_coverage.get("explicit_rejection_count") or 0),
            "paper_entries": int(rh_coverage.get("paper_entry_count") or 0),
            "settled_entries": int(rh_coverage.get("settled_entry_count") or 0),
            "pending_settlement": int(rh_coverage.get("pending_settlement_count") or 0),
            "coverage_debt": int(rh_coverage.get("coverage_debt_count") or 0),
        }
    total = sum(int(row.get("canonical_candidates") or 0) for row in by_surface.values())
    slo = _dict(evidence.get("forward_proof_slo"))
    recent = int(slo.get("stage_events_last_60m") or 0)
    debt = int(slo.get("coverage_debt_count") or 0)
    return {
        "canonical_candidate_count": total,
        "by_surface": by_surface,
        "recent_stage_events_last_60m": recent,
        "coverage_debt_count": debt,
        "has_prospective_candidates": total > 0 and recent > 0,
        "candidate_flow_complete": bool(total > 0 and recent > 0 and debt == 0),
        "candidate_source_of_truth": "canonical_append_only_candidate_ledger_plus_isolated_robinhood_candidate_ledger",
        "paper_only": True,
        "live_money_authority": False,
    }


def _settlement_proof(store: Any, robinhood_proof: dict[str, Any] | None) -> dict[str, Any]:
    by_surface: dict[str, dict[str, int]] = {}
    if _table_exists(store, "v51_candidate_current_state"):
        with store._lock:
            rows = store.db.execute(
                "SELECT surface,stage,status,COUNT(*) AS count FROM v51_candidate_current_state "
                "WHERE stage IN ('position','settlement') GROUP BY surface,stage,status"
            ).fetchall()
        for row in rows:
            target = by_surface.setdefault(str(row["surface"]), {})
            target[f"{row['stage']}:{row['status']}"] = int(row["count"] or 0)
    entered = settled = 0
    for row in by_surface.values():
        entered += int(row.get("position:paper_position_authorized") or 0)
        settled += int(row.get("settlement:complete") or 0)
    rh = _dict(_dict(robinhood_proof or {}).get("candidate_coverage"))
    if rh:
        rh_entered = int(rh.get("paper_entry_count") or 0)
        rh_settled = int(rh.get("settled_entry_count") or 0)
        by_surface["ROBINHOOD_CHAIN"] = {
            "paper_entries": rh_entered,
            "settled_entries": rh_settled,
            "pending_settlement": max(0, rh_entered - rh_settled),
        }
        entered += rh_entered
        settled += rh_settled
    pending = max(0, entered - settled)
    return {
        "paper_entry_count": entered,
        "settled_entry_count": settled,
        "pending_settlement_count": pending,
        "settlement_lineage_complete": pending == 0,
        "settlement_evidence_present": settled > 0,
        "settlement_proof_complete": pending == 0 and settled > 0,
        "by_surface": by_surface,
        "paper_only": True,
        "live_money_authority": False,
    }


def _audit_records(store: Any, robinhood_proof: dict[str, Any] | None) -> list[dict[str, Any]]:
    rows = [dict(row) for row in local_audit_records(store)]
    secondary = _dict(robinhood_proof or {}).get("audit_records")
    if isinstance(secondary, list):
        rows.extend(dict(row) for row in secondary if isinstance(row, dict))
    return rows


def _research_vs_promoted_nav(
    audit_rows: list[dict[str, Any]],
    promotion_rows: list[dict[str, Any]],
    promotion: dict[str, Any],
) -> dict[str, Any]:
    promoted_families = {
        str(family)
        for family, proof in _dict(promotion.get("families")).items()
        if isinstance(proof, dict) and bool(proof.get("promotion_claim_valid"))
    }
    promoted_keys = {
        (
            str(row.get("surface") or ""),
            str(row.get("source_signature") or row.get("trial_id") or row.get("id") or ""),
        )
        for row in promotion_rows
        if str(row.get("family") or "") in promoted_families
    }
    promoted: list[dict[str, Any]] = []
    research: list[dict[str, Any]] = []
    for row in audit_rows:
        key = (
            str(row.get("surface") or ""),
            str(row.get("source_signature") or row.get("trial_id") or row.get("id") or ""),
        )
        if key in promoted_keys:
            promoted.append(row)
        else:
            research.append(row)
    return {
        "promoted_families": sorted(promoted_families),
        "promoted_strategy_nav": _portfolio_reconcile(promoted),
        "research_probe_nav": _portfolio_reconcile(research),
        "combined_audit_nav": _portfolio_reconcile(audit_rows),
        "research_and_promoted_capital_reported_separately": True,
        "active_allocation_changed": False,
        "paper_only": True,
        "live_money_authority": False,
    }


def _wallet_entity_challengers(audit_rows: list[dict[str, Any]]) -> dict[str, Any]:
    attribution = incremental_alpha_attribution(audit_rows)
    candidates: list[dict[str, Any]] = []
    for proof in _dict(attribution.get("entity_family_attribution")).values():
        if not isinstance(proof, dict):
            continue
        residual = _dict(proof.get("residual_profile"))
        candidates.append(
            {
                "family": str(proof.get("family") or "UNKNOWN"),
                "entity": str(proof.get("entity") or "unknown"),
                "matched_residual_sample_count": int(proof.get("matched_residual_sample_count") or 0),
                "wallet_identity_adds_forward_edge": bool(proof.get("wallet_identity_adds_forward_edge")),
                "residual_mean_roi_fraction": residual.get("mean_return"),
                "residual_expected_log_growth": residual.get("best_expected_log_growth"),
                "promotion_authority": False,
            }
        )
    candidates.sort(
        key=lambda row: (
            bool(row["wallet_identity_adds_forward_edge"]),
            float(row["residual_expected_log_growth"] or float("-inf")),
            int(row["matched_residual_sample_count"]),
        ),
        reverse=True,
    )
    return {
        "ranking_unit": "forward_percentage_roi_residual_not_dollar_profit",
        "assignment_scope": "family_x_context_x_entity; no_cross_context_success_transfer",
        "challengers": candidates,
        "positive_forward_identity_challengers": [
            row for row in candidates if row["wallet_identity_adds_forward_edge"]
        ],
        "automatic_strategy_mutation": False,
        "paper_only": True,
        "live_money_authority": False,
    }


def compose_alpha_certificate(
    *,
    forward: dict[str, Any],
    evidence: dict[str, Any],
    candidate_flow: dict[str, Any],
    settlement: dict[str, Any],
    wallet_entity: dict[str, Any],
    capital_views: dict[str, Any],
) -> dict[str, Any]:
    spec = authority()
    promotion = _dict(evidence.get("promotion_certification"))
    families = _family_states(promotion)
    promoted = sorted(name for name, proof in families.items() if proof["state"] == "promoted")
    killed = sorted(name for name, proof in families.items() if proof["state"] == "killed")
    degrading = sorted(name for name, proof in families.items() if proof["state"] == "degrading")

    counterfactual = _dict(evidence.get("rejected_counterfactuals"))
    counter_pending = int(counterfactual.get("pending_count") or 0)
    counter_resolved = int(counterfactual.get("resolved_count") or 0)
    counter_positive = int(counterfactual.get("resolved_positive_count") or 0)
    counter_complete = counter_pending == 0

    continuity_ok = bool(forward.get("hard_operational_gates_ok"))
    candidate_ok = bool(candidate_flow.get("candidate_flow_complete"))
    settlement_ok = bool(settlement.get("settlement_proof_complete"))
    holdout_ok = bool(promoted)
    independent_ok = any(
        proof["independent_event_cluster_count"] >= max(1, proof["minimum_independent_outcomes"])
        and proof["validation_cluster_count"] > 0
        and proof["holdout_cluster_count"] > 0
        and proof["fdr_accepted"]
        for proof in families.values()
    )

    promoted_nav = _dict(capital_views.get("promoted_strategy_nav"))
    promoted_roi = promoted_nav.get("portfolio_roi_fraction")
    after_cost_alpha_proven = bool(
        continuity_ok
        and candidate_ok
        and settlement_ok
        and counter_complete
        and holdout_ok
        and independent_ok
        and promoted_roi is not None
        and float(promoted_roi) > 0.0
    )

    maturity = _dict(evidence.get("maturity_allocation_proof"))
    future_scaling = sorted(
        str(name)
        for name, proof in _dict(maturity.get("families")).items()
        if isinstance(proof, dict) and bool(proof.get("future_50pct_eligibility"))
    )

    if not continuity_ok:
        state = "operationally_degraded"
    elif not candidate_ok:
        state = "collecting_candidate_evidence"
    elif not settlement_ok:
        state = "collecting_settlement_evidence"
    elif not counter_complete:
        state = "resolving_rejected_counterfactuals"
    elif not promoted:
        state = "collecting_validation_holdout"
    elif after_cost_alpha_proven:
        state = "forward_alpha_proven"
    else:
        state = "alpha_not_yet_proven"

    blockers: list[str] = []
    if not continuity_ok:
        blockers.append("production_transport_or_measurement_continuity_not_ready")
    if not candidate_ok:
        blockers.append("prospective_candidate_flow_incomplete")
    if not settlement_ok:
        blockers.append("settled_entry_evidence_incomplete")
    if not counter_complete:
        blockers.append("rejected_opportunity_counterfactuals_pending")
    if not promoted:
        blockers.append("no_family_has_locked_holdout_promotion_claim")
    if not independent_ok:
        blockers.append("independent_event_statistical_proof_incomplete")

    return {
        "alpha_certificate_version": ALPHA_CERTIFICATE_VERSION,
        "authority_id": spec["authority_id"],
        "strategy_version": spec["strategy_version"],
        "economic_freeze_epoch": spec["economic_freeze_epoch"],
        "release_commit": forward.get("release_commit"),
        "state": state,
        "after_cost_positive_compounded_alpha_proven": after_cost_alpha_proven,
        "blockers": blockers,
        "checks": {
            "47_production_transport_continuity": {
                "pass": continuity_ok,
                "forward_certificate_state": forward.get("state"),
                "forward_blockers": forward.get("blockers"),
            },
            "48_prospective_candidate_flow_completeness": {
                "pass": candidate_ok,
                **candidate_flow,
            },
            "49_rejected_opportunity_counterfactual_resolution": {
                "pass": counter_complete,
                "rejected_candidate_count": int(counterfactual.get("rejected_candidate_count") or 0),
                "resolved_count": counter_resolved,
                "pending_count": counter_pending,
                "resolved_positive_count": counter_positive,
                "retrospective_entry_authority": False,
            },
            "50_settlement_and_realized_execution_proof": {
                "pass": settlement_ok,
                **settlement,
            },
            "51_locked_validation_holdout_accumulation": {
                "pass": holdout_ok,
                "families": families,
            },
            "52_independent_event_statistical_proof": {
                "pass": independent_ok,
                "multiple_testing_control": "Benjamini-Hochberg FDR on clustered validation/locked-holdout evidence",
                "families": families,
            },
            "53_family_promotion_engine": {
                "pass": bool(promoted),
                "promoted_families": promoted,
                "degrading_families": degrading,
                "killed_families": killed,
                "read_only_evidence_lifecycle": True,
            },
            "54_adaptive_wallet_entity_intelligence": {
                "pass": True,
                **wallet_entity,
            },
            "55_research_vs_promoted_capital": {
                "pass": bool(capital_views.get("research_and_promoted_capital_reported_separately")),
                **capital_views,
            },
            "56_portfolio_level_scaling_proof": {
                "pass": bool(_dict(evidence.get("portfolio_reconciliation")).get("family_navs_are_not_summed_as_independent_capital")),
                "future_mature_scaling_eligible_families": future_scaling,
                "active_frozen_family_cap": spec["allocation"]["immature_family_max_weight"],
                "permanent_authority_ceiling": spec["allocation"]["permanent_family_max_weight"],
                "active_cap_changed": False,
            },
            "57_promotion_degradation_kill_logic": {
                "pass": True,
                "families": families,
                "changes_strategy_authority": False,
            },
            "58_forward_alpha_certificate": {
                "pass": after_cost_alpha_proven,
                "state": state,
                "promoted_families": promoted,
                "after_cost_positive_compounded_alpha_proven": after_cost_alpha_proven,
            },
        },
        "promoted_families": promoted,
        "degrading_families": degrading,
        "killed_families": killed,
        "future_mature_scaling_eligible_families": future_scaling,
        "changes_strategy_authority": False,
        "changes_economic_thresholds": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


def build_alpha_certificate(
    store: Any,
    *,
    forward: dict[str, Any],
    robinhood_proof: dict[str, Any] | None,
    robinhood_proof_state: str,
) -> dict[str, Any]:
    evidence = build_cross_surface_evidence_bundle(
        store,
        robinhood_proof,
        robinhood_proof_state=robinhood_proof_state,
    )
    audit = _audit_records(store, robinhood_proof)
    promotion_rows = combined_promotion_records(store, robinhood_proof)
    promotion = _dict(evidence.get("promotion_certification"))
    return compose_alpha_certificate(
        forward=forward,
        evidence=evidence,
        candidate_flow=_candidate_flow(store, robinhood_proof, evidence),
        settlement=_settlement_proof(store, robinhood_proof),
        wallet_entity=_wallet_entity_challengers(audit),
        capital_views=_research_vs_promoted_nav(audit, promotion_rows, promotion),
    )


def _route_payload(app: Any, path: str) -> dict[str, Any]:
    for route in app.routes:
        if getattr(route, "path", None) == path:
            payload = route.endpoint()
            if isinstance(payload, dict):
                return payload
            raise RuntimeError(f"{path} returned non-dict payload")
    raise RuntimeError(f"route not found: {path}")


def install_alpha_validation(
    app: Any,
    *,
    runtime_provider: Callable[[], Any],
    robinhood_status_provider: Callable[[], dict[str, Any]] | None = None,
) -> None:
    if bool(getattr(app.state, "roi_v51_alpha_validation_47_58", False)):
        return
    if "/v1/strategy/alpha-certificate" not in {getattr(route, "path", None) for route in app.routes}:
        def alpha_certificate_status() -> dict[str, Any]:
            runtime = runtime_provider()
            proof, proof_state = _cached_robinhood_proof(robinhood_status_provider)
            forward = _route_payload(app, "/v1/strategy/forward-certification")
            expected = os.getenv("RENDER_GIT_COMMIT", "").strip()
            if expected and str(forward.get("release_commit") or "") != expected:
                forward = dict(forward)
                forward["hard_operational_gates_ok"] = False
                forward["state"] = "release_mismatch"
            return build_alpha_certificate(
                runtime.store,
                forward=forward,
                robinhood_proof=proof,
                robinhood_proof_state=proof_state,
            )

        app.add_api_route(
            "/v1/strategy/alpha-certificate",
            alpha_certificate_status,
            methods=["GET"],
            name="v51_alpha_certificate_47_58",
        )
    app.state.roi_v51_alpha_validation_47_58 = True


__all__ = [
    "ALPHA_CERTIFICATE_VERSION",
    "build_alpha_certificate",
    "compose_alpha_certificate",
    "install_alpha_validation",
]
