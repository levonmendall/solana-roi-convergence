from __future__ import annotations

from typing import Any

PHASE10_VERSION = "v51-phase10-system-proof-70-74-v1"


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _stage_count(coverage: dict[str, Any], stage: str, *statuses: str) -> int:
    total = 0
    summary = _dict(coverage.get("stage_summary"))
    for surface in summary.values():
        stage_row = _dict(_dict(surface).get(stage))
        if statuses:
            total += sum(_int(stage_row.get(status)) for status in statuses)
        else:
            total += sum(_int(value) for value in stage_row.values())
    return total


def family_proof_confidence(
    certification: dict[str, Any],
    promotion: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    """Expose the economic confidence components without creating new authority."""
    audit_families = _dict(certification.get("families"))
    promotion_families = _dict(promotion.get("families"))
    names = sorted(set(audit_families) | set(promotion_families))
    result: dict[str, dict[str, Any]] = {}
    for family in names:
        audit = _dict(audit_families.get(family))
        promo = _dict(promotion_families.get(family))
        robust = _dict(audit.get("robust_profile"))
        if not robust:
            robust = _dict(promo.get("robust_profile"))
        hierarchy = _dict(audit.get("promotion_kill_profile"))
        if not hierarchy:
            hierarchy = _dict(promo.get("promotion_kill_profile"))
        promotion_valid = bool(promo.get("promotion_claim_valid"))
        result[family] = {
            "raw_n": _int(promo.get("raw_outcome_count") or audit.get("closed_outcome_count")),
            "independent_n": _int(
                promo.get("independent_event_cluster_count") or audit.get("independent_event_count")
            ),
            "holdout_n": _int(promo.get("holdout_cluster_count")),
            "net_roi": audit.get("net_roi_sum"),
            "compounded_nav": audit.get("compounded_nav_multiple"),
            "expected_log_growth": robust.get("best_expected_log_growth"),
            "lcb_expected_log_growth": robust.get("expected_log_growth_ci95_lower"),
            "es20": robust.get("expected_shortfall_20"),
            "max_drawdown": robust.get("max_drawdown_at_best_fraction"),
            "winner_concentration": robust.get("winner_concentration"),
            "top_1_removed": robust.get("leave_best_trade_out_mean"),
            "top_3_removed": robust.get("remove_top_3_mean"),
            "latency_sensitivity": audit.get("latency_sensitivity") or {},
            "cost_sensitivity": audit.get("execution_cost_sensitivity") or {},
            "stress_performance": audit.get("execution_stress") or promo.get("execution_stress") or {},
            "promotion_state": "promoted" if promotion_valid else str(hierarchy.get("state") or "unproven"),
            "promotion_claim_valid": promotion_valid,
            "validation_n": _int(promo.get("validation_cluster_count")),
            "minimum_independent_n": _int(promo.get("minimum_independent_outcomes") or hierarchy.get("minimum_independent_outcomes")),
            "measurement_basis": "frozen_v51_forward_outcomes_and_locked_holdout_clusters",
        }
    return result


def _counterfactual_counts(payload: dict[str, Any] | None) -> tuple[int, int]:
    row = _dict(payload)
    return _int(row.get("rejected_candidate_count")), _int(row.get("resolved_positive_count"))


def dashboard_funnel(
    *,
    local_coverage: dict[str, Any],
    merged_coverage: dict[str, Any],
    certification: dict[str, Any],
    promotion: dict[str, Any],
    local_counterfactuals: dict[str, Any],
    robinhood_proof: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """One non-overlapping operational funnel for the dashboard/system proof."""
    rh_proof = _dict(robinhood_proof)
    phase9 = _dict(rh_proof.get("phase9_65_69"))
    rh_dispositions = _dict(phase9.get("candidate_dispositions"))
    rh_candidates = _int(rh_dispositions.get("candidate_count"))
    rh_evaluated = _int(rh_dispositions.get("terminal_disposition_count"))
    rh_rejected = _int(rh_dispositions.get("rejected_candidate_count"))
    rh_entries = max(0, rh_evaluated - rh_rejected)

    local_detected = _stage_count(local_coverage, "candidate", "complete")
    local_evaluated = _stage_count(local_coverage, "decision", "complete")
    local_entries = _stage_count(local_coverage, "position", "paper_position_authorized")
    local_settled = _stage_count(local_coverage, "settlement", "complete")

    rh_counterfactual = _dict(rh_proof.get("rejected_counterfactuals"))
    local_probe_n, local_missed = _counterfactual_counts(local_counterfactuals)
    rh_probe_n, rh_missed = _counterfactual_counts(rh_counterfactual)

    promoted_families = {
        name
        for name, row in _dict(promotion.get("families")).items()
        if isinstance(row, dict) and bool(row.get("promotion_claim_valid"))
    }
    promoted_trades = sum(
        _int(_dict(row).get("closed_outcome_count"))
        for name, row in _dict(certification.get("families")).items()
        if name in promoted_families
    )
    settled = max(local_settled, _int(certification.get("closed_outcome_count")))

    return {
        "detected_opportunities": local_detected + rh_candidates,
        "evaluated_opportunities": local_evaluated + rh_evaluated,
        "coverage_debt": _int(merged_coverage.get("coverage_debt_count")),
        "paper_entries": local_entries + rh_entries,
        "settled_trades": settled,
        "promoted_trades": promoted_trades,
        "research_probes": local_probe_n + rh_probe_n,
        "missed_opportunities": local_missed + rh_missed,
        "definitions": {
            "detected_opportunities": "canonical pre-strategy candidates observed in the current measurement epoch",
            "evaluated_opportunities": "candidates with a terminal decision-stage disposition",
            "coverage_debt": "candidates/stages lacking required canonical accounting",
            "paper_entries": "authorized paper positions only",
            "settled_trades": "closed paper outcomes in the frozen economic epoch",
            "promoted_trades": "settled outcomes belonging to families with a currently valid promotion claim",
            "research_probes": "rejected candidates retained for forward counterfactual measurement",
            "missed_opportunities": "rejected research probes later resolving to a positive forward outcome",
        },
        "paper_only": True,
        "live_money_authority": False,
    }


__all__ = ["PHASE10_VERSION", "dashboard_funnel", "family_proof_confidence"]
