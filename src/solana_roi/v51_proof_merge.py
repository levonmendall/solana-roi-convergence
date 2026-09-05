from __future__ import annotations

import copy
from typing import Any

from .strategy_v51_authority import authority


def merge_economic_certifications(primary: dict[str, Any], secondary: dict[str, Any] | None) -> dict[str, Any]:
    result = copy.deepcopy(primary)
    if not secondary:
        result["robinhood_proof_available"] = False
        return result
    if secondary.get("authority_id") != result.get("authority_id") or secondary.get("economic_freeze_epoch") != result.get("economic_freeze_epoch"):
        result["robinhood_proof_available"] = False
        result["robinhood_proof_error"] = "isolated_robinhood_authority_or_epoch_mismatch"
        return result

    secondary_families = dict(secondary.get("families") or {})
    robinhood = secondary_families.get("ROBINHOOD_CHAIN")
    if robinhood is not None:
        result.setdefault("families", {})["ROBINHOOD_CHAIN"] = copy.deepcopy(robinhood)

    primary_alpha = result.setdefault("incremental_alpha", {})
    secondary_alpha = dict(secondary.get("incremental_alpha") or {})
    primary_entities = primary_alpha.setdefault("entity_family_attribution", {})
    for key, value in dict(secondary_alpha.get("entity_family_attribution") or {}).items():
        if str(value.get("family") or "") == "ROBINHOOD_CHAIN":
            primary_entities[key] = copy.deepcopy(value)
    primary_alpha["attributable_outcome_count"] = sum(
        int(item.get("matched_residual_sample_count") or 0) for item in primary_entities.values()
    )

    families = dict(result.get("families") or {})
    scores = {name: float(value.get("capital_efficiency_score") or 0.0) for name, value in families.items()}
    priority = list(authority()["research_family_priority"])
    ordered = sorted(
        set(priority) | set(families),
        key=lambda family: (-scores.get(family, 0.0), priority.index(family) if family in priority else len(priority), family),
    )
    positive = [family for family in ordered if scores.get(family, 0.0) > 0.0]
    total = sum(scores[family] for family in positive)
    weights: dict[str, float] = {}
    remaining = 1.0
    for family in positive:
        raw = scores[family] / total if total > 0.0 else 0.0
        weight = min(0.25, raw, remaining)
        weights[family] = weight
        remaining -= weight
    result["closed_outcome_count"] = sum(int(value.get("closed_outcome_count") or 0) for value in families.values())
    result["research_family_ranking"] = ordered
    result["paper_allocation_weights"] = weights
    result["paper_cash_weight"] = max(0.0, remaining)
    result["robinhood_proof_available"] = True
    result["robinhood_proof_transport"] = "nonblocking_worker_status_cache"
    return result


def merge_candidate_coverages(primary: dict[str, Any], robinhood: dict[str, Any] | None) -> dict[str, Any]:
    result = copy.deepcopy(primary)
    if not robinhood:
        result["robinhood_proof_available"] = False
        result["coverage_complete"] = False
        result["coverage_debt_reason"] = "isolated_robinhood_candidate_proof_not_available"
        return result
    if robinhood.get("authority_id") != result.get("authority_id") or robinhood.get("economic_freeze_epoch") != result.get("economic_freeze_epoch"):
        result["robinhood_proof_available"] = False
        result["coverage_complete"] = False
        result["coverage_debt_reason"] = "isolated_robinhood_candidate_authority_or_epoch_mismatch"
        return result

    result.setdefault("source_candidates_seen", {})["robinhood"] = int(robinhood.get("canonical_candidate_count") or 0)
    result.setdefault("stage_summary", {})["ROBINHOOD_CHAIN"] = copy.deepcopy(robinhood.get("stage_summary") or {})
    primary_debt = int(result.get("coverage_debt_count") or 0)
    robinhood_debt = int(robinhood.get("coverage_debt_count") or 0)
    result["coverage_debt_count"] = primary_debt + robinhood_debt
    result["coverage_complete"] = bool(result.get("coverage_complete", True)) and bool(robinhood.get("coverage_complete", False))
    result["robinhood"] = copy.deepcopy(robinhood)
    result["robinhood_proof_available"] = True
    result["robinhood_proof_transport"] = "nonblocking_worker_status_cache"
    return result


__all__ = ["merge_economic_certifications", "merge_candidate_coverages"]
