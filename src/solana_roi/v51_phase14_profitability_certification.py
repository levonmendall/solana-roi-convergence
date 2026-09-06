from __future__ import annotations

import math
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Iterable

from .strategy_v51_authority import AUTHORITY_ID, ECONOMIC_FREEZE_EPOCH, authority
from .v51_cross_surface_proof import combined_promotion_records
from .v51_economic_clustering import cluster_economic_rows, cluster_economic_rows_legacy_pre103
from .v51_economic_core import execution_stress_profiles, robust_profile
from .v51_evidence_analytics import _family_minimum
from .v51_measurement_integrity import MEASUREMENT_EPOCH
from .v51_return_validation import STATISTICS_VERSION, return_integrity_summary, validate_row_return


PHASE14_VERSION = "v51-phase14-profitability-proof-103-108-v2"
MIN_CONTINUOUS_PRODUCTION_SECONDS = 24.0 * 60.0 * 60.0
CLASS_ECONOMICALLY_PROMISING = "ECONOMICALLY_PROMISING"
CLASS_PRODUCTION_PROVEN = "PRODUCTION-PROVEN"
CLASS_INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _safe(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return default
    return result if math.isfinite(result) else default


def _dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return None


def _now(value: datetime | None = None) -> datetime:
    current = value or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return current.astimezone(timezone.utc)


def _growth_positive(profile: dict[str, Any], *, hurdle: float = 0.0) -> bool:
    growth = profile.get("lower_confidence_expected_log_growth")
    return growth is not None and _safe(growth, float("-inf")) > hurdle


def _leave_best_positive(profile: dict[str, Any]) -> bool:
    value = profile.get("leave_best_trade_out_mean")
    return value is not None and _safe(value) > 0.0


def _cluster_values(clusters: Iterable[dict[str, Any]]) -> list[float]:
    values: list[float] = []
    for row in clusters:
        validated = validate_row_return(row)
        if validated.validity and validated.normalized_fraction is not None:
            values.append(validated.normalized_fraction)
    return values


def _remove_top(values: Iterable[float], count: int) -> list[float]:
    series = sorted((float(value) for value in values), reverse=True)
    if len(series) <= count:
        return []
    return series[count:]


def _top_winner_proof(values: list[float], *, hurdle: float, fixed_fraction: float) -> dict[str, Any]:
    full = robust_profile(values, fixed_fraction=fixed_fraction)
    remove_one = robust_profile(_remove_top(values, 1), fixed_fraction=fixed_fraction)
    remove_three = robust_profile(_remove_top(values, 3), fixed_fraction=fixed_fraction)
    remove_five = robust_profile(_remove_top(values, 5), fixed_fraction=fixed_fraction)
    full_pass = _growth_positive(full, hurdle=hurdle) and _leave_best_positive(full)
    top_one_pass = _growth_positive(remove_one, hurdle=hurdle)
    top_three_pass = _growth_positive(remove_three, hurdle=hurdle)
    return {
        "pass": bool(full_pass and top_one_pass and top_three_pass),
        "profitability_definition": "bootstrap_lower_expected_log_growth_above_existing_family_hurdle",
        "existing_family_hurdle": hurdle,
        "selected_fraction": fixed_fraction,
        "selected_fraction_source": "validation",
        "full_sample_pass": full_pass,
        "remove_top_1_pass": top_one_pass,
        "remove_top_3_pass": top_three_pass,
        "remove_top_5_is_diagnostic_not_gate": True,
        "full_sample": full,
        "remove_top_1": remove_one,
        "remove_top_3": remove_three,
        "remove_top_5": remove_five,
    }


def _stress_proof(values: list[float], *, fixed_fraction: float) -> dict[str, Any]:
    profiles = execution_stress_profiles(values, fixed_fraction=fixed_fraction)
    scenarios: dict[str, Any] = {}
    for name, profile in profiles.items():
        passed = _growth_positive(profile) and _leave_best_positive(profile)
        scenarios[name] = {
            "pass": passed,
            "required_positive_bootstrap_lower_expected_log_growth": True,
            "required_positive_leave_best_trade_out_mean": True,
            "selected_fraction": fixed_fraction,
            "profile": profile,
        }
    return {
        "pass": bool(scenarios) and all(bool(row.get("pass")) for row in scenarios.values()),
        "all_frozen_execution_stress_scenarios_required": True,
        "selected_fraction_source": "validation",
        "scenarios": scenarios,
    }


def _partition_values(clusters: list[dict[str, Any]], partition: str) -> list[float]:
    return _cluster_values(
        row for row in clusters if str(row.get("evidence_partition") or "") == partition
    )


def _locked_partition_profiles(clusters: list[dict[str, Any]]) -> tuple[list[float], dict[str, Any], list[float], dict[str, Any], float]:
    validation_values = _partition_values(clusters, "validation")
    validation_profile = robust_profile(validation_values)
    selected_fraction = float(validation_profile.get("best_fraction") or 0.0)
    holdout_values = _partition_values(clusters, "holdout")
    holdout_profile = robust_profile(holdout_values, fixed_fraction=selected_fraction)
    return validation_values, validation_profile, holdout_values, holdout_profile, selected_fraction


def _legacy_raw_accepted_count(rows: Iterable[dict[str, Any]]) -> int:
    count = 0
    for row in rows:
        try:
            value = float(row.get("net_return"))
        except (TypeError, ValueError, OverflowError):
            continue
        count += int(math.isfinite(value) and value > -1.0)
    return count


def _family_economic_evaluation(
    clusters: list[dict[str, Any]],
    *,
    hurdle: float,
    minimum_n: int,
    integrity_pass: bool,
) -> dict[str, Any]:
    values = _cluster_values(clusters)
    validation_values, validation_profile, holdout_values, holdout_profile, selected_fraction = _locked_partition_profiles(clusters)
    maturity_pass = len(clusters) >= minimum_n and integrity_pass
    top_winner = _top_winner_proof(values, hurdle=hurdle, fixed_fraction=selected_fraction)
    stress = _stress_proof(values, fixed_fraction=selected_fraction)
    holdout_pass = bool(
        holdout_values
        and _growth_positive(holdout_profile)
        and _leave_best_positive(holdout_profile)
    )
    return {
        "values": values,
        "validation_values": validation_values,
        "validation_profile": validation_profile,
        "holdout_values": holdout_values,
        "holdout_profile": holdout_profile,
        "selected_fraction": selected_fraction,
        "maturity_pass": maturity_pass,
        "top_winner": top_winner,
        "stress": stress,
        "holdout_pass": holdout_pass,
        "economic_gates_pass": bool(maturity_pass and top_winner.get("pass") and stress.get("pass") and holdout_pass),
    }


def _family_certification(
    family: str,
    family_rows: list[dict[str, Any]],
    promotion_family: dict[str, Any],
    *,
    global_production_gates_pass: bool,
) -> dict[str, Any]:
    integrity = return_integrity_summary(family_rows)
    clusters = cluster_economic_rows(family_rows, family=family, promotion_only=True)
    legacy_clusters = cluster_economic_rows_legacy_pre103(family_rows, family=family, promotion_only=True)
    minimum_n, exact_min, hurdle = _family_minimum(family, family_rows)

    current = _family_economic_evaluation(
        clusters,
        hurdle=hurdle,
        minimum_n=minimum_n,
        integrity_pass=bool(integrity.get("proof_eligible")),
    )
    legacy = _family_economic_evaluation(
        legacy_clusters,
        hurdle=hurdle,
        minimum_n=minimum_n,
        integrity_pass=True,
    )

    existing_promotion_claim = bool(promotion_family.get("promotion_claim_valid"))
    legacy_promotion_claim = bool(
        promotion_family.get("legacy_pre103_promotion_claim_valid", existing_promotion_claim)
    )
    economically_promising = bool(existing_promotion_claim and current["economic_gates_pass"])
    legacy_economically_promising = bool(legacy_promotion_claim and legacy["economic_gates_pass"])
    production_proven = bool(economically_promising and global_production_gates_pass)
    legacy_production_proven = bool(legacy_economically_promising and global_production_gates_pass)
    classification = (
        CLASS_PRODUCTION_PROVEN
        if production_proven
        else CLASS_ECONOMICALLY_PROMISING
        if economically_promising
        else CLASS_INSUFFICIENT_EVIDENCE
    )
    legacy_classification = (
        CLASS_PRODUCTION_PROVEN
        if legacy_production_proven
        else CLASS_ECONOMICALLY_PROMISING
        if legacy_economically_promising
        else CLASS_INSUFFICIENT_EVIDENCE
    )

    blockers: list[str] = []
    if not bool(integrity.get("proof_eligible")):
        blockers.append("invalid_economic_measurement_integrity_threshold_exceeded")
    if not existing_promotion_claim:
        blockers.append("existing_v51_promotion_claim_not_valid")
    if not current["maturity_pass"]:
        blockers.append("family_forward_maturity_minimum_not_met")
    if not bool(current["top_winner"].get("pass")):
        blockers.append("top_winner_removal_not_robustly_profitable")
    if not bool(current["stress"].get("pass")):
        blockers.append("execution_stress_not_robustly_profitable")
    if not current["holdout_pass"]:
        blockers.append("locked_holdout_not_robustly_profitable")
    if economically_promising and not global_production_gates_pass:
        blockers.append("global_production_proof_incomplete")

    current_full_profile = robust_profile(current["values"], fixed_fraction=current["selected_fraction"])
    legacy_full_profile = robust_profile(legacy["values"], fixed_fraction=legacy["selected_fraction"])
    return {
        "family": family,
        "classification": classification,
        "economically_promising": economically_promising,
        "production_proven": production_proven,
        "blockers": blockers,
        "economic_measurement_integrity": integrity,
        "statistics_rebuild": {
            "statistics_version": STATISTICS_VERSION,
            "recomputed_from_immutable_outcomes": True,
            "mutates_source_outcomes": False,
            "previously_accepted_outcome_count": _legacy_raw_accepted_count(family_rows),
            "newly_included_exact_total_loss_outcome_count": int(integrity.get("exact_total_loss_count") or 0),
            "malformed_rows_removed_from_economic_proof": int(integrity.get("measurement_debt_count") or 0),
            "before_family_statistics": legacy_full_profile,
            "after_family_statistics": current_full_profile,
            "before_promotion_state": legacy_promotion_claim,
            "after_promotion_state": existing_promotion_claim,
            "before_phase14_classification": legacy_classification,
            "after_phase14_classification": classification,
        },
        "95_forward_family_maturity": {
            "pass": current["maturity_pass"],
            "independent_event_cluster_count": len(clusters),
            "minimum_independent_outcomes": minimum_n,
            "minimum_exact_outcomes_reference": exact_min,
            "uses_cross_family_total_for_maturity": False,
            "invalid_economic_measurements_count_toward_maturity": False,
        },
        "96_top_winner_removal": current["top_winner"],
        "97_stressed_profitability": current["stress"],
        "98_locked_holdout_profitability": {
            "pass": current["holdout_pass"],
            "holdout_cluster_count": len(current["holdout_values"]),
            "validation_cluster_count": len(current["validation_values"]),
            "selected_fraction": current["selected_fraction"],
            "selected_fraction_source": "validation",
            "holdout_fraction_reoptimized": False,
            "holdout_profile": current["holdout_profile"],
            "validation_profile": current["validation_profile"],
            "no_holdout_can_be_production_proven": True,
        },
        "existing_v51_promotion_claim_valid": existing_promotion_claim,
        "promotion_family_snapshot": promotion_family,
        "changes_selection_authority": False,
        "changes_sizing_authority": False,
        "changes_exit_authority": False,
        "changes_promotion_economics": False,
        "paper_only": True,
        "live_money_authority": False,
    }


def _coverage_gate(candidate_coverage: dict[str, Any]) -> dict[str, Any]:
    coverage = _dict(candidate_coverage)
    complete = bool(coverage.get("coverage_complete"))
    debt = int(coverage.get("coverage_debt_count") or 0)
    proof_state = str(coverage.get("proof_state") or "unavailable")
    passed = bool(complete and debt == 0 and proof_state == "confirmed")
    return {
        "pass": passed,
        "coverage_complete": complete,
        "coverage_debt_count": debt,
        "proof_state": proof_state,
        "canonical_candidate_ledger_required": True,
    }


def _measurement_gate(
    promotion: dict[str, Any],
    candidate_coverage: dict[str, Any],
    forward_certification: dict[str, Any],
) -> dict[str, Any]:
    coverage = _dict(candidate_coverage)
    forward = _dict(forward_certification)
    checks = _dict(forward.get("checks"))
    release_check = _dict(checks.get("35_exact_live_release"))
    attestation_check = _dict(checks.get("41_current_release_attestation"))
    promotion_epoch = str(promotion.get("measurement_epoch") or "")
    coverage_epoch = str(coverage.get("measurement_epoch") or "")
    epoch_match = bool(
        promotion_epoch == MEASUREMENT_EPOCH
        and coverage_epoch == MEASUREMENT_EPOCH
    )
    promotion_measurement_eligible = bool(promotion.get("promotion_eligible_measurement"))
    exact_release_bound = bool(release_check.get("pass"))
    release_attested = bool(attestation_check.get("pass"))
    passed = bool(epoch_match and promotion_measurement_eligible and exact_release_bound and release_attested)
    return {
        "pass": passed,
        "required_measurement_epoch": MEASUREMENT_EPOCH,
        "promotion_measurement_epoch": promotion_epoch or None,
        "candidate_coverage_measurement_epoch": coverage_epoch or None,
        "epoch_match": epoch_match,
        "promotion_eligible_measurement": promotion_measurement_eligible,
        "exact_release_bound": exact_release_bound,
        "current_release_attested": release_attested,
    }


def _continuity_gate(operations_proof: dict[str, Any], *, now: datetime) -> dict[str, Any]:
    operations = _dict(operations_proof)
    continuity = _dict(operations.get("continuity"))
    backpressure = _dict(operations.get("backpressure"))
    started = _dt(continuity.get("process_started_at"))
    uptime = max(0.0, (now - started).total_seconds()) if started is not None else 0.0
    epoch_match = str(continuity.get("continuity_epoch") or "") == ECONOMIC_FREEZE_EPOCH
    backpressure_healthy = bool(backpressure.get("healthy"))
    uptime_pass = uptime >= MIN_CONTINUOUS_PRODUCTION_SECONDS
    passed = bool(epoch_match and backpressure_healthy and uptime_pass)
    return {
        "pass": passed,
        "minimum_continuous_production_seconds": MIN_CONTINUOUS_PRODUCTION_SECONDS,
        "minimum_continuous_production_hours": MIN_CONTINUOUS_PRODUCTION_SECONDS / 3600.0,
        "process_started_at": continuity.get("process_started_at"),
        "current_uninterrupted_uptime_seconds": uptime,
        "current_uninterrupted_uptime_hours": uptime / 3600.0,
        "continuity_epoch": continuity.get("continuity_epoch"),
        "economic_epoch_match": epoch_match,
        "backpressure_healthy": backpressure_healthy,
        "current_process_uptime_not_accumulated_across_restarts": True,
        "changes_economic_epoch_on_restart": False,
    }


def compose_phase14_profitability_certification(
    records: Iterable[dict[str, Any]],
    *,
    promotion_certification: dict[str, Any],
    candidate_coverage: dict[str, Any],
    forward_certification: dict[str, Any],
    operations_proof: dict[str, Any],
    now: datetime | None = None,
) -> dict[str, Any]:
    rows = [dict(row) for row in records if isinstance(row, dict)]
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get("family") or "UNKNOWN")].append(row)

    promotion = _dict(promotion_certification)
    promotion_families = _dict(promotion.get("families"))
    coverage = _coverage_gate(candidate_coverage)
    measurement = _measurement_gate(promotion, candidate_coverage, forward_certification)
    continuity = _continuity_gate(operations_proof, now=_now(now))
    global_production_gates_pass = bool(
        coverage.get("pass") and measurement.get("pass") and continuity.get("pass")
    )

    family_names = sorted(set(grouped) | set(str(name) for name in promotion_families))
    families: dict[str, Any] = {}
    for family in family_names:
        families[family] = _family_certification(
            family,
            grouped.get(family, []),
            _dict(promotion_families.get(family)),
            global_production_gates_pass=global_production_gates_pass,
        )

    promising = sorted(name for name, proof in families.items() if bool(proof.get("economically_promising")))
    proven = sorted(name for name, proof in families.items() if bool(proof.get("production_proven")))
    classification = (
        CLASS_PRODUCTION_PROVEN
        if proven
        else CLASS_ECONOMICALLY_PROMISING
        if promising
        else CLASS_INSUFFICIENT_EVIDENCE
    )
    legacy_promising = sorted(
        name
        for name, proof in families.items()
        if str(_dict(proof.get("statistics_rebuild")).get("before_phase14_classification"))
        in {CLASS_ECONOMICALLY_PROMISING, CLASS_PRODUCTION_PROVEN}
    )
    legacy_proven = sorted(
        name
        for name, proof in families.items()
        if str(_dict(proof.get("statistics_rebuild")).get("before_phase14_classification")) == CLASS_PRODUCTION_PROVEN
    )
    legacy_classification = (
        CLASS_PRODUCTION_PROVEN
        if legacy_proven
        else CLASS_ECONOMICALLY_PROMISING
        if legacy_promising
        else CLASS_INSUFFICIENT_EVIDENCE
    )
    blockers: list[str] = []
    if not promising:
        blockers.append("no_family_satisfies_phase14_economic_proof")
    if not bool(coverage.get("pass")):
        blockers.append("canonical_opportunity_coverage_incomplete")
    if not bool(measurement.get("pass")):
        blockers.append("measurement_epoch_or_release_attestation_invalid")
    if not bool(continuity.get("pass")):
        blockers.append("continuous_production_runtime_not_yet_sufficient")

    return {
        "phase14_version": PHASE14_VERSION,
        "statistics_version": STATISTICS_VERSION,
        "authority_id": AUTHORITY_ID,
        "economic_freeze_epoch": ECONOMIC_FREEZE_EPOCH,
        "classification": classification,
        "economically_promising": bool(promising),
        "production_proven": bool(proven),
        "economically_promising_families": promising,
        "production_proven_families": proven,
        "blockers": blockers,
        "103_108_statistics_migration": {
            "recomputed_from_immutable_outcomes": True,
            "proof_statistics_version": STATISTICS_VERSION,
            "before_phase14_classification": legacy_classification,
            "after_phase14_classification": classification,
            "previously_accepted_outcome_count": sum(
                int(_dict(proof.get("statistics_rebuild")).get("previously_accepted_outcome_count") or 0)
                for proof in families.values()
            ),
            "newly_included_exact_total_loss_outcome_count": sum(
                int(_dict(proof.get("statistics_rebuild")).get("newly_included_exact_total_loss_outcome_count") or 0)
                for proof in families.values()
            ),
            "malformed_rows_removed_from_economic_proof": sum(
                int(_dict(proof.get("statistics_rebuild")).get("malformed_rows_removed_from_economic_proof") or 0)
                for proof in families.values()
            ),
        },
        "95_family_forward_maturity_is_independent": True,
        "99_opportunity_coverage": coverage,
        "100_measurement_epoch_validity": measurement,
        "101_operational_continuity": continuity,
        "102_classification_contract": {
            "economically_promising_label": CLASS_ECONOMICALLY_PROMISING,
            "production_proven_label": CLASS_PRODUCTION_PROVEN,
            "economic_success_does_not_imply_production_proof": True,
            "production_proven_requires_95_through_101": True,
        },
        "families": families,
        "changes_strategy_authority": False,
        "changes_economic_thresholds": False,
        "changes_selection_authority": False,
        "changes_sizing_authority": False,
        "changes_exit_authority": False,
        "changes_promotion_economics": False,
        "retrospective_entry_authority": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


def build_phase14_profitability_certification(
    store: Any,
    *,
    promotion_certification: dict[str, Any],
    candidate_coverage: dict[str, Any],
    forward_certification: dict[str, Any],
    operations_proof: dict[str, Any],
    robinhood_proof: dict[str, Any] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    records = combined_promotion_records(store, robinhood_proof)
    return compose_phase14_profitability_certification(
        records,
        promotion_certification=promotion_certification,
        candidate_coverage=candidate_coverage,
        forward_certification=forward_certification,
        operations_proof=operations_proof,
        now=now,
    )


__all__ = [
    "CLASS_ECONOMICALLY_PROMISING",
    "CLASS_INSUFFICIENT_EVIDENCE",
    "CLASS_PRODUCTION_PROVEN",
    "MIN_CONTINUOUS_PRODUCTION_SECONDS",
    "PHASE14_VERSION",
    "build_phase14_profitability_certification",
    "compose_phase14_profitability_certification",
]
