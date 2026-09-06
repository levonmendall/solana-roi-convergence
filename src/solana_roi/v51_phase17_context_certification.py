from __future__ import annotations

import json
import math
from collections import defaultdict
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Iterable

from .strategy_v51_authority import ECONOMIC_FREEZE_EPOCH, hazard_requirements
from .v51_cross_surface_proof import combined_promotion_records
from .v51_economic_clustering import cluster_economic_rows
from .v51_economic_core import execution_stress_profiles, robust_profile
from .v51_exit_execution_terminal_fomo_followup import ACTIVE_EXECUTION_MODEL_EPOCH
from .v51_measurement_integrity import MEASUREMENT_EPOCH
from .v51_return_validation import validate_row_return


PHASE17_VERSION = "v51-phase17-context-certification-115-120-v1"
CONTEXT_DIMENSIONS = (
    "family",
    "venue",
    "lifecycle",
    "regime",
    "risk_signature",
    "flow_state",
    "chase_band",
    "latency_band",
    "cost_band",
)
SURFACE_SCOPES = ("SOLANA", "FOMO", "ROBINHOOD_CHAIN")
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
STRATEGY_MUTATION_AUTHORITY = False
LIVE_PROMOTION_AUTHORITY = False
RETROSPECTIVE_ENTRY_AUTHORITY = False


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
        result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if result.tzinfo is None:
            result = result.replace(tzinfo=timezone.utc)
        return result.astimezone(timezone.utc)
    except Exception:
        return None


def _now(value: datetime | None = None) -> datetime:
    result = value or datetime.now(timezone.utc)
    if result.tzinfo is None:
        result = result.replace(tzinfo=timezone.utc)
    return result.astimezone(timezone.utc)


def _upper(value: Any, default: str = "UNKNOWN") -> str:
    text = str(value or "").strip()
    return text.upper() if text else default


def _surface(value: Any) -> str:
    text = _upper(value)
    if text == "SOLANA_ALPHA":
        return "SOLANA"
    if text == "ROBINHOOD":
        return "ROBINHOOD_CHAIN"
    return text


def _family(row: dict[str, Any]) -> str:
    value = _upper(row.get("family"))
    if value != "UNKNOWN":
        return value
    surface = _surface(row.get("surface"))
    if surface == "ROBINHOOD_CHAIN":
        return "ROBINHOOD_CHAIN"
    venue = _upper(row.get("venue"))
    if venue in {"PUMP_AMM", "PUMP_FUN", "RAYDIUM"}:
        return venue
    return "UNKNOWN"


def _venue(row: dict[str, Any], family: str) -> str:
    value = _upper(row.get("venue"))
    if value != "UNKNOWN":
        return value
    if family in {"PUMP_AMM", "PUMP_FUN", "RAYDIUM", "ROBINHOOD_CHAIN"}:
        return family
    if _surface(row.get("surface")) == "FOMO":
        return "FOMO"
    return "UNKNOWN"


def _chase_band(row: dict[str, Any]) -> str:
    explicit = str(row.get("chase_band") or "").strip()
    if explicit:
        return explicit
    value = row.get("chase_fraction", row.get("raw_chase_fraction"))
    if value is None:
        return "UNKNOWN"
    chase = max(0.0, _safe(value))
    if chase <= 0.05:
        return "0_5pct"
    if chase <= 0.10:
        return "5_10pct"
    if chase <= 0.15:
        return "10_15pct"
    if chase <= 0.25:
        return "15_25pct_challenger"
    if chase <= 0.40:
        return "25_40pct_challenger"
    return "gt40pct_observe_only"


def _latency_band(row: dict[str, Any]) -> str:
    explicit = str(row.get("latency_band") or "").strip()
    if explicit:
        return explicit
    raw_ms = row.get("latency_ms", row.get("raw_latency_ms"))
    raw_seconds = row.get("latency_seconds")
    if raw_ms is None and raw_seconds is None:
        return "UNKNOWN"
    seconds = max(0.0, _safe(raw_seconds) if raw_seconds is not None else _safe(raw_ms) / 1000.0)
    if seconds <= 5.0:
        return "0_5s"
    if seconds <= 10.0:
        return "5_10s"
    if seconds <= 20.0:
        return "10_20s"
    return "gt20s_research_only"


def _cost_band(row: dict[str, Any]) -> str:
    explicit = str(row.get("cost_band") or row.get("execution_cost_band") or "").strip()
    if explicit:
        return explicit
    value = row.get("round_trip_cost_fraction")
    if value is None:
        return "UNKNOWN"
    cost = max(0.0, _safe(value))
    if cost <= 0.01:
        return "0_1pct"
    if cost <= 0.03:
        return "1_3pct"
    if cost <= 0.05:
        return "3_5pct"
    return "gt5pct"


def exact_context(row: dict[str, Any]) -> dict[str, str]:
    family = _family(row)
    return {
        "family": family,
        "venue": _venue(row, family),
        "lifecycle": str(row.get("lifecycle") or row.get("lifecycle_stage") or "UNKNOWN"),
        "regime": str(row.get("regime") or row.get("market_regime") or "UNKNOWN"),
        "risk_signature": str(row.get("risk_signature") or "clean"),
        "flow_state": str(row.get("flow_state") or row.get("creator_flow_state") or "UNKNOWN"),
        "chase_band": _chase_band(row),
        "latency_band": _latency_band(row),
        "cost_band": _cost_band(row),
    }


def context_key(context: dict[str, str]) -> str:
    return "|".join(f"{name}={context.get(name, 'UNKNOWN')}" for name in CONTEXT_DIMENSIONS)


def _required_surfaces(rows: list[dict[str, Any]], family: str) -> list[str]:
    surfaces = {_surface(row.get("surface")) for row in rows}
    required: set[str] = set()
    if family == "ROBINHOOD_CHAIN" or "ROBINHOOD_CHAIN" in surfaces:
        required.add("ROBINHOOD_CHAIN")
    else:
        required.add("SOLANA")
    if "FOMO" in surfaces or family.startswith("FOMO") or any("fomo" in str(row.get("lane") or "").lower() for row in rows):
        required.add("FOMO")
    return [surface for surface in SURFACE_SCOPES if surface in required]


def _surface_attestations(forward_certification: dict[str, Any]) -> dict[str, dict[str, Any]]:
    checks = _dict(_dict(forward_certification).get("checks"))
    release = _dict(checks.get("41_current_release_attestation"))
    raw_surfaces = _dict(release.get("surfaces"))
    aliases = {
        "SOLANA": ("solana", "SOLANA"),
        "FOMO": ("fomo", "FOMO"),
        "ROBINHOOD_CHAIN": ("robinhood", "ROBINHOOD_CHAIN", "robinhood_chain"),
    }
    result: dict[str, dict[str, Any]] = {}
    for surface, names in aliases.items():
        payload: dict[str, Any] = {}
        for name in names:
            if isinstance(raw_surfaces.get(name), dict):
                payload = _dict(raw_surfaces.get(name))
                break
        if payload:
            attested = bool(payload.get("attested"))
            present = bool(payload.get("present", True))
            reasons = list(payload.get("reasons") or [])
            source = "surface_scoped_release_attestation"
        else:
            # Backward compatibility for immutable historical fixtures that predate
            # the surface split. This never converts a failed aggregate into a pass.
            attested = bool(release.get("attested", release.get("pass")))
            present = bool(attested)
            reasons = [] if attested else ["surface_attestation_unavailable"]
            source = "legacy_aggregate_attestation_fallback"
        result[surface] = {
            "present": present,
            "attested": attested,
            "reasons": reasons,
            "source": source,
        }
    return result


def _transport_status(forward_certification: dict[str, Any]) -> dict[str, dict[str, Any]]:
    checks = _dict(_dict(forward_certification).get("checks"))
    mapping = {
        "SOLANA": "37_solana_transport",
        "FOMO": "38_fomo_transport",
        "ROBINHOOD_CHAIN": "39_robinhood_transport",
    }
    result: dict[str, dict[str, Any]] = {}
    for surface, name in mapping.items():
        row = _dict(checks.get(name))
        ready = bool(row.get("ready", row.get("pass", False)))
        result[surface] = {"ready": ready, "detail": row}
    return result


def _release_identity_ok(forward_certification: dict[str, Any]) -> bool:
    checks = _dict(_dict(forward_certification).get("checks"))
    return bool(_dict(checks.get("35_exact_live_release")).get("pass"))


def _safety_ok(forward_certification: dict[str, Any]) -> bool:
    checks = _dict(_dict(forward_certification).get("checks"))
    row = _dict(checks.get("36_paper_only_safety_boundary"))
    return bool(row.get("pass"))


def _continuity(operations_proof: dict[str, Any], *, now: datetime) -> dict[str, Any]:
    operations = _dict(operations_proof)
    continuity = _dict(operations.get("continuity"))
    backpressure = _dict(operations.get("backpressure"))
    started = _dt(continuity.get("process_started_at"))
    uptime = max(0.0, (now - started).total_seconds()) if started is not None else 0.0
    epoch_ok = str(continuity.get("continuity_epoch") or "") == ECONOMIC_FREEZE_EPOCH
    backpressure_ok = bool(backpressure.get("healthy"))
    pass_global = bool(epoch_ok and backpressure_ok and uptime >= 24.0 * 60.0 * 60.0)
    surface_rows = _dict(operations.get("surface_continuity") or operations.get("continuity_by_surface"))
    surfaces: dict[str, bool] = {}
    for surface in SURFACE_SCOPES:
        payload = _dict(surface_rows.get(surface) or surface_rows.get(surface.lower()))
        surfaces[surface] = bool(payload.get("pass", payload.get("healthy", pass_global))) if payload else pass_global
    return {
        "pass": pass_global,
        "surface_pass": surfaces,
        "uptime_seconds": uptime,
        "economic_epoch_match": epoch_ok,
        "backpressure_healthy": backpressure_ok,
    }


def _surface_debt_counts(candidate_coverage: dict[str, Any]) -> dict[str, int]:
    coverage = _dict(candidate_coverage)
    summary = _dict(coverage.get("stage_summary"))
    result = {surface: 0 for surface in SURFACE_SCOPES}
    for raw_surface, stages in summary.items():
        surface = _surface(raw_surface)
        if surface not in result:
            continue
        total = 0
        for statuses in _dict(stages).values():
            total += int(_dict(statuses).get("coverage_debt") or 0)
        result[surface] += total
    robinhood = _dict(coverage.get("robinhood"))
    if result["ROBINHOOD_CHAIN"] == 0 and robinhood:
        result["ROBINHOOD_CHAIN"] = int(robinhood.get("coverage_debt_count") or 0)
    total = int(coverage.get("coverage_debt_count") or 0)
    attributed = sum(result.values())
    if total > attributed:
        # The merged payload cannot prove which local surface owns the remainder.
        # Keep it visible as unattributed instead of silently charging Robinhood or
        # clearing the Solana family.
        result["UNATTRIBUTED"] = total - attributed
    else:
        result["UNATTRIBUTED"] = 0
    return result


def _explicit_debt_rows(candidate_coverage: dict[str, Any], supplied: Iterable[dict[str, Any]] | None) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for source in (
        supplied,
        _dict(candidate_coverage).get("coverage_debt_rows"),
        _dict(candidate_coverage).get("debt_rows"),
        _dict(candidate_coverage).get("unobserved_candidates"),
    ):
        if isinstance(source, list):
            rows.extend(dict(row) for row in source if isinstance(row, dict))
    return rows


def _debt_rollups(rows: list[dict[str, Any]], surface_counts: dict[str, int]) -> dict[str, Any]:
    dimensions = ("venue", "family", "lifecycle", "regime", "fomo_lane", "risk_signature")
    rollups: dict[str, dict[str, int]] = {dimension: {} for dimension in dimensions}
    for row in rows:
        for dimension in dimensions:
            value = row.get(dimension)
            if value in (None, "") and dimension == "fomo_lane":
                value = row.get("lane")
            label = str(value or "UNKNOWN")
            rollups[dimension][label] = rollups[dimension].get(label, 0) + 1
    return {
        "by_surface": dict(surface_counts),
        "by_dimension": rollups,
        "explicit_debt_row_count": len(rows),
        "dimensions": list(dimensions),
    }


def _debt_matches_context(debt: dict[str, Any], context: dict[str, str], *, rows: list[dict[str, Any]]) -> bool:
    family = context["family"]
    required_surfaces = set(_required_surfaces(rows, family))
    debt_surface = _surface(debt.get("surface"))
    if debt_surface != "UNKNOWN" and debt_surface not in required_surfaces:
        return False
    for dimension in ("venue", "family", "lifecycle", "regime", "risk_signature"):
        raw = debt.get(dimension)
        if raw in (None, "", "UNKNOWN"):
            continue
        expected = context[dimension]
        if dimension in {"family", "venue"}:
            if _upper(raw) != _upper(expected):
                return False
        elif str(raw) != str(expected):
            return False
    lane = debt.get("fomo_lane", debt.get("lane"))
    if lane not in (None, "", "UNKNOWN"):
        lanes = {str(row.get("lane") or "UNKNOWN") for row in rows}
        if str(lane) not in lanes:
            return False
    return True


def _context_coverage(
    context: dict[str, str],
    rows: list[dict[str, Any]],
    *,
    candidate_coverage: dict[str, Any],
    explicit_debt_rows: list[dict[str, Any]],
    surface_debt_counts: dict[str, int],
) -> dict[str, Any]:
    required = _required_surfaces(rows, context["family"])
    matching = [debt for debt in explicit_debt_rows if _debt_matches_context(debt, context, rows=rows)]
    surface_unknown_debt = sum(int(surface_debt_counts.get(surface, 0)) for surface in required)
    if explicit_debt_rows:
        relevant_debt = len(matching)
        attribution = "exact_or_dimension_scoped_debt_rows"
    else:
        relevant_debt = surface_unknown_debt
        attribution = "surface_scoped_fail_closed_without_context_debt_rows"
    unattributed = int(surface_debt_counts.get("UNATTRIBUTED", 0))
    # Unattributed merged debt blocks only when no scoped evidence can show it belongs
    # to an unrelated surface/context. This is deliberately fail-closed.
    if unattributed and not explicit_debt_rows:
        relevant_debt += unattributed
    return {
        "healthy": relevant_debt == 0,
        "relevant_coverage_debt_count": relevant_debt,
        "matching_debt_rows": matching,
        "required_surfaces": required,
        "surface_debt_counts": {surface: int(surface_debt_counts.get(surface, 0)) for surface in required},
        "unattributed_global_debt_count": unattributed,
        "attribution_precision": attribution,
        "unrelated_surface_debt_does_not_block": True,
    }


def _row_values(clusters: Iterable[dict[str, Any]]) -> list[float]:
    values: list[float] = []
    for row in clusters:
        validated = validate_row_return(row)
        if validated.validity and validated.normalized_fraction is not None:
            values.append(validated.normalized_fraction)
    return values


def _epoch_compatibility(rows: list[dict[str, Any]], required_surfaces: list[str]) -> dict[str, Any]:
    measurements = sorted({str(row.get("measurement_epoch") or "") for row in rows if row.get("measurement_epoch")})
    executions = sorted({str(row.get("execution_model_epoch") or "") for row in rows if row.get("execution_model_epoch")})
    measurement_ok = len(measurements) == 1 and measurements[0] == MEASUREMENT_EPOCH
    execution_single = len(executions) == 1
    if "SOLANA" in required_surfaces or "FOMO" in required_surfaces:
        execution_ok = execution_single and executions[0] == ACTIVE_EXECUTION_MODEL_EPOCH
    else:
        execution_ok = execution_single
    return {
        "pass": bool(measurement_ok and execution_ok),
        "measurement_epochs": measurements,
        "execution_model_epochs": executions,
        "required_measurement_epoch": MEASUREMENT_EPOCH,
        "required_solana_fomo_execution_model_epoch": ACTIVE_EXECUTION_MODEL_EPOCH,
        "silent_epoch_pooling_allowed": False,
    }


def _explicit_kill(rows: list[dict[str, Any]], promotion_family: dict[str, Any]) -> tuple[bool, str]:
    if bool(promotion_family.get("killed")):
        return True, "family_promotion_record_killed"
    state = str(promotion_family.get("state") or promotion_family.get("promotion_state") or "")
    if state.startswith("killed"):
        return True, state
    for row in rows:
        if bool(row.get("killed")) or str(row.get("kill_state") or "").startswith("killed"):
            return True, str(row.get("kill_state") or "row_marked_killed")
    return False, "not_killed"


def _context_statistics(rows: list[dict[str, Any]], family: str) -> dict[str, Any]:
    clusters = cluster_economic_rows(rows, family=family, promotion_only=True)
    validation = [row for row in clusters if str(row.get("evidence_partition") or "") == "validation"]
    holdout = [row for row in clusters if str(row.get("evidence_partition") or "") == "holdout"]
    values = _row_values(clusters)
    validation_values = _row_values(validation)
    holdout_values = _row_values(holdout)
    validation_profile = robust_profile(validation_values)
    selected_fraction = float(validation_profile.get("best_fraction") or 0.0)
    all_profile = robust_profile(values, fixed_fraction=selected_fraction)
    holdout_profile = robust_profile(holdout_values, fixed_fraction=selected_fraction)
    severity = max((_safe(row.get("risk_severity"), 0.0) for row in rows), default=0.0)
    signature = str(rows[0].get("risk_signature") or "clean") if rows else "clean"
    burden = hazard_requirements(severity, signature)
    minimum_independent = int(burden["minimum_independent_outcomes"])
    minimum_exact = int(burden["minimum_exact_outcomes"])
    hurdle = float(burden["minimum_expected_log_growth"])
    maturity = len(clusters) >= minimum_independent and len(clusters) >= minimum_exact
    validation_pass = bool(
        validation_values
        and validation_profile.get("lower_confidence_expected_log_growth") is not None
        and _safe(validation_profile.get("lower_confidence_expected_log_growth"), -1.0) > hurdle
        and validation_profile.get("leave_best_trade_out_mean") is not None
        and _safe(validation_profile.get("leave_best_trade_out_mean"), -1.0) > 0.0
    )
    holdout_pass = bool(
        holdout_values
        and holdout_profile.get("lower_confidence_expected_log_growth") is not None
        and _safe(holdout_profile.get("lower_confidence_expected_log_growth"), -1.0) > 0.0
        and holdout_profile.get("leave_best_trade_out_mean") is not None
        and _safe(holdout_profile.get("leave_best_trade_out_mean"), -1.0) > 0.0
    )
    top_winner_pass = bool(
        all_profile.get("leave_best_trade_out_mean") is not None
        and _safe(all_profile.get("leave_best_trade_out_mean"), -1.0) > 0.0
        and all_profile.get("lower_confidence_expected_log_growth") is not None
        and _safe(all_profile.get("lower_confidence_expected_log_growth"), -1.0) > hurdle
        and all_profile.get("remove_top_1_expected_log_growth_bootstrap_ci95_lower") is not None
        and _safe(all_profile.get("remove_top_1_expected_log_growth_bootstrap_ci95_lower"), -1.0) > hurdle
        and all_profile.get("remove_top_3_expected_log_growth_bootstrap_ci95_lower") is not None
        and _safe(all_profile.get("remove_top_3_expected_log_growth_bootstrap_ci95_lower"), -1.0) > hurdle
    )
    stress_profiles = execution_stress_profiles(values, fixed_fraction=selected_fraction)
    stress_scenarios = {
        name: bool(
            profile.get("lower_confidence_expected_log_growth") is not None
            and _safe(profile.get("lower_confidence_expected_log_growth"), -1.0) > 0.0
            and profile.get("leave_best_trade_out_mean") is not None
            and _safe(profile.get("leave_best_trade_out_mean"), -1.0) > 0.0
        )
        for name, profile in stress_profiles.items()
    }
    stress_pass = bool(stress_scenarios) and all(stress_scenarios.values())
    return {
        "raw_n": len(rows),
        "independent_n": len(clusters),
        "validation_n": len(validation),
        "holdout_n": len(holdout),
        "minimum_independent_outcomes": minimum_independent,
        "minimum_exact_outcomes": minimum_exact,
        "minimum_expected_log_growth": hurdle,
        "selected_fraction": selected_fraction,
        "log_growth": all_profile.get("best_expected_log_growth"),
        "robust_lower_bound": all_profile.get("lower_confidence_expected_log_growth"),
        "es20": all_profile.get("expected_shortfall_20"),
        "drawdown": all_profile.get("max_drawdown_at_best_fraction"),
        "maturity_pass": maturity,
        "validation_pass": validation_pass,
        "holdout_pass": holdout_pass,
        "top_winner_robustness_pass": top_winner_pass,
        "stress_pass": stress_pass,
        "stress_result": stress_scenarios,
        "validation_profile": validation_profile,
        "holdout_profile": holdout_profile,
        "full_profile": all_profile,
    }


def build_phase17_context_certification(
    records: Iterable[dict[str, Any]],
    *,
    promotion_certification: dict[str, Any],
    candidate_coverage: dict[str, Any],
    forward_certification: dict[str, Any],
    operations_proof: dict[str, Any],
    base_family_certifications: dict[str, Any] | None = None,
    coverage_debt_rows: Iterable[dict[str, Any]] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    rows = [dict(row) for row in records if isinstance(row, dict)]
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    contexts: dict[str, dict[str, str]] = {}
    for row in rows:
        context = exact_context(row)
        key = context_key(context)
        grouped[key].append(row)
        contexts[key] = context

    forward = _dict(forward_certification)
    attestations = _surface_attestations(forward)
    transports = _transport_status(forward)
    release_identity_ok = _release_identity_ok(forward)
    safety_ok = _safety_ok(forward)
    continuity = _continuity(operations_proof, now=_now(now))
    promotion = _dict(promotion_certification)
    promotion_families = _dict(promotion.get("families"))
    base_families = _dict(base_family_certifications)
    surface_debt = _surface_debt_counts(candidate_coverage)
    debt_rows = _explicit_debt_rows(candidate_coverage, coverage_debt_rows)
    debt_rollups = _debt_rollups(debt_rows, surface_debt)

    ledger: dict[str, Any] = {}
    by_family: dict[str, list[str]] = defaultdict(list)
    for key in sorted(grouped):
        context_rows = grouped[key]
        context = contexts[key]
        family = context["family"]
        required_surfaces = _required_surfaces(context_rows, family)
        statistics = _context_statistics(context_rows, family)
        coverage = _context_coverage(
            context,
            context_rows,
            candidate_coverage=candidate_coverage,
            explicit_debt_rows=debt_rows,
            surface_debt_counts=surface_debt,
        )
        epochs = _epoch_compatibility(context_rows, required_surfaces)
        killed, kill_state = _explicit_kill(context_rows, _dict(promotion_families.get(family)))
        attestation_pass = all(bool(attestations[surface]["attested"]) for surface in required_surfaces)
        transport_pass = all(bool(transports[surface]["ready"]) for surface in required_surfaces)
        continuity_pass = all(bool(continuity["surface_pass"].get(surface)) for surface in required_surfaces)
        blockers: list[str] = []
        if not statistics["maturity_pass"]:
            blockers.append("exact_context_evidence_maturity_not_met")
        if not statistics["validation_pass"] or not statistics["top_winner_robustness_pass"]:
            blockers.append("exact_context_robust_criteria_not_met")
        if not statistics["holdout_pass"]:
            blockers.append("exact_context_locked_holdout_not_proven")
        if not statistics["stress_pass"]:
            blockers.append("exact_context_execution_stress_not_proven")
        if not epochs["pass"]:
            blockers.append("exact_context_measurement_or_execution_epoch_incompatible")
        if not release_identity_ok:
            blockers.append("current_release_identity_not_proven")
        if not safety_ok:
            blockers.append("paper_only_safety_boundary_not_proven")
        if not attestation_pass:
            blockers.append("required_surface_release_attestation_missing")
        if not transport_pass:
            blockers.append("required_surface_transport_unhealthy")
        if not coverage["healthy"]:
            blockers.append("relevant_context_coverage_debt")
        if not continuity_pass:
            blockers.append("relevant_surface_continuity_unhealthy")
        if killed:
            blockers.append("exact_context_killed")
        production_proven = not blockers
        ledger[key] = {
            "context_key": key,
            "context": context,
            **statistics,
            "required_surfaces": required_surfaces,
            "surface_release_attestation": {surface: attestations[surface] for surface in required_surfaces},
            "surface_transport": {surface: transports[surface] for surface in required_surfaces},
            "epoch_compatibility": epochs,
            "coverage": coverage,
            "continuity_pass": continuity_pass,
            "promotion_state": "production_proven_exact_context" if production_proven else "blocked_exact_context",
            "kill_state": kill_state,
            "production_proven": production_proven,
            "blockers": blockers,
            "family_level_evidence_cannot_subsidize_this_context": True,
        }
        by_family[family].append(key)

    family_certification: dict[str, Any] = {}
    family_names = sorted(set(by_family) | set(base_families) | set(promotion_families))
    for family in family_names:
        keys = by_family.get(family, [])
        context_rows = [ledger[key] for key in keys]
        economic = _dict(base_families.get(family))
        economically_promising = bool(economic.get("economically_promising"))
        blockers: list[str] = []
        if not economically_promising:
            blockers.append("family_economic_rollup_not_promising")
        if not context_rows:
            blockers.append("no_exact_context_evidence")
        failed_contexts = [row["context_key"] for row in context_rows if not bool(row.get("production_proven"))]
        if failed_contexts:
            blockers.append("one_or_more_active_exact_contexts_unproven")
        production_proven = bool(economically_promising and context_rows and not failed_contexts)
        family_certification[family] = {
            "family": family,
            "economically_promising": economically_promising,
            "production_proven": production_proven,
            "active_exact_context_count": len(context_rows),
            "proven_exact_context_count": len(context_rows) - len(failed_contexts),
            "unproven_exact_context_keys": failed_contexts,
            "blockers": blockers,
            "family_rollup_reporting_only_for_authority": True,
            "family_n_cannot_subsidize_unproven_context": True,
        }

    global_coverage = _dict(candidate_coverage)
    global_coverage_pass = bool(global_coverage.get("coverage_complete")) and int(global_coverage.get("coverage_debt_count") or 0) == 0
    global_measurement_pass = bool(
        str(promotion.get("measurement_epoch") or "") == MEASUREMENT_EPOCH
        and str(global_coverage.get("measurement_epoch") or "") == MEASUREMENT_EPOCH
        and bool(promotion.get("promotion_eligible_measurement"))
    )
    system_forward = bool(forward.get("system_forward_certified"))
    system_pass = bool(system_forward and global_coverage_pass and global_measurement_pass and continuity["pass"] and safety_ok)
    system_blockers: list[str] = []
    if not system_forward:
        system_blockers.append("global_forward_certification_incomplete")
    if not global_coverage_pass:
        system_blockers.append("global_candidate_coverage_incomplete")
    if not global_measurement_pass:
        system_blockers.append("global_measurement_epoch_or_eligibility_invalid")
    if not continuity["pass"]:
        system_blockers.append("global_operational_continuity_incomplete")
    if not safety_ok:
        system_blockers.append("paper_only_safety_boundary_not_proven")

    return {
        "phase17_version": PHASE17_VERSION,
        "system_certification": {
            "pass": system_pass,
            "question": "are_all_configured_production_research_surfaces_healthy",
            "global_forward_certified": system_forward,
            "global_candidate_coverage_pass": global_coverage_pass,
            "global_measurement_pass": global_measurement_pass,
            "global_continuity_pass": continuity["pass"],
            "surface_release_attestation": attestations,
            "surface_transport": transports,
            "blockers": system_blockers,
        },
        "family_certification": family_certification,
        "exact_context_proof_ledger": ledger,
        "context_specific_coverage_debt": debt_rollups,
        "context_dimensions": list(CONTEXT_DIMENSIONS),
        "surface_scopes": list(SURFACE_SCOPES),
        "cost_and_latency_bands_are_reporting_bins_not_new_authority_thresholds": True,
        "family_level_sample_subsidy_allowed": False,
        "silent_measurement_or_execution_epoch_pooling_allowed": False,
        "changes_strategy_authority": False,
        "changes_economic_thresholds": False,
        "changes_selection_authority": False,
        "changes_sizing_authority": False,
        "changes_exit_authority": False,
        "retrospective_entry_authority": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


def _store_coverage_debt_rows(store: Any) -> list[dict[str, Any]]:
    """Read canonical coverage-debt context without creating schema or writing state."""
    try:
        with store._lock:
            state_rows = store.db.execute(
                "SELECT surface,candidate_id,payload_json FROM v51_candidate_current_state "
                "WHERE status='coverage_debt' ORDER BY surface,candidate_id,stage_index"
            ).fetchall()
    except Exception:
        return []
    result: list[dict[str, Any]] = []
    for state in state_rows:
        surface = _surface(state["surface"] if hasattr(state, "keys") else state[0])
        candidate_id = str(state["candidate_id"] if hasattr(state, "keys") else state[1])
        raw_payload = state["payload_json"] if hasattr(state, "keys") else state[2]
        try:
            payload = json.loads(str(raw_payload or "{}"))
        except Exception:
            payload = {}
        item: dict[str, Any] = {"surface": surface, "candidate_id": candidate_id}
        if isinstance(payload, dict):
            item.update({key: payload.get(key) for key in ("venue", "family", "lifecycle", "regime", "risk_signature", "lane") if payload.get(key) is not None})
        try:
            with store._lock:
                candidate = store.db.execute(
                    "SELECT venue,lifecycle,payload_json FROM v51_candidates WHERE surface=? AND candidate_id=? LIMIT 1",
                    (surface, candidate_id),
                ).fetchone()
            if candidate is not None:
                venue = candidate["venue"] if hasattr(candidate, "keys") else candidate[0]
                lifecycle = candidate["lifecycle"] if hasattr(candidate, "keys") else candidate[1]
                if venue and not item.get("venue"):
                    item["venue"] = venue
                if lifecycle and not item.get("lifecycle"):
                    item["lifecycle"] = lifecycle
        except Exception:
            pass
        result.append(item)
    return result


def build_phase17_profitability_certification(
    store: Any,
    *,
    promotion_certification: dict[str, Any],
    candidate_coverage: dict[str, Any],
    forward_certification: dict[str, Any],
    operations_proof: dict[str, Any],
    robinhood_proof: dict[str, Any] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    # Phase 17 is an authority-classification layer over the existing frozen Phase 14
    # economic rollup. It does not alter any return, threshold, selection, sizing, or
    # execution rule.
    from .v51_phase14_profitability_certification import (
        CLASS_ECONOMICALLY_PROMISING,
        CLASS_INSUFFICIENT_EVIDENCE,
        CLASS_PRODUCTION_PROVEN,
        build_phase14_profitability_certification,
    )

    base = build_phase14_profitability_certification(
        store,
        promotion_certification=promotion_certification,
        candidate_coverage=candidate_coverage,
        forward_certification=forward_certification,
        operations_proof=operations_proof,
        robinhood_proof=robinhood_proof,
        now=now,
    )
    records = combined_promotion_records(store, robinhood_proof)
    phase17 = build_phase17_context_certification(
        records,
        promotion_certification=promotion_certification,
        candidate_coverage=candidate_coverage,
        forward_certification=forward_certification,
        operations_proof=operations_proof,
        base_family_certifications=_dict(base.get("families")),
        coverage_debt_rows=_store_coverage_debt_rows(store),
        now=now,
    )

    families = _dict(base.get("families"))
    phase17_families = _dict(phase17.get("family_certification"))
    for family, proof in families.items():
        if not isinstance(proof, dict):
            continue
        contextual = _dict(phase17_families.get(family))
        promising = bool(proof.get("economically_promising"))
        context_proven = bool(contextual.get("production_proven"))
        proof["phase17_family_certification"] = contextual
        proof["production_proven"] = bool(promising and context_proven)
        proof["classification"] = (
            CLASS_PRODUCTION_PROVEN
            if proof["production_proven"]
            else CLASS_ECONOMICALLY_PROMISING
            if promising
            else CLASS_INSUFFICIENT_EVIDENCE
        )
        blockers = [str(value) for value in (proof.get("blockers") or []) if str(value) != "global_production_proof_incomplete"]
        for value in contextual.get("blockers") or []:
            if str(value) not in blockers:
                blockers.append(str(value))
        proof["blockers"] = blockers
        proof["global_system_health_is_not_family_authority"] = True

    promising = sorted(name for name, proof in families.items() if bool(_dict(proof).get("economically_promising")))
    proven = sorted(name for name, proof in families.items() if bool(_dict(proof).get("production_proven")))
    all_admitted_contexts_proven = bool(promising) and all(name in proven for name in promising)
    base["families"] = families
    base["economically_promising_families"] = promising
    base["production_proven_families"] = proven
    base["economically_promising"] = bool(promising)
    base["production_proven"] = all_admitted_contexts_proven
    base["classification"] = (
        CLASS_PRODUCTION_PROVEN
        if all_admitted_contexts_proven
        else CLASS_ECONOMICALLY_PROMISING
        if promising
        else CLASS_INSUFFICIENT_EVIDENCE
    )
    base["phase17_version"] = PHASE17_VERSION
    base["system_certification"] = phase17["system_certification"]
    base["system_certification_pass"] = bool(_dict(phase17.get("system_certification")).get("pass"))
    base["family_certification"] = phase17["family_certification"]
    base["exact_context_proof_ledger"] = phase17["exact_context_proof_ledger"]
    base["context_specific_coverage_debt"] = phase17["context_specific_coverage_debt"]
    base["all_actively_admitted_authority_contexts_proven"] = all_admitted_contexts_proven
    base["system_health_failure_does_not_automatically_invalidate_unrelated_family"] = True
    base["family_level_sample_subsidy_allowed"] = False
    base["changes_strategy_authority"] = False
    base["changes_economic_thresholds"] = False
    base["paper_only"] = True
    base["live_money_authority"] = False
    base["signing_available"] = False
    base["transaction_submission_available"] = False
    return base


def status() -> dict[str, Any]:
    return {
        "phase17_version": PHASE17_VERSION,
        "repairs": [115, 116, 117, 118, 119, 120],
        "context_dimensions": list(CONTEXT_DIMENSIONS),
        "surface_scopes": list(SURFACE_SCOPES),
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
        "signing_available": SIGNING_AVAILABLE,
        "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
        "strategy_mutation_authority": STRATEGY_MUTATION_AUTHORITY,
        "live_promotion_authority": LIVE_PROMOTION_AUTHORITY,
        "retrospective_entry_authority": RETROSPECTIVE_ENTRY_AUTHORITY,
    }


__all__ = [
    "CONTEXT_DIMENSIONS",
    "PHASE17_VERSION",
    "SURFACE_SCOPES",
    "build_phase17_context_certification",
    "build_phase17_profitability_certification",
    "context_key",
    "exact_context",
    "status",
]
