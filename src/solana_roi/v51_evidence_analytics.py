from __future__ import annotations

import json
import math
from collections import defaultdict
from datetime import datetime, timezone
from statistics import mean, median
from typing import Any, Iterable

from .strategy_v51_authority import AUTHORITY_ID, ECONOMIC_FREEZE_EPOCH, authority
from .v51_economic_clustering import cluster_economic_rows, cluster_economic_rows_legacy_pre103
from .v51_economic_core import execution_stress_profiles, hierarchical_profile, robust_profile
from .v51_promotion_proof import (
    FDR_Q,
    benjamini_hochberg,
    positive_edge_p_value,
    refresh_release_attestation,
)
from .v51_return_validation import (
    STATISTICS_VERSION,
    persist_invalid_measurement_debt,
    return_integrity_summary,
    validate_return,
    validate_row_return,
)


ANALYTICS_VERSION = "v51-evidence-validity-analytics-v2-statistical-integrity"
PORTFOLIO_VERSION = "v51-single-capital-base-reconciliation-v2-return-integrity"
SLO_VERSION = "v51-forward-proof-slo-v1"
COUNTERFACTUAL_VERSION = "v51-rejected-counterfactual-ledger-v1"
COST_LEDGER_VERSION = "v51-normalized-execution-cost-ledger-v1"
CORRELATION_MIN_ALIGNED_PERIODS = 10
FUTURE_MATURE_CORRELATION_MAX_ABS = 0.50


def _utcnow_dt() -> datetime:
    return datetime.now(timezone.utc)


def _utcnow() -> str:
    return _utcnow_dt().isoformat()


def _table_exists(store: Any, table: str) -> bool:
    try:
        with store._lock:
            return store.db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (table,)
            ).fetchone() is not None
    except Exception:
        return False


def _columns(store: Any, table: str) -> set[str]:
    if not _table_exists(store, table):
        return set()
    with store._lock:
        return {str(row["name"]) for row in store.db.execute(f"PRAGMA table_info({table})").fetchall()}


def _safe(value: Any, default: float = 0.0) -> float:
    """Finite numeric helper for non-return operational fields only."""
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


def _surface_for_row(row: dict[str, Any]) -> str:
    surface = str(row.get("surface") or "")
    if surface == "SOLANA_ALPHA":
        return "SOLANA"
    return surface or "UNKNOWN"


def _valid_cluster_values(rows: Iterable[dict[str, Any]]) -> list[float]:
    values: list[float] = []
    for row in rows:
        validated = validate_row_return(row)
        if validated.validity and validated.normalized_fraction is not None:
            values.append(validated.normalized_fraction)
    return values


def _family_minimum(family: str, family_rows: list[dict[str, Any]]) -> tuple[int, int, float]:
    risk_signature = "clean"
    severity = 0.0
    for row in family_rows:
        signature = str(row.get("risk_signature") or "clean")
        if signature != "clean":
            risk_signature = signature
            severity = max(severity, _safe(row.get("risk_severity"), 0.45))
    requirements = authority()["hazard_evidence_burden"]
    if risk_signature == "clean":
        cfg = requirements["clean"]
    elif severity >= 0.70:
        cfg = requirements["extreme"]
    elif severity >= 0.50:
        cfg = requirements["high"]
    elif severity >= 0.30:
        cfg = requirements["moderate"]
    else:
        cfg = requirements["low"]
    return (
        int(cfg["minimum_independent_outcomes"]),
        int(cfg["minimum_exact_outcomes"]),
        float(cfg["minimum_expected_log_growth"]),
    )


def _capital_efficiency(profile: dict[str, Any], evidence_n: int, minimum_n: int) -> float:
    growth = profile.get("lower_confidence_expected_log_growth")
    if growth is None or _safe(growth, -1.0) <= 0.0:
        return 0.0
    shortfall = min(0.0, _safe(profile.get("expected_shortfall_20")))
    drawdown = max(0.0, _safe(profile.get("max_drawdown_at_best_fraction")))
    confidence = min(1.0, evidence_n / max(1.0, float(minimum_n)))
    return _safe(growth) * confidence / (1.0 + drawdown + abs(shortfall))


def _audit_records(store: Any) -> list[dict[str, Any]]:
    from .v51_economic_certification import _records

    return [dict(row) for row in _records(store)]


def ensure_execution_cost_schema(store: Any) -> None:
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS v51_execution_cost_ledger ("
            "surface TEXT NOT NULL,family TEXT NOT NULL,release_commit TEXT,source_signature TEXT NOT NULL,"
            "token_mint TEXT,entry_cost_fraction REAL,exit_cost_fraction REAL,round_trip_cost_fraction REAL,"
            "cost_source TEXT NOT NULL,normalized_at TEXT NOT NULL,paper_only INTEGER NOT NULL,"
            "live_money_authority INTEGER NOT NULL,PRIMARY KEY(surface,source_signature))"
        )


def _fomo_cost_map(store: Any) -> dict[tuple[str, str], float]:
    result: dict[tuple[str, str], float] = {}
    table = "profit_first_final_trials"
    cols = _columns(store, table)
    if not {"release_commit", "source_signature", "round_trip_cost_fraction"}.issubset(cols):
        return result
    lane_filter = " AND lane='unified_profit_maximizer'" if "lane" in cols else ""
    with store._lock:
        rows = store.db.execute(
            "SELECT release_commit,source_signature,round_trip_cost_fraction FROM profit_first_final_trials "
            "WHERE round_trip_cost_fraction IS NOT NULL" + lane_filter + " ORDER BY rowid"
        ).fetchall()
    for row in rows:
        result[(str(row["release_commit"] or ""), str(row["source_signature"]))] = _safe(
            row["round_trip_cost_fraction"], -1.0
        )
    return {key: value for key, value in result.items() if value >= 0.0}


def refresh_execution_cost_ledger(store: Any) -> dict[str, Any]:
    ensure_execution_cost_schema(store)
    records = _audit_records(store)
    fomo_costs = _fomo_cost_map(store)
    normalized = 0
    unknown = 0
    now = _utcnow()
    with store._lock, store.db:
        for row in records:
            surface = _surface_for_row(row)
            signature = str(row.get("source_signature") or row.get("trial_id") or row.get("id") or "")
            if not signature:
                continue
            value = row.get("round_trip_cost_fraction")
            source = "settled_trial_context"
            if surface == "FOMO" and value is None:
                value = fomo_costs.get((str(row.get("release_commit") or ""), signature))
                source = "profit_first_amount_specific_round_trip_cost"
            cost = _safe(value, -1.0)
            if cost < 0.0:
                unknown += 1
                continue
            store.db.execute(
                "INSERT INTO v51_execution_cost_ledger(surface,family,release_commit,source_signature,token_mint,"
                "entry_cost_fraction,exit_cost_fraction,round_trip_cost_fraction,cost_source,normalized_at,paper_only,live_money_authority) "
                "VALUES (?,?,?,?,?,NULL,NULL,?,?,?,1,0) ON CONFLICT(surface,source_signature) DO UPDATE SET "
                "family=excluded.family,release_commit=excluded.release_commit,token_mint=excluded.token_mint,"
                "round_trip_cost_fraction=excluded.round_trip_cost_fraction,cost_source=excluded.cost_source,"
                "normalized_at=excluded.normalized_at,paper_only=1,live_money_authority=0",
                (
                    surface,
                    str(row.get("family") or "UNKNOWN"),
                    row.get("release_commit"),
                    signature,
                    row.get("token_mint"),
                    cost,
                    source,
                    now,
                ),
            )
            normalized += 1
    return {
        "cost_ledger_version": COST_LEDGER_VERSION,
        "normalized_outcome_count": normalized,
        "unknown_cost_outcome_count": unknown,
        "unit": "fraction_of_notional_round_trip_after_amount_specific_quotes",
        "paper_only": True,
        "live_money_authority": False,
    }


def _cost_overlay(store: Any) -> dict[tuple[str, str], float]:
    refresh_execution_cost_ledger(store)
    with store._lock:
        rows = store.db.execute(
            "SELECT surface,source_signature,round_trip_cost_fraction FROM v51_execution_cost_ledger"
        ).fetchall()
    return {
        (str(row["surface"]), str(row["source_signature"])): float(row["round_trip_cost_fraction"])
        for row in rows
    }


def promotion_records(store: Any) -> list[dict[str, Any]]:
    from .v51_measurement_integrity import EXECUTION_MODEL_EPOCH, MEASUREMENT_EPOCH

    records = _audit_records(store)
    costs = _cost_overlay(store)
    if not _table_exists(store, "v51_release_attestation"):
        return []
    with store._lock:
        attested_rows = store.db.execute(
            "SELECT release_commit,surface FROM v51_release_attestation WHERE measurement_epoch=? AND attested=1",
            (MEASUREMENT_EPOCH,),
        ).fetchall()
    attested = {(str(row["release_commit"]), str(row["surface"])) for row in attested_rows}
    selected: list[dict[str, Any]] = []
    for row in records:
        surface = _surface_for_row(row)
        release = str(row.get("release_commit") or "")
        if (release, surface) not in attested:
            continue
        item = dict(row)
        item.setdefault("surface", surface)
        item.setdefault("measurement_epoch", MEASUREMENT_EPOCH)
        item.setdefault("execution_model_epoch", EXECUTION_MODEL_EPOCH)
        signature = str(item.get("source_signature") or item.get("trial_id") or item.get("id") or "")
        key = (surface, signature)
        if key in costs:
            item["round_trip_cost_fraction"] = costs[key]
        selected.append(item)
    return selected


def _legacy_promotion_claim(
    clusters: list[dict[str, Any]],
    *,
    minimum_n: int,
    hurdle: float,
    fdr_accepted: bool,
) -> tuple[bool, dict[str, Any]]:
    values = _valid_cluster_values(clusters)
    profile = robust_profile(values)
    validation_n = sum(1 for row in clusters if row.get("evidence_partition") == "validation")
    holdout_n = sum(1 for row in clusters if row.get("evidence_partition") == "holdout")
    robust_positive = bool(
        profile.get("leave_best_trade_out_mean") is not None
        and _safe(profile.get("leave_best_trade_out_mean")) > 0.0
        and profile.get("best_expected_log_growth") is not None
        and _safe(profile.get("best_expected_log_growth")) > hurdle
    )
    return bool(
        len(clusters) >= minimum_n
        and validation_n > 0
        and holdout_n > 0
        and fdr_accepted
        and robust_positive
    ), profile


def _promotion_certification_from_records(rows: list[dict[str, Any]]) -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get("family") or "UNKNOWN")].append(row)

    family_clusters = {
        family: cluster_economic_rows(items, family=family, promotion_only=True)
        for family, items in grouped.items()
    }
    legacy_clusters = {
        family: cluster_economic_rows_legacy_pre103(items, family=family, promotion_only=True)
        for family, items in grouped.items()
    }
    p_values = {
        family: positive_edge_p_value(_valid_cluster_values(clusters))
        for family, clusters in family_clusters.items()
    }
    legacy_p_values = {
        family: positive_edge_p_value(_valid_cluster_values(clusters))
        for family, clusters in legacy_clusters.items()
    }
    fdr = benjamini_hochberg(p_values, q=FDR_Q)
    legacy_fdr = benjamini_hochberg(legacy_p_values, q=FDR_Q)

    families: dict[str, Any] = {}
    scores: dict[str, float] = {}
    for family, clusters in family_clusters.items():
        values = _valid_cluster_values(clusters)
        validation_values = _valid_cluster_values(
            row for row in clusters if row.get("evidence_partition") == "validation"
        )
        holdout_values = _valid_cluster_values(
            row for row in clusters if row.get("evidence_partition") == "holdout"
        )
        validation_n = len(validation_values)
        holdout_n = len(holdout_values)
        minimum_n, exact_min, hurdle = _family_minimum(family, grouped[family])
        integrity = return_integrity_summary(grouped[family])
        validation_profile = robust_profile(validation_values)
        selected_fraction = float(validation_profile.get("best_fraction") or 0.0)
        profile = robust_profile(values, fixed_fraction=selected_fraction)
        holdout_profile = robust_profile(holdout_values, fixed_fraction=selected_fraction)
        hp = hierarchical_profile(values, (), (), risk_signature="clean", max_fraction=0.20)
        robust_positive = bool(
            integrity.get("proof_eligible")
            and profile.get("leave_best_trade_out_mean") is not None
            and _safe(profile.get("leave_best_trade_out_mean")) > 0.0
            and profile.get("lower_confidence_expected_log_growth") is not None
            and _safe(profile.get("lower_confidence_expected_log_growth"), -1.0) > hurdle
            and holdout_profile.get("leave_best_trade_out_mean") is not None
            and _safe(holdout_profile.get("leave_best_trade_out_mean")) > 0.0
            and holdout_profile.get("lower_confidence_expected_log_growth") is not None
            and _safe(holdout_profile.get("lower_confidence_expected_log_growth"), -1.0) > 0.0
        )
        promotion_claim_valid = bool(
            len(clusters) >= minimum_n
            and validation_n > 0
            and holdout_n > 0
            and fdr.get(family, False)
            and robust_positive
        )
        legacy_claim, legacy_profile = _legacy_promotion_claim(
            legacy_clusters.get(family, []),
            minimum_n=minimum_n,
            hurdle=hurdle,
            fdr_accepted=bool(legacy_fdr.get(family, False)),
        )
        score = _capital_efficiency(profile, len(clusters), minimum_n) if promotion_claim_valid else 0.0
        scores[family] = score
        families[family] = {
            "statistics_version": STATISTICS_VERSION,
            "raw_outcome_count": len(grouped[family]),
            "independent_event_cluster_count": len(clusters),
            "validation_cluster_count": validation_n,
            "holdout_cluster_count": holdout_n,
            "minimum_independent_outcomes": minimum_n,
            "minimum_exact_outcomes_reference": exact_min,
            "minimum_expected_log_growth": hurdle,
            "positive_edge_p_value": p_values.get(family, 1.0),
            "fdr_q": FDR_Q,
            "fdr_accepted": bool(fdr.get(family, False)),
            "promotion_claim_valid": promotion_claim_valid,
            "legacy_pre103_promotion_claim_valid": legacy_claim,
            "economic_measurement_integrity": integrity,
            "selected_fraction": selected_fraction,
            "selected_fraction_source": "validation",
            "holdout_fraction_reoptimized": False,
            "validation_profile": validation_profile,
            "holdout_profile": holdout_profile,
            "robust_profile": profile,
            "legacy_pre103_robust_profile": legacy_profile,
            "promotion_kill_profile": hp,
            "capital_efficiency_score": score,
            "execution_stress": execution_stress_profiles(values, fixed_fraction=selected_fraction),
            "partition_policy": "discovery_excluded; validation_selects_fraction; locked_holdout_evaluates_preselected_fraction",
        }

    priority = list(authority()["research_family_priority"])
    ordered = sorted(
        set(priority) | set(families),
        key=lambda family: (-scores.get(family, 0.0), priority.index(family) if family in priority else len(priority), family),
    )
    positive = [family for family in ordered if scores.get(family, 0.0) > 0.0]
    total = sum(scores[family] for family in positive)
    remaining = 1.0
    weights: dict[str, float] = {}
    for family in positive:
        raw = scores[family] / total if total > 0.0 else 0.0
        weight = min(float(authority()["allocation"]["immature_family_max_weight"]), raw, remaining)
        weights[family] = weight
        remaining -= weight
    integrity = return_integrity_summary(rows)
    return {
        "promotion_certification_version": ANALYTICS_VERSION,
        "statistics_version": STATISTICS_VERSION,
        "authority_id": AUTHORITY_ID,
        "economic_freeze_epoch": ECONOMIC_FREEZE_EPOCH,
        "evidence_scope": "live_attested_measurement_compatible_validation_plus_locked_holdout_event_clusters",
        "audit_evidence_scope_is_separate": True,
        "raw_attested_outcome_count": len(rows),
        "independent_event_cluster_count": sum(len(value) for value in family_clusters.values()),
        "economic_measurement_integrity": integrity,
        "families": families,
        "research_family_ranking": ordered,
        "paper_allocation_weights": weights,
        "paper_cash_weight": max(0.0, remaining),
        "active_family_cap": float(authority()["allocation"]["immature_family_max_weight"]),
        "fdr_q": FDR_Q,
        "paper_only": True,
        "live_money_authority": False,
    }


def build_promotion_certification(store: Any) -> dict[str, Any]:
    refresh_release_attestation(store)
    records = promotion_records(store)
    debt = persist_invalid_measurement_debt(store, records)
    result = _promotion_certification_from_records(records)
    result["persisted_economic_measurement_debt"] = debt
    return result


def _pearson(left: list[float], right: list[float]) -> float | None:
    if len(left) != len(right) or len(left) < 2:
        return None
    lmean, rmean = mean(left), mean(right)
    num = sum((a - lmean) * (b - rmean) for a, b in zip(left, right))
    lden = math.sqrt(sum((a - lmean) ** 2 for a in left))
    rden = math.sqrt(sum((b - rmean) ** 2 for b in right))
    if lden <= 0.0 or rden <= 0.0:
        return None
    return num / (lden * rden)


def build_cross_family_correlation(store: Any, rows: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    records = rows if rows is not None else promotion_records(store)
    clustered: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for family, items in _group_by_family(records).items():
        clustered[family] = cluster_economic_rows(items, family=family, promotion_only=True)
    by_family_day: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    invalid_count = 0
    for family, items in clustered.items():
        for row in items:
            at = _dt(row.get("settled_at"))
            if at is None:
                continue
            validated = validate_row_return(row)
            if not validated.validity or validated.normalized_fraction is None:
                invalid_count += 1
                continue
            by_family_day[family][at.date().isoformat()].append(validated.normalized_fraction)
    families = sorted(by_family_day)
    pairs: dict[str, Any] = {}
    for index, left_name in enumerate(families):
        for right_name in families[index + 1 :]:
            shared = sorted(set(by_family_day[left_name]) & set(by_family_day[right_name]))
            left = [mean(by_family_day[left_name][day]) for day in shared]
            right = [mean(by_family_day[right_name][day]) for day in shared]
            corr = _pearson(left, right)
            key = f"{left_name}|{right_name}"
            pairs[key] = {
                "left": left_name,
                "right": right_name,
                "aligned_period_count": len(shared),
                "pearson_correlation": corr if len(shared) >= CORRELATION_MIN_ALIGNED_PERIODS else None,
                "both_negative_rate": (
                    sum(1 for a, b in zip(left, right) if a < 0.0 and b < 0.0) / len(shared)
                    if shared else None
                ),
                "mature": len(shared) >= CORRELATION_MIN_ALIGNED_PERIODS and corr is not None,
            }
    return {
        "correlation_proof_version": ANALYTICS_VERSION,
        "statistics_version": STATISTICS_VERSION,
        "aligned_period": "UTC settlement day",
        "minimum_aligned_periods": CORRELATION_MIN_ALIGNED_PERIODS,
        "invalid_economic_measurement_count": invalid_count,
        "pairs": pairs,
        "unknown_correlation_is_zero": False,
        "paper_only": True,
        "live_money_authority": False,
    }


def _group_by_family(records: Iterable[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in records:
        result[str(row.get("family") or "UNKNOWN")].append(dict(row))
    return result


def build_maturity_allocation_proof(store: Any) -> dict[str, Any]:
    certification = build_promotion_certification(store)
    correlation = build_cross_family_correlation(store)
    family_pairs: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for pair in correlation["pairs"].values():
        family_pairs[str(pair["left"])].append(pair)
        family_pairs[str(pair["right"])].append(pair)
    families: dict[str, Any] = {}
    current_cap = float(authority()["allocation"]["immature_family_max_weight"])
    permanent_cap = float(authority()["allocation"]["permanent_family_max_weight"])
    for family, proof in certification["families"].items():
        pairs = family_pairs.get(family, [])
        mature_pairs = [pair for pair in pairs if pair.get("mature")]
        correlations_known = bool(mature_pairs)
        max_abs_corr = max(
            (abs(float(pair["pearson_correlation"])) for pair in mature_pairs if pair["pearson_correlation"] is not None),
            default=None,
        )
        material = (proof.get("execution_stress") or {}).get("material") or {}
        stressed_positive = _safe(material.get("lower_confidence_expected_log_growth"), -1.0) > 0.0
        future_eligible = bool(
            proof.get("promotion_claim_valid")
            and correlations_known
            and max_abs_corr is not None
            and max_abs_corr <= FUTURE_MATURE_CORRELATION_MAX_ABS
            and stressed_positive
        )
        families[family] = {
            "current_frozen_cap": current_cap,
            "permanent_authority_ceiling": permanent_cap,
            "future_50pct_eligibility": future_eligible,
            "correlation_evidence_mature": correlations_known,
            "max_abs_mature_pair_correlation": max_abs_corr,
            "material_stress_positive_expected_log_growth": stressed_positive,
            "promotion_claim_valid": bool(proof.get("promotion_claim_valid")),
            "active_cap_changed_by_this_proof": False,
        }
    return {
        "allocation_maturity_version": ANALYTICS_VERSION,
        "statistics_version": STATISTICS_VERSION,
        "active_allocation_cap_remains_frozen": current_cap,
        "future_permanent_ceiling": permanent_cap,
        "families": families,
        "paper_only": True,
        "live_money_authority": False,
    }


def _entry_time_map(store: Any) -> dict[tuple[str, str], str]:
    result: dict[tuple[str, str], str] = {}
    specs = (
        ("risk_conditioned_alpha_v5_trials", "SOLANA", "source_signature", ("observed_at", "created_at")),
        ("fomo_paper_trials", "FOMO", "source_signature", ("entry_observed_at", "created_at")),
        ("robinhood_paper_trials", "ROBINHOOD_CHAIN", "id", ("entry_observed_at", "opened_at", "created_at")),
    )
    for table, surface, key_column, time_columns in specs:
        cols = _columns(store, table)
        if key_column not in cols:
            continue
        time_col = next((column for column in time_columns if column in cols), None)
        if time_col is None:
            continue
        with store._lock:
            rows = store.db.execute(
                f"SELECT {key_column} AS key,{time_col} AS entered_at FROM {table} WHERE {time_col} IS NOT NULL"
            ).fetchall()
        for row in rows:
            result[(surface, str(row["key"]))] = str(row["entered_at"])
    return result


def _portfolio_reconcile(records: list[dict[str, Any]]) -> dict[str, Any]:
    events: list[tuple[datetime, int, dict[str, Any]]] = []
    fallback_count = 0
    invalid_count = 0
    valid_records: list[dict[str, Any]] = []
    for raw in records:
        validated = validate_row_return(raw)
        if not validated.validity or validated.normalized_fraction is None:
            invalid_count += 1
            continue
        row = dict(raw)
        row["_validated_net_return"] = validated.normalized_fraction
        valid_records.append(row)
        entry = _dt(row.get("entry_at"))
        settled = _dt(row.get("settled_at"))
        if settled is None:
            continue
        if entry is None:
            entry = settled
            fallback_count += 1
        if entry > settled:
            entry = settled
            fallback_count += 1
        events.append((entry, 0, row))
        events.append((settled, 1, row))
    events.sort(key=lambda item: (item[0], item[1], str(item[2].get("source_signature") or item[2].get("trial_id") or "")))
    cash = 1.0
    active: dict[str, float] = {}
    peak = 1.0
    worst_drawdown = 0.0
    shortfalls = 0
    entered = 0
    for _at, kind, row in events:
        identity = f"{_surface_for_row(row)}:{row.get('source_signature') or row.get('trial_id') or row.get('id')}"
        if kind == 0:
            requested_fraction = max(0.0, _safe(row.get("position_fraction")))
            nav = cash + sum(active.values())
            requested = requested_fraction * nav
            allocated = min(cash, requested)
            if allocated + 1e-12 < requested:
                shortfalls += 1
            if allocated > 0.0:
                active[identity] = allocated
                cash -= allocated
                entered += 1
        else:
            allocated = active.pop(identity, 0.0)
            if allocated > 0.0:
                cash += max(0.0, allocated * (1.0 + float(row["_validated_net_return"])))
        nav = cash + sum(active.values())
        peak = max(peak, nav)
        if peak > 0.0:
            worst_drawdown = max(worst_drawdown, 1.0 - nav / peak)
    final_nav = cash + sum(active.values())
    return {
        "portfolio_version": PORTFOLIO_VERSION,
        "statistics_version": STATISTICS_VERSION,
        "starting_nav": 1.0,
        "ending_nav": final_nav,
        "portfolio_roi_fraction": final_nav - 1.0,
        "portfolio_roi_pct": (final_nav - 1.0) * 100.0,
        "max_realized_drawdown_fraction": worst_drawdown,
        "raw_settled_record_count": len(records),
        "settled_record_count": len(valid_records),
        "invalid_economic_measurement_count": invalid_count,
        "invalid_economic_measurements_are_not_imputed": True,
        "chronological_entry_count": entered,
        "cash_capacity_shortfall_count": shortfalls,
        "entry_time_fallback_to_settlement_count": fallback_count,
        "overlapping_positions_share_one_capital_base": True,
        "unrealized_positions_marked_at_cost": True,
    }


def build_portfolio_reconciliation(store: Any) -> dict[str, Any]:
    entry_map = _entry_time_map(store)
    audit = _audit_records(store)
    promo = promotion_records(store)
    for rows in (audit, promo):
        for row in rows:
            surface = _surface_for_row(row)
            key = str(row.get("source_signature") or row.get("trial_id") or row.get("id") or "")
            row["entry_at"] = entry_map.get((surface, key))
    return {
        "portfolio_reconciliation_version": PORTFOLIO_VERSION,
        "statistics_version": STATISTICS_VERSION,
        "audit_epoch_portfolio": _portfolio_reconcile(audit),
        "promotion_compatible_portfolio": _portfolio_reconcile(promo),
        "family_navs_are_not_summed_as_independent_capital": True,
        "paper_only": True,
        "live_money_authority": False,
    }


def ensure_counterfactual_schema(store: Any) -> None:
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS v51_rejected_counterfactuals ("
            "surface TEXT NOT NULL,candidate_id TEXT NOT NULL,release_commit TEXT,token_mint TEXT,decision_reason TEXT NOT NULL,"
            "decision_observed_at TEXT,forward_net_return REAL,resolution_source TEXT,counterfactual_state TEXT NOT NULL,"
            "hazard_signature TEXT,hazard_severity REAL,payload_json TEXT NOT NULL,updated_at TEXT NOT NULL,"
            "retrospective_entry_authority INTEGER NOT NULL,paper_only INTEGER NOT NULL,live_money_authority INTEGER NOT NULL,"
            "PRIMARY KEY(surface,candidate_id))"
        )


def _counterfactual_return(store: Any, surface: str, candidate_id: str) -> tuple[float | None, str | None]:
    candidates = []
    if surface == "SOLANA":
        candidates = [("profit_first_final_outcomes", "source_signature", "net_return")]
    elif surface == "FOMO":
        candidates = [
            ("fomo_shadow_outcomes", "source_signature", "net_return"),
            ("profit_first_final_outcomes", "source_signature", "net_return"),
        ]
    for table, key_col, value_col in candidates:
        cols = _columns(store, table)
        if not {key_col, value_col}.issubset(cols):
            continue
        with store._lock:
            row = store.db.execute(
                f"SELECT {value_col} AS net_return FROM {table} WHERE {key_col}=? ORDER BY rowid DESC LIMIT 1",
                (candidate_id,),
            ).fetchone()
        if row is not None and row["net_return"] is not None:
            validated = validate_return(
                row["net_return"],
                source_surface=surface,
                source_signature=candidate_id,
            )
            if validated.validity and validated.normalized_fraction is not None:
                return validated.normalized_fraction, table
    return None, None


def refresh_rejected_counterfactuals(store: Any) -> dict[str, Any]:
    ensure_counterfactual_schema(store)
    if not _table_exists(store, "v51_candidate_current_state"):
        return {"counterfactual_version": COUNTERFACTUAL_VERSION, "rejected_candidate_count": 0, "resolved_count": 0}
    with store._lock:
        rows = store.db.execute(
            "SELECT surface,candidate_id,release_commit,reason,payload_json,observed_at FROM v51_candidate_current_state "
            "WHERE stage='position' AND status='not_opened' ORDER BY observed_at"
        ).fetchall()
    candidate_meta: dict[tuple[str, str], dict[str, Any]] = {}
    if _table_exists(store, "v51_candidates"):
        with store._lock:
            for row in store.db.execute("SELECT surface,candidate_id,token_mint,payload_json FROM v51_candidates").fetchall():
                candidate_meta[(str(row["surface"]), str(row["candidate_id"]))] = dict(row)
    resolved = 0
    positive = 0
    now = _utcnow()
    for raw in rows:
        row = dict(raw)
        surface = str(row["surface"])
        candidate_id = str(row["candidate_id"])
        meta = candidate_meta.get((surface, candidate_id), {})
        forward_return, source = _counterfactual_return(store, surface, candidate_id)
        state = "resolved_shadow_forward_outcome" if forward_return is not None else "pending_forward_resolution"
        if forward_return is not None:
            resolved += 1
            positive += int(forward_return > 0.0)
        hazard_signature = None
        hazard_severity = None
        if surface == "SOLANA" and _table_exists(store, "risk_conditioned_alpha_v5_trials"):
            cols = _columns(store, "risk_conditioned_alpha_v5_trials")
            if {"source_signature", "risk_signature", "risk_severity"}.issubset(cols):
                with store._lock:
                    risk = store.db.execute(
                        "SELECT risk_signature,risk_severity FROM risk_conditioned_alpha_v5_trials "
                        "WHERE source_signature=? ORDER BY id DESC LIMIT 1", (candidate_id,)
                    ).fetchone()
                if risk is not None:
                    hazard_signature = str(risk["risk_signature"] or "clean")
                    hazard_severity = _safe(risk["risk_severity"])
        with store._lock, store.db:
            store.db.execute(
                "INSERT INTO v51_rejected_counterfactuals(surface,candidate_id,release_commit,token_mint,decision_reason,"
                "decision_observed_at,forward_net_return,resolution_source,counterfactual_state,hazard_signature,hazard_severity,"
                "payload_json,updated_at,retrospective_entry_authority,paper_only,live_money_authority) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,0,1,0) ON CONFLICT(surface,candidate_id) DO UPDATE SET "
                "release_commit=excluded.release_commit,token_mint=excluded.token_mint,decision_reason=excluded.decision_reason,"
                "decision_observed_at=excluded.decision_observed_at,forward_net_return=excluded.forward_net_return,"
                "resolution_source=excluded.resolution_source,counterfactual_state=excluded.counterfactual_state,"
                "hazard_signature=excluded.hazard_signature,hazard_severity=excluded.hazard_severity,payload_json=excluded.payload_json,"
                "updated_at=excluded.updated_at,retrospective_entry_authority=0,paper_only=1,live_money_authority=0",
                (
                    surface,
                    candidate_id,
                    row.get("release_commit"),
                    meta.get("token_mint"),
                    str(row.get("reason") or "unspecified_reject"),
                    row.get("observed_at"),
                    forward_return,
                    source,
                    state,
                    hazard_signature,
                    hazard_severity,
                    str(row.get("payload_json") or "{}"),
                    now,
                ),
            )
    return {
        "counterfactual_version": COUNTERFACTUAL_VERSION,
        "rejected_candidate_count": len(rows),
        "resolved_count": resolved,
        "pending_count": len(rows) - resolved,
        "resolved_positive_count": positive,
        "retrospective_entry_authority": False,
        "paper_only": True,
        "live_money_authority": False,
    }


def _hazard_bin(severity: float, signature: str) -> str:
    if signature == "clean" or severity <= 0.0:
        return "clean"
    if severity >= 0.70:
        return "extreme"
    if severity >= 0.50:
        return "high"
    if severity >= 0.30:
        return "moderate"
    return "low"


def build_hazard_calibration(store: Any) -> dict[str, Any]:
    refresh_rejected_counterfactuals(store)
    grouped: dict[str, list[float]] = defaultdict(list)
    debt_by_bin: dict[str, int] = defaultdict(int)
    for row in _audit_records(store):
        signature = str(row.get("risk_signature") or "clean")
        severity = _safe(row.get("risk_severity"), 0.0 if signature == "clean" else 0.45)
        bin_name = _hazard_bin(severity, signature)
        validated = validate_row_return(row)
        if validated.validity and validated.normalized_fraction is not None:
            grouped[bin_name].append(validated.normalized_fraction)
        else:
            debt_by_bin[bin_name] += 1
    rejected: dict[str, dict[str, int]] = defaultdict(lambda: {"rejected": 0, "resolved": 0, "resolved_positive": 0})
    if _table_exists(store, "v51_rejected_counterfactuals"):
        with store._lock:
            rows = store.db.execute(
                "SELECT hazard_signature,hazard_severity,forward_net_return FROM v51_rejected_counterfactuals"
            ).fetchall()
        for row in rows:
            signature = str(row["hazard_signature"] or "clean")
            bin_name = _hazard_bin(_safe(row["hazard_severity"], 0.0), signature)
            rejected[bin_name]["rejected"] += 1
            if row["forward_net_return"] is not None:
                rejected[bin_name]["resolved"] += 1
                rejected[bin_name]["resolved_positive"] += int(float(row["forward_net_return"]) > 0.0)
    bins: dict[str, Any] = {}
    for name in ("clean", "low", "moderate", "high", "extreme"):
        values = grouped.get(name, [])
        bins[name] = {
            "settled_entered_count": len(values),
            "invalid_economic_measurement_count": debt_by_bin.get(name, 0),
            "entered_return_profile": robust_profile(values),
            "rejected_candidate_count": rejected[name]["rejected"],
            "rejected_resolved_count": rejected[name]["resolved"],
            "rejected_resolved_positive_count": rejected[name]["resolved_positive"],
        }
    return {
        "hazard_calibration_version": ANALYTICS_VERSION,
        "statistics_version": STATISTICS_VERSION,
        "bins": bins,
        "invalid_economic_measurement_count": sum(debt_by_bin.values()),
        "invalid_economic_measurements_are_not_imputed": True,
        "current_hazard_evidence_burden": authority()["hazard_evidence_burden"],
        "changes_current_hazard_multipliers": False,
        "purpose": "diagnose whether a future economic epoch should recalibrate hazard evidence burden or sizing",
        "paper_only": True,
        "live_money_authority": False,
    }


def _percentile(values: list[float], p: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int(math.ceil(p * len(ordered))) - 1))
    return ordered[index]


def build_forward_proof_slo(store: Any) -> dict[str, Any]:
    now = _utcnow_dt()
    if not _table_exists(store, "v51_candidate_stage_events"):
        return {
            "slo_version": SLO_VERSION,
            "proof_state": "unavailable",
            "reason": "candidate_stage_events_missing",
            "paper_only": True,
            "live_money_authority": False,
        }
    with store._lock:
        rows = [dict(row) for row in store.db.execute(
            "SELECT surface,candidate_id,stage,status,reason,observed_at FROM v51_candidate_stage_events ORDER BY id"
        ).fetchall()]
    first: dict[tuple[str, str, str], datetime] = {}
    for row in rows:
        at = _dt(row.get("observed_at"))
        if at is None:
            continue
        key = (str(row["surface"]), str(row["candidate_id"]), str(row["stage"]))
        first.setdefault(key, at)
    transitions = (("candidate", "context"), ("context", "execution_evidence"), ("execution_evidence", "decision"))
    latency: dict[str, Any] = {}
    for start, end in transitions:
        values: list[float] = []
        identities = {(surface, candidate) for surface, candidate, stage in first if stage == start}
        for surface, candidate in identities:
            a = first.get((surface, candidate, start))
            b = first.get((surface, candidate, end))
            if a is not None and b is not None and b >= a:
                values.append((b - a).total_seconds())
        latency[f"{start}_to_{end}_seconds"] = {
            "sample_count": len(values),
            "p50": median(values) if values else None,
            "p95": _percentile(values, 0.95),
            "max": max(values) if values else None,
        }
    debt_ages: list[float] = []
    pending_settlement_ages: list[float] = []
    if _table_exists(store, "v51_candidate_current_state"):
        with store._lock:
            current = [dict(row) for row in store.db.execute(
                "SELECT stage,status,observed_at FROM v51_candidate_current_state"
            ).fetchall()]
        for row in current:
            at = _dt(row.get("observed_at"))
            if at is None:
                continue
            age = max(0.0, (now - at).total_seconds())
            if row.get("status") == "coverage_debt":
                debt_ages.append(age)
            if row.get("stage") == "settlement" and row.get("status") == "pending":
                pending_settlement_ages.append(age)
    recent_5 = sum(1 for row in rows if (at := _dt(row.get("observed_at"))) is not None and (now - at).total_seconds() <= 300)
    recent_60 = sum(1 for row in rows if (at := _dt(row.get("observed_at"))) is not None and (now - at).total_seconds() <= 3600)
    degraded = bool(debt_ages and max(debt_ages) > 120.0)
    return {
        "slo_version": SLO_VERSION,
        "proof_state": "degraded" if degraded else "confirmed",
        "stage_latency": latency,
        "coverage_debt_count": len(debt_ages),
        "oldest_coverage_debt_age_seconds": max(debt_ages) if debt_ages else None,
        "pending_settlement_count": len(pending_settlement_ages),
        "oldest_pending_settlement_age_seconds": max(pending_settlement_ages) if pending_settlement_ages else None,
        "stage_events_last_5m": recent_5,
        "stage_events_last_60m": recent_60,
        "coverage_debt_slo_seconds": 120.0,
        "paper_only": True,
        "live_money_authority": False,
    }


def build_evidence_validity_bundle(store: Any) -> dict[str, Any]:
    attestation = refresh_release_attestation(store)
    cost = refresh_execution_cost_ledger(store)
    promotion = build_promotion_certification(store)
    counterfactual = refresh_rejected_counterfactuals(store)
    hazard = build_hazard_calibration(store)
    correlation = build_cross_family_correlation(store)
    maturity = build_maturity_allocation_proof(store)
    portfolio = build_portfolio_reconciliation(store)
    slo = build_forward_proof_slo(store)
    return {
        "analytics_version": ANALYTICS_VERSION,
        "statistics_version": STATISTICS_VERSION,
        "release_attestation": attestation,
        "execution_cost_ledger": cost,
        "promotion_certification": promotion,
        "rejected_counterfactuals": counterfactual,
        "hazard_calibration": hazard,
        "cross_family_correlation": correlation,
        "maturity_allocation_proof": maturity,
        "portfolio_reconciliation": portfolio,
        "forward_proof_slo": slo,
        "paper_only": True,
        "live_money_authority": False,
    }


__all__ = [
    "ANALYTICS_VERSION",
    "build_cross_family_correlation",
    "build_evidence_validity_bundle",
    "build_forward_proof_slo",
    "build_hazard_calibration",
    "build_maturity_allocation_proof",
    "build_portfolio_reconciliation",
    "build_promotion_certification",
    "promotion_records",
    "refresh_execution_cost_ledger",
    "refresh_rejected_counterfactuals",
]
