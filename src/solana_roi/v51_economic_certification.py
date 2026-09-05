from __future__ import annotations

import math
from collections import defaultdict
from statistics import mean
from typing import Any

from . import risk_conditioned_alpha_v51 as v51
from .strategy_v51_authority import AUTHORITY_ID, ECONOMIC_FREEZE_EPOCH, authority
from .v51_economic_core import execution_stress_profiles, hierarchical_profile, robust_profile

CERTIFICATION_VERSION = "v51-economic-certification-v1"
STARTING_NAV = 1.0


def _table_exists(store: Any, table: str) -> bool:
    try:
        with store._lock:
            row = store.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (table,)).fetchone()
        return row is not None
    except Exception:
        return False


def _safe(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def _family(surface: str, venue: str, risk_signature: str) -> str:
    if surface == "ROBINHOOD_CHAIN":
        return "ROBINHOOD_CHAIN"
    if surface == "FOMO":
        return "FOMO_CLEAN" if risk_signature == "clean" else "FOMO_HAZARD"
    upper = venue.upper()
    if upper == "PUMP_AMM":
        return "PUMP_AMM"
    if upper == "RAYDIUM":
        return "RAYDIUM"
    if upper == "PUMP_FUN":
        return "PUMP_FUN"
    return f"SOLANA_OTHER:{upper or 'UNKNOWN'}"


def _risk_severity_from_signature(signature: str) -> float:
    if signature == "clean":
        return 0.0
    parts = [part for part in signature.split("+") if part]
    if len(parts) >= 4:
        return 0.75
    if len(parts) == 3:
        return 0.55
    if len(parts) == 2:
        return 0.35
    return 0.15


def _records(store: Any) -> list[dict[str, Any]]:
    if not _table_exists(store, "v51_economic_freeze_releases"):
        return []
    records: list[dict[str, Any]] = []
    if _table_exists(store, "risk_conditioned_alpha_v5_outcomes") and _table_exists(store, "risk_conditioned_alpha_v5_trials"):
        with store._lock:
            rows = store.db.execute(
                "SELECT o.id,o.release_commit,o.source_signature,o.token_mint,o.lane,o.venue,o.lifecycle,o.regime,"
                "o.risk_signature,o.context_key,o.position_fraction,o.net_return,o.settled_at,"
                "t.trigger_wallet,t.flow_state,t.risk_severity,t.chase_band,t.latency_band,t.round_trip_cost_fraction "
                "FROM risk_conditioned_alpha_v5_outcomes o "
                "JOIN v51_economic_freeze_releases e ON e.release_commit=o.release_commit "
                "LEFT JOIN risk_conditioned_alpha_v5_trials t ON t.release_commit=o.release_commit "
                "AND t.source_signature=o.source_signature AND t.lane=o.lane "
                "WHERE e.economic_freeze_epoch=? AND e.authority_id=? ORDER BY o.id",
                (ECONOMIC_FREEZE_EPOCH, AUTHORITY_ID),
            ).fetchall()
        for row in rows:
            d = dict(row)
            d.update({"surface": "SOLANA_ALPHA", "entity": str(d.get("trigger_wallet") or "unknown")})
            d["family"] = _family("SOLANA_ALPHA", str(d.get("venue") or "UNKNOWN"), str(d.get("risk_signature") or "clean"))
            records.append(d)
    if _table_exists(store, "fomo_paper_outcomes") and _table_exists(store, "fomo_paper_trials"):
        with store._lock:
            rows = store.db.execute(
                "SELECT o.id,o.release_commit,o.source_signature,o.token_mint,o.trigger_wallet,o.venue,o.lifecycle,o.regime,"
                "o.position_fraction,o.net_return,o.settled_at,t.fomo_state,t.signal_to_entry_seconds,t.entry_cost_sol "
                "FROM fomo_paper_outcomes o JOIN v51_economic_freeze_releases e ON e.release_commit=o.release_commit "
                "LEFT JOIN fomo_paper_trials t ON t.release_commit=o.release_commit AND t.source_signature=o.source_signature "
                "WHERE e.economic_freeze_epoch=? AND e.authority_id=? ORDER BY o.id",
                (ECONOMIC_FREEZE_EPOCH, AUTHORITY_ID),
            ).fetchall()
        shadow: dict[tuple[str, str], str] = {}
        if _table_exists(store, "fomo_shadow_observations"):
            with store._lock:
                for row in store.db.execute("SELECT release_commit,source_signature,state_json FROM fomo_shadow_observations").fetchall():
                    shadow[(str(row["release_commit"]), str(row["source_signature"]))] = str(row["state_json"] or "{}")
        import json
        for row in rows:
            d = dict(row)
            raw = shadow.get((str(d.get("release_commit") or ""), str(d.get("source_signature") or "")), "{}")
            try:
                state = json.loads(raw)
            except Exception:
                state = {}
            signature = v51.fomo_hazard_signature(state if isinstance(state, dict) else {})
            d.update({
                "surface": "FOMO",
                "entity": str(d.get("trigger_wallet") or "unknown"),
                "lane": "fomo_continuation",
                "risk_signature": signature,
                "risk_severity": v51.fomo_hazard_severity(state if isinstance(state, dict) else {}),
                "flow_state": str(d.get("fomo_state") or "unknown"),
                "latency_band": _latency_band(_safe(d.get("signal_to_entry_seconds"), -1.0)),
                "round_trip_cost_fraction": None,
                "chase_band": "unknown",
                "context_key": "",
            })
            d["family"] = _family("FOMO", str(d.get("venue") or "UNKNOWN"), signature)
            records.append(d)
    if _table_exists(store, "robinhood_paper_outcomes") and _table_exists(store, "robinhood_v5_trial_context") and _table_exists(store, "robinhood_paper_trials"):
        with store._lock:
            rows = store.db.execute(
                "SELECT o.id,o.release_commit,o.trial_id,o.net_return,o.settled_at,t.token AS token_mint,t.trigger_entity AS entity,"
                "t.venue,t.lifecycle,t.position_fraction,t.entry_round_trip_cost_fraction AS round_trip_cost_fraction,"
                "c.lane,c.regime,c.flow_state,c.risk_signature,c.risk_severity,c.context_key,c.latency_band "
                "FROM robinhood_paper_outcomes o JOIN v51_economic_freeze_releases e ON e.release_commit=o.release_commit "
                "JOIN robinhood_paper_trials t ON t.id=o.trial_id JOIN robinhood_v5_trial_context c ON c.trial_id=o.trial_id "
                "WHERE e.economic_freeze_epoch=? AND e.authority_id=? ORDER BY o.id",
                (ECONOMIC_FREEZE_EPOCH, AUTHORITY_ID),
            ).fetchall()
        for row in rows:
            d = dict(row)
            d.update({"surface": "ROBINHOOD_CHAIN", "source_signature": f"robinhood_trial:{d.get('trial_id')}", "chase_band": "unknown"})
            d["family"] = "ROBINHOOD_CHAIN"
            records.append(d)
    return records


def _latency_band(value: float) -> str:
    if value < 0:
        return "unknown"
    if value <= 2:
        return "le_2s"
    if value <= 5:
        return "2_5s"
    if value <= 10:
        return "5_10s"
    if value <= 20:
        return "10_20s"
    return "gt_20s"


def _cost_band(value: Any) -> str:
    if value is None:
        return "unknown"
    numeric = _safe(value, -1.0)
    if numeric < 0:
        return "unknown"
    if numeric <= 0.03:
        return "le_3pct"
    if numeric <= 0.07:
        return "3_7pct"
    if numeric <= 0.15:
        return "7_15pct"
    return "gt_15pct"


def _compounded_nav(rows: list[dict[str, Any]]) -> float:
    nav = STARTING_NAV
    for row in rows:
        fraction = max(0.0, _safe(row.get("position_fraction")))
        net = _safe(row.get("net_return"))
        nav *= max(1e-9, 1.0 + fraction * net)
    return nav


def _capital_efficiency(profile: dict[str, Any], evidence_n: int, minimum_n: int) -> float:
    growth = profile.get("best_expected_log_growth")
    if growth is None or _safe(growth) <= 0.0:
        return 0.0
    shortfall = min(0.0, _safe(profile.get("expected_shortfall_20")))
    drawdown = max(0.0, _safe(profile.get("max_drawdown_at_best_fraction")))
    confidence = min(1.0, evidence_n / max(1.0, float(minimum_n)))
    return _safe(growth) * confidence / (1.0 + drawdown + abs(shortfall))


def _sensitivity(rows: list[dict[str, Any]], field: str) -> dict[str, Any]:
    grouped: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        if field == "round_trip_cost_fraction":
            key = _cost_band(row.get(field))
        else:
            key = str(row.get(field) or "unknown")
        grouped[key].append(_safe(row.get("net_return")))
    return {key: robust_profile(values) for key, values in sorted(grouped.items())}


def _identity_free_key(row: dict[str, Any]) -> tuple[str, ...]:
    parsed = v51._parse_context_key(str(row.get("context_key") or ""))
    if parsed:
        return tuple(str(parsed.get(name) or "unknown") for name in (
            "lane", "venue", "lifecycle", "regime", "risk_signature", "flow_state", "chase_band", "latency_band", "execution_cost_band"
        ))
    return (
        str(row.get("lane") or "unknown"),
        str(row.get("venue") or "UNKNOWN"),
        str(row.get("lifecycle") or "unknown"),
        str(row.get("regime") or "unknown"),
        str(row.get("risk_signature") or "clean"),
        str(row.get("flow_state") or "unknown"),
        str(row.get("chase_band") or "unknown"),
        str(row.get("latency_band") or "unknown"),
        _cost_band(row.get("round_trip_cost_fraction")),
    )


def incremental_alpha_attribution(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_context: dict[tuple[str, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_context[_identity_free_key(row)].append(row)
    residual_by_entity_family: dict[tuple[str, str], list[float]] = defaultdict(list)
    attributable = 0
    for group in by_context.values():
        entities = {str(row.get("entity") or "unknown") for row in group}
        if len(entities) < 2:
            continue
        for row in group:
            entity = str(row.get("entity") or "unknown")
            peers = [_safe(peer.get("net_return")) for peer in group if str(peer.get("entity") or "unknown") != entity]
            if not peers:
                continue
            residual_by_entity_family[(entity, str(row.get("family") or "unknown"))].append(_safe(row.get("net_return")) - mean(peers))
            attributable += 1
    result: dict[str, Any] = {}
    for (entity, family), values in residual_by_entity_family.items():
        profile = robust_profile(values)
        key = f"{family}|{entity}"
        result[key] = {
            "family": family,
            "entity": entity,
            "matched_residual_sample_count": len(values),
            "residual_profile": profile,
            "wallet_identity_adds_forward_edge": bool(
                len(values) >= 20
                and profile["leave_best_trade_out_mean"] is not None
                and _safe(profile["leave_best_trade_out_mean"]) > 0.0
                and profile["best_expected_log_growth"] is not None
                and _safe(profile["best_expected_log_growth"]) > 0.0
            ),
        }
    return {
        "baseline": "matched_forward_context_excluding_entity_identity",
        "baseline_dimensions": authority()["incremental_alpha"]["baseline_dimensions"],
        "attributable_outcome_count": attributable,
        "entity_family_attribution": result,
        "wallet_research_priority_rule": "wallet_identity_requires_positive_forward_residual_lift; otherwise context_without_identity_remains_the_signal",
    }


def build_economic_certification(store: Any) -> dict[str, Any]:
    rows = _records(store)
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get("family") or "unknown")].append(row)
    families: dict[str, Any] = {}
    scores: dict[str, float] = {}
    for family, family_rows in grouped.items():
        returns = [_safe(row.get("net_return")) for row in family_rows]
        profile = robust_profile(returns)
        signatures = {str(row.get("source_signature") or row.get("trial_id") or row.get("id")) for row in family_rows}
        independent_n = len(signatures)
        dominant_risk = "clean" if all(str(row.get("risk_signature") or "clean") == "clean" for row in family_rows) else "hazard"
        severity = max((_safe(row.get("risk_severity")) for row in family_rows), default=0.0)
        hierarchical = hierarchical_profile(returns, (), (), risk_severity=severity, risk_signature="clean" if dominant_risk == "clean" else "hazard")
        minimum_n = int(hierarchical["minimum_independent_outcomes"])
        score = 0.0 if bool(hierarchical.get("killed")) else _capital_efficiency(profile, independent_n, minimum_n)
        scores[family] = score
        families[family] = {
            "closed_outcome_count": len(family_rows),
            "independent_event_count": independent_n,
            "net_roi_sum": sum(returns),
            "compounded_nav_multiple": _compounded_nav(family_rows),
            "robust_profile": profile,
            "promotion_kill_profile": hierarchical,
            "capital_efficiency_score": score,
            "latency_sensitivity": _sensitivity(family_rows, "latency_band"),
            "execution_cost_sensitivity": _sensitivity(family_rows, "round_trip_cost_fraction"),
            "execution_stress": execution_stress_profiles(returns),
        }
    priority = list(authority()["research_family_priority"])
    ordered = sorted(
        set(priority) | set(families),
        key=lambda family: (-scores.get(family, 0.0), priority.index(family) if family in priority else len(priority), family),
    )
    positive = [family for family in ordered if scores.get(family, 0.0) > 0.0]
    total = sum(scores[family] for family in positive)
    weights: dict[str, float] = {}
    remaining = 1.0
    # Unknown correlation is not treated as zero. Each research family is capped
    # at 25% until aligned correlation evidence matures.
    for family in positive:
        raw = scores[family] / total if total > 0 else 0.0
        weight = min(0.25, raw, remaining)
        weights[family] = weight
        remaining -= weight
    return {
        "certification_version": CERTIFICATION_VERSION,
        "authority_id": AUTHORITY_ID,
        "economic_freeze_epoch": ECONOMIC_FREEZE_EPOCH,
        "economic_rules_frozen": True,
        "historical_pre_epoch_promotion_authority": False,
        "closed_outcome_count": len(rows),
        "families": families,
        "research_family_ranking": ordered,
        "paper_allocation_weights": weights,
        "paper_cash_weight": max(0.0, remaining),
        "incremental_alpha": incremental_alpha_attribution(rows),
        "kill_policy": authority()["kill_policy"],
        "paper_live_boundary_stress_policy": authority()["execution_stress"],
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


__all__ = ["CERTIFICATION_VERSION", "build_economic_certification", "incremental_alpha_attribution"]
