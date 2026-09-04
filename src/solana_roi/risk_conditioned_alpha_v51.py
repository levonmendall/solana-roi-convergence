from __future__ import annotations

import json
import math
import time
from datetime import datetime, timezone
from statistics import median
from typing import Any, Iterable

from . import risk_conditioned_alpha_v5 as v5

V51_VERSION = "roi-convergence-v5.1-context-exactness-1"
ROBINHOOD_V51_VERSION = "robinhood-chain-risk-conditioned-v2.1-context-exactness"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
MAX_SOLANA_SIZING_REQUOTES = 2
SOLANA_CONTEXT_MIN_SAMPLES = 30
SOLANA_RELAXED_SAME_ENTITY_SAMPLES = 45
ROBINHOOD_CONTEXT_MIN_SAMPLES = 30
ROBINHOOD_RELAXED_SAME_ENTITY_SAMPLES = 45
ROBINHOOD_BROAD_SAME_ENTITY_SAMPLES = 60
ALLOCATOR_CACHE_SECONDS = 30.0

_ORIGINAL_RISK_DESCRIPTOR = v5.risk_descriptor
_ORIGINAL_FINAL_BUY: Any = None
_ORIGINAL_FINAL_STATUS: Any = None
_ORIGINAL_ROBINHOOD_STATUS: Any = None
_INSTALLED = False


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def execution_cost_band(value: float | None) -> str:
    if value is None or not math.isfinite(float(value)):
        return "unknown"
    numeric = max(0.0, float(value))
    if numeric <= 0.03:
        return "le_3pct"
    if numeric <= 0.07:
        return "3_7pct"
    if numeric <= 0.15:
        return "7_15pct"
    return "gt_15pct"


def _context_key_v51(
    pre: dict[str, Any],
    lane: str,
    *,
    chase: float | None,
    latency: float | None,
) -> str:
    return "|".join(
        (
            str(pre.get("trigger_entity") or "entity:unknown"),
            str(lane),
            str(pre.get("venue") or "UNKNOWN"),
            str(pre.get("lifecycle") or "unknown"),
            str(pre.get("regime") or "unknown"),
            str(pre.get("role") or "unknown"),
            str((pre.get("risk") or {}).get("risk_signature") or "clean"),
            str(pre.get("flow_state") or "neutral"),
            v5.chase_band(chase),
            v5.latency_band(latency),
            execution_cost_band(v5._finite(pre.get("round_trip_cost_fraction"))),
        )
    )


def _parse_context_key(context_key: str) -> dict[str, str]:
    parts = str(context_key or "").split("|")
    if len(parts) < 11:
        return {}
    return {
        "entity": parts[0],
        "lane": parts[1],
        "venue": parts[2],
        "lifecycle": parts[3],
        "regime": parts[4],
        "role": parts[5],
        "risk_signature": parts[6],
        "flow_state": parts[7],
        "chase_band": parts[8],
        "latency_band": parts[9],
        "execution_cost_band": parts[10],
    }


def _context_returns_v51(
    adapter: Any,
    *,
    lane: str,
    venue: str,
    lifecycle: str,
    regime: str,
    context_key: str,
) -> tuple[list[float], str]:
    parsed = _parse_context_key(context_key)
    entity = parsed.get("entity")
    risk_signature = parsed.get("risk_signature")
    with adapter.store._lock:
        exact = adapter.store.db.execute(
            "SELECT net_return FROM risk_conditioned_alpha_v5_outcomes WHERE release_commit=? AND context_key=? ORDER BY id",
            (adapter.release_commit, context_key),
        ).fetchall()
        if len(exact) >= 20:
            return [float(row["net_return"]) for row in exact], "exact_entity_context"
        if entity:
            rows = adapter.store.db.execute(
                "SELECT net_return FROM risk_conditioned_alpha_v5_outcomes WHERE release_commit=? AND lane=? AND venue=? "
                "AND lifecycle=? AND regime=? AND risk_signature=? AND context_key LIKE ? ORDER BY id",
                (
                    adapter.release_commit,
                    lane,
                    venue,
                    lifecycle,
                    regime,
                    risk_signature or "clean",
                    entity + "|%",
                ),
            ).fetchall()
            if len(rows) >= SOLANA_CONTEXT_MIN_SAMPLES:
                return [float(row["net_return"]) for row in rows], "same_entity_lane_venue_lifecycle_regime_risk"
            relaxed = adapter.store.db.execute(
                "SELECT net_return FROM risk_conditioned_alpha_v5_outcomes WHERE release_commit=? AND lane=? AND venue=? "
                "AND lifecycle=? AND risk_signature=? AND context_key LIKE ? ORDER BY id",
                (
                    adapter.release_commit,
                    lane,
                    venue,
                    lifecycle,
                    risk_signature or "clean",
                    entity + "|%",
                ),
            ).fetchall()
            if len(relaxed) >= SOLANA_RELAXED_SAME_ENTITY_SAMPLES:
                return [float(row["net_return"]) for row in relaxed], "same_entity_lane_venue_lifecycle_risk"
    return [float(row["net_return"]) for row in exact], "exact_entity_bootstrap" if exact else "none"


def _risk_descriptor_v51(
    *,
    soft_flags: Iterable[str],
    hard_flags: Iterable[str],
    creator_flow_state: str = "neutral",
    creator_linked_trigger: bool = False,
    early_exit_fraction: float = 0.0,
    extra_hazards: Iterable[str] = (),
) -> dict[str, Any]:
    result = _ORIGINAL_RISK_DESCRIPTOR(
        soft_flags=soft_flags,
        hard_flags=hard_flags,
        creator_flow_state=creator_flow_state,
        creator_linked_trigger=creator_linked_trigger,
        early_exit_fraction=early_exit_fraction,
        extra_hazards=extra_hazards,
    )
    unclassified = list(result.get("other_hard_flags") or ())
    if unclassified:
        result["unclassified_hard_stops"] = sorted(set(str(x) for x in unclassified))
        result["structurally_tradeable"] = False
    else:
        result["unclassified_hard_stops"] = []
    numeric_exit = max(0.0, min(1.0, float(early_exit_fraction or 0.0)))
    result["continuous_features"] = {"early_holder_exit_fraction": numeric_exit}
    if numeric_exit > 0.20:
        extra = min(0.20, (numeric_exit - 0.20) * 0.40)
        result["risk_severity"] = min(1.0, float(result.get("risk_severity") or 0.0) + extra)
        severity = float(result["risk_severity"])
        result["risk_severity_bin"] = (
            "low" if severity < 0.20
            else "moderate" if severity < 0.45
            else "high" if severity < 0.70
            else "extreme"
        )
    return result


def _constraints_cap(adapter: Any, creator_entity: str | None, exit_executable: bool, requested: float) -> float:
    try:
        constraints = adapter._constraints(creator_entity, exit_executable)
        return float(constraints.cap(float(requested)))
    except Exception:
        return 0.0


def _reconstruct_pre(adapter: Any, selected: dict[str, Any], unified: dict[str, Any], lane_rows: list[dict[str, Any]]) -> dict[str, Any]:
    opportunity = v5._safe_json(unified.get("opportunity_json"))
    risk = v5._safe_json(selected.get("risk_json"))
    return {
        "trigger_entity": str(opportunity.get("trigger_entity") or f"entity:{selected.get('trigger_wallet') or 'unknown'}"),
        "creator_entity": opportunity.get("creator_entity"),
        "venue": str(selected.get("venue") or "UNKNOWN"),
        "lifecycle": str(selected.get("lifecycle") or "unknown"),
        "regime": str(selected.get("regime") or "unknown"),
        "role": str(selected.get("trigger_role") or "unknown"),
        "flow_state": str(selected.get("flow_state") or "neutral"),
        "risk": risk,
        "lanes": [str(row.get("lane") or "") for row in lane_rows if str(row.get("lane") or "")],
    }


def _set_v5_rows_observe(adapter: Any, signature: str, reason: str) -> None:
    with adapter.store._lock, adapter.store.db:
        adapter.store.db.execute(
            "UPDATE risk_conditioned_alpha_v5_trials SET selected=0,decision='paper_observe_sizing_not_converged',decision_reason=? "
            "WHERE release_commit=? AND source_signature=?",
            (reason, adapter.release_commit, signature),
        )


def _write_sizing_audit(
    adapter: Any,
    *,
    signature: str,
    initial_fraction: float,
    final_fraction: float,
    selected_lane: str | None,
    requotes: int,
    converged: bool,
    reason: str,
) -> None:
    with adapter.store._lock, adapter.store.db:
        adapter.store.db.execute(
            "CREATE TABLE IF NOT EXISTS risk_conditioned_alpha_v51_sizing_audit ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, release_commit TEXT NOT NULL, source_signature TEXT NOT NULL, "
            "initial_fraction REAL NOT NULL, final_fraction REAL NOT NULL, selected_lane TEXT, requote_count INTEGER NOT NULL, "
            "converged INTEGER NOT NULL, reason TEXT NOT NULL, created_at TEXT NOT NULL, UNIQUE(release_commit,source_signature))"
        )
        adapter.store.db.execute(
            "INSERT OR REPLACE INTO risk_conditioned_alpha_v51_sizing_audit("
            "release_commit,source_signature,initial_fraction,final_fraction,selected_lane,requote_count,converged,reason,created_at) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (
                adapter.release_commit,
                signature,
                float(initial_fraction),
                float(final_fraction),
                selected_lane,
                int(requotes),
                1 if converged else 0,
                reason,
                _utcnow(),
            ),
        )


async def _repair_exact_sizing(adapter: Any, row: dict[str, Any]) -> None:
    signature = str(row.get("signature") or "")
    if not signature:
        return
    with adapter.store._lock:
        raw_rows = adapter.store.db.execute(
            "SELECT * FROM risk_conditioned_alpha_v5_trials WHERE release_commit=? AND source_signature=? ORDER BY id",
            (adapter.release_commit, signature),
        ).fetchall()
        unified_raw = adapter.store.db.execute(
            "SELECT * FROM profit_first_final_trials WHERE epoch_id=? AND source_signature=? "
            "AND lane='unified_profit_maximizer' ORDER BY id DESC LIMIT 1",
            (adapter.epoch_id, signature),
        ).fetchone()
    if not raw_rows or unified_raw is None:
        return
    lane_rows = [dict(item) for item in raw_rows]
    unified = dict(unified_raw)
    selected = next((item for item in lane_rows if int(item.get("selected") or 0) == 1), lane_rows[0])
    initial_fraction = float(unified.get("assigned_position_fraction") or selected.get("position_fraction") or 0.0)
    if initial_fraction <= 0.0:
        return
    pre = _reconstruct_pre(adapter, selected, unified, lane_rows)
    if not bool((pre.get("risk") or {}).get("structurally_tradeable", True)):
        _set_v5_rows_observe(adapter, signature, "v5.1_untradeable_risk_context")
        _write_sizing_audit(
            adapter,
            signature=signature,
            initial_fraction=initial_fraction,
            final_fraction=0.0,
            selected_lane=None,
            requotes=0,
            converged=False,
            reason="untradeable_risk_context",
        )
        return

    execution: dict[str, Any] | None = None
    current_fraction = initial_fraction
    current_chase = v5._finite(row.get("chase_fraction"))
    current_latency = v5._finite(unified.get("signal_to_entry_seconds"))
    current_round_trip = v5._finite(unified.get("round_trip_cost_fraction"))
    if unified.get("entry_all_in_price_sol") is not None:
        wallet_price = v5._finite(row.get("wallet_price_sol"))
        entry_price = v5._finite(unified.get("entry_all_in_price_sol"))
        if wallet_price and entry_price and wallet_price > 0:
            current_chase = max(0.0, entry_price / wallet_price - 1.0)
    pre["round_trip_cost_fraction"] = current_round_trip

    selected_lane: str | None = str(selected.get("lane") or "") or None
    desired_fraction = current_fraction
    converged = False
    requotes = 0
    reason = "existing_amount_specific_fraction_already_exact"

    for attempt in range(MAX_SOLANA_SIZING_REQUOTES + 1):
        lane, requested, _profiles = v5._choose_lane_and_fraction(
            adapter,
            pre,
            chase=current_chase,
            latency=current_latency,
        )
        requested = _constraints_cap(
            adapter,
            pre.get("creator_entity"),
            bool(unified.get("exit_executable")),
            requested,
        )
        selected_lane = lane
        desired_fraction = requested
        if (
            lane is None
            or desired_fraction <= 0.0
            or current_latency is None
            or current_latency > 20.0
            or (current_chase is not None and current_chase > 0.40)
        ):
            reason = "reselected_context_not_actionable"
            break
        if abs(desired_fraction - current_fraction) <= 1e-6:
            converged = True
            reason = "amount_specific_fraction_converged"
            break
        if attempt >= MAX_SOLANA_SIZING_REQUOTES:
            reason = "amount_specific_fraction_failed_to_converge"
            break

        with adapter.store._lock, adapter.store.db:
            adapter.store.db.execute(
                "UPDATE risk_conditioned_alpha_v5_trials SET selected=0,decision='paper_sizing_requote_pending',"
                "decision_reason='v5.1_amount_specific_fraction_requote' WHERE release_commit=? AND source_signature=?",
                (adapter.release_commit, signature),
            )
        execution = await adapter._execution(row, desired_fraction)
        requotes += 1
        if execution is None or execution.get("exit_net_sol") is None:
            reason = "amount_specific_requote_execution_incomplete"
            break
        current_fraction = desired_fraction
        current_chase = float(execution["chase_fraction"])
        current_latency = float(execution["signal_to_entry_seconds"])
        current_round_trip = float(execution["round_trip_cost_fraction"])
        pre["round_trip_cost_fraction"] = current_round_trip
        unified["exit_executable"] = 1

    if not converged or not selected_lane or desired_fraction <= 0.0:
        _set_v5_rows_observe(adapter, signature, reason)
        _write_sizing_audit(
            adapter,
            signature=signature,
            initial_fraction=initial_fraction,
            final_fraction=max(0.0, desired_fraction),
            selected_lane=selected_lane,
            requotes=requotes,
            converged=False,
            reason=reason,
        )
        return

    if execution is None:
        execution = {
            "input_lamports": unified.get("quote_input_lamports"),
            "entry_fee_lamports": unified.get("entry_fee_lamports"),
            "token_raw": unified.get("entry_token_raw"),
            "decimals": unified.get("token_decimals"),
            "entry_price_sol": unified.get("entry_all_in_price_sol"),
            "exit_net_sol": unified.get("immediate_exit_net_sol"),
            "round_trip_cost_fraction": current_round_trip,
            "signal_to_entry_seconds": current_latency,
            "quote_latency_ms": unified.get("quote_latency_ms"),
            "chase_fraction": current_chase,
        }

    threshold = bool(current_chase is not None and current_chase > 0.15)
    decision = "paper_enter_threshold_challenger" if threshold else "paper_enter"
    now = _utcnow()
    with adapter.store._lock, adapter.store.db:
        adapter.store.db.execute(
            "UPDATE profit_first_final_trials SET assigned_position_fraction=?,quote_input_lamports=?,entry_fee_lamports=?,"
            "entry_token_raw=?,token_decimals=?,entry_all_in_price_sol=?,immediate_exit_net_sol=?,round_trip_cost_fraction=?,"
            "signal_to_entry_seconds=?,quote_latency_ms=?,entry_executable=1,exit_executable=1 "
            "WHERE epoch_id=? AND source_signature=?",
            (
                float(current_fraction),
                execution.get("input_lamports"),
                execution.get("entry_fee_lamports"),
                execution.get("token_raw"),
                execution.get("decimals"),
                execution.get("entry_price_sol"),
                execution.get("exit_net_sol"),
                execution.get("round_trip_cost_fraction"),
                execution.get("signal_to_entry_seconds"),
                execution.get("quote_latency_ms"),
                adapter.epoch_id,
                signature,
            ),
        )
        for lane_row in lane_rows:
            lane = str(lane_row.get("lane") or "")
            key = _context_key_v51(pre, lane, chase=current_chase, latency=current_latency)
            is_selected = lane == selected_lane
            adapter.store.db.execute(
                "UPDATE risk_conditioned_alpha_v5_trials SET selected=?,decision=?,decision_reason=?,context_key=?,chase_band=?,"
                "latency_band=?,threshold_challenger=?,position_fraction=?,quote_input_lamports=?,entry_fee_lamports=?,entry_token_raw=?,"
                "entry_cost_sol=?,immediate_exit_net_sol=?,round_trip_cost_fraction=?,entry_executable=1,exit_executable=1 WHERE id=?",
                (
                    1 if is_selected else 0,
                    decision if is_selected else "paper_observe",
                    "v5.1_entity_exact_amount_specific_context",
                    key,
                    v5.chase_band(current_chase),
                    v5.latency_band(current_latency),
                    1 if threshold else 0,
                    float(current_fraction),
                    execution.get("input_lamports"),
                    execution.get("entry_fee_lamports"),
                    execution.get("token_raw"),
                    ((int(execution.get("input_lamports") or 0) + int(execution.get("entry_fee_lamports") or 0)) / 1_000_000_000.0),
                    execution.get("exit_net_sol"),
                    execution.get("round_trip_cost_fraction"),
                    int(lane_row["id"]),
                ),
            )
        adapter.store.db.execute(
            "CREATE TABLE IF NOT EXISTS risk_conditioned_alpha_v51_context_audit ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, release_commit TEXT NOT NULL, source_signature TEXT NOT NULL, "
            "trigger_entity TEXT NOT NULL, selected_lane TEXT NOT NULL, execution_cost_band TEXT NOT NULL, "
            "risk_signature TEXT NOT NULL, created_at TEXT NOT NULL, UNIQUE(release_commit,source_signature))"
        )
        adapter.store.db.execute(
            "INSERT OR REPLACE INTO risk_conditioned_alpha_v51_context_audit("
            "release_commit,source_signature,trigger_entity,selected_lane,execution_cost_band,risk_signature,created_at) "
            "VALUES (?,?,?,?,?,?,?)",
            (
                adapter.release_commit,
                signature,
                str(pre["trigger_entity"]),
                selected_lane,
                execution_cost_band(current_round_trip),
                str((pre.get("risk") or {}).get("risk_signature") or "clean"),
                now,
            ),
        )
    _write_sizing_audit(
        adapter,
        signature=signature,
        initial_fraction=initial_fraction,
        final_fraction=current_fraction,
        selected_lane=selected_lane,
        requotes=requotes,
        converged=True,
        reason=reason,
    )


async def _buy_with_v51(self: Any, row: dict[str, Any]) -> None:
    if _ORIGINAL_FINAL_BUY is None:
        raise RuntimeError("v5.1 buy wrapper not installed")
    await _ORIGINAL_FINAL_BUY(self, row)
    try:
        await _repair_exact_sizing(self, row)
    except Exception as exc:
        signature = str(row.get("signature") or "")
        if signature:
            _set_v5_rows_observe(self, signature, f"v5.1_sizing_repair_failed:{type(exc).__name__}")


_FOMO_EXPERIMENT_VARIANTS = {
    "wallet_signal_only",
    "wallet_plus_entity_confirmation",
    "wallet_plus_fomo_acceleration",
    "pure_entity_flow_fomo",
    "hazard_fomo",
    "clean_fomo",
}


def fomo_hazard_signature(state_payload: dict[str, Any]) -> str:
    variants = {str(value) for value in (state_payload.get("experiment_variants") or ()) if str(value)}
    hazards = sorted(value for value in variants if value not in _FOMO_EXPERIMENT_VARIANTS)
    return "clean" if not hazards else "+".join(hazards)


def fomo_hazard_severity(state_payload: dict[str, Any]) -> float:
    signature = fomo_hazard_signature(state_payload)
    if signature == "clean":
        return 0.0
    total = 0.0
    for hazard in signature.split("+"):
        if hazard == "challenger_15_25pct":
            total += 0.08
        elif hazard == "challenger_25_40pct":
            total += 0.15
        else:
            total += float(v5.HAZARD_WEIGHTS.get(hazard, 0.12))
    return min(1.0, total)


def _fomo_context_returns_v51(
    adapter: Any,
    *,
    wallet: str,
    venue: str,
    lifecycle: str,
    regime: str,
    hazard_signature: str,
) -> list[float]:
    with adapter.store._lock:
        rows = adapter.store.db.execute(
            "SELECT s.state_json,o.net_return,t.trigger_wallet FROM fomo_shadow_observations s "
            "JOIN profit_first_final_trials t ON t.epoch_id=? AND t.source_signature=s.source_signature "
            "AND t.lane='unified_profit_maximizer' "
            "JOIN fomo_shadow_outcomes o ON o.release_commit=s.release_commit AND o.source_signature=s.source_signature "
            "WHERE s.release_commit=? AND s.venue=? AND s.lifecycle=? AND s.regime=? ORDER BY s.id",
            (adapter.epoch_id, adapter.release_commit, venue, lifecycle, regime),
        ).fetchall()
    values: list[float] = []
    for row in rows:
        if str(row["trigger_wallet"] or "") != wallet:
            continue
        payload = v5._safe_json(row["state_json"])
        if str(payload.get("state") or "") not in {"pre_fomo", "active_fomo"}:
            continue
        if fomo_hazard_signature(payload) != hazard_signature:
            continue
        value = v5._finite(row["net_return"])
        if value is not None:
            values.append(value)
    return values


def _fomo_paper_decision_v51(adapter: Any, *, observation: dict[str, Any], trial: dict[str, Any]) -> dict[str, Any]:
    import solana_roi.fomo_paper_strategy as paper

    state_payload = v5._safe_json(observation.get("state_json"))
    fomo_state = str(state_payload.get("state") or "unknown")
    accessible = bool(state_payload.get("structurally_accessible"))
    wallet = str(trial.get("trigger_wallet") or "")
    venue = str(observation.get("venue") or "UNKNOWN")
    lifecycle = str(observation.get("lifecycle") or "unknown")
    regime = str(observation.get("regime") or trial.get("regime") or "unknown")
    signature = fomo_hazard_signature(state_payload)
    severity = fomo_hazard_severity(state_payload)
    values = _fomo_context_returns_v51(
        adapter,
        wallet=wallet,
        venue=venue,
        lifecycle=lifecycle,
        regime=regime,
        hazard_signature=signature,
    )
    profile = paper.classify_fomo_wallet_returns(values)
    profile.update(
        {
            "wallet": wallet,
            "venue": venue,
            "lifecycle": lifecycle,
            "regime": regime,
            "risk_signature": signature,
            "risk_severity": severity,
            "risk_class": "clean_fomo" if signature == "clean" else "hazard_fomo",
        }
    )
    if fomo_state not in {"pre_fomo", "active_fomo"}:
        return {"decision": "no_entry_nonactionable_fomo_state", "reason": fomo_state, "position_fraction": 0.0, "profile": profile}
    if not accessible:
        blockers = ",".join(str(x) for x in (state_payload.get("blockers") or ()))
        return {"decision": "no_entry_structurally_inaccessible", "reason": blockers or "fomo_accessibility_failed", "position_fraction": 0.0, "profile": profile}
    if not bool(trial.get("entry_executable")) or not bool(trial.get("exit_executable")):
        return {"decision": "no_entry_execution_incomplete", "reason": "entry_and_exit_executable_evidence_required", "position_fraction": 0.0, "profile": profile}
    if paper._token_already_open(adapter, str(trial.get("token_mint") or "")):
        return {"decision": "no_entry_token_already_open", "reason": "one_open_fomo_paper_position_per_token", "position_fraction": 0.0, "profile": profile}
    if profile["state"] == "demoted_fomo_wallet":
        return {"decision": "no_entry_demoted_fomo_wallet", "reason": "nonpositive_robust_expected_log_growth", "position_fraction": 0.0, "profile": profile}

    hazard = signature != "clean"
    if profile["state"] == "promoted_fomo_wallet":
        requested = float(profile.get("best_paper_position_fraction") or 0.0)
        decision = "paper_enter_promoted_hazard_fomo" if hazard else "paper_enter_promoted_clean_fomo"
    else:
        requested = 0.005 if hazard else 0.01
        decision = "paper_enter_hazard_fomo_probe" if hazard else "paper_enter_clean_fomo_probe"
    if hazard:
        requested = min(0.02, requested)
    requested *= v5._regime_multiplier(regime)
    requested *= max(0.30, 1.0 - 0.60 * severity)
    available = max(0.0, 1.0 - paper._open_position_fraction(adapter))
    fraction = min(0.05, requested, available)
    if fraction <= 0.0:
        return {"decision": "no_entry_paper_capacity_exhausted", "reason": "open_fomo_paper_fraction_at_capacity", "position_fraction": 0.0, "profile": profile}
    return {
        "decision": decision,
        "reason": f"risk_conditioned_exact_signature:{signature}",
        "position_fraction": fraction,
        "profile": profile,
    }


def robust_cost_ceiling(profile: dict[str, Any], base_ceiling: float) -> float:
    if profile.get("state") != "promoted_positive_log_growth":
        return float(base_ceiling)
    trimmed = v5._finite(profile.get("trimmed_mean_ex_best"))
    shortfall = v5._finite(profile.get("expected_shortfall_20"))
    if trimmed is None or trimmed <= 0.0:
        return float(base_ceiling)
    tail_penalty = max(0.0, -(shortfall or 0.0))
    robust_edge = max(0.0, trimmed - 0.50 * tail_penalty)
    return min(0.30, float(base_ceiling) + 0.25 * robust_edge)


def _rh_context_returns_v51(
    self: Any,
    *,
    entity: str,
    role: str,
    lane: str,
    venue: str,
    lifecycle: str,
    regime: str,
    risk_signature: str,
    flow_state: str,
) -> tuple[list[float], str]:
    key = self._v5_context_key(
        entity=entity,
        role=role,
        lane=lane,
        venue=venue,
        lifecycle=lifecycle,
        regime=regime,
        risk_signature=risk_signature,
        flow_state=flow_state,
    )
    with self.store._lock:
        exact = self.store.db.execute(
            "SELECT o.net_return FROM robinhood_paper_outcomes o JOIN robinhood_v5_trial_context c ON c.trial_id=o.trial_id "
            "WHERE o.release_commit=? AND c.context_key=? ORDER BY o.id",
            (self.release_commit, key),
        ).fetchall()
        if len(exact) >= 20:
            return [float(r["net_return"]) for r in exact], "exact_entity_context"
        rows = self.store.db.execute(
            "SELECT o.net_return FROM robinhood_paper_outcomes o JOIN robinhood_v5_trial_context c ON c.trial_id=o.trial_id "
            "JOIN robinhood_paper_trials t ON t.id=o.trial_id WHERE o.release_commit=? AND t.trigger_entity=? AND c.trigger_role=? "
            "AND c.lane=? AND t.venue=? AND t.lifecycle=? AND c.regime=? AND c.risk_signature=? ORDER BY o.id",
            (self.release_commit, entity, role, lane, venue, lifecycle, regime, risk_signature),
        ).fetchall()
        if len(rows) >= ROBINHOOD_CONTEXT_MIN_SAMPLES:
            return [float(r["net_return"]) for r in rows], "same_entity_lane_venue_lifecycle_regime_risk"
        relaxed = self.store.db.execute(
            "SELECT o.net_return FROM robinhood_paper_outcomes o JOIN robinhood_v5_trial_context c ON c.trial_id=o.trial_id "
            "JOIN robinhood_paper_trials t ON t.id=o.trial_id WHERE o.release_commit=? AND t.trigger_entity=? AND c.trigger_role=? "
            "AND c.lane=? AND t.venue=? AND t.lifecycle=? AND c.risk_signature=? ORDER BY o.id",
            (self.release_commit, entity, role, lane, venue, lifecycle, risk_signature),
        ).fetchall()
        if len(relaxed) >= ROBINHOOD_RELAXED_SAME_ENTITY_SAMPLES:
            return [float(r["net_return"]) for r in relaxed], "same_entity_lane_venue_lifecycle_risk"
    return [float(r["net_return"]) for r in exact], "exact_entity_bootstrap" if exact else "none"


async def _rh_maybe_open_v2_v51(self: Any, curve: Any) -> None:
    from . import robinhood_chain_profit_maximizer as rh
    from .robinhood_chain_core import MAX_IMMEDIATE_ROUND_TRIP_COST

    if not self._caught_up or self._token_open(curve.token):
        return
    metrics = await self._v5_flow_metrics(curve.recent_swaps, deployer=curve.deployer)
    if not bool(metrics.get("entity_resolution_complete")):
        return
    actor = rh._clean_address(str(metrics.get("trigger_actor") or ""))
    entity = rh._clean_address(str(metrics.get("trigger_entity") or ""))
    if not actor or not entity or actor in rh.KNOWN_NON_ACTORS:
        return
    role = "creator_deployer" if bool(metrics.get("trigger_is_creator")) else "independent_entity"
    eth_usd = await self._eth_usd()
    if eth_usd is None or eth_usd <= 0:
        return
    try:
        state = await self.rpc.pons_v2_launch_state(curve.token)
        if int(state["phase"]) != 0:
            return
        real_quote = await self.rpc.call_uint(curve.curve, "realQuoteReserve()")
        threshold = max(1, int(state["graduation_threshold"] or curve.graduation_threshold))
        progress = real_quote / threshold
    except Exception:
        return

    extra: list[str] = []
    if progress >= 0.85:
        extra.append("late_lifecycle")
    if float(metrics.get("creator_sell_pressure") or 0.0) >= 0.25:
        extra.append("creator_distributing")
    risk = _risk_descriptor_v51(
        soft_flags=(),
        hard_flags=(),
        creator_flow_state="distributing" if "creator_distributing" in extra else "neutral",
        creator_linked_trigger=role == "creator_deployer",
        extra_hazards=extra,
    )
    regime = self._v5_regime(metrics)
    lanes = self._v5_candidate_lanes(metrics=metrics, hazards=list(risk["hazards"]), lifecycle_progress=progress)
    if metrics["state"] == "neutral" and role != "creator_deployer" and progress < 0.70:
        profile = self._v5_profile(
            entity=entity,
            role=role,
            lane="elite_entity_continuation",
            venue="PONS_V2_CURVE",
            lifecycle="bonding_curve",
            regime=regime,
            risk_signature=str(risk["risk_signature"]),
            flow_state=str(metrics["state"]),
        )
        if profile["state"] != "promoted_positive_log_growth":
            return

    lane, fraction, _ = self._v5_choose_lane_fraction(
        entity=entity,
        role=role,
        venue="PONS_V2_CURVE",
        lifecycle="bonding_curve",
        regime=regime,
        risk_signature=str(risk["risk_signature"]),
        risk_severity=float(risk["risk_severity"]),
        flow_state=str(metrics["state"]),
        lanes=lanes,
    )
    if not lane or fraction <= 0.0:
        return

    for quote_attempt in range(2):
        amount_in = int((self._paper_nav_usd() * fraction / eth_usd) * 1e18)
        if amount_in <= 0:
            return
        try:
            buy = await self.rpc.pons_v2_curve_quote(curve=curve.curve, quote_in=amount_in, recipient=self.paper_recipient)
            if int(buy["tokens_out"]) <= 0:
                return
        except Exception:
            return

        high_tax = int(buy.get("snipe_tax_bps") or 0) > 500
        if high_tax and "high_snipe_tax" not in extra:
            extra.append("high_snipe_tax")
            risk = _risk_descriptor_v51(
                soft_flags=(),
                hard_flags=(),
                creator_flow_state="distributing" if "creator_distributing" in extra else "neutral",
                creator_linked_trigger=role == "creator_deployer",
                extra_hazards=extra,
            )
            lanes = self._v5_candidate_lanes(metrics=metrics, hazards=list(risk["hazards"]), lifecycle_progress=progress)
            new_lane, new_fraction, _ = self._v5_choose_lane_fraction(
                entity=entity,
                role=role,
                venue="PONS_V2_CURVE",
                lifecycle="bonding_curve",
                regime=regime,
                risk_signature=str(risk["risk_signature"]),
                risk_severity=float(risk["risk_severity"]),
                flow_state=str(metrics["state"]),
                lanes=lanes,
            )
            if not new_lane or new_fraction <= 0.0:
                return
            if quote_attempt == 0 and abs(new_fraction - fraction) > 1e-6:
                lane, fraction = new_lane, new_fraction
                continue
            lane, fraction = new_lane, new_fraction

        try:
            exit_out = await self.rpc.pons_v2_curve_sell_quote(curve=curve.curve, tokens_in=buy["tokens_out"])
            gas_price = await self.rpc.gas_price()
        except Exception:
            return
        entry_gas_wei = 220_000 * gas_price
        exit_gas_wei = 220_000 * gas_price
        total_cost = buy["spent"] + entry_gas_wei
        immediate_net = max(0, exit_out - exit_gas_wei)
        round_trip = 1.0 - immediate_net / max(1, total_cost)
        quote = {
            "amount_in_wei": buy["spent"],
            "token_out": buy["tokens_out"],
            "entry_gas_wei": entry_gas_wei,
            "exit_gas_wei": exit_gas_wei,
            "entry_total_cost_wei": total_cost,
            "immediate_exit_wei": immediate_net,
            "round_trip_cost_fraction": round_trip,
            "entry_price_eth": (buy["spent"] / 1e18) / (buy["tokens_out"] / 1e18),
        }
        profile = self._v5_profile(
            entity=entity,
            role=role,
            lane=lane,
            venue="PONS_V2_CURVE",
            lifecycle="bonding_curve",
            regime=regime,
            risk_signature=str(risk["risk_signature"]),
            flow_state=str(metrics["state"]),
        )
        if float(quote["round_trip_cost_fraction"]) > robust_cost_ceiling(profile, MAX_IMMEDIATE_ROUND_TRIP_COST):
            return
        self._v5_insert_trial(
            token=curve.token,
            market=curve.curve,
            venue="PONS_V2_CURVE",
            lifecycle="bonding_curve",
            trigger_actor=actor,
            trigger_entity=entity,
            flow_state=str(metrics["state"]),
            fraction=fraction,
            quote=quote,
            lane=lane,
            role=role,
            regime=regime,
            risk=risk,
            lifecycle_progress=progress,
            threshold_challenger=progress >= 0.85,
            candidate_lanes=lanes,
        )
        return


def _rh_learned_exit_policy_v51(self: Any, trial: dict[str, Any]) -> dict[str, Any]:
    from .robinhood_chain_core import HARVEST_FRACTION, MAX_HOLD_SECONDS, STOP_LOSS_FRACTION

    trial_id = int(trial["id"])
    with self.store._lock:
        context = self.store.db.execute(
            "SELECT lane,regime,risk_signature FROM robinhood_v5_trial_context WHERE trial_id=? LIMIT 1",
            (trial_id,),
        ).fetchone()
    if context is None:
        return {"source": "bootstrap", "stop": STOP_LOSS_FRACTION, "harvest": HARVEST_FRACTION, "max_hold": MAX_HOLD_SECONDS}

    lane = str(context["lane"])
    regime = str(context["regime"])
    risk_signature = str(context["risk_signature"])
    venue = str(trial["venue"])
    lifecycle = str(trial["lifecycle"])
    entity = str(trial["trigger_entity"])
    cohort_specs = (
        ("exact_regime_risk", ROBINHOOD_CONTEXT_MIN_SAMPLES, "AND t.trigger_entity=? AND c.regime=? AND c.risk_signature=?", (entity, regime, risk_signature)),
        ("same_entity_regime", ROBINHOOD_RELAXED_SAME_ENTITY_SAMPLES, "AND t.trigger_entity=? AND c.regime=?", (entity, regime)),
        ("same_entity_broad", ROBINHOOD_BROAD_SAME_ENTITY_SAMPLES, "AND t.trigger_entity=?", (entity,)),
    )
    ids: list[int] = []
    source = "bootstrap"
    with self.store._lock:
        for name, minimum, predicate, extra in cohort_specs:
            rows = self.store.db.execute(
                "SELECT o.trial_id FROM robinhood_paper_outcomes o JOIN robinhood_v5_trial_context c ON c.trial_id=o.trial_id "
                "JOIN robinhood_paper_trials t ON t.id=o.trial_id WHERE o.release_commit=? AND c.lane=? AND t.venue=? "
                "AND t.lifecycle=? " + predicate + " ORDER BY o.id",
                (self.release_commit, lane, venue, lifecycle, *extra),
            ).fetchall()
            if len(rows) >= minimum:
                ids = [int(row["trial_id"]) for row in rows]
                source = name
                break
    if not ids:
        return {"source": "bootstrap", "stop": STOP_LOSS_FRACTION, "harvest": HARVEST_FRACTION, "max_hold": MAX_HOLD_SECONDS}

    placeholders = ",".join("?" for _ in ids)
    with self.store._lock:
        marks = self.store.db.execute(
            f"SELECT trial_id,elapsed_seconds,net_return FROM robinhood_v5_marks WHERE release_commit=? AND trial_id IN ({placeholders}) ORDER BY id",
            (self.release_commit, *ids),
        ).fetchall()
    grouped: dict[int, list[tuple[float, float]]] = {}
    for row in marks:
        grouped.setdefault(int(row["trial_id"]), []).append((float(row["elapsed_seconds"]), float(row["net_return"])))
    if len(grouped) < ROBINHOOD_CONTEXT_MIN_SAMPLES:
        return {"source": "bootstrap", "stop": STOP_LOSS_FRACTION, "harvest": HARVEST_FRACTION, "max_hold": MAX_HOLD_SECONDS}

    mfes: list[float] = []
    maes: list[float] = []
    time_to_mfe: list[float] = []
    for points in grouped.values():
        if not points:
            continue
        best = max(points, key=lambda item: item[1])
        mfes.append(best[1])
        maes.append(min(value for _, value in points))
        time_to_mfe.append(best[0])
    if len(mfes) < ROBINHOOD_CONTEXT_MIN_SAMPLES:
        return {"source": "bootstrap", "stop": STOP_LOSS_FRACTION, "harvest": HARVEST_FRACTION, "max_hold": MAX_HOLD_SECONDS}

    median_mfe = median(mfes)
    median_mae = median(maes)
    harvest = min(0.75, max(0.15, median_mfe * 0.70)) if median_mfe > 0 else HARVEST_FRACTION
    stop = min(-0.08, max(-0.30, median_mae * 1.20)) if median_mae < 0 else STOP_LOSS_FRACTION
    max_hold = min(20 * 60, max(120.0, median(time_to_mfe) * 1.50))
    return {"source": f"forward_mfe_mae:{source}", "stop": stop, "harvest": harvest, "max_hold": max_hold}


def _allocator_cached(adapter: Any) -> dict[str, Any]:
    from .cross_regime_paper_allocator import build_cross_regime_allocation

    now = time.monotonic()
    cached = getattr(adapter, "_roi_v51_allocator_cache", None)
    if isinstance(cached, tuple) and len(cached) == 2 and now - float(cached[0]) <= ALLOCATOR_CACHE_SECONDS:
        return dict(cached[1])
    value = build_cross_regime_allocation(adapter.store, adapter.release_commit)
    setattr(adapter, "_roi_v51_allocator_cache", (now, value))
    return value


def _status_with_v51(self: Any) -> dict[str, Any]:
    if _ORIGINAL_FINAL_STATUS is None:
        raise RuntimeError("v5.1 status wrapper not installed")
    payload = _ORIGINAL_FINAL_STATUS(self)
    try:
        with self.store._lock:
            table = self.store.db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='risk_conditioned_alpha_v51_sizing_audit' LIMIT 1"
            ).fetchone()
            if table is not None:
                converged = int(self.store.db.execute(
                    "SELECT COUNT(*) FROM risk_conditioned_alpha_v51_sizing_audit WHERE release_commit=? AND converged=1",
                    (self.release_commit,),
                ).fetchone()[0])
                failed = int(self.store.db.execute(
                    "SELECT COUNT(*) FROM risk_conditioned_alpha_v51_sizing_audit WHERE release_commit=? AND converged=0",
                    (self.release_commit,),
                ).fetchone()[0])
            else:
                converged = failed = 0
        payload["risk_conditioned_alpha_v51"] = {
            "strategy_version": V51_VERSION,
            "active_context_key": "entity_x_lane_x_venue_x_lifecycle_x_regime_x_role_x_risk_signature_x_flow_x_chase_x_latency_x_execution_cost",
            "cross_entity_promotion_transfer_allowed": False,
            "context_backoff": "same_entity_only_with_risk_preservation",
            "amount_specific_sizing_convergence": True,
            "max_selective_sizing_requotes": MAX_SOLANA_SIZING_REQUOTES,
            "sizing_converged_rows": converged,
            "sizing_failed_closed_rows": failed,
            "unknown_hard_flags_fail_closed": True,
            "fomo_exact_hazard_signature": True,
            "robinhood_high_snipe_tax_reselects_and_requotes": True,
            "robinhood_exit_learning_regime_risk_conditioned": True,
            "cross_regime_allocator": _allocator_cached(self),
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        }
    except Exception as exc:
        payload["risk_conditioned_alpha_v51"] = {
            "strategy_version": V51_VERSION,
            "failed_closed": True,
            "error": f"{type(exc).__name__}: v5.1 status unavailable",
            "paper_only": True,
            "live_money_authority": False,
        }
    return payload


def _robinhood_status_with_v51(self: Any) -> dict[str, Any]:
    if _ORIGINAL_ROBINHOOD_STATUS is None:
        raise RuntimeError("v5.1 Robinhood status wrapper not installed")
    payload = _ORIGINAL_ROBINHOOD_STATUS(self)
    payload["strategy_version"] = ROBINHOOD_V51_VERSION
    payload["risk_conditioned_v51"] = {
        "entity_exact_promotion": True,
        "cross_entity_promotion_transfer_allowed": False,
        "high_snipe_tax_reselects_lane_and_fraction": True,
        "robust_cost_ceiling_uses_trimmed_tail_adjusted_edge": True,
        "learned_exit_context": "same_entity_x_lane_x_venue_x_lifecycle_x_regime_x_risk_signature_then_conservative_backoff",
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }
    return payload


def install_risk_conditioned_alpha_v51() -> None:
    global _INSTALLED, _ORIGINAL_FINAL_BUY, _ORIGINAL_FINAL_STATUS, _ORIGINAL_ROBINHOOD_STATUS
    if _INSTALLED:
        return

    from .profit_first_entity_final_research import FinalProfitFirstResearchAdapter
    from . import fomo_paper_strategy as fomo_paper
    from . import robinhood_chain_profit_maximizer as rh
    from . import robinhood_chain_runtime as rh_runtime

    v5.risk_descriptor = _risk_descriptor_v51
    v5._context_key = _context_key_v51
    v5._context_returns = _context_returns_v51
    v5.STRATEGY_VERSION = V51_VERSION
    fomo_paper._paper_decision = _fomo_paper_decision_v51

    current_buy = FinalProfitFirstResearchAdapter._buy
    if not bool(getattr(current_buy, "_roi_risk_conditioned_v51", False)):
        _ORIGINAL_FINAL_BUY = current_buy
        setattr(_buy_with_v51, "_roi_risk_conditioned_v51", True)
        FinalProfitFirstResearchAdapter._buy = _buy_with_v51  # type: ignore[method-assign]

    current_status = FinalProfitFirstResearchAdapter.status
    if not bool(getattr(current_status, "_roi_risk_conditioned_v51", False)):
        _ORIGINAL_FINAL_STATUS = current_status
        setattr(_status_with_v51, "_roi_risk_conditioned_v51", True)
        FinalProfitFirstResearchAdapter.status = _status_with_v51  # type: ignore[method-assign]

    rh.risk_descriptor = _risk_descriptor_v51
    rh.ROBINHOOD_V5_VERSION = ROBINHOOD_V51_VERSION
    rh.RobinhoodProfitMaximizerMixin._v5_context_returns = _rh_context_returns_v51  # type: ignore[method-assign]
    rh.RobinhoodProfitMaximizerMixin._maybe_open_v2 = _rh_maybe_open_v2_v51  # type: ignore[method-assign]
    rh.RobinhoodProfitMaximizerMixin._v5_learned_exit_policy = _rh_learned_exit_policy_v51  # type: ignore[method-assign]

    current_rh_status = rh_runtime.RobinhoodRuntimeMixin.status
    if not bool(getattr(current_rh_status, "_roi_risk_conditioned_v51", False)):
        _ORIGINAL_ROBINHOOD_STATUS = current_rh_status
        setattr(_robinhood_status_with_v51, "_roi_risk_conditioned_v51", True)
        rh_runtime.RobinhoodRuntimeMixin.status = _robinhood_status_with_v51  # type: ignore[method-assign]
    _INSTALLED = True


__all__ = [
    "V51_VERSION",
    "ROBINHOOD_V51_VERSION",
    "execution_cost_band",
    "fomo_hazard_signature",
    "fomo_hazard_severity",
    "robust_cost_ceiling",
    "install_risk_conditioned_alpha_v51",
]
