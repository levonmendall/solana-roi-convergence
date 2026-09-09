from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Callable

from . import fomo_paper_strategy as fomo_paper
from . import risk_conditioned_alpha_v5 as solana_strategy
from . import robinhood_chain_profit_maximizer as robinhood_strategy
from .profit_first_entity_final_research import FinalProfitFirstResearchAdapter
from .robinhood_chain_profit_maximizer import RobinhoodProfitMaximizerMixin
from .strategy_v52_authority import (
    AUTHORITY_ID,
    ECONOMIC_FREEZE_EPOCH,
    LIVE_MONEY_AUTHORITY,
    PAPER_ONLY,
    STRATEGY_VERSION,
    authority_fingerprint,
    execution_policy,
    position_policy,
    target_sizing_policy,
)


CONSOLIDATION_VERSION = "v52-authoritative-economic-owner-v2-forward-epoch"
_INSTALLED = False
_BASE_SOLANA_CHOOSE: Callable[..., Any] | None = None
_BASE_FOMO_DECISION: Callable[..., Any] | None = None
_BASE_RH_CHOOSE: Callable[..., Any] | None = None
_BASE_RH_PROFILE: Callable[..., Any] | None = None
_BASE_EXECUTION: Callable[..., Any] | None = None


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _table_exists(store: Any, table: str) -> bool:
    try:
        with store._lock:
            row = store.db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                (table,),
            ).fetchone()
        return row is not None
    except Exception:
        return False


def _ensure_v52_epoch(owner: Any) -> None:
    store = getattr(owner, "store", None)
    release = str(getattr(owner, "release_commit", "") or "").strip()
    if store is None or not release:
        return
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS v52_economic_freeze_releases ("
            "release_commit TEXT PRIMARY KEY, economic_freeze_epoch TEXT NOT NULL, authority_id TEXT NOT NULL, "
            "authority_fingerprint TEXT NOT NULL, strategy_version TEXT NOT NULL, registered_at TEXT NOT NULL, "
            "paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL)"
        )
        store.db.execute(
            "INSERT OR REPLACE INTO v52_economic_freeze_releases("
            "release_commit,economic_freeze_epoch,authority_id,authority_fingerprint,strategy_version,registered_at,"
            "paper_only,live_money_authority) VALUES (?,?,?,?,?,?,1,0)",
            (
                release,
                ECONOMIC_FREEZE_EPOCH,
                AUTHORITY_ID,
                authority_fingerprint(),
                STRATEGY_VERSION,
                _utcnow(),
            ),
        )
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS v52_authoritative_candidate_state ("
            "asset_id TEXT PRIMARY KEY, release_commit TEXT NOT NULL, state TEXT NOT NULL, "
            "surface TEXT NOT NULL, lifecycle TEXT NOT NULL, target_fraction REAL NOT NULL, "
            "open_fraction REAL NOT NULL, last_entry_price REAL, evidence_sequence INTEGER NOT NULL, "
            "last_reason TEXT NOT NULL, updated_at TEXT NOT NULL, authority_id TEXT NOT NULL, "
            "strategy_version TEXT NOT NULL, paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL)"
        )


def _record_candidate_state(
    owner: Any,
    *,
    asset_id: str,
    state: str,
    surface: str,
    lifecycle: str,
    target_fraction: float,
    open_fraction: float,
    last_entry_price: float | None,
    reason: str,
) -> None:
    _ensure_v52_epoch(owner)
    store = getattr(owner, "store", None)
    release = str(getattr(owner, "release_commit", "") or "").strip()
    if store is None or not release or not asset_id:
        return
    with store._lock, store.db:
        current = store.db.execute(
            "SELECT evidence_sequence FROM v52_authoritative_candidate_state WHERE asset_id=?",
            (asset_id,),
        ).fetchone()
        sequence = int(current["evidence_sequence"] or 0) + 1 if current is not None else 1
        store.db.execute(
            "INSERT INTO v52_authoritative_candidate_state("
            "asset_id,release_commit,state,surface,lifecycle,target_fraction,open_fraction,last_entry_price,"
            "evidence_sequence,last_reason,updated_at,authority_id,strategy_version,paper_only,live_money_authority) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,1,0) "
            "ON CONFLICT(asset_id) DO UPDATE SET release_commit=excluded.release_commit,state=excluded.state,"
            "surface=excluded.surface,lifecycle=excluded.lifecycle,target_fraction=excluded.target_fraction,"
            "open_fraction=excluded.open_fraction,last_entry_price=COALESCE(excluded.last_entry_price,v52_authoritative_candidate_state.last_entry_price),"
            "evidence_sequence=excluded.evidence_sequence,last_reason=excluded.last_reason,updated_at=excluded.updated_at,"
            "authority_id=excluded.authority_id,strategy_version=excluded.strategy_version,paper_only=1,live_money_authority=0",
            (
                asset_id,
                release,
                state,
                surface,
                lifecycle,
                float(target_fraction),
                float(open_fraction),
                last_entry_price,
                sequence,
                reason,
                _utcnow(),
                AUTHORITY_ID,
                STRATEGY_VERSION,
            ),
        )


def _lane_cap(lane: str, severity: float) -> float:
    caps = dict(target_sizing_policy()["solana_lane_caps"])
    if lane in caps:
        cap = float(caps[lane])
    else:
        cap = 0.05
    if lane == "elite_wallet_continuation" and severity >= 0.20:
        cap = min(cap, 0.05)
    return max(0.0, cap)


def _v52_solana_values(adapter: Any, *, lane: str, context_key: str) -> list[float]:
    _ensure_v52_epoch(adapter)
    if not _table_exists(adapter.store, "risk_conditioned_alpha_v5_outcomes"):
        return []
    with adapter.store._lock:
        rows = adapter.store.db.execute(
            "SELECT o.net_return FROM risk_conditioned_alpha_v5_outcomes o "
            "JOIN v52_economic_freeze_releases e ON e.release_commit=o.release_commit "
            "WHERE e.economic_freeze_epoch=? AND e.authority_id=? AND e.strategy_version=? "
            "AND o.strategy_version=? AND o.lane=? AND o.context_key=? ORDER BY o.id",
            (
                ECONOMIC_FREEZE_EPOCH,
                AUTHORITY_ID,
                STRATEGY_VERSION,
                STRATEGY_VERSION,
                lane,
                context_key,
            ),
        ).fetchall()
    return [float(row["net_return"]) for row in rows]


def _v52_solana_target(
    adapter: Any,
    pre: dict[str, Any],
    *,
    chase: float | None,
    latency: float | None,
) -> tuple[str | None, float, dict[str, Any]]:
    severity = float((pre.get("risk") or {}).get("risk_severity") or 0.0)
    profiles: dict[str, Any] = {}
    promoted: list[tuple[str, dict[str, Any]]] = []
    viable: list[tuple[str, dict[str, Any]]] = []
    minimum = int(target_sizing_policy()["minimum_forward_samples"])
    for lane in list(pre.get("lanes") or ()):
        key = solana_strategy._context_key(pre, lane, chase=chase, latency=latency)
        values = _v52_solana_values(adapter, lane=lane, context_key=key)
        profile_obj = solana_strategy.robust_return_profile(
            values,
            max_fraction=_lane_cap(lane, severity),
            min_samples=minimum,
        )
        profile = {
            **profile_obj.__dict__,
            "context_key": key,
            "evidence_source": "v52_authoritative_forward_epoch_only",
            "authority_id": AUTHORITY_ID,
            "economic_freeze_epoch": ECONOMIC_FREEZE_EPOCH,
            "v51_promotion_evidence_used": False,
        }
        profiles[lane] = profile
        if profile_obj.state == "promoted_positive_log_growth" and profile_obj.best_fraction > 0.0:
            promoted.append((lane, profile))
        if profile_obj.state != "demoted_nonpositive_log_growth":
            viable.append((lane, profile))

    if promoted:
        lane, profile = max(
            promoted,
            key=lambda item: float(item[1].get("best_expected_log_growth") or float("-inf")),
        )
        target = float(profile.get("best_fraction") or 0.0)
    elif viable:
        priority = {
            "graduation_continuation": 6,
            "raydium_cross_venue_persistence": 5,
            "creator_insider_continuation": 4,
            "entity_flow_momentum": 3,
            "elite_wallet_continuation": 2,
            "hazard_continuation": 1,
        }
        lane, _profile = max(viable, key=lambda item: priority.get(item[0], 0))
        sizing = target_sizing_policy()
        target = (
            float(sizing["bootstrap_fraction_hazard_or_high_severity"])
            if lane == "hazard_continuation" or severity >= 0.45
            else float(sizing["bootstrap_fraction_clean"])
        )
    else:
        return None, 0.0, profiles

    target *= solana_strategy._regime_multiplier(str(pre.get("regime") or "unknown"))
    target *= max(0.25, 1.0 - 0.60 * severity)
    target = min(_lane_cap(lane, severity), target)
    return (lane if target > 0.0 else None), max(0.0, target), profiles


def _open_solana_rows(adapter: Any, token: str) -> list[dict[str, Any]]:
    if not _table_exists(adapter.store, "risk_conditioned_alpha_v5_trials"):
        return []
    with adapter.store._lock:
        rows = adapter.store.db.execute(
            "SELECT t.source_signature,t.position_fraction,t.lane,t.venue,t.lifecycle,t.regime,t.flow_state,t.risk_severity,"
            "f.entry_all_in_price_sol,f.opportunity_json FROM risk_conditioned_alpha_v5_trials t "
            "LEFT JOIN risk_conditioned_alpha_v5_outcomes o ON o.release_commit=t.release_commit "
            "AND o.source_signature=t.source_signature AND o.lane=t.lane "
            "LEFT JOIN profit_first_final_trials f ON f.epoch_id=? AND f.source_signature=t.source_signature "
            "AND f.lane='unified_profit_maximizer' "
            "WHERE t.release_commit=? AND t.strategy_version=? AND t.token_mint=? "
            "AND t.selected=1 AND t.decision LIKE 'paper_enter%' AND o.id IS NULL ORDER BY t.id",
            (adapter.epoch_id, adapter.release_commit, STRATEGY_VERSION, token),
        ).fetchall()
    return [dict(row) for row in rows]


def _current_entry_price(adapter: Any, pre: dict[str, Any], chase: float | None) -> float | None:
    if chase is None:
        return None
    try:
        with adapter.store._lock:
            row = adapter.store.db.execute(
                "SELECT wallet_price_sol FROM wallet_discovery_forward_observations "
                "WHERE token_mint=? AND wallet=? ORDER BY received_at DESC LIMIT 1",
                (str(pre.get("token") or ""), str(pre.get("wallet") or "")),
            ).fetchone()
        base = float(row["wallet_price_sol"] or 0.0) if row is not None else 0.0
        return base * (1.0 + max(0.0, float(chase))) if base > 0.0 else None
    except Exception:
        return None


def _new_solana_scale_evidence(pre: dict[str, Any], prior: list[dict[str, Any]], lane: str) -> bool:
    if not prior:
        return True
    last = prior[-1]
    if str(pre.get("venue") or "") != str(last.get("venue") or ""):
        return True
    if str(pre.get("lifecycle") or "") != str(last.get("lifecycle") or ""):
        return True
    if lane != str(last.get("lane") or ""):
        return True
    if bool(pre.get("cross_venue_persistence")):
        return True
    current_severity = float((pre.get("risk") or {}).get("risk_severity") or 0.0)
    prior_min = min(float(item.get("risk_severity") or 0.0) for item in prior)
    if current_severity + 1e-12 < prior_min:
        return True
    current_independent = int(pre.get("independent_count") or 0)
    prior_independent = 0
    for item in prior:
        try:
            payload = json.loads(str(item.get("opportunity_json") or "{}"))
            prior_independent = max(prior_independent, int(payload.get("independent_confirmation_count") or 0))
        except Exception:
            continue
    return current_independent > prior_independent


def _authority_metadata(
    *,
    target_fraction: float,
    final_fraction: float,
    capture_stage: str,
    open_fraction_before: float = 0.0,
    reason: str = "",
) -> dict[str, Any]:
    return {
        "authority_id": AUTHORITY_ID,
        "strategy_version": STRATEGY_VERSION,
        "economic_freeze_epoch": ECONOMIC_FREEZE_EPOCH,
        "authority_fingerprint": authority_fingerprint(),
        "decision_owner": "v52",
        "evidence_epoch": "v52_forward_only",
        "v51_promotion_evidence_used": False,
        "target_fraction": float(target_fraction),
        "open_fraction_before": float(open_fraction_before),
        "final_fraction": float(final_fraction),
        "capture_stage": capture_stage,
        "reason": reason,
        "scale_requires_new_forward_evidence": True,
        "averaging_down_allowed": False,
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
    }


async def _v52_execution(self: Any, row: dict[str, Any], fraction: float) -> dict[str, Any] | None:
    if _BASE_EXECUTION is None:
        raise RuntimeError("v52_execution_base_not_installed")
    result = await _BASE_EXECUTION(self, row, fraction)
    if result is None:
        return None
    coverage = float(position_policy()["minimum_exit_depth_coverage_ratio"])
    raw = int(result.get("token_raw") or 0)
    if raw <= 0:
        return None
    required_raw = max(raw, int(round(raw * coverage)))
    if required_raw == raw:
        result["v52_exit_depth_coverage_ratio"] = 1.0
        result["v52_exit_depth_quote_available"] = bool(result.get("exit_net_sol") is not None)
        return result if result["v52_exit_depth_quote_available"] else None
    try:
        depth_route = await self.execution._route(str(row.get("token_mint") or ""), solana_strategy.WSOL_MINT if hasattr(solana_strategy, "WSOL_MINT") else "So11111111111111111111111111111111111111112", required_raw)
    except Exception:
        depth_route = None
    if depth_route is None or int(depth_route.get("out_amount") or 0) <= 0:
        return None
    result["v52_exit_depth_coverage_ratio"] = coverage
    result["v52_exit_depth_quote_available"] = True
    result["v52_exit_depth_quote_input_raw"] = required_raw
    result["v52_exit_depth_quote_output_lamports"] = int(depth_route.get("out_amount") or 0)
    return result


def _v52_solana_choose(
    adapter: Any,
    pre: dict[str, Any],
    *,
    chase: float | None = None,
    latency: float | None = None,
) -> tuple[str | None, float, dict[str, Any]]:
    _ensure_v52_epoch(adapter)
    lane, target, profiles = _v52_solana_target(adapter, pre, chase=chase, latency=latency)
    if not lane or target <= 0.0:
        return None, 0.0, profiles
    limits = execution_policy()
    if latency is not None and float(latency) > float(limits["latency_hard_max_seconds"]):
        return None, 0.0, profiles
    if chase is not None and float(chase) > float(limits["chase_observe_only_above_fraction"]):
        return None, 0.0, profiles

    token = str(pre.get("token") or "")
    prior = _open_solana_rows(adapter, token)
    open_fraction = sum(max(0.0, float(item.get("position_fraction") or 0.0)) for item in prior)
    current_price = _current_entry_price(adapter, pre, chase)
    policy = position_policy()
    stage = "starter" if not prior else "scale"
    reason = "v52_fractional_starter"
    if not prior:
        final = min(target, target * float(policy["starter_fraction_of_target"]))
    else:
        if not _new_solana_scale_evidence(pre, prior, lane):
            final = 0.0
            reason = "scale_blocked_no_new_forward_evidence"
        else:
            last_prices = [float(item["entry_all_in_price_sol"]) for item in prior if item.get("entry_all_in_price_sol")]
            if current_price is not None and last_prices and current_price + 1e-12 < last_prices[-1]:
                final = 0.0
                reason = "scale_blocked_averaging_down"
            else:
                add_cap = target * float(policy["max_scale_fraction_of_target_per_add"])
                final = max(0.0, min(add_cap, target - open_fraction))
                reason = "v52_scale_new_forward_evidence"

    profile = dict(profiles.get(lane) or {})
    profile["v52_authority"] = _authority_metadata(
        target_fraction=target,
        final_fraction=final,
        capture_stage=stage,
        open_fraction_before=open_fraction,
        reason=reason,
    )
    profiles[lane] = profile
    _record_candidate_state(
        adapter,
        asset_id=token,
        state=("entered" if stage == "starter" and final > 0 else "scaling" if final > 0 else "pre_actionable"),
        surface=str(pre.get("venue") or "UNKNOWN"),
        lifecycle=str(pre.get("lifecycle") or "unknown"),
        target_fraction=target,
        open_fraction=open_fraction + final,
        last_entry_price=current_price,
        reason=reason,
    )
    return (lane if final > 0.0 else None), final, profiles


def _v52_fomo_values(adapter: Any, *, wallet: str, venue: str, lifecycle: str, regime: str) -> list[float]:
    _ensure_v52_epoch(adapter)
    if not _table_exists(adapter.store, "fomo_paper_outcomes"):
        return []
    with adapter.store._lock:
        rows = adapter.store.db.execute(
            "SELECT o.net_return FROM fomo_paper_outcomes o "
            "JOIN v52_economic_freeze_releases e ON e.release_commit=o.release_commit "
            "WHERE e.economic_freeze_epoch=? AND e.authority_id=? AND o.strategy_version=? "
            "AND o.trigger_wallet=? AND o.venue=? AND o.lifecycle=? AND o.regime=? ORDER BY o.id",
            (ECONOMIC_FREEZE_EPOCH, AUTHORITY_ID, STRATEGY_VERSION, wallet, venue, lifecycle, regime),
        ).fetchall()
    return [float(row["net_return"]) for row in rows]


def _open_fomo_rows(adapter: Any, token: str) -> list[dict[str, Any]]:
    if not _table_exists(adapter.store, "fomo_paper_trials"):
        return []
    with adapter.store._lock:
        rows = adapter.store.db.execute(
            "SELECT t.position_fraction,t.entry_all_in_price_sol,t.fomo_state,t.lifecycle,t.regime "
            "FROM fomo_paper_trials t LEFT JOIN fomo_paper_outcomes o "
            "ON o.release_commit=t.release_commit AND o.source_signature=t.source_signature "
            "WHERE t.release_commit=? AND t.strategy_version=? AND t.token_mint=? "
            "AND t.decision LIKE 'paper_enter_%' AND o.id IS NULL ORDER BY t.id",
            (adapter.release_commit, STRATEGY_VERSION, token),
        ).fetchall()
    return [dict(row) for row in rows]


def _v52_fomo_decision(
    adapter: Any,
    *,
    observation: dict[str, Any],
    trial: dict[str, Any],
) -> dict[str, Any]:
    _ensure_v52_epoch(adapter)
    state_payload = fomo_paper._safe_json(observation.get("state_json"))
    fomo_state = str(state_payload.get("state") or "unknown")
    profile_stub: dict[str, Any] = {}
    if fomo_state not in {"pre_fomo", "active_fomo"}:
        return {"decision": "no_entry_nonactionable_fomo_state", "reason": fomo_state, "position_fraction": 0.0, "profile": profile_stub}
    if not bool(state_payload.get("structurally_accessible")):
        return {"decision": "no_entry_structurally_inaccessible", "reason": "v52_fomo_accessibility_failed", "position_fraction": 0.0, "profile": profile_stub}
    latency = float(trial.get("signal_to_entry_seconds") or 0.0)
    if latency > float(execution_policy()["latency_hard_max_seconds"]) or not bool(trial.get("entry_executable")) or not bool(trial.get("exit_executable")):
        return {"decision": "no_entry_v52_execution_boundary", "reason": "v52_exact_two_sided_execution_and_20s_boundary_required", "position_fraction": 0.0, "profile": profile_stub}

    wallet = str(trial.get("trigger_wallet") or "")
    venue = str(observation.get("venue") or "UNKNOWN")
    lifecycle = str(observation.get("lifecycle") or "unknown")
    regime = str(observation.get("regime") or trial.get("regime") or "unknown")
    values = _v52_fomo_values(adapter, wallet=wallet, venue=venue, lifecycle=lifecycle, regime=regime)
    max_target = float(target_sizing_policy()["fomo_max_target_fraction"])
    fresh = solana_strategy.robust_return_profile(
        values,
        grid=solana_strategy.FOMO_ACTIVE_GRID,
        max_fraction=max_target,
        min_samples=int(target_sizing_policy()["minimum_forward_samples"]),
    )
    profile = {
        **fresh.__dict__,
        "evidence_source": "v52_authoritative_forward_epoch_only",
        "v51_promotion_evidence_used": False,
    }
    if fresh.state == "demoted_nonpositive_log_growth":
        return {"decision": "no_entry_v52_demoted_fomo_context", "reason": "nonpositive_v52_forward_log_growth", "position_fraction": 0.0, "profile": profile}
    risk_class = solana_strategy._fomo_risk_class(state_payload)
    sizing = target_sizing_policy()
    target = (
        float(fresh.best_fraction)
        if fresh.state == "promoted_positive_log_growth" and fresh.best_fraction > 0.0
        else float(sizing["bootstrap_fraction_hazard_or_high_severity"] if risk_class == "hazard_fomo" else sizing["bootstrap_fraction_clean"])
    )
    target *= solana_strategy._regime_multiplier(regime)
    target = min(max_target, target)

    token = str(trial.get("token_mint") or observation.get("token_mint") or "")
    prior = _open_fomo_rows(adapter, token)
    open_fraction = sum(float(item.get("position_fraction") or 0.0) for item in prior)
    policy = position_policy()
    current_price = float(trial.get("entry_all_in_price_sol") or 0.0) or None
    if not prior:
        stage = "starter"
        final = min(target, target * float(policy["starter_fraction_of_target"]))
        reason = "v52_fractional_starter"
    else:
        stage = "scale"
        last_prices = [float(item["entry_all_in_price_sol"]) for item in prior if item.get("entry_all_in_price_sol")]
        if fomo_state != "active_fomo":
            final = 0.0
            reason = "scale_blocked_requires_active_fomo_new_evidence"
        elif current_price is not None and last_prices and current_price + 1e-12 < last_prices[-1]:
            final = 0.0
            reason = "scale_blocked_averaging_down"
        else:
            add_cap = target * float(policy["max_scale_fraction_of_target_per_add"])
            final = max(0.0, min(add_cap, target - open_fraction))
            reason = "v52_scale_active_fomo_new_evidence"

    profile["v52_authority"] = _authority_metadata(
        target_fraction=target,
        final_fraction=final,
        capture_stage=stage,
        open_fraction_before=open_fraction,
        reason=reason,
    )
    _record_candidate_state(
        adapter,
        asset_id=token,
        state=("entered" if stage == "starter" and final > 0 else "scaling" if final > 0 else "pre_actionable"),
        surface="FOMO",
        lifecycle=lifecycle,
        target_fraction=target,
        open_fraction=open_fraction + final,
        last_entry_price=current_price,
        reason=reason,
    )
    return {
        "decision": (f"paper_enter_v52_{stage}" if final > 0 else "no_entry_v52_capture_gate"),
        "reason": reason,
        "position_fraction": final,
        "profile": profile,
        "v52_authority": profile["v52_authority"],
    }


def _v52_robinhood_profile(self: Any, **context: Any) -> dict[str, Any]:
    _ensure_v52_epoch(self)
    entity = str(context.get("entity") or "")
    role = str(context.get("role") or "unknown")
    lane = str(context.get("lane") or "unknown")
    venue = str(context.get("venue") or "UNKNOWN")
    lifecycle = str(context.get("lifecycle") or "unknown")
    regime = str(context.get("regime") or "unknown")
    risk_signature = str(context.get("risk_signature") or "clean")
    flow_state = str(context.get("flow_state") or "neutral")
    values: list[float] = []
    if _table_exists(self.store, "robinhood_paper_outcomes") and _table_exists(self.store, "robinhood_v5_trial_context"):
        with self.store._lock:
            rows = self.store.db.execute(
                "SELECT o.net_return FROM robinhood_paper_outcomes o "
                "JOIN robinhood_paper_trials t ON t.id=o.trial_id "
                "JOIN robinhood_v5_trial_context c ON c.trial_id=o.trial_id "
                "JOIN v52_economic_freeze_releases e ON e.release_commit=o.release_commit "
                "WHERE e.economic_freeze_epoch=? AND e.authority_id=? AND t.strategy_version=? AND c.strategy_version=? "
                "AND t.trigger_entity=? AND c.trigger_role=? AND c.lane=? AND t.venue=? AND t.lifecycle=? "
                "AND c.regime=? AND c.risk_signature=? AND c.flow_state=? ORDER BY o.id",
                (
                    ECONOMIC_FREEZE_EPOCH,
                    AUTHORITY_ID,
                    STRATEGY_VERSION,
                    STRATEGY_VERSION,
                    entity,
                    role,
                    lane,
                    venue,
                    lifecycle,
                    regime,
                    risk_signature,
                    flow_state,
                ),
            ).fetchall()
        values = [float(row["net_return"]) for row in rows]
    hp = solana_strategy.robust_return_profile(
        values,
        grid=robinhood_strategy.ROBINHOOD_V5_POSITION_GRID,
        max_fraction=float(target_sizing_policy()["robinhood_max_target_fraction"]),
        min_samples=int(target_sizing_policy()["minimum_forward_samples"]),
    )
    if hp.state == "promoted_positive_log_growth":
        legacy_state = "promoted_positive_log_growth"
    elif hp.state == "demoted_nonpositive_log_growth":
        legacy_state = "demoted_nonpositive_log_growth"
    else:
        legacy_state = "bootstrap_forward_evidence"
    return {
        "sample_count": hp.sample_count,
        "state": legacy_state,
        "best_fraction": hp.best_fraction,
        "best_expected_log_growth": hp.best_expected_log_growth,
        "mean_return": hp.mean_return,
        "median_return": hp.median_return,
        "hit_rate": hp.hit_rate,
        "trimmed_mean_ex_best": hp.trimmed_mean_ex_best,
        "expected_shortfall_20": hp.expected_shortfall_20,
        "winner_concentration": hp.winner_concentration,
        "max_drawdown": hp.max_drawdown_at_best_fraction,
        "evidence_source": "v52_authoritative_forward_epoch_only",
        "v51_promotion_evidence_used": False,
        "hit_rate_is_promotion_veto": False,
    }


def _v52_robinhood_choose(self: Any, **kwargs: Any) -> tuple[str | None, float, dict[str, Any]]:
    if _BASE_RH_CHOOSE is None:
        raise RuntimeError("v52_robinhood_base_not_installed")
    _ensure_v52_epoch(self)
    lane, target, profiles = _BASE_RH_CHOOSE(self, **kwargs)
    copied = {key: dict(value) if isinstance(value, dict) else value for key, value in dict(profiles).items()}
    if not lane or float(target or 0.0) <= 0.0:
        return lane, 0.0, copied
    target = min(float(target), float(target_sizing_policy()["robinhood_max_target_fraction"]))
    final = min(target, target * float(position_policy()["starter_fraction_of_target"]))
    profile = dict(copied.get(lane) or {})
    profile["v52_authority"] = _authority_metadata(
        target_fraction=target,
        final_fraction=final,
        capture_stage="starter",
        reason="v52_fractional_starter",
    )
    copied[lane] = profile
    return (lane if final > 0.0 else None), final, copied


def install_v52_authoritative_strategy() -> None:
    """Install v5.2 last so it is the sole final economic decision owner."""
    global _INSTALLED, _BASE_SOLANA_CHOOSE, _BASE_FOMO_DECISION, _BASE_RH_CHOOSE, _BASE_RH_PROFILE, _BASE_EXECUTION
    if _INSTALLED:
        return
    _BASE_SOLANA_CHOOSE = solana_strategy._choose_lane_and_fraction
    _BASE_FOMO_DECISION = fomo_paper._paper_decision
    _BASE_RH_CHOOSE = RobinhoodProfitMaximizerMixin._v5_choose_lane_fraction
    _BASE_RH_PROFILE = RobinhoodProfitMaximizerMixin._v5_profile
    _BASE_EXECUTION = FinalProfitFirstResearchAdapter._execution

    # New rows must carry v5.2 identity. The tables keep their historical names as
    # compatibility storage only; version/authority columns distinguish the epoch.
    solana_strategy.STRATEGY_VERSION = STRATEGY_VERSION
    fomo_paper.FOMO_PAPER_STRATEGY_VERSION = STRATEGY_VERSION
    robinhood_strategy.ROBINHOOD_V5_VERSION = STRATEGY_VERSION

    FinalProfitFirstResearchAdapter._execution = _v52_execution  # type: ignore[method-assign]
    solana_strategy._choose_lane_and_fraction = _v52_solana_choose
    fomo_paper._paper_decision = _v52_fomo_decision
    RobinhoodProfitMaximizerMixin._v5_profile = _v52_robinhood_profile  # type: ignore[method-assign]
    RobinhoodProfitMaximizerMixin._v5_choose_lane_fraction = _v52_robinhood_choose  # type: ignore[method-assign]

    setattr(FinalProfitFirstResearchAdapter._execution, "_roi_v52_exact_depth_authority", True)
    setattr(solana_strategy._choose_lane_and_fraction, "_roi_v52_final_authority", True)
    setattr(fomo_paper._paper_decision, "_roi_v52_final_authority", True)
    setattr(RobinhoodProfitMaximizerMixin._v5_profile, "_roi_v52_forward_profile", True)
    setattr(RobinhoodProfitMaximizerMixin._v5_choose_lane_fraction, "_roi_v52_final_authority", True)
    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "consolidation_version": CONSOLIDATION_VERSION,
        "installed": _INSTALLED,
        "authority_id": AUTHORITY_ID,
        "strategy_version": STRATEGY_VERSION,
        "authority_fingerprint": authority_fingerprint(),
        "economic_freeze_epoch": ECONOMIC_FREEZE_EPOCH,
        "final_decision_owner": "v52",
        "solana_final_owner": bool(getattr(solana_strategy._choose_lane_and_fraction, "_roi_v52_final_authority", False)),
        "fomo_final_owner": bool(getattr(fomo_paper._paper_decision, "_roi_v52_final_authority", False)),
        "robinhood_final_owner": bool(getattr(RobinhoodProfitMaximizerMixin._v5_choose_lane_fraction, "_roi_v52_final_authority", False)),
        "robinhood_forward_profile_owner": bool(getattr(RobinhoodProfitMaximizerMixin._v5_profile, "_roi_v52_forward_profile", False)),
        "exact_exit_depth_authority": bool(getattr(FinalProfitFirstResearchAdapter._execution, "_roi_v52_exact_depth_authority", False)),
        "v51_outcomes_can_grant_v52_promotion": False,
        "new_rows_carry_v52_strategy_version": True,
        "solana_and_fomo_scale_in_authority": True,
        "robinhood_scale_in_authority": False,
        "staged_derisk_runner_authority": False,
        "v51_named_substrate_role": "compatibility_transport_storage_exact_execution_and_settlement_only",
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
    }


__all__ = ["CONSOLIDATION_VERSION", "install_v52_authoritative_strategy", "status"]
