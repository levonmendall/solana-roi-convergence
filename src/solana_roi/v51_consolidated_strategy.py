from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Callable

from . import fomo_paper_strategy as fomo_paper
from . import risk_conditioned_alpha_v5 as v5
from . import risk_conditioned_alpha_v51 as v51
from .robinhood_chain_profit_maximizer import RobinhoodProfitMaximizerMixin
from .strategy_v51_authority import (
    AUTHORITY_ID,
    ECONOMIC_FREEZE_EPOCH,
    PAPER_ONLY,
    LIVE_MONEY_AUTHORITY,
    authority_fingerprint,
    hazard_requirements,
)
from .v51_economic_core import bootstrap_execution_multiplier, hierarchical_profile

CONSOLIDATION_VERSION = "v51-consolidated-economic-authority-v1"
_INSTALLED = False
_ORIGINAL_SOLANA_CHOOSE: Callable[..., Any] | None = None
_ORIGINAL_FOMO_DECISION: Callable[..., Any] | None = None
_ORIGINAL_RH_PROFILE: Callable[..., Any] | None = None


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


def _ensure_epoch(store: Any, release_commit: str | None) -> None:
    release = str(release_commit or "").strip()
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS v51_economic_freeze_releases ("
            "release_commit TEXT PRIMARY KEY, economic_freeze_epoch TEXT NOT NULL, authority_id TEXT NOT NULL, "
            "authority_fingerprint TEXT NOT NULL, registered_at TEXT NOT NULL, paper_only INTEGER NOT NULL, "
            "live_money_authority INTEGER NOT NULL)"
        )
        if release:
            store.db.execute(
                "INSERT OR REPLACE INTO v51_economic_freeze_releases("
                "release_commit,economic_freeze_epoch,authority_id,authority_fingerprint,registered_at,paper_only,live_money_authority) "
                "VALUES (?,?,?,?,?,1,0)",
                (release, ECONOMIC_FREEZE_EPOCH, AUTHORITY_ID, authority_fingerprint(), _utcnow()),
            )


def _dedup(rows: list[Any], key: str) -> list[Any]:
    seen: set[str] = set()
    result: list[Any] = []
    for row in rows:
        value = str(row[key] if row[key] is not None else "")
        if not value or value in seen:
            continue
        seen.add(value)
        result.append(row)
    return result


def _solana_evidence(adapter: Any, *, lane: str, pre: dict[str, Any], context_key: str) -> tuple[list[float], list[float]]:
    _ensure_epoch(adapter.store, getattr(adapter, "release_commit", None))
    parsed = v51._parse_context_key(context_key)
    entity = str(parsed.get("entity") or pre.get("trigger_entity") or "")
    risk_signature = str((pre.get("risk") or {}).get("risk_signature") or "clean")
    if not _table_exists(adapter.store, "risk_conditioned_alpha_v5_outcomes"):
        return [], []
    with adapter.store._lock:
        exact_rows = adapter.store.db.execute(
            "SELECT o.source_signature,o.net_return FROM risk_conditioned_alpha_v5_outcomes o "
            "JOIN v51_economic_freeze_releases e ON e.release_commit=o.release_commit "
            "WHERE e.economic_freeze_epoch=? AND e.authority_id=? AND o.context_key=? ORDER BY o.id",
            (ECONOMIC_FREEZE_EPOCH, AUTHORITY_ID, context_key),
        ).fetchall()
        parent_rows = adapter.store.db.execute(
            "SELECT o.source_signature,o.net_return FROM risk_conditioned_alpha_v5_outcomes o "
            "JOIN v51_economic_freeze_releases e ON e.release_commit=o.release_commit "
            "WHERE e.economic_freeze_epoch=? AND e.authority_id=? AND o.lane=? AND o.venue=? AND o.lifecycle=? "
            "AND o.risk_signature=? AND o.context_key LIKE ? AND o.context_key<>? ORDER BY o.id",
            (
                ECONOMIC_FREEZE_EPOCH,
                AUTHORITY_ID,
                lane,
                str(pre.get("venue") or "UNKNOWN"),
                str(pre.get("lifecycle") or "unknown"),
                risk_signature,
                entity + "|%",
                context_key,
            ),
        ).fetchall()
    exact = _dedup(list(exact_rows), "source_signature")
    exact_signatures = {str(row["source_signature"]) for row in exact}
    parent = [row for row in _dedup(list(parent_rows), "source_signature") if str(row["source_signature"]) not in exact_signatures]
    return [float(row["net_return"]) for row in exact], [float(row["net_return"]) for row in parent]


def _solana_choose_consolidated(
    adapter: Any,
    pre: dict[str, Any],
    *,
    chase: float | None = None,
    latency: float | None = None,
) -> tuple[str | None, float, dict[str, Any]]:
    profiles: dict[str, Any] = {}
    candidates: list[tuple[str, dict[str, Any]]] = []
    risk = dict(pre.get("risk") or {})
    severity = float(risk.get("risk_severity") or 0.0)
    signature = str(risk.get("risk_signature") or "clean")
    for lane in list(pre.get("lanes") or ()):
        key = v51._context_key_v51(pre, lane, chase=chase, latency=latency)
        exact, parent = _solana_evidence(adapter, lane=lane, pre=pre, context_key=key)
        cap = float(v5._lane_cap(lane, severity))
        profile = hierarchical_profile(
            exact,
            parent,
            (),
            risk_severity=severity,
            risk_signature=signature,
            max_fraction=cap,
        )
        profile.update(
            {
                "context_key": key,
                "evidence_source": "v51_frozen_epoch_exact_plus_same_entity_parent_shrinkage",
                "authority_id": AUTHORITY_ID,
                "economic_freeze_epoch": ECONOMIC_FREEZE_EPOCH,
            }
        )
        profiles[lane] = profile
        candidates.append((lane, profile))
    if not candidates:
        return None, 0.0, profiles

    promoted = [(lane, profile) for lane, profile in candidates if bool(profile.get("promoted"))]
    viable = [(lane, profile) for lane, profile in candidates if not bool(profile.get("killed"))]
    if promoted:
        lane, chosen = max(
            promoted,
            key=lambda item: float(item[1].get("best_expected_log_growth") or float("-inf")),
        )
        requested = float(chosen.get("best_fraction") or 0.0)
        evidence_state = "promoted"
    elif viable:
        priority = {
            "graduation_continuation": 6,
            "raydium_cross_venue_persistence": 5,
            "creator_insider_continuation": 4,
            "entity_flow_momentum": 3,
            "elite_wallet_continuation": 2,
            "hazard_continuation": 1,
        }
        lane, chosen = max(viable, key=lambda item: priority.get(item[0], 0))
        requested = float(v5._bootstrap_fraction(lane, severity))
        evidence_state = "bootstrap"
    else:
        return None, 0.0, profiles

    requested *= float(v5._regime_multiplier(str(pre.get("regime") or "unknown")))
    requested *= max(0.25, 1.0 - 0.60 * severity)
    requested = min(float(v5._lane_cap(lane, severity)), requested)
    if evidence_state == "bootstrap" and latency is not None:
        requested *= bootstrap_execution_multiplier(
            latency_seconds=latency,
            chase_fraction=chase,
            round_trip_cost_fraction=v5._finite(pre.get("round_trip_cost_fraction")),
            risk_severity=severity,
            risk_signature=signature,
        )
    if latency is not None and float(latency) > 20.0:
        requested = 0.0
    if chase is not None and float(chase) > 0.40:
        requested = 0.0
    if requested <= 0.0:
        return None, 0.0, profiles
    # Do not restore the legacy 0.25% floor after evidence-based decay. A small
    # but positive probe is allowed to remain small rather than being inflated.
    return lane, requested, profiles


def _fomo_epoch_returns(
    adapter: Any,
    *,
    wallet: str,
    venue: str,
    lifecycle: str,
    regime: str,
    hazard_signature: str,
) -> list[float]:
    _ensure_epoch(adapter.store, getattr(adapter, "release_commit", None))
    if not (_table_exists(adapter.store, "fomo_shadow_observations") and _table_exists(adapter.store, "fomo_shadow_outcomes")):
        return []
    try:
        with adapter.store._lock:
            rows = adapter.store.db.execute(
                "SELECT s.source_signature,s.state_json,o.net_return,t.trigger_wallet FROM fomo_shadow_observations s "
                "JOIN v51_economic_freeze_releases e ON e.release_commit=s.release_commit "
                "JOIN profit_first_final_trials t ON t.release_commit=s.release_commit AND t.source_signature=s.source_signature "
                "AND t.lane='unified_profit_maximizer' "
                "JOIN fomo_shadow_outcomes o ON o.release_commit=s.release_commit AND o.source_signature=s.source_signature "
                "WHERE e.economic_freeze_epoch=? AND e.authority_id=? AND s.venue=? AND s.lifecycle=? AND s.regime=? ORDER BY s.id",
                (ECONOMIC_FREEZE_EPOCH, AUTHORITY_ID, venue, lifecycle, regime),
            ).fetchall()
    except Exception:
        return []
    values: list[float] = []
    for row in _dedup(list(rows), "source_signature"):
        if str(row["trigger_wallet"] or "") != wallet:
            continue
        state = v5._safe_json(row["state_json"])
        if str(state.get("state") or "") not in {"pre_fomo", "active_fomo"}:
            continue
        if v51.fomo_hazard_signature(state) != hazard_signature:
            continue
        value = v5._finite(row["net_return"])
        if value is not None:
            values.append(float(value))
    return values


def _fomo_decision_consolidated(adapter: Any, *, observation: dict[str, Any], trial: dict[str, Any]) -> dict[str, Any]:
    if _ORIGINAL_FOMO_DECISION is None:
        raise RuntimeError("consolidated FOMO decision is not installed")
    result = dict(_ORIGINAL_FOMO_DECISION(adapter, observation=observation, trial=trial))
    decision = str(result.get("decision") or "")
    if not decision.startswith("paper_enter"):
        return result
    profile = dict(result.get("profile") or {})
    signature = str(profile.get("risk_signature") or "clean")
    severity = float(profile.get("risk_severity") or 0.0)
    requirements = hazard_requirements(severity, signature)
    sample_count = int(profile.get("sample_count") or 0)
    promoted = "promoted" in decision and sample_count >= int(requirements["minimum_independent_outcomes"])
    fraction = max(0.0, float(result.get("position_fraction") or 0.0))
    if signature != "clean" and not promoted:
        fraction = min(fraction, 0.005 * float(requirements["bootstrap_size_multiplier"]))
        result["decision"] = "paper_enter_hazard_fomo_probe_consolidated"
        result["reason"] = "hazard_requires_stronger_frozen_epoch_forward_evidence"
    result["position_fraction"] = fraction
    profile["hazard_evidence_requirements"] = requirements
    profile["economic_freeze_epoch"] = ECONOMIC_FREEZE_EPOCH
    profile["authority_id"] = AUTHORITY_ID
    result["profile"] = profile
    return result


def _rh_epoch_profile(self: Any, **context: Any) -> dict[str, Any]:
    _ensure_epoch(self.store, getattr(self, "release_commit", None))
    entity = str(context.get("entity") or "")
    role = str(context.get("role") or "unknown")
    lane = str(context.get("lane") or "unknown")
    venue = str(context.get("venue") or "UNKNOWN")
    lifecycle = str(context.get("lifecycle") or "unknown")
    regime = str(context.get("regime") or "unknown")
    risk_signature = str(context.get("risk_signature") or "clean")
    flow_state = str(context.get("flow_state") or "neutral")
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
    exact_rows: list[Any] = []
    parent_rows: list[Any] = []
    if _table_exists(self.store, "robinhood_paper_outcomes") and _table_exists(self.store, "robinhood_v5_trial_context"):
        with self.store._lock:
            exact_rows = list(self.store.db.execute(
                "SELECT o.trial_id,o.net_return FROM robinhood_paper_outcomes o "
                "JOIN v51_economic_freeze_releases e ON e.release_commit=o.release_commit "
                "JOIN robinhood_v5_trial_context c ON c.trial_id=o.trial_id "
                "WHERE e.economic_freeze_epoch=? AND e.authority_id=? AND c.context_key=? ORDER BY o.id",
                (ECONOMIC_FREEZE_EPOCH, AUTHORITY_ID, key),
            ).fetchall())
            parent_rows = list(self.store.db.execute(
                "SELECT o.trial_id,o.net_return FROM robinhood_paper_outcomes o "
                "JOIN v51_economic_freeze_releases e ON e.release_commit=o.release_commit "
                "JOIN robinhood_v5_trial_context c ON c.trial_id=o.trial_id "
                "JOIN robinhood_paper_trials t ON t.id=o.trial_id "
                "WHERE e.economic_freeze_epoch=? AND e.authority_id=? AND t.trigger_entity=? AND c.trigger_role=? "
                "AND c.lane=? AND t.venue=? AND t.lifecycle=? AND c.risk_signature=? AND c.context_key<>? ORDER BY o.id",
                (ECONOMIC_FREEZE_EPOCH, AUTHORITY_ID, entity, role, lane, venue, lifecycle, risk_signature, key),
            ).fetchall())
    exact = _dedup(exact_rows, "trial_id")
    exact_ids = {str(row["trial_id"]) for row in exact}
    parent = [row for row in _dedup(parent_rows, "trial_id") if str(row["trial_id"]) not in exact_ids]
    # Severity is not part of this method's historical signature. Use clean only
    # for clean signatures; hazardous Robinhood callers enforce their exact
    # severity in _v5_choose_lane_fraction after this profile is returned.
    severity = 0.0 if risk_signature == "clean" else 0.45
    hp = hierarchical_profile(
        [float(row["net_return"]) for row in exact],
        [float(row["net_return"]) for row in parent],
        (),
        risk_severity=severity,
        risk_signature=risk_signature,
        max_fraction=0.10,
    )
    if bool(hp.get("promoted")):
        legacy_state = "promoted_positive_log_growth"
    elif bool(hp.get("killed")):
        legacy_state = "demoted_nonpositive_log_growth"
    else:
        legacy_state = "bootstrap_forward_evidence"
    return {
        "sample_count": hp["exact_sample_count"],
        "state": legacy_state,
        "best_fraction": hp["best_fraction"],
        "best_expected_log_growth": hp["best_expected_log_growth"],
        "mean_return": hp["mean_return"],
        "median_return": hp["median_return"],
        "hit_rate": hp["hit_rate"],
        "trimmed_mean_ex_best": hp["leave_best_trade_out_mean"],
        "expected_shortfall_20": hp["expected_shortfall_20"],
        "winner_concentration": hp["winner_concentration"],
        "max_drawdown": hp["max_drawdown_at_best_fraction"],
        "evidence_source": "v51_frozen_epoch_exact_plus_same_entity_parent_shrinkage",
        "hierarchical_profile": hp,
        "hit_rate_is_promotion_veto": False,
    }


def install_v51_consolidated_strategy(*, store: Any | None = None, release_commit: str | None = None) -> None:
    global _INSTALLED, _ORIGINAL_SOLANA_CHOOSE, _ORIGINAL_FOMO_DECISION, _ORIGINAL_RH_PROFILE
    if _INSTALLED:
        if store is not None:
            _ensure_epoch(store, release_commit)
        return
    _ORIGINAL_SOLANA_CHOOSE = v5._choose_lane_and_fraction
    _ORIGINAL_FOMO_DECISION = fomo_paper._paper_decision
    _ORIGINAL_RH_PROFILE = RobinhoodProfitMaximizerMixin._v5_profile
    v5._choose_lane_and_fraction = _solana_choose_consolidated
    v51._fomo_context_returns_v51 = _fomo_epoch_returns
    fomo_paper._paper_decision = _fomo_decision_consolidated
    RobinhoodProfitMaximizerMixin._v5_profile = _rh_epoch_profile
    if store is not None:
        _ensure_epoch(store, release_commit)
    _INSTALLED = True


def status(store: Any | None = None, release_commit: str | None = None) -> dict[str, Any]:
    if store is not None:
        _ensure_epoch(store, release_commit)
    return {
        "consolidation_version": CONSOLIDATION_VERSION,
        "installed": _INSTALLED,
        "authority_id": AUTHORITY_ID,
        "authority_fingerprint": authority_fingerprint(),
        "economic_freeze_epoch": ECONOMIC_FREEZE_EPOCH,
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
        "economic_authority_surface": "single_final_strategy_boundary_over_legacy_transport_repairs",
        "pre_epoch_evidence_promotion_authority": False,
    }


__all__ = ["CONSOLIDATION_VERSION", "install_v51_consolidated_strategy", "status"]
