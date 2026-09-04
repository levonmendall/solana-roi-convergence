from __future__ import annotations

import json
import math
from collections import defaultdict
from typing import Any, Callable

from . import risk_conditioned_alpha_v5 as risk_v5
from . import strategy_specialist_wallet_allocator as allocator
from .strategy_specialist_wallet_allocator_repair import (
    install_strategy_specialist_wallet_allocator_repair,
)
from .wallet_context_router import WalletContextRouter
from .wallet_entity_universe_v4 import WalletEntityUniverseV4


AUTHORITY_VERSION = "regime-roi-wallet-authority-v2"
CANONICAL_REGIMES = (
    "weak_or_deteriorating",
    "neutral",
    "high_speculation",
    "broad_mania",
)
ACTIVE_SOLANA_STRATEGIES = frozenset(
    {
        "elite_wallet_continuation",
        "creator_insider_continuation",
        "entity_flow_momentum",
        "graduation_continuation",
        "raydium_cross_venue_persistence",
        "hazard_continuation",
        "fomo_continuation",
    }
)
TOP_STRATEGY_WALLETS = 3
CHALLENGER_FRACTION_CAP = 0.005
UNPROVEN_CONTEXT_FRACTION_CAP = 0.01
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
CROSS_CONTEXT_SUCCESS_TRANSFER_ALLOWED = False
HISTORICAL_PROMOTION_AUTHORITY = False

_ORIGINAL_SELECT: Callable[..., list[str]] | None = None
_ORIGINAL_BUILD: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_ROUTER_STATUS: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_MANIFEST: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_SOLANA_CHOOSE: Callable[..., tuple[str | None, float, dict[str, Any]]] | None = None
_ORIGINAL_FOMO_DECISION: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_ROBINHOOD_CHOOSE: Callable[..., tuple[str | None, float, dict[str, Any]]] | None = None
_INSTALLED = False


def _safe_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _roi_pct(row: dict[str, Any]) -> float | None:
    for pct_key in (
        "trimmed_mean_residual_roi_ex_best_1_pct",
        "copyable_return_on_deployed_fraction_pct",
        "mean_residual_roi_pct",
    ):
        value = _safe_float(row.get(pct_key))
        if value is not None:
            return value
    for fraction_key in (
        "trimmed_mean_residual_roi_ex_best_1",
        "copyable_return_on_deployed_fraction",
        "mean_return",
    ):
        value = _safe_float(row.get(fraction_key))
        if value is not None:
            return value * 100.0
    return None


def _copyable_pct(row: dict[str, Any]) -> float | None:
    value = _safe_float(row.get("copyable_return_on_deployed_fraction_pct"))
    if value is not None:
        return value
    value = _safe_float(row.get("copyable_return_on_deployed_fraction"))
    return value * 100.0 if value is not None else None


def _profit_rank(row: dict[str, Any]) -> tuple[float, float, float, int]:
    """Rank by robust percentage ROI first, then compounded profitability."""
    roi = _roi_pct(row)
    growth = _safe_float(row.get("best_expected_log_growth"))
    copyable = _copyable_pct(row)
    return (
        roi if roi is not None else float("-inf"),
        growth if growth is not None else float("-inf"),
        copyable if copyable is not None else float("-inf"),
        int(row.get("sample_count") or 0),
    )


def _eligible(row: dict[str, Any]) -> bool:
    roi = _roi_pct(row)
    return bool(
        allocator._profile_positive(row)
        and row.get("mature_forward_context")
        and roi is not None
        and roi > 0.0
    )


def _exact_key(row: dict[str, Any]) -> tuple[str, str, str, str, str, str]:
    return (
        str(row.get("strategy_family") or "unknown_strategy"),
        str(row.get("venue") or "UNKNOWN"),
        str(row.get("lifecycle_stage") or "unknown"),
        str(row.get("regime") or "unknown"),
        str(row.get("role") or "unknown"),
        str(row.get("risk_signature") or "clean"),
    )


def _leader_payload(row: dict[str, Any], rank: int) -> dict[str, Any]:
    return {
        "rank": rank,
        "wallet": str(row.get("wallet") or ""),
        "roi_pct": _roi_pct(row),
        "best_expected_log_growth": _safe_float(row.get("best_expected_log_growth")),
        "copyable_roi_pct": _copyable_pct(row),
        "sample_count": int(row.get("sample_count") or 0),
        "venue": str(row.get("venue") or "UNKNOWN"),
        "lifecycle_stage": str(row.get("lifecycle_stage") or "unknown"),
        "role": str(row.get("role") or "unknown"),
        "risk_signature": str(row.get("risk_signature") or "clean"),
        "risk_class": str(row.get("risk_class") or "clean_or_unspecified"),
        "source_kind": str(row.get("source_kind") or "unknown"),
        "paper_strategy_use_eligible": True,
        "assignment_alone_authorizes_entry": False,
    }


def build_regime_wallet_matrix(
    rows: list[dict[str, Any]], *, top_n: int = TOP_STRATEGY_WALLETS
) -> dict[str, Any]:
    """Build exact-context and strategy/regime leader sets from forward ROI percent."""
    top_n = max(1, int(top_n))
    eligible = [dict(row) for row in rows if _eligible(row)]
    strategies = sorted(
        {
            str(row.get("strategy_family") or "unknown_strategy")
            for row in rows
            if str(row.get("strategy_family") or "")
        }
    )

    exact_groups: dict[tuple[str, str, str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    regime_groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in eligible:
        exact_groups[_exact_key(row)].append(row)
        regime_groups[(str(row.get("strategy_family") or "unknown_strategy"), str(row.get("regime") or "unknown"))].append(row)

    exact_contexts: list[dict[str, Any]] = []
    exact_lookup: dict[tuple[str, str, str, str, str, str], list[dict[str, Any]]] = {}
    for key, group in sorted(exact_groups.items()):
        best_by_wallet: dict[str, dict[str, Any]] = {}
        for row in group:
            wallet = str(row.get("wallet") or "")
            current = best_by_wallet.get(wallet)
            if current is None or _profit_rank(row) > _profit_rank(current):
                best_by_wallet[wallet] = row
        ordered = sorted(best_by_wallet.values(), key=_profit_rank, reverse=True)[:top_n]
        leaders = [_leader_payload(row, index + 1) for index, row in enumerate(ordered)]
        exact_lookup[key] = leaders
        exact_contexts.append(
            {
                "strategy_family": key[0],
                "venue": key[1],
                "lifecycle_stage": key[2],
                "regime": key[3],
                "role": key[4],
                "risk_signature": key[5],
                "leaders": leaders,
                "state": "proven_profitable_leaders" if leaders else "no_proven_profitable_wallet",
                "cross_context_success_transfer_allowed": False,
            }
        )

    regimes: list[dict[str, Any]] = []
    for regime in CANONICAL_REGIMES:
        strategy_rows: list[dict[str, Any]] = []
        for strategy in strategies:
            group = regime_groups.get((strategy, regime), [])
            best_by_wallet: dict[str, dict[str, Any]] = {}
            for row in group:
                wallet = str(row.get("wallet") or "")
                current = best_by_wallet.get(wallet)
                if current is None or _profit_rank(row) > _profit_rank(current):
                    best_by_wallet[wallet] = row
            ordered = sorted(best_by_wallet.values(), key=_profit_rank, reverse=True)[:top_n]
            strategy_rows.append(
                {
                    "strategy_family": strategy,
                    "leaders": [_leader_payload(row, index + 1) for index, row in enumerate(ordered)],
                    "state": "proven_profitable_leaders" if ordered else "no_proven_profitable_wallet_yet",
                }
            )
        regimes.append({"regime": regime, "strategies": strategy_rows})

    return {
        "authority_version": AUTHORITY_VERSION,
        "ranking_objective": "robust_forward_roi_pct_then_expected_log_growth_after_costs",
        "dollar_profit_used_for_ranking": False,
        "top_wallets_per_context": top_n,
        "canonical_regimes": list(CANONICAL_REGIMES),
        "regimes": regimes,
        "exact_contexts": exact_contexts,
        "exact_lookup": exact_lookup,
        "forward_only": True,
        "historical_promotion_authority": False,
        "cross_context_success_transfer_allowed": False,
        "paper_only": True,
        "live_money_authority": False,
    }


def _row_assignment(row: dict[str, Any], source: str, *, rank: int | None = None) -> dict[str, Any]:
    return {
        "wallet": str(row.get("wallet") or ""),
        "strategy_family": str(row.get("strategy_family") or "unknown_strategy"),
        "venue": str(row.get("venue") or "UNKNOWN"),
        "lifecycle_stage": str(row.get("lifecycle_stage") or "unknown"),
        "regime": str(row.get("regime") or "unknown"),
        "role": str(row.get("role") or "unknown"),
        "risk_signature": str(row.get("risk_signature") or "clean"),
        "risk_class": str(row.get("risk_class") or "clean_or_unspecified"),
        "roi_pct": _roi_pct(row),
        "best_expected_log_growth": _safe_float(row.get("best_expected_log_growth")),
        "copyable_roi_pct": _copyable_pct(row),
        "sample_count": int(row.get("sample_count") or 0),
        "leader_rank": rank,
        "assignment_source": source,
        "observation_only": False,
        "paper_strategy_use_eligible": True,
        "assignment_alone_authorizes_entry": False,
        "current_live_money_authority": False,
        "cross_context_success_transfer_allowed": False,
    }


def build_regime_roi_tracking_plan(
    rows: list[dict[str, Any]],
    *,
    capacity: int,
    active_regime: str | None,
    candidate_states: dict[str, str] | None = None,
    fallback_wallets: list[str] | tuple[str, ...] = (),
) -> dict[str, Any]:
    """Track active-regime ROI leaders first, then preserve the repaired global plan."""
    if _ORIGINAL_BUILD is None:
        raise RuntimeError("regime ROI wallet authority is not installed")
    capacity = max(0, int(capacity))
    states = candidate_states or {}
    states_provided = candidate_states is not None
    active = str(active_regime or "unknown")
    usable = [
        dict(row)
        for row in rows
        if str(row.get("wallet") or "")
        and (not states_provided or states.get(str(row.get("wallet") or "")) == "tracking")
    ]
    positive_active = [
        row for row in usable
        if _eligible(row)
        and str(row.get("regime") or "unknown") == active
        and str(row.get("strategy_family") or "") in ACTIVE_SOLANA_STRATEGIES
    ]

    by_strategy: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in positive_active:
        by_strategy[str(row.get("strategy_family") or "unknown_strategy")].append(row)

    primary: list[dict[str, Any]] = []
    backups: list[dict[str, Any]] = []
    for group in by_strategy.values():
        best_by_wallet: dict[str, dict[str, Any]] = {}
        for row in group:
            wallet = str(row.get("wallet") or "")
            current = best_by_wallet.get(wallet)
            if current is None or _profit_rank(row) > _profit_rank(current):
                best_by_wallet[wallet] = row
        ordered = sorted(best_by_wallet.values(), key=_profit_rank, reverse=True)
        if ordered:
            primary.append(ordered[0])
            backups.extend(ordered[1:TOP_STRATEGY_WALLETS])

    for risk_class in ("clean_fomo", "hazard_fomo"):
        group = [
            row for row in positive_active
            if row.get("strategy_family") == "fomo_continuation" and row.get("risk_class") == risk_class
        ]
        if group:
            group.sort(key=_profit_rank, reverse=True)
            primary.append(group[0])
            backups.extend(group[1:TOP_STRATEGY_WALLETS])

    primary.sort(key=_profit_rank, reverse=True)
    backups.sort(key=_profit_rank, reverse=True)
    selected: list[str] = []
    selected_set: set[str] = set()
    assignments: list[dict[str, Any]] = []
    required_primary: list[tuple[str, str]] = []
    active_debt: list[dict[str, str]] = []

    for row in primary:
        key = (str(row.get("strategy_family") or "unknown_strategy"), active)
        if key not in required_primary:
            required_primary.append(key)
        wallet = str(row.get("wallet") or "")
        if wallet in selected_set:
            continue
        if len(selected) >= capacity:
            active_debt.append({"strategy_family": key[0], "regime": active, "reason": "active_regime_tracking_capacity_exhausted"})
            continue
        selected.append(wallet)
        selected_set.add(wallet)
        assignments.append(_row_assignment(row, "active_regime_roi_leader", rank=1))

    for row in backups:
        if len(selected) >= capacity:
            break
        wallet = str(row.get("wallet") or "")
        if wallet in selected_set:
            continue
        selected.append(wallet)
        selected_set.add(wallet)
        assignments.append(_row_assignment(row, "active_regime_roi_backup"))

    remaining_capacity = max(0, capacity - len(selected))
    bootstrap: list[str] = []
    base_debt: list[dict[str, Any]] = []
    if remaining_capacity:
        remainder = [row for row in usable if str(row.get("wallet") or "") not in selected_set]
        remaining_fallback = [wallet for wallet in fallback_wallets if str(wallet) not in selected_set]
        base = _ORIGINAL_BUILD(
            remainder,
            capacity=remaining_capacity,
            candidate_states=candidate_states,
            fallback_wallets=remaining_fallback,
        )
        for wallet in base.get("selected_challenger_wallets") or []:
            if wallet not in selected_set and len(selected) < capacity:
                selected.append(wallet)
                selected_set.add(wallet)
        assignments.extend(base.get("context_assignments") or [])
        bootstrap = [wallet for wallet in (base.get("bootstrap_observation_wallets") or []) if wallet in selected_set]
        base_debt = list(base.get("coverage_debt") or [])

    return {
        "allocation_version": AUTHORITY_VERSION,
        "capacity": capacity,
        "active_regime": active,
        "selected_challenger_wallets": selected,
        "context_assignments": assignments,
        "bootstrap_observation_wallets": bootstrap,
        "active_regime_primary_strategy_count": len(required_primary),
        "active_regime_primary_coverage_debt": active_debt,
        "coverage_debt": active_debt + base_debt,
        "active_regime_roi_leaders_first": True,
        "strategy_family_first_pass_enabled": True,
        "strategy_regime_specialist_floor_enabled": True,
        "ranking_objective": "robust_forward_roi_pct_then_expected_log_growth_after_costs",
        "dollar_profit_used_for_ranking": False,
        "top_wallets_per_context": TOP_STRATEGY_WALLETS,
        "remaining_capacity_filled_by_global_forward_roi": True,
        "fomo_clean_hazard_separated": True,
        "high_risk_alpha_observation_cohort_enabled": True,
        "mechanical_hard_stops_relaxed": False,
        "cross_context_success_transfer_allowed": False,
        "paper_only": True,
        "live_money_authority": False,
    }


def _latest_regime(store: Any) -> str:
    queries = (
        ("risk_conditioned_alpha_v5_trials", "SELECT regime FROM risk_conditioned_alpha_v5_trials ORDER BY id DESC LIMIT 1"),
        ("fomo_shadow_observations", "SELECT regime FROM fomo_shadow_observations ORDER BY id DESC LIMIT 1"),
    )
    for table, sql in queries:
        if not allocator._table_exists(store, table):
            continue
        try:
            with store._lock:
                row = store.db.execute(sql).fetchone()
            value = str(row["regime"] or "") if row is not None else ""
            if value in CANONICAL_REGIMES:
                return value
        except Exception:
            continue
    return "unknown"


def _all_rows(universe: WalletEntityUniverseV4) -> list[dict[str, Any]]:
    profiles = WalletContextRouter(universe).context_profiles()
    rows = allocator._base_specialist_rows(profiles)
    rows.extend(allocator._v5_specialist_rows(universe))
    rows.extend(allocator._fomo_specialist_rows(universe))
    return rows


def _active_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [row for row in rows if str(row.get("strategy_family") or "") in ACTIVE_SOLANA_STRATEGIES]


def _regime_select(self: WalletEntityUniverseV4) -> list[str]:
    if _ORIGINAL_SELECT is None:
        raise RuntimeError("regime ROI select wrapper is not installed")
    fallback = list(_ORIGINAL_SELECT(self))
    states = allocator._candidate_states(self)
    if not states:
        return fallback
    incumbents = [wallet for wallet, state in states.items() if state == "incumbent_tracking"]
    try:
        plan = build_regime_roi_tracking_plan(
            _all_rows(self),
            capacity=max(1, int(self.discovery.policy.max_tracked_challengers)),
            active_regime=_latest_regime(self.store),
            candidate_states=states,
            fallback_wallets=[wallet for wallet, state in states.items() if state == "tracking"],
        )
        setattr(self, "_roi_strategy_specialist_tracking_plan", plan)
        setattr(self, "_roi_regime_wallet_authority_last_error", None)
        return list(dict.fromkeys((*incumbents, *plan["selected_challenger_wallets"])))
    except Exception as exc:
        setattr(self, "_roi_regime_wallet_authority_last_error", f"{type(exc).__name__}: {exc}")
        return fallback


def _matrix_from_store(store: Any) -> dict[str, Any]:
    class Holder:
        pass
    holder = Holder()
    holder.store = store
    rows = allocator._v5_specialist_rows(holder)  # type: ignore[arg-type]
    rows.extend(allocator._fomo_specialist_rows(holder))  # type: ignore[arg-type]
    return build_regime_wallet_matrix(_active_rows(rows))


def _exact_assignment(
    matrix: dict[str, Any], *, strategy: str, venue: str, lifecycle: str, regime: str,
    role: str, risk_signature: str, wallet: str,
) -> dict[str, Any]:
    key = (strategy, venue, lifecycle, regime, role, risk_signature)
    leaders = list((matrix.get("exact_lookup") or {}).get(key) or [])
    for leader in leaders:
        if str(leader.get("wallet") or "") == wallet:
            return {
                "state": "assigned_regime_roi_leader",
                "assigned": True,
                "leader_rank": int(leader.get("rank") or 0),
                "leader_count": len(leaders),
                "leaders": leaders,
                "ranking_objective": matrix.get("ranking_objective"),
            }
    if leaders:
        return {
            "state": "regime_roi_challenger",
            "assigned": False,
            "leader_rank": None,
            "leader_count": len(leaders),
            "leaders": leaders,
            "ranking_objective": matrix.get("ranking_objective"),
        }
    return {
        "state": "no_proven_profitable_exact_context_wallet_yet",
        "assigned": False,
        "leader_rank": None,
        "leader_count": 0,
        "leaders": [],
        "ranking_objective": matrix.get("ranking_objective"),
    }


def apply_strategy_use_fraction(fraction: float, assignment: dict[str, Any]) -> float:
    fraction = max(0.0, float(fraction))
    state = str(assignment.get("state") or "")
    if state == "assigned_regime_roi_leader":
        rank = int(assignment.get("leader_rank") or 1)
        multiplier = {1: 1.0, 2: 0.85, 3: 0.70}.get(rank, 0.60)
        return fraction * multiplier
    if state == "regime_roi_challenger":
        return min(fraction, CHALLENGER_FRACTION_CAP)
    return min(fraction, UNPROVEN_CONTEXT_FRACTION_CAP)


def _solana_assignment(adapter: Any, pre: dict[str, Any], lane: str) -> dict[str, Any]:
    matrix = _matrix_from_store(adapter.store)
    return _exact_assignment(
        matrix,
        strategy=lane,
        venue=str(pre.get("venue") or "UNKNOWN"),
        lifecycle=str(pre.get("lifecycle") or "unknown"),
        regime=str(pre.get("regime") or "unknown"),
        role=str(pre.get("role") or "unknown"),
        risk_signature=str((pre.get("risk") or {}).get("risk_signature") or "clean"),
        wallet=str(pre.get("wallet") or ""),
    )


def _choose_with_regime_wallets(adapter: Any, pre: dict[str, Any], *, chase: float | None = None, latency: float | None = None) -> tuple[str | None, float, dict[str, Any]]:
    if _ORIGINAL_SOLANA_CHOOSE is None:
        raise RuntimeError("Solana regime wallet wrapper is not installed")
    lane, fraction, profiles = _ORIGINAL_SOLANA_CHOOSE(adapter, pre, chase=chase, latency=latency)
    if lane is None or fraction <= 0.0:
        return lane, fraction, profiles
    assignment = _solana_assignment(adapter, pre, lane)
    profiles = dict(profiles)
    profiles["_regime_roi_wallet_authority"] = assignment
    return lane, apply_strategy_use_fraction(fraction, assignment), profiles


def _fomo_decision_with_regime_wallets(adapter: Any, *, observation: dict[str, Any], trial: dict[str, Any]) -> dict[str, Any]:
    if _ORIGINAL_FOMO_DECISION is None:
        raise RuntimeError("FOMO regime wallet wrapper is not installed")
    result = dict(_ORIGINAL_FOMO_DECISION(adapter, observation=observation, trial=trial))
    if not str(result.get("decision") or "").startswith("paper_enter"):
        return result
    try:
        state_payload = json.loads(str(observation.get("state_json") or "{}"))
    except Exception:
        state_payload = {}
    variants = {str(value) for value in (state_payload.get("experiment_variants") or ())}
    risk_class = "hazard_fomo" if "hazard_fomo" in variants else "clean_fomo"
    matrix = _matrix_from_store(adapter.store)
    assignment = _exact_assignment(
        matrix,
        strategy="fomo_continuation",
        venue=str(observation.get("venue") or "UNKNOWN"),
        lifecycle=str(observation.get("lifecycle") or "unknown"),
        regime=str(observation.get("regime") or trial.get("regime") or "unknown"),
        role="fomo_trigger",
        risk_signature=risk_class,
        wallet=str(trial.get("trigger_wallet") or ""),
    )
    result["position_fraction"] = apply_strategy_use_fraction(float(result.get("position_fraction") or 0.0), assignment)
    profile = dict(result.get("profile") or {})
    profile["regime_roi_wallet_authority"] = assignment
    result["profile"] = profile
    if assignment["state"] == "regime_roi_challenger":
        result["reason"] = f"{result.get('reason') or 'fomo'}|regime_roi_challenger_probe"
    return result


def _robinhood_rows(plane: Any) -> list[dict[str, Any]]:
    try:
        with plane.store._lock:
            rows = plane.store.db.execute(
                "SELECT t.trigger_entity,c.lane,t.venue,t.lifecycle,c.regime,c.trigger_role,c.risk_signature,c.flow_state,o.net_return "
                "FROM robinhood_paper_outcomes o JOIN robinhood_paper_trials t ON t.id=o.trial_id "
                "JOIN robinhood_v5_trial_context c ON c.trial_id=o.trial_id "
                "WHERE o.release_commit=? ORDER BY o.id",
                (plane.release_commit,),
            ).fetchall()
    except Exception:
        return []
    grouped: dict[tuple[str, str, str, str, str, str, str, str], list[float]] = defaultdict(list)
    for row in rows:
        key = (
            str(row["trigger_entity"] or ""), str(row["lane"] or ""), str(row["venue"] or ""),
            str(row["lifecycle"] or ""), str(row["regime"] or "unknown"), str(row["trigger_role"] or "unknown"),
            str(row["risk_signature"] or "clean"), str(row["flow_state"] or "unknown"),
        )
        value = _safe_float(row["net_return"])
        if key[0] and value is not None:
            grouped[key].append(value)
    try:
        from .robinhood_chain_profit_maximizer import ROBINHOOD_V5_MIN_SAMPLES, ROBINHOOD_V5_POSITION_GRID, ROBINHOOD_V5_MAX_POSITION
    except Exception:
        return []
    result: list[dict[str, Any]] = []
    for key, values in grouped.items():
        profile = risk_v5.robust_return_profile(values, grid=ROBINHOOD_V5_POSITION_GRID, max_fraction=ROBINHOOD_V5_MAX_POSITION, min_samples=ROBINHOOD_V5_MIN_SAMPLES)
        result.append({
            "wallet": key[0], "strategy_family": key[1], "venue": key[2], "lifecycle_stage": key[3],
            "regime": key[4], "role": key[5], "risk_signature": key[6], "risk_class": "clean" if key[6] == "clean" else "hazard",
            "flow_state": key[7], "source_kind": "robinhood_chain_v5_forward", "sample_count": profile.sample_count,
            "mature_forward_context": profile.sample_count >= ROBINHOOD_V5_MIN_SAMPLES,
            "specialist_positive": profile.state == "promoted_positive_log_growth",
            "best_expected_log_growth": profile.best_expected_log_growth,
            "trimmed_mean_residual_roi_ex_best_1": profile.trimmed_mean_ex_best,
            "mean_return": profile.mean_return,
        })
    return result


def robinhood_regime_entity_authority_status(plane: Any) -> dict[str, Any]:
    matrix = build_regime_wallet_matrix(_robinhood_rows(plane))
    return {
        "authority_version": AUTHORITY_VERSION,
        "ranking_objective": matrix["ranking_objective"],
        "dollar_profit_used_for_ranking": False,
        "canonical_regimes": matrix["canonical_regimes"],
        "regimes": matrix["regimes"],
        "exact_contexts": matrix["exact_contexts"][:100],
        "entity_pool_isolated_from_solana_wallets": True,
        "nonleader_probe_cap": CHALLENGER_FRACTION_CAP,
        "paper_only": True,
        "live_money_authority": False,
    }


def _robinhood_choose_with_regime_entities(self: Any, **kwargs: Any) -> tuple[str | None, float, dict[str, Any]]:
    if _ORIGINAL_ROBINHOOD_CHOOSE is None:
        raise RuntimeError("Robinhood regime entity wrapper is not installed")
    lane, fraction, profiles = _ORIGINAL_ROBINHOOD_CHOOSE(self, **kwargs)
    if lane is None or fraction <= 0.0:
        return lane, fraction, profiles
    matrix = build_regime_wallet_matrix(_robinhood_rows(self))
    assignment = _exact_assignment(
        matrix,
        strategy=lane,
        venue=str(kwargs.get("venue") or "UNKNOWN"),
        lifecycle=str(kwargs.get("lifecycle") or "unknown"),
        regime=str(kwargs.get("regime") or "unknown"),
        role=str(kwargs.get("role") or "unknown"),
        risk_signature=str(kwargs.get("risk_signature") or "clean"),
        wallet=str(kwargs.get("entity") or ""),
    )
    profiles = dict(profiles)
    profiles["_regime_roi_entity_authority"] = assignment
    return lane, apply_strategy_use_fraction(fraction, assignment), profiles


def _status_with_regime_wallets(self: WalletContextRouter) -> dict[str, Any]:
    if _ORIGINAL_ROUTER_STATUS is None:
        raise RuntimeError("regime ROI status wrapper is not installed")
    payload = _ORIGINAL_ROUTER_STATUS(self)
    universe = getattr(self, "universe", None)
    if isinstance(universe, WalletEntityUniverseV4):
        try:
            matrix = build_regime_wallet_matrix(_active_rows(_all_rows(universe)))
            payload["regime_roi_wallet_authority"] = {
                "authority_version": AUTHORITY_VERSION,
                "active_regime": _latest_regime(universe.store),
                "ranking_objective": matrix["ranking_objective"],
                "dollar_profit_used_for_ranking": False,
                "canonical_regimes": matrix["canonical_regimes"],
                "regimes": matrix["regimes"],
                "exact_contexts": matrix["exact_contexts"][:100],
                "strategy_use_policy": "ranked_leaders_normal_paper_sizing_nonleaders_probe_capped",
                "challenger_fraction_cap": CHALLENGER_FRACTION_CAP,
                "unproven_context_fraction_cap": UNPROVEN_CONTEXT_FRACTION_CAP,
                "cross_context_success_transfer_allowed": False,
                "last_error": getattr(universe, "_roi_regime_wallet_authority_last_error", None),
                "paper_only": True,
                "live_money_authority": False,
            }
        except Exception as exc:
            payload["regime_roi_wallet_authority"] = {
                "authority_version": AUTHORITY_VERSION,
                "failed_closed": True,
                "error": f"{type(exc).__name__}: regime wallet status unavailable",
                "paper_only": True,
                "live_money_authority": False,
            }
    return payload


def _manifest_with_regime_wallets(self: Any) -> dict[str, Any]:
    if _ORIGINAL_MANIFEST is None:
        raise RuntimeError("regime ROI manifest wrapper is not installed")
    payload = _ORIGINAL_MANIFEST(self)
    payload.update({
        "regime_roi_wallet_authority": AUTHORITY_VERSION,
        "wallet_ranking_objective": "robust_forward_roi_pct_then_expected_log_growth_after_costs",
        "wallet_dollar_profit_used_for_ranking": False,
        "wallet_strategy_use_requires_exact_context_leader_or_bounded_challenger_probe": True,
        "wallet_top_strategy_leaders_per_context": TOP_STRATEGY_WALLETS,
        "wallet_active_regime_leaders_receive_tracking_priority": True,
        "wallet_cross_context_success_transfer_allowed": False,
        "historical_evidence_promotion_authority": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    })
    return payload


def install_regime_roi_wallet_authority() -> None:
    global _INSTALLED, _ORIGINAL_SELECT, _ORIGINAL_BUILD, _ORIGINAL_ROUTER_STATUS, _ORIGINAL_MANIFEST
    global _ORIGINAL_SOLANA_CHOOSE, _ORIGINAL_FOMO_DECISION, _ORIGINAL_ROBINHOOD_CHOOSE
    if _INSTALLED:
        return

    allocator.install_strategy_specialist_wallet_allocator()
    install_strategy_specialist_wallet_allocator_repair()
    _ORIGINAL_BUILD = allocator.build_strategy_specialist_tracking_plan

    current_select = WalletEntityUniverseV4.select_tracked_wallets
    _ORIGINAL_SELECT = current_select
    _regime_select.__dict__.update(getattr(current_select, "__dict__", {}))
    setattr(_regime_select, "_roi_regime_roi_wallet_authority", True)
    WalletEntityUniverseV4.select_tracked_wallets = _regime_select  # type: ignore[method-assign]

    current_status = WalletContextRouter.status
    _ORIGINAL_ROUTER_STATUS = current_status
    _status_with_regime_wallets.__dict__.update(getattr(current_status, "__dict__", {}))
    setattr(_status_with_regime_wallets, "_roi_regime_roi_wallet_authority", True)
    WalletContextRouter.status = _status_with_regime_wallets  # type: ignore[method-assign]

    from .profit_first_entity_final_research import FinalProfitFirstResearchAdapter
    current_manifest = FinalProfitFirstResearchAdapter._manifest
    _ORIGINAL_MANIFEST = current_manifest
    _manifest_with_regime_wallets.__dict__.update(getattr(current_manifest, "__dict__", {}))
    setattr(_manifest_with_regime_wallets, "_roi_regime_roi_wallet_authority", True)
    FinalProfitFirstResearchAdapter._manifest = _manifest_with_regime_wallets  # type: ignore[method-assign]

    _ORIGINAL_SOLANA_CHOOSE = risk_v5._choose_lane_and_fraction
    risk_v5._choose_lane_and_fraction = _choose_with_regime_wallets

    from . import fomo_paper_strategy as fomo_paper
    _ORIGINAL_FOMO_DECISION = fomo_paper._paper_decision
    fomo_paper._paper_decision = _fomo_decision_with_regime_wallets

    from .robinhood_chain_profit_maximizer import RobinhoodProfitMaximizerMixin
    _ORIGINAL_ROBINHOOD_CHOOSE = RobinhoodProfitMaximizerMixin._v5_choose_lane_fraction
    RobinhoodProfitMaximizerMixin._v5_choose_lane_fraction = _robinhood_choose_with_regime_entities  # type: ignore[method-assign]

    _INSTALLED = True


__all__ = [
    "AUTHORITY_VERSION",
    "CANONICAL_REGIMES",
    "ACTIVE_SOLANA_STRATEGIES",
    "TOP_STRATEGY_WALLETS",
    "build_regime_wallet_matrix",
    "build_regime_roi_tracking_plan",
    "apply_strategy_use_fraction",
    "robinhood_regime_entity_authority_status",
    "install_regime_roi_wallet_authority",
]
