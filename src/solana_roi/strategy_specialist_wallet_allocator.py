from __future__ import annotations

import json
import math
from collections import defaultdict
from typing import Any, Callable

from .profit_first_entity_final_research import FinalProfitFirstResearchAdapter
from .wallet_context_router import WalletContextRouter
from .wallet_entity_universe_v4 import MIN_MATURE_FORWARD_SAMPLES, WalletEntityUniverseV4


ALLOCATION_VERSION = "strategy-specialist-wallet-allocation-v1"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
ACTIVE_STRATEGY_MUTATION_ALLOWED = False
HISTORICAL_PROMOTION_AUTHORITY = False
CROSS_CONTEXT_SUCCESS_TRANSFER_ALLOWED = False
ROBINHOOD_WALLET_POOL_MODIFIED = False

_ORIGINAL_SELECT: Callable[..., list[str]] | None = None
_ORIGINAL_ROUTER_STATUS: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_MANIFEST: Callable[..., dict[str, Any]] | None = None


_ROLE_STRATEGIES: dict[str, tuple[str, ...]] = {
    "scout_alpha": ("elite_wallet_continuation",),
    "momentum_alpha": ("elite_wallet_continuation",),
    "confirmation_alpha": ("entity_flow_momentum",),
    "creator_alpha": ("creator_insider_continuation",),
    "distribution_warning_value": ("hazard_continuation",),
    "copyable_return_on_capital": ("elite_wallet_continuation",),
    "signal_decay": ("fomo_signal_decay_research",),
    "exit_alpha": ("exit_specialist_research",),
}


def _safe_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _table_exists(store: Any, name: str) -> bool:
    try:
        with store._lock:
            row = store.db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                (name,),
            ).fetchone()
        return row is not None
    except Exception:
        return False


def _candidate_states(universe: WalletEntityUniverseV4) -> dict[str, str]:
    try:
        with universe.store._lock:
            rows = universe.store.db.execute(
                "SELECT wallet,state FROM wallet_discovery_candidates"
            ).fetchall()
        return {str(row["wallet"]): str(row["state"]) for row in rows}
    except Exception:
        return {}


def _profile_positive(row: dict[str, Any]) -> bool:
    if "specialist_positive" in row:
        return bool(row.get("specialist_positive"))
    if not bool(row.get("mature_forward_context")):
        return False
    score = _safe_float(row.get("context_score"))
    trimmed = _safe_float(row.get("trimmed_mean_residual_roi_ex_best_1"))
    return bool(score is not None and score > 0.0 and trimmed is not None and trimmed > 0.0)


def _row_rank(row: dict[str, Any]) -> tuple[float, float, float, int]:
    growth = _safe_float(row.get("best_expected_log_growth"))
    trimmed = _safe_float(row.get("trimmed_mean_residual_roi_ex_best_1"))
    if trimmed is None:
        trimmed_pct = _safe_float(row.get("trimmed_mean_residual_roi_ex_best_1_pct"))
        trimmed = trimmed_pct / 100.0 if trimmed_pct is not None else None
    copyable = _safe_float(row.get("copyable_return_on_deployed_fraction"))
    if copyable is None:
        copyable_pct = _safe_float(row.get("copyable_return_on_deployed_fraction_pct"))
        copyable = copyable_pct / 100.0 if copyable_pct is not None else None
    score = _safe_float(row.get("context_score"))
    primary = growth if growth is not None else (trimmed if trimmed is not None else score)
    return (
        primary if primary is not None else float("-inf"),
        copyable if copyable is not None else float("-inf"),
        score if score is not None else float("-inf"),
        int(row.get("sample_count") or 0),
    )


def _strategy_regime_key(row: dict[str, Any]) -> tuple[str, str]:
    return (
        str(row.get("strategy_family") or "unknown_strategy"),
        str(row.get("regime") or "unknown"),
    )


def _base_specialist_rows(profiles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for source in profiles:
        wallet = str(source.get("wallet") or "")
        if not wallet:
            continue
        role = str(source.get("role") or "unknown")
        strategies = list(_ROLE_STRATEGIES.get(role, ()))
        venue = str(source.get("venue") or "UNKNOWN")
        lifecycle = str(source.get("lifecycle_stage") or "unknown")
        if venue == "PUMP_AMM" and lifecycle.startswith("pump_amm_") and role in {
            "scout_alpha",
            "momentum_alpha",
            "copyable_return_on_capital",
        }:
            strategies.append("graduation_continuation")
        if (
            venue == "RAYDIUM"
            and lifecycle == "raydium_post_pump_migration_evidence"
            and role in {"scout_alpha", "momentum_alpha", "copyable_return_on_capital"}
        ):
            strategies.append("raydium_cross_venue_persistence")
        for strategy in dict.fromkeys(strategies):
            item = dict(source)
            item.update(
                {
                    "strategy_family": strategy,
                    "risk_signature": (
                        "hazard_warning_role"
                        if role == "distribution_warning_value"
                        else "clean_or_unspecified"
                    ),
                    "risk_class": (
                        "hazard"
                        if role == "distribution_warning_value"
                        else "clean_or_unspecified"
                    ),
                    "source_kind": "wallet_context_router",
                    "specialist_positive": _profile_positive(source),
                    "exploration_only": False,
                }
            )
            rows.append(item)
    return rows


def _latest_release(store: Any, table: str) -> str | None:
    if not _table_exists(store, table):
        return None
    try:
        with store._lock:
            row = store.db.execute(
                f"SELECT release_commit FROM {table} ORDER BY id DESC LIMIT 1"
            ).fetchone()
        return str(row["release_commit"]) if row is not None and row["release_commit"] else None
    except Exception:
        return None


def _v5_specialist_rows(universe: WalletEntityUniverseV4) -> list[dict[str, Any]]:
    store = universe.store
    if not (
        _table_exists(store, "risk_conditioned_alpha_v5_trials")
        and _table_exists(store, "risk_conditioned_alpha_v5_outcomes")
    ):
        return []
    release = _latest_release(store, "risk_conditioned_alpha_v5_trials")
    if not release:
        return []

    grouped: dict[tuple[str, str, str, str, str, str, str], list[float]] = defaultdict(list)
    try:
        with store._lock:
            outcomes = store.db.execute(
                "SELECT t.trigger_wallet,t.lane,t.venue,t.lifecycle,t.regime,t.trigger_role,"
                "t.risk_signature,t.risk_severity,o.net_return "
                "FROM risk_conditioned_alpha_v5_trials t "
                "JOIN risk_conditioned_alpha_v5_outcomes o "
                "ON o.release_commit=t.release_commit AND o.source_signature=t.source_signature AND o.lane=t.lane "
                "WHERE t.release_commit=? AND t.decision LIKE 'paper_enter%' ORDER BY t.id",
                (release,),
            ).fetchall()
            trial_rows = store.db.execute(
                "SELECT trigger_wallet,lane,venue,lifecycle,regime,trigger_role,risk_signature,"
                "risk_severity,decision FROM risk_conditioned_alpha_v5_trials "
                "WHERE release_commit=? AND risk_signature<>'clean' ORDER BY id",
                (release,),
            ).fetchall()
    except Exception:
        return []

    for row in outcomes:
        key = (
            str(row["trigger_wallet"]),
            str(row["lane"]),
            str(row["venue"]),
            str(row["lifecycle"]),
            str(row["regime"]),
            str(row["trigger_role"]),
            str(row["risk_signature"]),
        )
        value = _safe_float(row["net_return"])
        if value is not None:
            grouped[key].append(value)

    try:
        from .risk_conditioned_alpha_v5 import MIN_FORWARD_SAMPLES, robust_return_profile
    except Exception:
        return []

    results: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str, str, str, str]] = set()
    for key, values in grouped.items():
        profile = robust_return_profile(values, min_samples=MIN_FORWARD_SAMPLES)
        seen.add(key)
        results.append(
            {
                "wallet": key[0],
                "strategy_family": key[1],
                "venue": key[2],
                "lifecycle_stage": key[3],
                "regime": key[4],
                "role": key[5],
                "risk_signature": key[6],
                "risk_class": "clean" if key[6] == "clean" else "hazard",
                "source_kind": "risk_conditioned_alpha_v5",
                "sample_count": profile.sample_count,
                "mature_forward_context": profile.sample_count >= MIN_FORWARD_SAMPLES,
                "specialist_positive": profile.state == "promoted_positive_log_growth",
                "best_expected_log_growth": profile.best_expected_log_growth,
                "trimmed_mean_residual_roi_ex_best_1": profile.trimmed_mean_ex_best,
                "context_score": profile.best_expected_log_growth,
                "exploration_only": False,
            }
        )

    # Risky wallets remain observable before promotion. Mechanical/latency/execution
    # rejections are intentionally excluded because they are not copyable opportunities.
    exploratory_counts: dict[
        tuple[str, str, str, str, str, str, str], tuple[int, float]
    ] = {}
    for row in trial_rows:
        decision = str(row["decision"] or "")
        if (
            decision.startswith("reject_mechanical")
            or decision.startswith("reject_latency")
            or decision.startswith("reject_execution")
        ):
            continue
        key = (
            str(row["trigger_wallet"]),
            str(row["lane"]),
            str(row["venue"]),
            str(row["lifecycle"]),
            str(row["regime"]),
            str(row["trigger_role"]),
            str(row["risk_signature"]),
        )
        if key in seen:
            continue
        count, max_severity = exploratory_counts.get(key, (0, 0.0))
        severity = _safe_float(row["risk_severity"]) or 0.0
        exploratory_counts[key] = (count + 1, max(max_severity, severity))

    for key, (count, severity) in exploratory_counts.items():
        results.append(
            {
                "wallet": key[0],
                "strategy_family": key[1],
                "venue": key[2],
                "lifecycle_stage": key[3],
                "regime": key[4],
                "role": key[5],
                "risk_signature": key[6],
                "risk_class": "hazard",
                "source_kind": "risk_conditioned_alpha_v5",
                "sample_count": count,
                "mature_forward_context": False,
                "specialist_positive": False,
                "context_score": severity,
                "exploration_only": True,
            }
        )
    return results


def _fomo_specialist_rows(universe: WalletEntityUniverseV4) -> list[dict[str, Any]]:
    store = universe.store
    required = (
        "fomo_shadow_observations",
        "fomo_shadow_outcomes",
        "profit_first_final_trials",
    )
    if any(not _table_exists(store, table) for table in required):
        return []
    release = _latest_release(store, "fomo_shadow_observations")
    if not release:
        return []
    try:
        with store._lock:
            rows = store.db.execute(
                "SELECT s.venue,s.lifecycle,s.regime,s.state_json,t.trigger_wallet,o.net_return "
                "FROM fomo_shadow_observations s "
                "JOIN profit_first_final_trials t "
                "ON t.release_commit=s.release_commit AND t.source_signature=s.source_signature "
                "AND t.lane='unified_profit_maximizer' "
                "JOIN fomo_shadow_outcomes o "
                "ON o.release_commit=s.release_commit AND o.source_signature=s.source_signature "
                "WHERE s.release_commit=? ORDER BY s.id",
                (release,),
            ).fetchall()
    except Exception:
        return []

    grouped: dict[tuple[str, str, str, str, str], list[float]] = defaultdict(list)
    for row in rows:
        try:
            payload = json.loads(str(row["state_json"] or "{}"))
        except Exception:
            payload = {}
        if str(payload.get("state") or "") not in {"pre_fomo", "active_fomo"}:
            continue
        variants = {str(value) for value in (payload.get("experiment_variants") or ())}
        risk_class = "hazard_fomo" if "hazard_fomo" in variants else "clean_fomo"
        wallet = str(row["trigger_wallet"] or "")
        value = _safe_float(row["net_return"])
        if not wallet or value is None:
            continue
        grouped[
            (
                wallet,
                str(row["venue"] or "UNKNOWN"),
                str(row["lifecycle"] or "unknown"),
                str(row["regime"] or "unknown"),
                risk_class,
            )
        ].append(value)

    try:
        from .risk_conditioned_alpha_v5 import FOMO_ACTIVE_GRID, robust_return_profile
    except Exception:
        return []

    results: list[dict[str, Any]] = []
    for key, values in grouped.items():
        profile = robust_return_profile(
            values,
            grid=FOMO_ACTIVE_GRID,
            max_fraction=0.05,
            min_samples=MIN_MATURE_FORWARD_SAMPLES,
        )
        results.append(
            {
                "wallet": key[0],
                "strategy_family": "fomo_continuation",
                "venue": key[1],
                "lifecycle_stage": key[2],
                "regime": key[3],
                "role": "fomo_trigger",
                "risk_signature": key[4],
                "risk_class": key[4],
                "source_kind": "fomo_v5_forward",
                "sample_count": profile.sample_count,
                "mature_forward_context": (
                    profile.sample_count >= MIN_MATURE_FORWARD_SAMPLES
                ),
                "specialist_positive": (
                    profile.state == "promoted_positive_log_growth"
                ),
                "best_expected_log_growth": profile.best_expected_log_growth,
                "trimmed_mean_residual_roi_ex_best_1": profile.trimmed_mean_ex_best,
                "context_score": profile.best_expected_log_growth,
                "exploration_only": profile.sample_count < MIN_MATURE_FORWARD_SAMPLES,
            }
        )
    return results


def build_strategy_specialist_tracking_plan(
    rows: list[dict[str, Any]],
    *,
    capacity: int,
    candidate_states: dict[str, str] | None = None,
    fallback_wallets: list[str] | tuple[str, ...] = (),
) -> dict[str, Any]:
    """Reserve strategy/regime specialist floors, then fill remaining slots by ROI.

    The floor is observation authority only. It does not grant paper-trade authority.
    Dedicated FOMO and hazard specialists remain eligible for observation even before
    mature promotion, while mechanical hard stops remain outside the opportunity set.
    """

    capacity = max(0, int(capacity))
    states_provided = candidate_states is not None
    states = candidate_states or {}

    usable: list[dict[str, Any]] = []
    for row in rows:
        wallet = str(row.get("wallet") or "")
        if not wallet:
            continue
        if states_provided and states.get(wallet) != "tracking":
            continue
        usable.append(dict(row))

    positive = [row for row in usable if _profile_positive(row)]
    positive.sort(key=_row_rank, reverse=True)
    exploration = [
        row
        for row in usable
        if bool(row.get("exploration_only"))
        and str(row.get("risk_class") or "") in {"hazard", "hazard_fomo"}
    ]
    exploration.sort(key=_row_rank, reverse=True)

    floor_candidates: list[tuple[int, tuple[str, str], dict[str, Any]]] = []
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in positive:
        grouped[_strategy_regime_key(row)].append(row)

    for key, group in grouped.items():
        group.sort(key=_row_rank, reverse=True)
        strategy = key[0]
        priority = 3
        if strategy == "fomo_continuation":
            priority = 6
        elif strategy == "hazard_continuation":
            priority = 5
        elif strategy in {
            "graduation_continuation",
            "raydium_cross_venue_persistence",
        }:
            priority = 4
        floor_candidates.append((priority, key, group[0]))

    # Clean and hazard FOMO are deliberately separate specialist surfaces.
    for risk_class, priority in (("clean_fomo", 7), ("hazard_fomo", 8)):
        candidates = [
            row
            for row in positive
            if row.get("strategy_family") == "fomo_continuation"
            and row.get("risk_class") == risk_class
        ]
        if candidates:
            candidates.sort(key=_row_rank, reverse=True)
            floor_candidates.append(
                (
                    priority,
                    (
                        f"fomo_continuation:{risk_class}",
                        str(candidates[0].get("regime") or "unknown"),
                    ),
                    candidates[0],
                )
            )

    floor_candidates.sort(
        key=lambda item: (item[0], _row_rank(item[2])), reverse=True
    )

    selected_wallets: list[str] = []
    selected_set: set[str] = set()
    assignments: list[dict[str, Any]] = []
    covered_floor_keys: list[tuple[str, str]] = []
    coverage_debt: list[dict[str, Any]] = []

    def add_row(
        row: dict[str, Any], source: str, *, observation_only: bool = False
    ) -> bool:
        wallet = str(row.get("wallet") or "")
        if not wallet or wallet in selected_set or len(selected_wallets) >= capacity:
            return False
        selected_set.add(wallet)
        selected_wallets.append(wallet)
        assignments.append(
            {
                "wallet": wallet,
                "strategy_family": str(
                    row.get("strategy_family") or "unknown_strategy"
                ),
                "venue": str(row.get("venue") or "UNKNOWN"),
                "lifecycle_stage": str(row.get("lifecycle_stage") or "unknown"),
                "regime": str(row.get("regime") or "unknown"),
                "role": str(row.get("role") or "unknown"),
                "risk_signature": str(row.get("risk_signature") or "clean"),
                "risk_class": str(
                    row.get("risk_class") or "clean_or_unspecified"
                ),
                "sample_count": int(row.get("sample_count") or 0),
                "best_expected_log_growth": _safe_float(
                    row.get("best_expected_log_growth")
                ),
                "copyable_return_on_deployed_fraction_pct": _safe_float(
                    row.get("copyable_return_on_deployed_fraction_pct")
                ),
                "assignment_source": source,
                "observation_only": bool(observation_only),
                "current_paper_strategy_authority": False,
                "future_paper_strategy_eligible": (
                    bool(_profile_positive(row)) and not observation_only
                ),
                "cross_context_success_transfer_allowed": False,
            }
        )
        return True

    for _, key, row in floor_candidates:
        wallet = str(row.get("wallet") or "")
        if wallet in selected_set:
            covered_floor_keys.append(key)
            continue
        if add_row(row, "strategy_regime_specialist_floor"):
            covered_floor_keys.append(key)
        else:
            coverage_debt.append(
                {
                    "strategy_family": key[0],
                    "regime": key[1],
                    "reason": "tracking_capacity_exhausted",
                }
            )

    # Preserve a separate high-risk observation cohort even when it has not earned
    # promotion. This keeps dangerous-but-potentially-copyable alpha measurable.
    if len(selected_wallets) < capacity:
        for row in exploration:
            if add_row(
                row,
                "hazard_or_fomo_exploration_floor",
                observation_only=True,
            ):
                break

    # Remaining high-priority tracking is still earned globally by forward ROI.
    if len(selected_wallets) < capacity:
        for row in positive:
            add_row(row, "global_forward_roi_fill")

    bootstrap: list[str] = []
    if len(selected_wallets) < capacity:
        for raw_wallet in fallback_wallets:
            wallet = str(raw_wallet or "")
            if not wallet or wallet in selected_set:
                continue
            if states_provided and states.get(wallet) != "tracking":
                continue
            selected_set.add(wallet)
            selected_wallets.append(wallet)
            bootstrap.append(wallet)
            if len(selected_wallets) >= capacity:
                break

    return {
        "allocation_version": ALLOCATION_VERSION,
        "capacity": capacity,
        "selected_challenger_wallets": selected_wallets,
        "context_assignments": assignments,
        "bootstrap_observation_wallets": bootstrap,
        "required_specialist_floor_count": len(floor_candidates),
        "covered_specialist_floor_count": len(covered_floor_keys),
        "covered_specialist_floor_keys": [
            {"strategy_family": key[0], "regime": key[1]}
            for key in covered_floor_keys
        ],
        "coverage_debt": coverage_debt,
        "strategy_regime_specialist_floor_enabled": True,
        "exact_context_metadata_retained": True,
        "fomo_dedicated_specialist_pool_enabled": True,
        "fomo_clean_hazard_separated": True,
        "high_risk_alpha_observation_cohort_enabled": True,
        "mechanical_hard_stops_relaxed": False,
        "remaining_capacity_filled_by_global_forward_roi": True,
        "cross_context_success_transfer_allowed": False,
        "bootstrap_slots_have_strategy_authority": False,
        "paper_only": True,
        "live_money_authority": False,
    }


def _specialist_select(self: WalletEntityUniverseV4) -> list[str]:
    if _ORIGINAL_SELECT is None:
        raise RuntimeError("strategy specialist wallet allocator is not installed")
    fallback = list(_ORIGINAL_SELECT(self))
    states = _candidate_states(self)
    if not states:
        setattr(
            self,
            "_roi_strategy_specialist_last_error",
            "candidate_state_unavailable_fail_closed_to_existing_tracking",
        )
        return fallback
    incumbents = [
        wallet for wallet in fallback if states.get(wallet) == "incumbent_tracking"
    ]
    fallback_challengers = [
        wallet for wallet, state in states.items() if state == "tracking"
    ]
    try:
        profiles = WalletContextRouter(self).context_profiles()
        rows = _base_specialist_rows(profiles)
        rows.extend(_v5_specialist_rows(self))
        rows.extend(_fomo_specialist_rows(self))
        plan = build_strategy_specialist_tracking_plan(
            rows,
            capacity=max(1, int(self.discovery.policy.max_tracked_challengers)),
            candidate_states=states,
            fallback_wallets=fallback_challengers,
        )
        setattr(self, "_roi_strategy_specialist_tracking_plan", plan)
        setattr(self, "_roi_strategy_specialist_last_error", None)
        return list(dict.fromkeys((*incumbents, *plan["selected_challenger_wallets"])))
    except Exception as exc:
        setattr(
            self,
            "_roi_strategy_specialist_last_error",
            f"{type(exc).__name__}: {exc}",
        )
        return fallback


def _status_with_specialists(self: WalletContextRouter) -> dict[str, Any]:
    if _ORIGINAL_ROUTER_STATUS is None:
        raise RuntimeError("strategy specialist status wrapper is not installed")
    payload = _ORIGINAL_ROUTER_STATUS(self)
    universe = getattr(self, "universe", None)
    plan = (
        getattr(universe, "_roi_strategy_specialist_tracking_plan", None)
        if isinstance(universe, WalletEntityUniverseV4)
        else None
    )
    if isinstance(universe, WalletEntityUniverseV4) and isinstance(plan, dict):
        states = _candidate_states(universe)
        current = payload.get("venue_lifecycle_tracking_assignment")
        if isinstance(current, dict):
            incumbents = [
                wallet
                for wallet, state in states.items()
                if state == "incumbent_tracking"
            ]
            current.update(
                {
                    "allocation_version": ALLOCATION_VERSION,
                    "context_assignments": list(
                        plan.get("context_assignments") or []
                    ),
                    "bootstrap_observation_wallets": list(
                        plan.get("bootstrap_observation_wallets") or []
                    ),
                    "selected_challenger_wallets": list(
                        plan.get("selected_challenger_wallets") or []
                    ),
                    "effective_tracked_wallets": list(
                        dict.fromkeys(
                            (
                                *incumbents,
                                *(plan.get("selected_challenger_wallets") or []),
                            )
                        )
                    ),
                    "tracking_capacity_partitioned_by_venue_lifecycle_before_global_fill": False,
                    "strategy_regime_specialist_floor_enabled": True,
                    "remaining_capacity_filled_by_global_forward_roi": True,
                    "fomo_dedicated_specialist_pool_enabled": True,
                    "high_risk_alpha_observation_cohort_enabled": True,
                    "coverage_debt": list(plan.get("coverage_debt") or []),
                }
            )

    payload["strategy_specialist_wallet_allocation"] = {
        "allocation_version": ALLOCATION_VERSION,
        "assignment_key": (
            "strategy_x_venue_x_lifecycle_x_regime_x_role_x_risk_signature"
        ),
        "strategy_regime_specialist_floor_enabled": True,
        "remaining_capacity_filled_by_global_forward_roi": True,
        "fomo_dedicated_specialist_pool_enabled": True,
        "fomo_clean_hazard_separated": True,
        "high_risk_alpha_observation_cohort_enabled": True,
        "danger_labels_are_blanket_tracking_vetoes": False,
        "mechanical_hard_stops_relaxed": False,
        "robinhood_wallet_pool_modified": False,
        "current_plan": plan,
        "last_error": (
            getattr(universe, "_roi_strategy_specialist_last_error", None)
            if isinstance(universe, WalletEntityUniverseV4)
            else "production_wallet_universe_unavailable"
        ),
        "current_paper_strategy_authority": False,
        "historical_promotion_authority": False,
        "cross_context_success_transfer_allowed": False,
        "paper_only": True,
        "live_money_authority": False,
    }
    return payload


def _manifest_with_specialists(
    self: FinalProfitFirstResearchAdapter,
) -> dict[str, Any]:
    if _ORIGINAL_MANIFEST is None:
        raise RuntimeError("strategy specialist manifest wrapper is not installed")
    payload = _ORIGINAL_MANIFEST(self)
    payload.update(
        {
            "strategy_specialist_wallet_allocation": ALLOCATION_VERSION,
            "wallet_assignment_key": (
                "strategy_x_venue_x_lifecycle_x_regime_x_role_x_risk_signature"
            ),
            "wallet_strategy_regime_specialist_floor_enabled": True,
            "wallet_remaining_capacity_global_forward_roi_fill": True,
            "wallet_fomo_dedicated_specialist_pool_enabled": True,
            "wallet_fomo_clean_hazard_separated": True,
            "wallet_high_risk_alpha_observation_cohort_enabled": True,
            "wallet_danger_label_blanket_tracking_veto": False,
            "wallet_mechanical_hard_stops_relaxed": False,
            "wallet_robinhood_pool_modified": False,
            "wallet_cross_context_success_transfer_allowed": False,
            "wallet_current_paper_trade_authority": False,
            "historical_evidence_promotion_authority": False,
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        }
    )
    return payload


def install_strategy_specialist_wallet_allocator() -> None:
    """Install strategy-aware Solana tracking above PR120's risk-conditioned v5."""
    global _ORIGINAL_SELECT, _ORIGINAL_ROUTER_STATUS, _ORIGINAL_MANIFEST

    if _ORIGINAL_SELECT is None:
        current_select = WalletEntityUniverseV4.select_tracked_wallets
        _ORIGINAL_SELECT = current_select
        try:
            _specialist_select.__dict__.update(getattr(current_select, "__dict__", {}))
        except Exception:
            pass
        setattr(
            _specialist_select,
            "_roi_strategy_specialist_wallet_allocator",
            True,
        )
        WalletEntityUniverseV4.select_tracked_wallets = _specialist_select  # type: ignore[method-assign]

    if _ORIGINAL_ROUTER_STATUS is None:
        current_status = WalletContextRouter.status
        _ORIGINAL_ROUTER_STATUS = current_status
        try:
            _status_with_specialists.__dict__.update(
                getattr(current_status, "__dict__", {})
            )
        except Exception:
            pass
        setattr(
            _status_with_specialists,
            "_roi_strategy_specialist_wallet_allocator",
            True,
        )
        WalletContextRouter.status = _status_with_specialists  # type: ignore[method-assign]

    if _ORIGINAL_MANIFEST is None:
        current_manifest = FinalProfitFirstResearchAdapter._manifest
        _ORIGINAL_MANIFEST = current_manifest
        try:
            _manifest_with_specialists.__dict__.update(
                getattr(current_manifest, "__dict__", {})
            )
        except Exception:
            pass
        setattr(
            _manifest_with_specialists,
            "_roi_strategy_specialist_wallet_allocator",
            True,
        )
        FinalProfitFirstResearchAdapter._manifest = _manifest_with_specialists  # type: ignore[method-assign]


__all__ = [
    "ALLOCATION_VERSION",
    "build_strategy_specialist_tracking_plan",
    "install_strategy_specialist_wallet_allocator",
]
