from __future__ import annotations

import hashlib
import math
from datetime import datetime, timezone
from typing import Any, Callable

from . import context_research_bandwidth_governor as bandwidth_module
from . import wallet_context_tracking_assignment as tracking_module
from .profit_first_entity_final_research import FinalProfitFirstResearchAdapter
from .wallet_context_router import WalletContextRouter
from .wallet_venue_lifecycle_research import (
    RAYDIUM_POST_PUMP,
    RAYDIUM_UNPROVEN,
    lifecycle_stage,
    venue_from_source,
)


RESOURCE_GOVERNANCE_VERSION = "venue-resource-governance-v1"
RAYDIUM_POST_PUMP_BOOTSTRAP_FRACTION = 0.50
RAYDIUM_UNPROVEN_BOOTSTRAP_FRACTION = 0.25

PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
HISTORICAL_PROMOTION_AUTHORITY = False
MARKET_OBSERVATION_SCOPE_REDUCED = False
CANDIDATE_CERTIFICATION_THROTTLED = False
EXIT_RESEARCH_THROTTLED = False
FIXED_VENUE_HIGH_PRIORITY_RESERVATIONS = False
RAYDIUM_OBSERVATION_RETAINED = True
RAYDIUM_LAUNCHLAB_EXACT_SUBTYPE_INFERRED = False

_ORIGINAL_TRACKING_PLAN: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_SCHEDULE: Callable[..., Any] | None = None
_ORIGINAL_STATUS: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_MANIFEST: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_ROUTER_STATUS: Callable[..., dict[str, Any]] | None = None

_POSITIVE_ACTIONS = frozenset(
    {
        "promote_for_future_context_influence",
        "keep_for_future_context_influence",
    }
)
_NEGATIVE_ACTIONS = frozenset(
    {
        "demote_for_future_context_influence",
        "withhold_from_future_context_influence",
    }
)


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _evidence_rank(row: dict[str, Any]) -> tuple[float, float, float, int]:
    """Rank exact contexts by robust copyable percentage ROI, never dollars."""

    trimmed = _safe_float(row.get("trimmed_mean_residual_roi_ex_best_1"))
    copyable = _safe_float(row.get("copyable_return_on_deployed_fraction"))
    context_score = _safe_float(row.get("context_score"))
    return (
        trimmed if trimmed is not None else float("-inf"),
        copyable if copyable is not None else float("-inf"),
        context_score if context_score is not None else float("-inf"),
        int(row.get("sample_count") or 0),
    )


def build_roi_earned_tracking_plan(
    profiles: list[dict[str, Any]],
    *,
    capacity: int,
    candidate_states: dict[str, str] | None = None,
    fallback_wallets: list[str] | tuple[str, ...] = (),
) -> dict[str, Any]:
    """Allocate scarce wallet tracking to the best proven exact contexts globally.

    Raw venue observation is a separate plane and remains full scope. This function
    therefore does not reserve a high-priority wallet slot merely to represent a
    venue. A Raydium wallet can consume many scarce slots when its own forward,
    copyable percentage ROI is stronger, or zero mature slots when it is weaker.
    Bootstrap wallets fill only capacity that no mature positive context has earned.
    """

    capacity = max(0, int(capacity))
    states_provided = candidate_states is not None
    states = candidate_states or {}

    eligible: list[dict[str, Any]] = []
    for row in profiles:
        wallet = str(row.get("wallet") or "")
        if not wallet or not tracking_module._robust_positive(row):
            continue
        if states_provided and states.get(wallet) != "tracking":
            continue
        eligible.append(row)
    eligible.sort(key=_evidence_rank, reverse=True)

    selected: list[dict[str, Any]] = []
    selected_wallets: set[str] = set()
    for row in eligible:
        wallet = str(row.get("wallet") or "")
        if not wallet or wallet in selected_wallets:
            continue
        selected.append(row)
        selected_wallets.add(wallet)
        if len(selected) >= capacity:
            break

    bootstrap: list[str] = []
    if len(selected) < capacity:
        for raw_wallet in fallback_wallets:
            wallet = str(raw_wallet or "")
            if not wallet or wallet in selected_wallets:
                continue
            if states_provided and states.get(wallet) != "tracking":
                continue
            bootstrap.append(wallet)
            selected_wallets.add(wallet)
            if len(selected) + len(bootstrap) >= capacity:
                break

    assignments: list[dict[str, Any]] = []
    for row in selected:
        venue, lifecycle, regime, role = tracking_module._context_key(row)
        assignments.append(
            {
                "wallet": str(row.get("wallet") or ""),
                "venue": venue,
                "lifecycle_stage": lifecycle,
                "regime": regime,
                "role": role,
                "context_score": _safe_float(row.get("context_score")),
                "copyable_return_on_deployed_fraction_pct": _safe_float(
                    row.get("copyable_return_on_deployed_fraction_pct")
                ),
                "trimmed_mean_residual_roi_ex_best_1_pct": _safe_float(
                    row.get("trimmed_mean_residual_roi_ex_best_1_pct")
                ),
                "sample_count": int(row.get("sample_count") or 0),
                "assignment_source": "mature_positive_exact_context_global_roi_rank",
                "current_paper_strategy_authority": False,
                "future_paper_strategy_eligible": True,
                "cross_context_success_transfer_allowed": False,
            }
        )

    bootstrap_rows = [
        {
            "wallet": wallet,
            "assignment_source": "bootstrap_observation_only_after_earned_capacity",
            "current_paper_strategy_authority": False,
            "future_paper_strategy_eligible": False,
            "cross_context_success_transfer_allowed": False,
        }
        for wallet in bootstrap
    ]
    venue_lifecycle = {
        tracking_module._venue_lifecycle_key(row) for row in selected
    }
    return {
        "capacity": capacity,
        "context_assigned_wallets": [row["wallet"] for row in assignments],
        "bootstrap_observation_wallets": bootstrap,
        "selected_challenger_wallets": [row["wallet"] for row in assignments] + bootstrap,
        "context_assignments": assignments,
        "bootstrap_assignments": bootstrap_rows,
        "venue_lifecycle_coverage": [
            {"venue": venue, "lifecycle_stage": lifecycle}
            for venue, lifecycle in sorted(venue_lifecycle)
        ],
        "fixed_venue_high_priority_reservations": False,
        "high_priority_capacity_earned_by_forward_roi": True,
        "ranking_unit": "copyable_percentage_roi_not_dollars",
        "minimum_market_observation_coverage_independent_of_wallet_capacity": True,
        "raydium_observation_retained": True,
        "raydium_high_priority_capacity_fixed": False,
        "raydium_capacity_can_expand_on_positive_exact_context": True,
        "cross_context_success_transfer_allowed": False,
        "bootstrap_slots_have_strategy_authority": False,
    }


def venue_resource_policy(
    *,
    venue: str | None,
    lifecycle: str,
    actions: list[str] | tuple[str, ...],
    side: str,
    candidate_certification: bool,
) -> dict[str, Any]:
    """Return the extra noncritical research-compute admission fraction.

    This is deliberately an *additional* prefilter above the existing context
    bandwidth governor. It never throttles raw market observation, candidate
    certification, or sell/exit research. Mature negative contexts are delegated to
    the existing governor so their current exploration floor is not multiplied twice.
    """

    if candidate_certification:
        return {
            "tier": "candidate_certification_exempt",
            "fraction": 1.0,
            "reason": "candidate_certification_is_never_throttled_by_venue_priority",
        }
    if str(side or "").lower() == "sell":
        return {
            "tier": "exit_research_exempt",
            "fraction": 1.0,
            "reason": "sell_and_exit_research_remain_full_rate",
        }
    if str(venue or "") != "RAYDIUM":
        return {
            "tier": "non_raydium_delegate_existing",
            "fraction": 1.0,
            "reason": "non_raydium_priority_is_unchanged_by_this_policy",
        }

    clean_actions = [str(value) for value in actions if str(value)]
    if any(action in _POSITIVE_ACTIONS for action in clean_actions):
        return {
            "tier": "raydium_positive_exact_context_full_rate",
            "fraction": 1.0,
            "reason": "raydium_earned_full_research_rate_from_positive_forward_exact_context",
        }
    if clean_actions and all(action in _NEGATIVE_ACTIONS for action in clean_actions):
        return {
            "tier": "raydium_mature_negative_delegate_existing",
            "fraction": 1.0,
            "reason": "existing_context_governor_retains_its_unchanged_negative_exploration_floor",
        }
    if lifecycle == RAYDIUM_POST_PUMP:
        return {
            "tier": "raydium_continuation_bootstrap",
            "fraction": RAYDIUM_POST_PUMP_BOOTSTRAP_FRACTION,
            "reason": "post_pump_raydium_continuation_keeps_medium_exploration_until_roi_is_proven",
        }
    return {
        "tier": "raydium_unproven_low_priority_bootstrap",
        "fraction": RAYDIUM_UNPROVEN_BOOTSTRAP_FRACTION,
        "reason": "unproven_raydium_context_keeps_low_exploration_until_exact_forward_roi_earns_more",
    }


def _deterministic_selected(signature: str, fraction: float) -> bool:
    fraction = max(0.0, min(1.0, float(fraction)))
    if fraction >= 1.0:
        return True
    if fraction <= 0.0:
        return False
    digest = hashlib.sha256(
        f"{RESOURCE_GOVERNANCE_VERSION}|{signature}".encode("utf-8")
    ).digest()
    return int.from_bytes(digest[:8], "big") / float(2**64) < fraction


def _schema(store: Any) -> None:
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS venue_resource_governance_decisions ("
            "signature TEXT PRIMARY KEY, wallet TEXT NOT NULL, token_mint TEXT NOT NULL, "
            "venue TEXT, lifecycle_stage TEXT NOT NULL, tier TEXT NOT NULL, "
            "sampling_fraction REAL NOT NULL, selected INTEGER NOT NULL, reason TEXT NOT NULL, "
            "decided_at TEXT NOT NULL, paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL)"
        )
        store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_venue_resource_governance_tier "
            "ON venue_resource_governance_decisions(tier,selected,decided_at)"
        )


def _record_resource_decision(
    adapter: FinalProfitFirstResearchAdapter,
    *,
    signature: str,
    row: dict[str, Any] | None,
    venue: str | None,
    stage: str,
    policy: dict[str, Any],
    selected: bool,
) -> None:
    try:
        _schema(adapter.store)
        with adapter.store._lock, adapter.store.db:
            adapter.store.db.execute(
                "INSERT OR IGNORE INTO venue_resource_governance_decisions("
                "signature,wallet,token_mint,venue,lifecycle_stage,tier,sampling_fraction,selected,reason,"
                "decided_at,paper_only,live_money_authority) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    signature,
                    str((row or {}).get("wallet") or "unknown"),
                    str((row or {}).get("token_mint") or "unknown"),
                    venue,
                    stage,
                    str(policy["tier"]),
                    float(policy["fraction"]),
                    1 if selected else 0,
                    str(policy["reason"]),
                    _utcnow_iso(),
                    1,
                    0,
                ),
            )
    except Exception:
        pass


def _schedule_with_venue_priority(
    self: FinalProfitFirstResearchAdapter,
    signature: str,
) -> None:
    if _ORIGINAL_SCHEDULE is None:
        raise RuntimeError("venue resource governance schedule is not installed")
    signature = str(signature or "")
    if not signature:
        return

    row = bandwidth_module._observation(self, signature)
    source = str((row or {}).get("source") or "")
    candidate_certification = source.startswith("direct-candidate-v4:")
    venue = venue_from_source(source) if row else None
    prior_pump = bool(
        row
        and venue == "RAYDIUM"
        and bandwidth_module._prior_pump_evidence(self, row)
    )
    stage = lifecycle_stage(venue, prior_pump_evidence=prior_pump)
    actions = bandwidth_module._matching_actions(
        self,
        wallet=str((row or {}).get("wallet") or ""),
        venue=venue,
        stage=stage,
    )
    policy = venue_resource_policy(
        venue=venue,
        lifecycle=stage,
        actions=actions,
        side=str((row or {}).get("side") or ""),
        candidate_certification=candidate_certification,
    )
    selected = _deterministic_selected(signature, float(policy["fraction"]))
    _record_resource_decision(
        self,
        signature=signature,
        row=row,
        venue=venue,
        stage=stage,
        policy=policy,
        selected=selected,
    )
    if selected:
        _ORIGINAL_SCHEDULE(self, signature)


setattr(_schedule_with_venue_priority, "_roi_venue_resource_governance", True)


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


def _robinhood_outcome_count(adapter: FinalProfitFirstResearchAdapter) -> int:
    if not _table_exists(adapter.store, "robinhood_paper_outcomes"):
        return 0
    try:
        with adapter.store._lock:
            row = adapter.store.db.execute(
                "SELECT COUNT(*) AS n FROM robinhood_paper_outcomes WHERE release_commit=?",
                (adapter.release_commit,),
            ).fetchone()
        return int(row["n"] or 0) if row is not None else 0
    except Exception:
        return 0


def _resource_status(adapter: FinalProfitFirstResearchAdapter) -> dict[str, Any]:
    _schema(adapter.store)
    with adapter.store._lock:
        totals = adapter.store.db.execute(
            "SELECT COUNT(*) AS n,SUM(selected) AS selected FROM venue_resource_governance_decisions"
        ).fetchone()
        tiers = adapter.store.db.execute(
            "SELECT tier,COUNT(*) AS n,SUM(selected) AS selected,AVG(sampling_fraction) AS fraction "
            "FROM venue_resource_governance_decisions GROUP BY tier ORDER BY tier"
        ).fetchall()
    n = int(totals["n"] or 0) if totals is not None else 0
    selected = int(totals["selected"] or 0) if totals is not None else 0
    robinhood_outcomes = _robinhood_outcome_count(adapter)
    return {
        "version": RESOURCE_GOVERNANCE_VERSION,
        "decision_count": n,
        "selected_for_existing_final_research": selected,
        "deferred_from_existing_final_research": max(0, n - selected),
        "tiers": [
            {
                "tier": str(row["tier"]),
                "decision_count": int(row["n"] or 0),
                "selected_count": int(row["selected"] or 0),
                "configured_mean_fraction": float(row["fraction"] or 0.0),
            }
            for row in tiers
        ],
        "strategic_roles": {
            "PUMP_FUN": "earliest_discovery_and_entity_wallet_intelligence",
            "PUMP_AMM": "primary_solana_speculative_continuation",
            "FOMO": "cross_venue_demand_acceleration_state_and_paper_lane",
            "RAYDIUM": "deeper_liquidity_continuation_fomo_and_exit_venue",
            "RAYDIUM_LAUNCHLAB": "low_priority_opportunistic_launch_discovery_when_exact_subtype_evidence_exists",
            "ROBINHOOD_CHAIN": "independent_paper_alpha_source_admitted_when_forward_paper_outcomes_exist",
        },
        "fixed_venue_high_priority_reservations": False,
        "high_priority_wallet_capacity_earned_by_forward_roi": True,
        "raydium_observation_coverage_retained": True,
        "raydium_post_pump_bootstrap_research_fraction": RAYDIUM_POST_PUMP_BOOTSTRAP_FRACTION,
        "raydium_unproven_bootstrap_research_fraction": RAYDIUM_UNPROVEN_BOOTSTRAP_FRACTION,
        "raydium_positive_exact_context_full_rate": True,
        "raydium_launchlab_exact_subtype_available_downstream": False,
        "raydium_launchlab_not_inferred_from_unproven_bucket": True,
        "robinhood_paper_outcome_count_current_release": robinhood_outcomes,
        "robinhood_admitted_to_cross_chain_competition": robinhood_outcomes > 0,
        "market_observation_scope_reduced": False,
        "candidate_certification_throttled": False,
        "exit_research_throttled": False,
        "cross_context_success_transfer_allowed": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


def _router_status_with_resource_governance(self: WalletContextRouter) -> dict[str, Any]:
    if _ORIGINAL_ROUTER_STATUS is None:
        raise RuntimeError("venue resource router status is not installed")
    payload = _ORIGINAL_ROUTER_STATUS(self)
    assignment = payload.get("venue_lifecycle_tracking_assignment")
    if isinstance(assignment, dict):
        assignment["tracking_capacity_partitioned_by_venue_lifecycle_before_global_fill"] = False
        assignment["fixed_venue_high_priority_reservations"] = False
        assignment["high_priority_capacity_earned_by_forward_roi"] = True
        assignment["ranking_unit"] = "copyable_percentage_roi_not_dollars"
        assignment["minimum_market_observation_coverage_independent_of_wallet_capacity"] = True
        assignment["raydium_observation_retained"] = True
    payload["venue_resource_governance_version"] = RESOURCE_GOVERNANCE_VERSION
    payload["fixed_venue_high_priority_reservations"] = False
    payload["high_priority_wallet_capacity_earned_by_forward_roi"] = True
    return payload


def _status_with_resource_governance(
    self: FinalProfitFirstResearchAdapter,
) -> dict[str, Any]:
    if _ORIGINAL_STATUS is None:
        raise RuntimeError("venue resource governance status is not installed")
    payload = _ORIGINAL_STATUS(self)
    try:
        payload["venue_resource_governance"] = _resource_status(self)
    except Exception as exc:
        payload["venue_resource_governance"] = {
            "version": RESOURCE_GOVERNANCE_VERSION,
            "failed_closed": True,
            "error": f"{type(exc).__name__}: venue resource status unavailable",
            "market_observation_scope_reduced": False,
            "candidate_certification_throttled": False,
            "paper_only": True,
            "live_money_authority": False,
        }
    return payload


def _manifest_with_resource_governance(
    self: FinalProfitFirstResearchAdapter,
) -> dict[str, Any]:
    if _ORIGINAL_MANIFEST is None:
        raise RuntimeError("venue resource governance manifest is not installed")
    payload = _ORIGINAL_MANIFEST(self)
    payload.update(
        {
            "venue_resource_governance": RESOURCE_GOVERNANCE_VERSION,
            "wallet_tracking_capacity_partitioned_by_venue_lifecycle": False,
            "wallet_high_priority_capacity_fixed_by_venue": False,
            "wallet_high_priority_capacity_earned_by_forward_percentage_roi": True,
            "raydium_observation_coverage_retained": True,
            "raydium_high_priority_wallet_capacity_fixed": False,
            "raydium_post_pump_bootstrap_research_fraction": RAYDIUM_POST_PUMP_BOOTSTRAP_FRACTION,
            "raydium_unproven_bootstrap_research_fraction": RAYDIUM_UNPROVEN_BOOTSTRAP_FRACTION,
            "raydium_positive_exact_context_can_restore_full_research_rate": True,
            "raydium_launchlab_exact_subtype_not_inferred_from_coarse_source": True,
            "robinhood_competition_requires_actual_forward_paper_outcomes": True,
            "market_observation_scope_reduced": False,
            "candidate_certification_throttled": False,
            "exit_research_throttled": False,
            "strategy_thresholds_unchanged": True,
            "historical_evidence_promotion_authority": False,
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        }
    )
    return payload


def _inherit_markers(wrapper: Callable[..., Any], wrapped: Callable[..., Any]) -> None:
    try:
        wrapper.__dict__.update(getattr(wrapped, "__dict__", {}))
    except Exception:
        pass


def install_venue_resource_governance() -> None:
    """Install evidence-earned venue resource allocation exactly once."""

    global _ORIGINAL_TRACKING_PLAN, _ORIGINAL_SCHEDULE, _ORIGINAL_STATUS, _ORIGINAL_MANIFEST, _ORIGINAL_ROUTER_STATUS

    if _ORIGINAL_TRACKING_PLAN is None:
        _ORIGINAL_TRACKING_PLAN = tracking_module.build_context_tracking_plan
        tracking_module.build_context_tracking_plan = build_roi_earned_tracking_plan

    if _ORIGINAL_SCHEDULE is None:
        current_schedule = FinalProfitFirstResearchAdapter.schedule
        _ORIGINAL_SCHEDULE = current_schedule
        _inherit_markers(_schedule_with_venue_priority, current_schedule)
        setattr(_schedule_with_venue_priority, "_roi_venue_resource_governance", True)
        FinalProfitFirstResearchAdapter.schedule = _schedule_with_venue_priority  # type: ignore[method-assign]

    if _ORIGINAL_ROUTER_STATUS is None:
        current_router_status = WalletContextRouter.status
        _ORIGINAL_ROUTER_STATUS = current_router_status
        _inherit_markers(_router_status_with_resource_governance, current_router_status)
        WalletContextRouter.status = _router_status_with_resource_governance  # type: ignore[method-assign]

    if _ORIGINAL_STATUS is None:
        current_status = FinalProfitFirstResearchAdapter.status
        _ORIGINAL_STATUS = current_status
        _inherit_markers(_status_with_resource_governance, current_status)
        FinalProfitFirstResearchAdapter.status = _status_with_resource_governance  # type: ignore[method-assign]

    if _ORIGINAL_MANIFEST is None:
        current_manifest = FinalProfitFirstResearchAdapter._manifest
        _ORIGINAL_MANIFEST = current_manifest
        _inherit_markers(_manifest_with_resource_governance, current_manifest)
        FinalProfitFirstResearchAdapter._manifest = _manifest_with_resource_governance  # type: ignore[method-assign]


__all__ = [
    "CANDIDATE_CERTIFICATION_THROTTLED",
    "EXIT_RESEARCH_THROTTLED",
    "FIXED_VENUE_HIGH_PRIORITY_RESERVATIONS",
    "HISTORICAL_PROMOTION_AUTHORITY",
    "LIVE_MONEY_AUTHORITY",
    "MARKET_OBSERVATION_SCOPE_REDUCED",
    "PAPER_ONLY",
    "RAYDIUM_LAUNCHLAB_EXACT_SUBTYPE_INFERRED",
    "RAYDIUM_OBSERVATION_RETAINED",
    "RAYDIUM_POST_PUMP_BOOTSTRAP_FRACTION",
    "RAYDIUM_UNPROVEN_BOOTSTRAP_FRACTION",
    "RESOURCE_GOVERNANCE_VERSION",
    "SIGNING_AVAILABLE",
    "TRANSACTION_SUBMISSION_AVAILABLE",
    "build_roi_earned_tracking_plan",
    "install_venue_resource_governance",
    "venue_resource_policy",
]
