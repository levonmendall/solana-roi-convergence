from __future__ import annotations

import math
from collections import defaultdict
from typing import Any, Callable

from .profit_first_entity_final_research import FinalProfitFirstResearchAdapter
from .wallet_context_router import WalletContextRouter
from .wallet_entity_universe_v4 import WalletEntityUniverseV4


ASSIGNMENT_VERSION = "wallet-context-tracking-assignment-v1"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
ACTIVE_STRATEGY_MUTATION_ALLOWED = False
HISTORICAL_PROMOTION_AUTHORITY = False
CROSS_CONTEXT_SUCCESS_TRANSFER_ALLOWED = False

_ORIGINAL_SELECT: Callable[..., list[str]] | None = None
_ORIGINAL_ROUTER_STATUS: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_MANIFEST: Callable[..., dict[str, Any]] | None = None


def _safe_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _context_key(row: dict[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(row.get("venue") or "UNKNOWN"),
        str(row.get("lifecycle_stage") or "unknown_or_unsupported_venue"),
        str(row.get("regime") or "unknown"),
        str(row.get("role") or "unknown"),
    )


def _venue_lifecycle_key(row: dict[str, Any]) -> tuple[str, str]:
    return (
        str(row.get("venue") or "UNKNOWN"),
        str(row.get("lifecycle_stage") or "unknown_or_unsupported_venue"),
    )


def _robust_positive(row: dict[str, Any]) -> bool:
    if not bool(row.get("mature_forward_context")):
        return False
    score = _safe_float(row.get("context_score"))
    trimmed = _safe_float(row.get("trimmed_mean_residual_roi_ex_best_1"))
    return bool(score is not None and score > 0.0 and trimmed is not None and trimmed > 0.0)


def _rank(row: dict[str, Any]) -> tuple[float, float, int]:
    return (
        _safe_float(row.get("context_score")) or float("-inf"),
        _safe_float(row.get("copyable_return_on_deployed_fraction")) or float("-inf"),
        int(row.get("sample_count") or 0),
    )


def build_context_tracking_plan(
    profiles: list[dict[str, Any]],
    *,
    capacity: int,
    candidate_states: dict[str, str] | None = None,
    fallback_wallets: list[str] | tuple[str, ...] = (),
) -> dict[str, Any]:
    """Allocate scarce challenger slots by exact venue/lifecycle context.

    Mature positive contexts win scarce capacity first. Unassigned fallback wallets are
    allowed only as bootstrap observation slots and receive no strategy authority.
    """

    capacity = max(0, int(capacity))
    states = candidate_states or {}
    eligible: list[dict[str, Any]] = []
    for row in profiles:
        wallet = str(row.get("wallet") or "")
        if not wallet or not _robust_positive(row):
            continue
        state = states.get(wallet)
        if states and state != "tracking":
            continue
        eligible.append(row)
    eligible.sort(key=_rank, reverse=True)

    selected: list[dict[str, Any]] = []
    selected_wallets: set[str] = set()
    covered_venue_lifecycle: set[tuple[str, str]] = set()
    covered_contexts: set[tuple[str, str, str, str]] = set()

    # First pass prevents a strong wallet on one venue from consuming every scarce slot.
    for row in eligible:
        key = _venue_lifecycle_key(row)
        wallet = str(row["wallet"])
        if key in covered_venue_lifecycle or wallet in selected_wallets:
            continue
        selected.append(row)
        selected_wallets.add(wallet)
        covered_venue_lifecycle.add(key)
        covered_contexts.add(_context_key(row))
        if len(selected) >= capacity:
            break

    # Second pass expands exact venue/lifecycle/regime/role coverage.
    if len(selected) < capacity:
        for row in eligible:
            key = _context_key(row)
            wallet = str(row["wallet"])
            if key in covered_contexts or wallet in selected_wallets:
                continue
            selected.append(row)
            selected_wallets.add(wallet)
            covered_contexts.add(key)
            covered_venue_lifecycle.add(_venue_lifecycle_key(row))
            if len(selected) >= capacity:
                break

    # Third pass fills remaining slots with the best unused exact-context evidence.
    if len(selected) < capacity:
        for row in eligible:
            wallet = str(row["wallet"])
            if wallet in selected_wallets:
                continue
            selected.append(row)
            selected_wallets.add(wallet)
            if len(selected) >= capacity:
                break

    bootstrap: list[str] = []
    if len(selected) < capacity:
        for wallet in fallback_wallets:
            wallet = str(wallet)
            if not wallet or wallet in selected_wallets:
                continue
            if states and states.get(wallet) != "tracking":
                continue
            bootstrap.append(wallet)
            selected_wallets.add(wallet)
            if len(selected) + len(bootstrap) >= capacity:
                break

    assignments = [
        {
            "wallet": str(row["wallet"]),
            "venue": _context_key(row)[0],
            "lifecycle_stage": _context_key(row)[1],
            "regime": _context_key(row)[2],
            "role": _context_key(row)[3],
            "context_score": _safe_float(row.get("context_score")),
            "copyable_return_on_deployed_fraction_pct": _safe_float(
                row.get("copyable_return_on_deployed_fraction_pct")
            ),
            "sample_count": int(row.get("sample_count") or 0),
            "assignment_source": "mature_positive_exact_context",
            "current_paper_strategy_authority": False,
            "future_paper_strategy_eligible": True,
            "cross_context_success_transfer_allowed": False,
        }
        for row in selected
    ]
    bootstrap_rows = [
        {
            "wallet": wallet,
            "assignment_source": "bootstrap_observation_only",
            "current_paper_strategy_authority": False,
            "future_paper_strategy_eligible": False,
            "cross_context_success_transfer_allowed": False,
        }
        for wallet in bootstrap
    ]
    return {
        "capacity": capacity,
        "context_assigned_wallets": [row["wallet"] for row in assignments],
        "bootstrap_observation_wallets": bootstrap,
        "selected_challenger_wallets": [*([row["wallet"] for row in assignments]), *bootstrap],
        "context_assignments": assignments,
        "bootstrap_assignments": bootstrap_rows,
        "venue_lifecycle_coverage": [
            {"venue": venue, "lifecycle_stage": lifecycle}
            for venue, lifecycle in sorted(covered_venue_lifecycle)
        ],
        "cross_context_success_transfer_allowed": False,
        "bootstrap_slots_have_strategy_authority": False,
    }


def build_future_strategy_assignments(profiles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in profiles:
        if _robust_positive(row):
            grouped[_context_key(row)].append(row)

    result: list[dict[str, Any]] = []
    for key, rows in sorted(grouped.items()):
        rows.sort(key=_rank, reverse=True)
        leaders = []
        for row in rows[:5]:
            leaders.append(
                {
                    "wallet": str(row.get("wallet") or ""),
                    "context_score": _safe_float(row.get("context_score")),
                    "copyable_return_on_deployed_fraction_pct": _safe_float(
                        row.get("copyable_return_on_deployed_fraction_pct")
                    ),
                    "trimmed_mean_residual_roi_ex_best_1_pct": _safe_float(
                        row.get("trimmed_mean_residual_roi_ex_best_1_pct")
                    ),
                    "sample_count": int(row.get("sample_count") or 0),
                    "current_paper_strategy_authority": False,
                    "future_paper_strategy_eligible": True,
                }
            )
        result.append(
            {
                "venue": key[0],
                "lifecycle_stage": key[1],
                "regime": key[2],
                "role": key[3],
                "wallets": leaders,
                "exact_context_match_required_for_future_paper_authority": True,
                "cross_context_success_transfer_allowed": False,
                "current_paper_strategy_authority": False,
            }
        )
    return result


def _candidate_states(universe: WalletEntityUniverseV4) -> dict[str, str]:
    try:
        with universe.store._lock:
            rows = universe.store.db.execute(
                "SELECT wallet,state FROM wallet_discovery_candidates"
            ).fetchall()
        return {str(row["wallet"]): str(row["state"]) for row in rows}
    except Exception:
        return {}


def _partitioned_select(self: WalletEntityUniverseV4) -> list[str]:
    if _ORIGINAL_SELECT is None:
        raise RuntimeError("context tracking assignment is not installed")
    fallback = list(_ORIGINAL_SELECT(self))
    states = _candidate_states(self)
    incumbents = [wallet for wallet in fallback if states.get(wallet) == "incumbent_tracking"]
    fallback_challengers = [wallet for wallet in fallback if states.get(wallet) == "tracking"]
    try:
        profiles = WalletContextRouter(self).context_profiles()
        plan = build_context_tracking_plan(
            profiles,
            capacity=max(1, int(self.discovery.policy.max_tracked_challengers)),
            candidate_states=states,
            fallback_wallets=fallback_challengers,
        )
        setattr(self, "_roi_context_tracking_plan", plan)
        setattr(self, "_roi_context_tracking_last_error", None)
        return list(dict.fromkeys((*incumbents, *plan["selected_challenger_wallets"])))
    except Exception as exc:
        setattr(self, "_roi_context_tracking_last_error", f"{type(exc).__name__}: {exc}")
        return fallback


def _status_with_assignments(self: WalletContextRouter) -> dict[str, Any]:
    if _ORIGINAL_ROUTER_STATUS is None:
        raise RuntimeError("context tracking assignment status is not installed")
    payload = _ORIGINAL_ROUTER_STATUS(self)
    profiles = list(payload.get("context_profiles") or self.context_profiles())
    states = _candidate_states(self.universe)
    fallback = list(_ORIGINAL_SELECT(self.universe)) if _ORIGINAL_SELECT is not None else []
    incumbents = [wallet for wallet in fallback if states.get(wallet) == "incumbent_tracking"]
    fallback_challengers = [wallet for wallet in fallback if states.get(wallet) == "tracking"]
    plan = build_context_tracking_plan(
        profiles,
        capacity=max(1, int(self.universe.discovery.policy.max_tracked_challengers)),
        candidate_states=states,
        fallback_wallets=fallback_challengers,
    )
    payload["venue_lifecycle_tracking_assignment"] = {
        "assignment_version": ASSIGNMENT_VERSION,
        **plan,
        "incumbent_wallets_preserved": incumbents,
        "effective_tracked_wallets": list(
            dict.fromkeys((*incumbents, *plan["selected_challenger_wallets"]))
        ),
        "tracking_capacity_partitioned_by_venue_lifecycle_before_global_fill": True,
        "observation_bootstrap_preserved": True,
        "active_strategy_mutation_allowed": False,
        "paper_only": True,
        "live_money_authority": False,
        "last_error": getattr(self.universe, "_roi_context_tracking_last_error", None),
    }
    payload["future_strategy_assignments"] = {
        "assignment_version": ASSIGNMENT_VERSION,
        "assignment_key": "wallet_x_venue_x_lifecycle_x_regime_x_role",
        "contexts": build_future_strategy_assignments(profiles),
        "current_paper_strategy_authority": False,
        "exact_context_match_required_for_future_paper_authority": True,
        "cross_context_success_transfer_allowed": False,
        "historical_promotion_authority": False,
        "paper_only": True,
        "live_money_authority": False,
    }
    payload["context_recommendations_have_tracking_mutation_authority"] = True
    payload["tracking_mutation_scope"] = "research_wallet_tracking_only_exact_context_partitioned"
    payload["context_scores_have_trade_authority"] = False
    return payload


def _manifest_with_assignments(self: FinalProfitFirstResearchAdapter) -> dict[str, Any]:
    if _ORIGINAL_MANIFEST is None:
        raise RuntimeError("context tracking assignment manifest is not installed")
    payload = _ORIGINAL_MANIFEST(self)
    payload.update(
        {
            "wallet_context_tracking_assignment": ASSIGNMENT_VERSION,
            "wallet_tracking_capacity_partitioned_by_venue_lifecycle": True,
            "wallet_tracking_bootstrap_observation_floor_preserved": True,
            "wallet_future_strategy_assignment_requires_exact_venue_lifecycle_regime_role": True,
            "wallet_cross_context_success_transfer_allowed": False,
            "wallet_context_tracking_mutation_scope": "research_tracking_only",
            "wallet_context_current_paper_trade_authority": False,
            "historical_evidence_promotion_authority": False,
            "active_strategy_mutation_allowed": False,
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        }
    )
    return payload


def install_wallet_context_tracking_assignment() -> None:
    global _ORIGINAL_SELECT, _ORIGINAL_ROUTER_STATUS, _ORIGINAL_MANIFEST

    if _ORIGINAL_SELECT is None:
        current_select = WalletEntityUniverseV4.select_tracked_wallets
        _ORIGINAL_SELECT = current_select
        try:
            _partitioned_select.__dict__.update(getattr(current_select, "__dict__", {}))
        except Exception:
            pass
        setattr(_partitioned_select, "_roi_context_tracking_assignment", True)
        WalletEntityUniverseV4.select_tracked_wallets = _partitioned_select  # type: ignore[method-assign]

    if _ORIGINAL_ROUTER_STATUS is None:
        current_status = WalletContextRouter.status
        _ORIGINAL_ROUTER_STATUS = current_status
        try:
            _status_with_assignments.__dict__.update(getattr(current_status, "__dict__", {}))
        except Exception:
            pass
        setattr(_status_with_assignments, "_roi_context_tracking_assignment", True)
        WalletContextRouter.status = _status_with_assignments  # type: ignore[method-assign]

    if _ORIGINAL_MANIFEST is None:
        current_manifest = FinalProfitFirstResearchAdapter._manifest
        _ORIGINAL_MANIFEST = current_manifest
        try:
            _manifest_with_assignments.__dict__.update(getattr(current_manifest, "__dict__", {}))
        except Exception:
            pass
        setattr(_manifest_with_assignments, "_roi_context_tracking_assignment", True)
        FinalProfitFirstResearchAdapter._manifest = _manifest_with_assignments  # type: ignore[method-assign]


__all__ = [
    "ASSIGNMENT_VERSION",
    "build_context_tracking_plan",
    "build_future_strategy_assignments",
    "install_wallet_context_tracking_assignment",
]
