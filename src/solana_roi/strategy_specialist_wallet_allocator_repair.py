from __future__ import annotations

from collections import defaultdict
from typing import Any, Callable

from . import strategy_specialist_wallet_allocator as allocator
from .wallet_entity_universe_v4 import SEED_BY_ADDRESS


REPAIR_VERSION = "strategy-specialist-wallet-allocation-v1.1-fairness"
_ORIGINAL_BUILD: Callable[..., dict[str, Any]] | None = None


def _normalized_fallback(
    fallback_wallets: list[str] | tuple[str, ...],
    candidate_states: dict[str, str] | None,
) -> list[str]:
    states = candidate_states or {}
    values = list(dict.fromkeys(str(wallet) for wallet in fallback_wallets if str(wallet)))
    nonseeds = [wallet for wallet in values if wallet not in SEED_BY_ADDRESS]
    seeds = [wallet for wallet in values if wallet in SEED_BY_ADDRESS]

    # The original specialist wrapper can receive candidate-state iteration order,
    # which is SQLite insertion order. Keep discovered challengers ahead of seed
    # hypotheses so eight named seeds cannot consume eight of twelve bootstrap slots.
    if candidate_states is not None:
        extra_nonseeds = [
            wallet
            for wallet, state in states.items()
            if state == "tracking"
            and wallet not in SEED_BY_ADDRESS
            and wallet not in nonseeds
        ]
        extra_seeds = [
            wallet
            for wallet, state in states.items()
            if state == "tracking"
            and wallet in SEED_BY_ADDRESS
            and wallet not in seeds
        ]
        nonseeds.extend(extra_nonseeds)
        seeds.extend(extra_seeds)
    return nonseeds + seeds


def _primary_strategy_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    positive = [row for row in rows if allocator._profile_positive(row)]
    by_strategy: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in positive:
        by_strategy[str(row.get("strategy_family") or "unknown_strategy")].append(row)

    selected: list[dict[str, Any]] = []
    for strategy, group in by_strategy.items():
        del strategy
        group.sort(key=allocator._row_rank, reverse=True)
        selected.append(group[0])

    # PR120 makes clean and hazard FOMO materially different opportunity surfaces.
    # Keep one best row for each risk class in the first-pass specialist set.
    for risk_class in ("clean_fomo", "hazard_fomo"):
        candidates = [
            row
            for row in positive
            if row.get("strategy_family") == "fomo_continuation"
            and row.get("risk_class") == risk_class
        ]
        if candidates:
            candidates.sort(key=allocator._row_rank, reverse=True)
            selected.append(candidates[0])

    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str, str]] = set()
    for row in selected:
        key = (
            str(row.get("wallet") or ""),
            str(row.get("strategy_family") or ""),
            str(row.get("regime") or ""),
            str(row.get("risk_class") or ""),
            str(row.get("risk_signature") or ""),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


def _coverage(
    rows: list[dict[str, Any]], selected_wallets: list[str]
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    positive = [row for row in rows if allocator._profile_positive(row)]
    selected = set(selected_wallets)
    required: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in positive:
        key = (
            str(row.get("strategy_family") or "unknown_strategy"),
            str(row.get("regime") or "unknown"),
        )
        required[key].append(row)

    # Clean/hazard FOMO are additionally audited as separate specialist surfaces.
    for risk_class in ("clean_fomo", "hazard_fomo"):
        candidates = [
            row
            for row in positive
            if row.get("strategy_family") == "fomo_continuation"
            and row.get("risk_class") == risk_class
        ]
        if candidates:
            best = max(candidates, key=allocator._row_rank)
            required[
                (
                    f"fomo_continuation:{risk_class}",
                    str(best.get("regime") or "unknown"),
                )
            ] = candidates

    covered: list[dict[str, str]] = []
    debt: list[dict[str, str]] = []
    for key, candidates in sorted(required.items()):
        if any(str(row.get("wallet") or "") in selected for row in candidates):
            covered.append({"strategy_family": key[0], "regime": key[1]})
        else:
            debt.append(
                {
                    "strategy_family": key[0],
                    "regime": key[1],
                    "reason": "tracking_capacity_exhausted",
                }
            )
    return covered, debt


def build_strategy_specialist_tracking_plan_fair(
    rows: list[dict[str, Any]],
    *,
    capacity: int,
    candidate_states: dict[str, str] | None = None,
    fallback_wallets: list[str] | tuple[str, ...] = (),
) -> dict[str, Any]:
    """Give every active strategy one specialist before adding second-regime slots."""
    if _ORIGINAL_BUILD is None:
        raise RuntimeError("strategy specialist fairness repair is not installed")

    capacity = max(0, int(capacity))
    fallback = _normalized_fallback(fallback_wallets, candidate_states)
    primary_rows = _primary_strategy_rows(rows)

    first = _ORIGINAL_BUILD(
        primary_rows,
        capacity=capacity,
        candidate_states=candidate_states,
        fallback_wallets=(),
    )
    selected = list(first.get("selected_challenger_wallets") or [])
    assignments = list(first.get("context_assignments") or [])
    for row in assignments:
        if row.get("assignment_source") == "strategy_regime_specialist_floor":
            row["assignment_source"] = "strategy_family_specialist_floor"

    remaining_capacity = max(0, capacity - len(selected))
    if remaining_capacity:
        selected_set = set(selected)
        remaining_rows = [
            row
            for row in rows
            if str(row.get("wallet") or "") not in selected_set
        ]
        remaining_fallback = [wallet for wallet in fallback if wallet not in selected_set]
        second = _ORIGINAL_BUILD(
            remaining_rows,
            capacity=remaining_capacity,
            candidate_states=candidate_states,
            fallback_wallets=remaining_fallback,
        )
        selected.extend(
            wallet
            for wallet in (second.get("selected_challenger_wallets") or [])
            if wallet not in selected
        )
        assignments.extend(second.get("context_assignments") or [])
        bootstrap = list(second.get("bootstrap_observation_wallets") or [])
    else:
        bootstrap = []

    selected = selected[:capacity]
    covered, debt = _coverage(rows, selected)
    return {
        "allocation_version": REPAIR_VERSION,
        "capacity": capacity,
        "selected_challenger_wallets": selected,
        "context_assignments": assignments,
        "bootstrap_observation_wallets": bootstrap,
        "required_specialist_floor_count": len(covered) + len(debt),
        "covered_specialist_floor_count": len(covered),
        "covered_specialist_floor_keys": covered,
        "coverage_debt": debt,
        "strategy_family_first_pass_enabled": True,
        "strategy_regime_specialist_floor_enabled": True,
        "exact_context_metadata_retained": True,
        "fomo_dedicated_specialist_pool_enabled": True,
        "fomo_clean_hazard_separated": True,
        "high_risk_alpha_observation_cohort_enabled": True,
        "mechanical_hard_stops_relaxed": False,
        "remaining_capacity_filled_by_global_forward_roi": True,
        "seed_hypotheses_bootstrap_after_discovered_challengers": True,
        "cross_context_success_transfer_allowed": False,
        "bootstrap_slots_have_strategy_authority": False,
        "paper_only": True,
        "live_money_authority": False,
    }


def install_strategy_specialist_wallet_allocator_repair() -> None:
    global _ORIGINAL_BUILD
    current = allocator.build_strategy_specialist_tracking_plan
    if bool(getattr(current, "_roi_strategy_specialist_fairness_repair", False)):
        return
    _ORIGINAL_BUILD = current
    setattr(
        build_strategy_specialist_tracking_plan_fair,
        "_roi_strategy_specialist_fairness_repair",
        True,
    )
    allocator.build_strategy_specialist_tracking_plan = (
        build_strategy_specialist_tracking_plan_fair
    )


__all__ = [
    "REPAIR_VERSION",
    "build_strategy_specialist_tracking_plan_fair",
    "install_strategy_specialist_wallet_allocator_repair",
]
