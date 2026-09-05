from __future__ import annotations

import asyncio
import json
import math
import statistics
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Callable, Iterable

from . import regime_roi_wallet_authority as authority
from . import risk_conditioned_alpha_v5 as risk_v5
from . import robinhood_strategy_alignment_repair as alignment
from .robinhood_chain_profit_maximizer import (
    ROBINHOOD_V5_MAX_POSITION,
    ROBINHOOD_V5_MIN_SAMPLES,
    ROBINHOOD_V5_POSITION_GRID,
    ROBINHOOD_V5_VERSION,
)


WATCHLIST_VERSION = "robinhood-lane-specialist-watchlist-v1"
LANES = (
    "elite_entity_continuation",
    "creator_deployer_continuation",
    "entity_flow_accumulation",
    "fomo_continuation",
    "lifecycle_transition_continuation",
    "hazard_continuation",
)
LANE_DESCRIPTIONS = {
    "elite_entity_continuation": "repeat high-ROI independent entities",
    "creator_deployer_continuation": "creator/deployer entities with copyable continuation edge",
    "entity_flow_accumulation": "entities whose buys precede broader accumulation",
    "fomo_continuation": "entities with repeatable pre-FOMO/FOMO continuation edge",
    "lifecycle_transition_continuation": "entities with repeatable lifecycle-transition edge",
    "hazard_continuation": "entities with positive copyable edge in hazardous contexts",
}
LEADER_SLOTS_PER_LANE = 3
CHALLENGER_SLOTS_PER_LANE = 4
GLOBAL_RESEARCH_CHALLENGER_SLOTS = 12
WATCHLIST_MIN_FORWARD_SAMPLES = 5

_ORIGINAL_POLL: Callable[..., Any] | None = None
_ORIGINAL_STATUS: Callable[..., dict[str, Any]] | None = None
_BASE_CHOOSE: Callable[..., tuple[str | None, float, dict[str, Any]]] | None = None


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _trimmed_mean_ex_best(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) > 1:
        ordered = ordered[:-1]
    return statistics.mean(ordered)


def _leader_rank(row: dict[str, Any]) -> tuple[float, float, float, int, int]:
    robust = _safe_float(row.get("robust_forward_roi_pct"))
    growth = _safe_float(row.get("best_expected_log_growth"))
    median = _safe_float(row.get("median_forward_roi_pct"))
    return (
        robust if robust is not None else float("-inf"),
        growth if growth is not None else float("-inf"),
        median if median is not None else float("-inf"),
        int(row.get("sample_count") or 0),
        int(row.get("distinct_tokens") or 0),
    )


def _challenger_rank(row: dict[str, Any]) -> tuple[int, float, float, int, int, int]:
    robust = _safe_float(row.get("robust_forward_roi_pct"))
    growth = _safe_float(row.get("best_expected_log_growth"))
    positive = robust is not None and robust > 0.0
    return (
        1 if positive else 0,
        robust if robust is not None else float("-inf"),
        growth if growth is not None else float("-inf"),
        int(row.get("sample_count") or 0),
        int(row.get("distinct_tokens") or 0),
        int(row.get("trial_count") or 0),
    )


def build_lane_specialist_watchlist(
    rows: Iterable[dict[str, Any]],
    *,
    leader_slots: int = LEADER_SLOTS_PER_LANE,
    challenger_slots: int = CHALLENGER_SLOTS_PER_LANE,
) -> dict[str, Any]:
    """Build one Robinhood specialist roster per profit lane, never per regime.

    Regime remains part of the underlying execution evidence and therefore still
    affects exact-context profitability, sizing and promotion. It is deliberately
    not a roster key. This mirrors the Pump.fun wallet universe: track the best
    performers for the profit job, then condition their use on current context.
    """

    leader_slots = max(1, int(leader_slots))
    challenger_slots = max(0, int(challenger_slots))
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for raw in rows:
        row = dict(raw)
        lane = str(row.get("lane") or row.get("strategy_family") or "")
        entity = str(row.get("entity") or row.get("wallet") or "")
        if lane not in LANES or not entity:
            continue
        item = grouped.setdefault(
            (lane, entity),
            {
                "lane": lane,
                "entity": entity,
                "returns": [],
                "tokens": set(),
                "regimes": set(),
                "venues": set(),
                "lifecycles": set(),
                "trial_count": 0,
            },
        )
        item["trial_count"] += 1
        token = str(row.get("token") or "")
        if token:
            item["tokens"].add(token)
        regime = str(row.get("regime") or "unknown")
        if regime:
            item["regimes"].add(regime)
        venue = str(row.get("venue") or "")
        if venue:
            item["venues"].add(venue)
        lifecycle = str(row.get("lifecycle") or row.get("lifecycle_stage") or "")
        if lifecycle:
            item["lifecycles"].add(lifecycle)
        value = _safe_float(row.get("net_return"))
        if value is not None:
            item["returns"].append(value)

    lane_candidates: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for (lane, entity), item in grouped.items():
        values = list(item["returns"])
        profile = risk_v5.robust_return_profile(
            values,
            grid=ROBINHOOD_V5_POSITION_GRID,
            max_fraction=ROBINHOOD_V5_MAX_POSITION,
            min_samples=WATCHLIST_MIN_FORWARD_SAMPLES,
        )
        robust = _trimmed_mean_ex_best(values)
        mean_return = statistics.mean(values) if values else None
        median_return = statistics.median(values) if values else None
        leader_eligible = bool(
            profile.sample_count >= WATCHLIST_MIN_FORWARD_SAMPLES
            and robust is not None
            and robust > 0.0
            and profile.best_expected_log_growth is not None
            and profile.best_expected_log_growth > 0.0
            and profile.state == "promoted_positive_log_growth"
        )
        lane_candidates[lane].append(
            {
                "entity": entity,
                "lane": lane,
                "trial_count": int(item["trial_count"]),
                "sample_count": int(profile.sample_count),
                "distinct_tokens": len(item["tokens"]),
                "robust_forward_roi_pct": robust * 100.0 if robust is not None else None,
                "mean_forward_roi_pct": mean_return * 100.0 if mean_return is not None else None,
                "median_forward_roi_pct": median_return * 100.0 if median_return is not None else None,
                "best_expected_log_growth": profile.best_expected_log_growth,
                "regimes_observed": sorted(item["regimes"]),
                "venues_observed": sorted(item["venues"]),
                "lifecycles_observed": sorted(item["lifecycles"]),
                "watchlist_forward_mature": profile.sample_count >= WATCHLIST_MIN_FORWARD_SAMPLES,
                "leader_eligible": leader_eligible,
                "paper_promotion_authority": False,
                "regime_is_roster_dimension": False,
            }
        )

    lanes: list[dict[str, Any]] = []
    exact_lookup: dict[tuple[str, str], dict[str, Any]] = {}
    tracked_entities: set[str] = set()
    for lane in LANES:
        candidates = list(lane_candidates.get(lane, ()))
        leaders = sorted(
            (row for row in candidates if row["leader_eligible"]),
            key=_leader_rank,
            reverse=True,
        )[:leader_slots]
        leader_entities = {str(row["entity"]) for row in leaders}
        challengers = sorted(
            (row for row in candidates if str(row["entity"]) not in leader_entities),
            key=_challenger_rank,
            reverse=True,
        )[:challenger_slots]

        leader_payloads: list[dict[str, Any]] = []
        for rank, row in enumerate(leaders, start=1):
            payload = dict(row)
            payload.update({"rank": rank, "roster_state": "incumbent_tracking"})
            leader_payloads.append(payload)
            exact_lookup[(lane, str(row["entity"]))] = payload
            tracked_entities.add(str(row["entity"]))

        challenger_payloads: list[dict[str, Any]] = []
        for rank, row in enumerate(challengers, start=1):
            payload = dict(row)
            payload.update({"rank": rank, "roster_state": "challenger_tracking"})
            challenger_payloads.append(payload)
            exact_lookup[(lane, str(row["entity"]))] = payload
            tracked_entities.add(str(row["entity"]))

        lanes.append(
            {
                "lane": lane,
                "purpose": LANE_DESCRIPTIONS[lane],
                "leaders": leader_payloads,
                "challengers": challenger_payloads,
                "state": "specialist_leaders_available" if leader_payloads else "building_lane_watchlist",
                "regime_is_roster_dimension": False,
            }
        )

    return {
        "watchlist_version": WATCHLIST_VERSION,
        "ranking_objective": "robust_forward_roi_pct_then_expected_log_growth_after_costs",
        "dollar_profit_used_for_ranking": False,
        "roster_key": "robinhood_chain_x_profit_lane_x_entity",
        "regime_is_roster_dimension": False,
        "regime_still_conditions_execution": True,
        "venue_lifecycle_risk_still_condition_execution": True,
        "leader_slots_per_lane": leader_slots,
        "challenger_slots_per_lane": challenger_slots,
        "watchlist_forward_maturity_samples": WATCHLIST_MIN_FORWARD_SAMPLES,
        "paper_promotion_forward_maturity_samples": ROBINHOOD_V5_MIN_SAMPLES,
        "lanes": lanes,
        "exact_lookup": exact_lookup,
        "tracked_entities": sorted(tracked_entities),
        "tracked_entity_count": len(tracked_entities),
        "paper_promotion_authority": False,
        "paper_only": True,
        "live_money_authority": False,
    }


def _ensure_schema(self: Any) -> None:
    if bool(getattr(self, "_roi_robinhood_lane_watchlist_schema_ready", False)):
        return
    with self.store._lock, self.store.db:
        self.store.db.execute(
            "CREATE TABLE IF NOT EXISTS robinhood_lane_specialist_watchlist ("
            "lane TEXT NOT NULL, entity TEXT NOT NULL, roster_state TEXT NOT NULL, lane_rank INTEGER NOT NULL, "
            "sample_count INTEGER NOT NULL, trial_count INTEGER NOT NULL, distinct_tokens INTEGER NOT NULL, "
            "robust_forward_roi_pct REAL, mean_forward_roi_pct REAL, median_forward_roi_pct REAL, "
            "best_expected_log_growth REAL, regimes_json TEXT NOT NULL, venues_json TEXT NOT NULL, lifecycles_json TEXT NOT NULL, "
            "first_seen_at TEXT NOT NULL, updated_at TEXT NOT NULL, paper_promotion_authority INTEGER NOT NULL, "
            "PRIMARY KEY(lane,entity))"
        )
        self.store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_robinhood_lane_watchlist_state "
            "ON robinhood_lane_specialist_watchlist(lane,roster_state,lane_rank)"
        )
    setattr(self, "_roi_robinhood_lane_watchlist_schema_ready", True)


def _evidence_rows(self: Any) -> list[dict[str, Any]]:
    with self.store._lock:
        rows = self.store.db.execute(
            "SELECT t.trigger_entity AS entity,c.lane,t.token,t.venue,t.lifecycle,c.regime,"
            "CASE WHEN o.paper_only=1 THEN o.net_return ELSE NULL END AS net_return "
            "FROM robinhood_v5_trial_context c JOIN robinhood_paper_trials t ON t.id=c.trial_id "
            "LEFT JOIN robinhood_paper_outcomes o ON o.trial_id=c.trial_id "
            "WHERE c.strategy_version=? AND c.paper_only=1 AND t.paper_only=1 ORDER BY c.trial_id",
            (ROBINHOOD_V5_VERSION,),
        ).fetchall()
    return [dict(row) for row in rows]


def _priority_research_challengers(self: Any) -> list[dict[str, Any]]:
    try:
        rankings = alignment._research_rankings(self)
    except Exception:
        return []
    return [
        dict(row)
        for row in rankings
        if bool(row.get("priority_research_challenger"))
    ][:GLOBAL_RESEARCH_CHALLENGER_SLOTS]


def _payload(self: Any) -> dict[str, Any]:
    result = build_lane_specialist_watchlist(_evidence_rows(self))
    research = _priority_research_challengers(self)
    tracked = set(result["tracked_entities"])
    tracked.update(str(row.get("entity") or "") for row in research if str(row.get("entity") or ""))
    result.update(
        {
            "global_unassigned_research_challengers": research,
            "global_research_challenger_slots": GLOBAL_RESEARCH_CHALLENGER_SLOTS,
            "tracked_entities": sorted(tracked),
            "tracked_entity_count": len(tracked),
            "tracking_transport": "chain_wide_ingestion_matches_specialists_in_already_ingested_swaps",
            "new_wallet_specific_provider_polling_added": False,
            "provider_requests_added": 0,
            "regime_roi_entity_authority_is_roster_authority": False,
            "regime_roi_entity_authority_retained_for_exact_context_diagnostics": True,
            "exact_executable_quotes_still_required": True,
            "entity_exact_forward_paper_promotion_still_required": True,
            "cross_chain_success_transfer_allowed": False,
            "paper_only": True,
            "live_money_authority": False,
        }
    )
    return result


def _persist(self: Any, payload: dict[str, Any]) -> None:
    _ensure_schema(self)
    now = _utcnow()
    selected: list[dict[str, Any]] = []
    for lane in payload.get("lanes") or []:
        selected.extend(lane.get("leaders") or [])
        selected.extend(lane.get("challengers") or [])
    with self.store._lock, self.store.db:
        existing_rows = self.store.db.execute(
            "SELECT lane,entity,first_seen_at FROM robinhood_lane_specialist_watchlist"
        ).fetchall()
        first_seen = {
            (str(row["lane"]), str(row["entity"])): str(row["first_seen_at"])
            for row in existing_rows
        }
        self.store.db.execute("DELETE FROM robinhood_lane_specialist_watchlist")
        for row in selected:
            lane = str(row["lane"])
            entity = str(row["entity"])
            self.store.db.execute(
                "INSERT INTO robinhood_lane_specialist_watchlist("
                "lane,entity,roster_state,lane_rank,sample_count,trial_count,distinct_tokens,robust_forward_roi_pct,"
                "mean_forward_roi_pct,median_forward_roi_pct,best_expected_log_growth,regimes_json,venues_json,lifecycles_json,"
                "first_seen_at,updated_at,paper_promotion_authority) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0)",
                (
                    lane,
                    entity,
                    str(row["roster_state"]),
                    int(row["rank"]),
                    int(row["sample_count"]),
                    int(row["trial_count"]),
                    int(row["distinct_tokens"]),
                    row.get("robust_forward_roi_pct"),
                    row.get("mean_forward_roi_pct"),
                    row.get("median_forward_roi_pct"),
                    row.get("best_expected_log_growth"),
                    json.dumps(row.get("regimes_observed") or []),
                    json.dumps(row.get("venues_observed") or []),
                    json.dumps(row.get("lifecycles_observed") or []),
                    first_seen.get((lane, entity), now),
                    now,
                ),
            )
    setattr(self, "_roi_robinhood_lane_watchlist_last_refresh", now)


def _assignment(payload: dict[str, Any], *, lane: str, entity: str) -> dict[str, Any]:
    row = (payload.get("exact_lookup") or {}).get((lane, entity))
    if row is not None:
        return {
            "state": str(row.get("roster_state") or "challenger_tracking"),
            "lane": lane,
            "entity": entity,
            "rank": int(row.get("rank") or 0),
            "robust_forward_roi_pct": row.get("robust_forward_roi_pct"),
            "sample_count": int(row.get("sample_count") or 0),
            "regime_is_roster_dimension": False,
            "paper_promotion_authority": False,
        }
    leaders_exist = any(
        str(item.get("lane") or "") == lane and bool(item.get("leaders"))
        for item in payload.get("lanes") or []
    )
    return {
        "state": "unranked_lane_challenger" if leaders_exist else "lane_bootstrap_tracking",
        "lane": lane,
        "entity": entity,
        "rank": None,
        "robust_forward_roi_pct": None,
        "sample_count": 0,
        "regime_is_roster_dimension": False,
        "paper_promotion_authority": False,
    }


def apply_lane_specialist_fraction(fraction: float, assignment: dict[str, Any]) -> float:
    fraction = max(0.0, float(fraction))
    state = str(assignment.get("state") or "")
    if state == "incumbent_tracking":
        rank = int(assignment.get("rank") or 1)
        return fraction * {1: 1.0, 2: 0.85, 3: 0.70}.get(rank, 0.60)
    if state in {"challenger_tracking", "unranked_lane_challenger"}:
        return min(fraction, authority.CHALLENGER_FRACTION_CAP)
    return min(fraction, authority.UNPROVEN_CONTEXT_FRACTION_CAP)


def _choose_with_lane_specialists(self: Any, **kwargs: Any) -> tuple[str | None, float, dict[str, Any]]:
    if _BASE_CHOOSE is None:
        raise RuntimeError("Robinhood lane specialist watchlist is not installed")
    # Bypass the old regime-roster wrapper only. The underlying chooser still uses
    # entity x lane x venue x lifecycle x regime x risk x flow context, so regime
    # remains a strategy/sizing input without becoming a wallet-list dimension.
    lane, fraction, profiles = _BASE_CHOOSE(self, **kwargs)
    if lane is None or fraction <= 0.0:
        return lane, fraction, profiles
    try:
        watchlist = _payload(self)
        assignment = _assignment(
            watchlist,
            lane=str(lane),
            entity=str(kwargs.get("entity") or ""),
        )
    except Exception as exc:
        assignment = {
            "state": "lane_bootstrap_tracking",
            "lane": str(lane),
            "entity": str(kwargs.get("entity") or ""),
            "rank": None,
            "regime_is_roster_dimension": False,
            "paper_promotion_authority": False,
            "error": f"{type(exc).__name__}: lane watchlist unavailable",
        }
    profiles = dict(profiles)
    profiles["_robinhood_lane_specialist_watchlist"] = assignment
    return lane, apply_lane_specialist_fraction(fraction, assignment), profiles


async def _poll_once_with_lane_watchlist(self: Any) -> None:
    if _ORIGINAL_POLL is None:
        raise RuntimeError("Robinhood lane watchlist poll wrapper is not installed")
    await _ORIGINAL_POLL(self)
    try:
        payload = _payload(self)
        _persist(self, payload)
        setattr(self, "_roi_robinhood_lane_watchlist_last_error", None)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        setattr(
            self,
            "_roi_robinhood_lane_watchlist_last_error",
            f"{type(exc).__name__}: lane specialist watchlist refresh unavailable",
        )


def _status_with_lane_watchlist(self: Any) -> dict[str, Any]:
    if _ORIGINAL_STATUS is None:
        raise RuntimeError("Robinhood lane watchlist status wrapper is not installed")
    payload = dict(_ORIGINAL_STATUS(self))
    try:
        watchlist = _payload(self)
        watchlist["last_refresh_at"] = getattr(self, "_roi_robinhood_lane_watchlist_last_refresh", None)
        watchlist["last_error"] = getattr(self, "_roi_robinhood_lane_watchlist_last_error", None)
        payload["lane_specialist_watchlist"] = watchlist
        payload["wallet_roster_structure"] = {
            "primary_structure": "profit_lane_then_ranked_entity",
            "mirrors_pumpfun_role_specialist_model": True,
            "regime_requires_dedicated_wallet": False,
            "regime_is_execution_context_not_wallet_roster": True,
            "paper_only": True,
            "live_money_authority": False,
        }
    except Exception as exc:
        payload["lane_specialist_watchlist"] = {
            "watchlist_version": WATCHLIST_VERSION,
            "failed_closed": True,
            "error": f"{type(exc).__name__}: lane specialist watchlist status unavailable",
            "regime_is_roster_dimension": False,
            "provider_requests_added": 0,
            "paper_only": True,
            "live_money_authority": False,
        }
    return payload


def install_robinhood_lane_specialist_watchlist(plane_cls: type[Any]) -> None:
    """Make Robinhood wallet tracking lane-first like Pump.fun's specialist roster.

    This replaces only Robinhood's regime-as-roster sizing wrapper. The underlying
    v5/v5.1 chooser still conditions profitability and sizing on regime, venue,
    lifecycle, role, risk, flow and exact executable quotes. The watchlist itself is
    observation/tracking authority, ranks by percentage ROI, adds no provider calls,
    and cannot independently promote paper or live-money execution.
    """

    global _ORIGINAL_POLL, _ORIGINAL_STATUS, _BASE_CHOOSE
    if bool(getattr(plane_cls, "_roi_robinhood_lane_specialist_watchlist_installed", False)):
        return

    # regime_roi_wallet_authority captured the pre-regime Robinhood chooser when it
    # installed. Reuse that exact base so regimes stop defining the roster while all
    # strategy/context logic beneath the wrapper remains intact.
    _BASE_CHOOSE = authority._ORIGINAL_ROBINHOOD_CHOOSE or plane_cls._v5_choose_lane_fraction
    setattr(_choose_with_lane_specialists, "_roi_robinhood_lane_specialist_watchlist", True)
    plane_cls._v5_choose_lane_fraction = _choose_with_lane_specialists  # type: ignore[method-assign]

    _ORIGINAL_POLL = plane_cls._poll_once
    setattr(_poll_once_with_lane_watchlist, "_roi_robinhood_lane_specialist_watchlist", True)
    plane_cls._poll_once = _poll_once_with_lane_watchlist  # type: ignore[method-assign]

    _ORIGINAL_STATUS = plane_cls.status
    setattr(_status_with_lane_watchlist, "_roi_robinhood_lane_specialist_watchlist", True)
    plane_cls.status = _status_with_lane_watchlist  # type: ignore[method-assign]

    setattr(plane_cls, "_roi_robinhood_lane_specialist_watchlist_installed", True)
    setattr(plane_cls, "_roi_robinhood_lane_specialist_watchlist_version", WATCHLIST_VERSION)


__all__ = [
    "WATCHLIST_VERSION",
    "LANES",
    "LEADER_SLOTS_PER_LANE",
    "CHALLENGER_SLOTS_PER_LANE",
    "WATCHLIST_MIN_FORWARD_SAMPLES",
    "build_lane_specialist_watchlist",
    "apply_lane_specialist_fraction",
    "install_robinhood_lane_specialist_watchlist",
]
