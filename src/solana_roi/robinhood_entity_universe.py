from __future__ import annotations

import asyncio
import math
import statistics
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Callable, Iterable

from . import regime_roi_wallet_authority as authority
from . import robinhood_strategy_alignment_repair as alignment
from .robinhood_chain_profit_maximizer import ROBINHOOD_V5_VERSION


UNIVERSE_VERSION = "robinhood-entity-universe-v1"
TRACKING_CAPACITY_LIMIT = 12
MIN_CHALLENGER_SLOTS = 4
MIN_MATURE_FORWARD_SAMPLES = 5

ROLES = (
    "scout_alpha",
    "creator_alpha",
    "momentum_alpha",
    "confirmation_alpha",
    "exit_alpha",
    "distribution_warning_value",
    "copyable_return_on_capital",
    "signal_decay",
)

LANE_ROLE_MAP: dict[str, tuple[str, ...]] = {
    "elite_entity_continuation": (
        "scout_alpha",
        "momentum_alpha",
        "copyable_return_on_capital",
    ),
    "creator_deployer_continuation": (
        "creator_alpha",
        "momentum_alpha",
        "copyable_return_on_capital",
    ),
    "entity_flow_accumulation": (
        "momentum_alpha",
        "confirmation_alpha",
        "copyable_return_on_capital",
    ),
    "fomo_continuation": (
        "momentum_alpha",
        "signal_decay",
        "copyable_return_on_capital",
    ),
    "lifecycle_transition_continuation": (
        "scout_alpha",
        "momentum_alpha",
        "copyable_return_on_capital",
    ),
    "hazard_continuation": (
        "momentum_alpha",
        "distribution_warning_value",
        "copyable_return_on_capital",
    ),
}

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


def score_role(role: str, residual_returns: Iterable[float]) -> dict[str, Any]:
    values = [float(value) for value in residual_returns if _safe_float(value) is not None]
    if not values:
        return {
            "role": role,
            "sample_count": 0,
            "mean_residual_return": None,
            "geometric_value": None,
            "positive_rate": None,
            "confidence": 0.0,
            "score": None,
        }
    log_terms = [math.log(max(1e-9, 1.0 + value)) for value in values]
    geometric = math.exp(statistics.mean(log_terms)) - 1.0
    confidence = min(1.0, math.sqrt(len(values) / 30.0))
    return {
        "role": role,
        "sample_count": len(values),
        "mean_residual_return": statistics.mean(values),
        "geometric_value": geometric,
        "positive_rate": sum(value > 0.0 for value in values) / len(values),
        "confidence": confidence,
        "score": statistics.mean(log_terms) * confidence,
    }


def _role_returns(rows: Iterable[dict[str, Any]]) -> dict[str, dict[str, list[float]]]:
    result: dict[str, dict[str, list[float]]] = {}
    for raw in rows:
        row = dict(raw)
        entity = str(row.get("entity") or row.get("trigger_entity") or "")
        lane = str(row.get("lane") or "")
        value = _safe_float(row.get("net_return"))
        if not entity or value is None:
            continue
        roles = LANE_ROLE_MAP.get(lane, ("copyable_return_on_capital",))
        for role in roles:
            result.setdefault(entity, {}).setdefault(role, []).append(value)
    return result


def _role_scores(rows: Iterable[dict[str, Any]]) -> dict[str, dict[str, dict[str, Any]]]:
    return {
        entity: {role: score_role(role, values) for role, values in roles.items()}
        for entity, roles in _role_returns(rows).items()
    }


def _forward_priority(entity: str, scores: dict[str, dict[str, dict[str, Any]]]) -> tuple[int, float, int]:
    role_scores = scores.get(entity, {})
    available = [item for item in role_scores.values() if item.get("score") is not None]
    samples = max((int(item.get("sample_count") or 0) for item in available), default=0)
    best = max((float(item["score"]) for item in available), default=float("-inf"))
    mature = 1 if samples >= MIN_MATURE_FORWARD_SAMPLES else 0
    return mature, best, samples


def _research_index(research_rows: Iterable[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for raw in research_rows:
        row = dict(raw)
        entity = str(row.get("entity") or "")
        if entity:
            result[entity] = row
    return result


def build_entity_universe(
    evidence_rows: Iterable[dict[str, Any]],
    research_rows: Iterable[dict[str, Any]] = (),
    *,
    capacity: int = TRACKING_CAPACITY_LIMIT,
) -> dict[str, Any]:
    """Select one dynamic Robinhood entity set, mirroring Pump.fun's wallet universe.

    Roles describe what each entity has proven useful for. Roles, lanes, regimes,
    venues and lifecycle states are not separate watchlists. A single high-priority
    set is selected from forward role evidence plus bounded discovery challengers.
    """

    evidence = [dict(row) for row in evidence_rows]
    research = [dict(row) for row in research_rows]
    capacity = max(1, int(capacity))
    scores = _role_scores(evidence)
    research_by_entity = _research_index(research)

    entities = set(scores)
    entities.update(research_by_entity)

    # Pump.fun reserves scarce observation bandwidth for newly discovered challengers
    # before filling the remaining global slots by forward role value. Do the same
    # here, but without creating a lane-specific or regime-specific roster.
    priority_research = [
        row for row in research
        if str(row.get("entity") or "") and bool(row.get("priority_research_challenger"))
    ]
    priority_research.sort(
        key=lambda row: (
            _safe_float(row.get("trimmed_mean_120s_followthrough_ex_best_1_pct"))
            if _safe_float(row.get("trimmed_mean_120s_followthrough_ex_best_1_pct")) is not None
            else float("-inf"),
            int(row.get("marked_buy_observations") or 0),
            int(row.get("distinct_tokens") or 0),
        ),
        reverse=True,
    )

    selected: list[str] = []
    challenger_slots = min(capacity, MIN_CHALLENGER_SLOTS, len(priority_research))
    for row in priority_research[:challenger_slots]:
        entity = str(row.get("entity") or "")
        if entity and entity not in selected:
            selected.append(entity)

    def numeric_priority(entity: str) -> tuple[float, ...]:
        mature, forward_score, samples = _forward_priority(entity, scores)
        research_row = research_by_entity.get(entity, {})
        research_roi = _safe_float(research_row.get("trimmed_mean_120s_followthrough_ex_best_1_pct"))
        research_rank = int(research_row.get("research_rank") or 999999)
        return (
            float(mature),
            forward_score if math.isfinite(forward_score) else -999.0,
            float(samples),
            research_roi if research_roi is not None else -999.0,
            -float(research_rank),
        )

    remaining = [entity for entity in entities if entity not in set(selected)]
    remaining.sort(key=numeric_priority, reverse=True)
    for entity in remaining:
        if len(selected) >= capacity:
            break
        selected.append(entity)

    role_leaders: dict[str, list[dict[str, Any]]] = {}
    for role in ROLES:
        leaders: list[dict[str, Any]] = []
        for entity, entity_scores in scores.items():
            score = entity_scores.get(role)
            if score is None or score.get("score") is None:
                continue
            leaders.append({"entity": entity, **score})
        leaders.sort(
            key=lambda row: (float(row.get("score") or float("-inf")), int(row.get("sample_count") or 0)),
            reverse=True,
        )
        role_leaders[role] = leaders[:5]

    current_roles: list[dict[str, Any]] = []
    blockers: list[dict[str, Any]] = []
    for rank, entity in enumerate(selected, start=1):
        entity_scores = scores.get(entity, {})
        ranked = sorted(
            (item for item in entity_scores.values() if item.get("score") is not None),
            key=lambda item: (float(item.get("score") or float("-inf")), int(item.get("sample_count") or 0)),
            reverse=True,
        )
        best = ranked[0] if ranked else None
        total_samples = max((int(item.get("sample_count") or 0) for item in entity_scores.values()), default=0)
        current_roles.append(
            {
                "rank": rank,
                "entity": entity,
                "current_role": str(best.get("role")) if best is not None else None,
                "role_is_forward_evidence_backed": best is not None,
                "forward_sample_count": total_samples,
                "research_rank": research_by_entity.get(entity, {}).get("research_rank"),
            }
        )
        entity_blockers: list[str] = []
        if total_samples < MIN_MATURE_FORWARD_SAMPLES:
            entity_blockers.append("insufficient_forward_role_samples")
        if best is None or best.get("score") is None or float(best.get("score") or 0.0) <= 0.0:
            entity_blockers.append("no_positive_forward_geometric_value")
        if entity_blockers:
            blockers.append({"entity": entity, "blockers": entity_blockers})

    regime_values: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    for row in evidence:
        entity = str(row.get("entity") or "")
        regime = str(row.get("regime") or "unknown")
        value = _safe_float(row.get("net_return"))
        if entity and value is not None:
            regime_values[regime][entity].append(value)
    regime_entity_value: dict[str, list[dict[str, Any]]] = {}
    for regime, by_entity in regime_values.items():
        values: list[dict[str, Any]] = []
        for entity, returns in by_entity.items():
            score = score_role("copyable_return_on_capital", returns)
            if score.get("score") is not None:
                values.append({"entity": entity, **score})
        values.sort(
            key=lambda row: (float(row.get("score") or float("-inf")), int(row.get("sample_count") or 0)),
            reverse=True,
        )
        regime_entity_value[regime] = values[:5]

    discovered_challengers = [
        str(row.get("entity") or "")
        for row in priority_research
        if str(row.get("entity") or "") in selected
    ]

    return {
        "universe_version": UNIVERSE_VERSION,
        "architecture": "large_observation_universe_to_economic_entities_to_role_alpha_to_dynamic_high_priority_set",
        "roster_key": "robinhood_chain_x_economic_entity",
        "tracking_capacity_limit": capacity,
        "high_priority_entities": selected,
        "high_priority_entity_count": len(selected),
        "active_seed_entities": [],
        "discovered_challengers": discovered_challengers,
        "current_role_for_high_priority_entity": current_roles,
        "role_leaders": role_leaders,
        "regime_entity_value": regime_entity_value,
        "candidate_promotion_blockers": blockers,
        "lane_specific_watchlists": False,
        "regime_specific_watchlists": False,
        "roles_are_scores_not_rosters": True,
        "lanes_are_strategy_context_not_rosters": True,
        "regimes_are_strategy_context_not_rosters": True,
        "named_seed_is_permanent_whitelist": False,
        "challengers_can_replace_incumbents_for_future_influence": True,
        "historical_or_mark_evidence_has_paper_promotion_authority": False,
        "exact_executable_quotes_still_required": True,
        "entity_exact_forward_paper_promotion_still_required": True,
        "tracking_selection_independently_authorizes_entry": False,
        "new_wallet_specific_provider_polling_added": False,
        "provider_requests_added": 0,
        "paper_only": True,
        "live_money_authority": False,
    }


def _ensure_schema(self: Any) -> None:
    if bool(getattr(self, "_roi_robinhood_entity_universe_schema_ready", False)):
        return
    with self.store._lock, self.store.db:
        self.store.db.execute(
            "CREATE TABLE IF NOT EXISTS robinhood_entity_universe ("
            "entity TEXT PRIMARY KEY, universe_rank INTEGER NOT NULL, state TEXT NOT NULL, "
            "current_role TEXT, forward_sample_count INTEGER NOT NULL, research_rank INTEGER, "
            "first_seen_at TEXT NOT NULL, updated_at TEXT NOT NULL, paper_promotion_authority INTEGER NOT NULL)"
        )
        self.store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_robinhood_entity_universe_rank "
            "ON robinhood_entity_universe(universe_rank,state)"
        )
    setattr(self, "_roi_robinhood_entity_universe_schema_ready", True)


def _evidence_rows(self: Any) -> list[dict[str, Any]]:
    with self.store._lock:
        rows = self.store.db.execute(
            "SELECT t.trigger_entity AS entity,c.lane,t.token,t.venue,t.lifecycle,c.regime,o.net_return "
            "FROM robinhood_v5_trial_context c JOIN robinhood_paper_trials t ON t.id=c.trial_id "
            "LEFT JOIN robinhood_paper_outcomes o ON o.trial_id=c.trial_id "
            "WHERE c.strategy_version=? AND c.paper_only=1 AND t.paper_only=1 ORDER BY c.trial_id",
            (ROBINHOOD_V5_VERSION,),
        ).fetchall()
    return [dict(row) for row in rows]


def _research_rows(self: Any) -> list[dict[str, Any]]:
    try:
        return [dict(row) for row in alignment._research_rankings(self)]
    except Exception:
        return []


def _payload(self: Any) -> dict[str, Any]:
    payload = build_entity_universe(_evidence_rows(self), _research_rows(self))
    try:
        with self.store._lock:
            row = self.store.db.execute("SELECT COUNT(DISTINCT actor) FROM robinhood_swaps").fetchone()
            payload["total_observed_addresses"] = int(row[0] if row is not None else 0)
            row = self.store.db.execute("SELECT COUNT(DISTINCT entity) FROM robinhood_entity_discovery_observations").fetchone()
            payload["known_candidate_entities"] = int(row[0] if row is not None else 0)
    except Exception:
        payload["total_observed_addresses"] = None
        payload["known_candidate_entities"] = None
    return payload


def _persist(self: Any, payload: dict[str, Any]) -> None:
    _ensure_schema(self)
    now = _utcnow()
    rows = list(payload.get("current_role_for_high_priority_entity") or [])
    with self.store._lock, self.store.db:
        existing = self.store.db.execute("SELECT entity,first_seen_at FROM robinhood_entity_universe").fetchall()
        first_seen = {str(row["entity"]): str(row["first_seen_at"]) for row in existing}
        self.store.db.execute("DELETE FROM robinhood_entity_universe")
        for row in rows:
            entity = str(row.get("entity") or "")
            if not entity:
                continue
            self.store.db.execute(
                "INSERT INTO robinhood_entity_universe("
                "entity,universe_rank,state,current_role,forward_sample_count,research_rank,first_seen_at,updated_at,paper_promotion_authority) "
                "VALUES (?,?,?,?,?,?,?,?,0)",
                (
                    entity,
                    int(row.get("rank") or 0),
                    "high_priority_tracking",
                    row.get("current_role"),
                    int(row.get("forward_sample_count") or 0),
                    row.get("research_rank"),
                    first_seen.get(entity, now),
                    now,
                ),
            )
    setattr(self, "_roi_robinhood_entity_universe_last_refresh", now)


def _assignment(payload: dict[str, Any], *, entity: str) -> dict[str, Any]:
    rows = list(payload.get("current_role_for_high_priority_entity") or [])
    for row in rows:
        if str(row.get("entity") or "") == entity:
            return {
                "state": "high_priority_tracking",
                "entity": entity,
                "rank": int(row.get("rank") or 0),
                "current_role": row.get("current_role"),
                "forward_sample_count": int(row.get("forward_sample_count") or 0),
                "tracking_selection_independently_authorizes_entry": False,
            }
    return {
        "state": "observed_global_challenger",
        "entity": entity,
        "rank": None,
        "current_role": None,
        "forward_sample_count": 0,
        "tracking_selection_independently_authorizes_entry": False,
    }


def _choose_with_entity_universe(self: Any, **kwargs: Any) -> tuple[str | None, float, dict[str, Any]]:
    if _BASE_CHOOSE is None:
        raise RuntimeError("Robinhood entity universe is not installed")
    # Bypass only the old regime-roster wrapper. The canonical chooser beneath it
    # still evaluates entity x lane x venue x lifecycle x regime x risk x flow and
    # exact executable quotes. The global tracking set itself does not resize or
    # authorize entries, matching Pump.fun's observation-universe separation.
    lane, fraction, profiles = _BASE_CHOOSE(self, **kwargs)
    profiles = dict(profiles)
    try:
        profiles["_robinhood_entity_universe"] = _assignment(
            _payload(self),
            entity=str(kwargs.get("entity") or ""),
        )
    except Exception as exc:
        profiles["_robinhood_entity_universe"] = {
            "state": "universe_status_unavailable",
            "entity": str(kwargs.get("entity") or ""),
            "error": f"{type(exc).__name__}: entity universe unavailable",
            "tracking_selection_independently_authorizes_entry": False,
        }
    return lane, fraction, profiles


async def _poll_once_with_entity_universe(self: Any) -> None:
    if _ORIGINAL_POLL is None:
        raise RuntimeError("Robinhood entity universe poll wrapper is not installed")
    await _ORIGINAL_POLL(self)
    try:
        _persist(self, _payload(self))
        setattr(self, "_roi_robinhood_entity_universe_last_error", None)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        setattr(
            self,
            "_roi_robinhood_entity_universe_last_error",
            f"{type(exc).__name__}: entity universe refresh unavailable",
        )


def _status_with_entity_universe(self: Any) -> dict[str, Any]:
    if _ORIGINAL_STATUS is None:
        raise RuntimeError("Robinhood entity universe status wrapper is not installed")
    payload = dict(_ORIGINAL_STATUS(self))
    payload.pop("lane_specialist_watchlist", None)
    try:
        universe = _payload(self)
        universe["last_refresh_at"] = getattr(self, "_roi_robinhood_entity_universe_last_refresh", None)
        universe["last_error"] = getattr(self, "_roi_robinhood_entity_universe_last_error", None)
        payload["entity_universe"] = universe
        payload["wallet_roster_structure"] = {
            "primary_structure": "single_global_entity_universe",
            "mirrors_pumpfun_wallet_entity_universe_v4": True,
            "lane_specific_wallet_watchlists": False,
            "regime_specific_wallet_watchlists": False,
            "roles_are_entity_scores_not_separate_rosters": True,
            "regime_is_execution_context_not_wallet_roster": True,
            "paper_only": True,
            "live_money_authority": False,
        }
        regime_payload = payload.get("regime_roi_entity_authority")
        if isinstance(regime_payload, dict):
            regime_payload["roster_authority"] = False
            regime_payload["diagnostic_context_only"] = True
    except Exception as exc:
        payload["entity_universe"] = {
            "universe_version": UNIVERSE_VERSION,
            "failed_closed": True,
            "error": f"{type(exc).__name__}: global Robinhood entity universe status unavailable",
            "lane_specific_watchlists": False,
            "regime_specific_watchlists": False,
            "paper_only": True,
            "live_money_authority": False,
        }
    return payload


def install_robinhood_entity_universe(plane_cls: type[Any]) -> None:
    """Install one Pump.fun-style global entity universe for Robinhood Chain."""

    global _ORIGINAL_POLL, _ORIGINAL_STATUS, _BASE_CHOOSE
    if bool(getattr(plane_cls, "_roi_robinhood_entity_universe_installed", False)):
        return

    _BASE_CHOOSE = authority._ORIGINAL_ROBINHOOD_CHOOSE or plane_cls._v5_choose_lane_fraction
    setattr(_choose_with_entity_universe, "_roi_robinhood_entity_universe", True)
    plane_cls._v5_choose_lane_fraction = _choose_with_entity_universe  # type: ignore[method-assign]

    _ORIGINAL_POLL = plane_cls._poll_once
    setattr(_poll_once_with_entity_universe, "_roi_robinhood_entity_universe", True)
    plane_cls._poll_once = _poll_once_with_entity_universe  # type: ignore[method-assign]

    _ORIGINAL_STATUS = plane_cls.status
    setattr(_status_with_entity_universe, "_roi_robinhood_entity_universe", True)
    plane_cls.status = _status_with_entity_universe  # type: ignore[method-assign]

    setattr(plane_cls, "_roi_robinhood_entity_universe_installed", True)
    setattr(plane_cls, "_roi_robinhood_entity_universe_version", UNIVERSE_VERSION)


__all__ = [
    "UNIVERSE_VERSION",
    "TRACKING_CAPACITY_LIMIT",
    "MIN_CHALLENGER_SLOTS",
    "MIN_MATURE_FORWARD_SAMPLES",
    "ROLES",
    "LANE_ROLE_MAP",
    "score_role",
    "build_entity_universe",
    "install_robinhood_entity_universe",
]
