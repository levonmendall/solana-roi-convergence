from __future__ import annotations

"""Narrow operational-integrity repairs for the frozen v5.2 paper strategy.

This module changes no qualification threshold, sizing policy, exit trigger, or
wallet-intelligence rule. It repairs two accounting/evidence defects at the
existing Robinhood production boundary:

* flow evidence is evaluated at an event-time decision cutoff, never with swaps
  that occur after that cutoff; and
* staged-exit NAV treats partial exits from one managed position as additive
  realized portfolio-return contributions instead of compounding each slice as
  if it were a new portfolio.

The module is intentionally installed before the v5.2 Robinhood lifecycle
installer captures its final methods.
"""

import asyncio
import math
import time
from collections import OrderedDict
from typing import Any, Iterable

from .robinhood_chain_core import KNOWN_NON_ACTORS, _clean_address
from .robinhood_chain_profit_maximizer import RobinhoodProfitMaximizerMixin
from . import v52_robinhood_position_lifecycle as robinhood_lifecycle


REPAIR_VERSION = "v52-operational-integrity-point-in-time-nav-1"
_INSTALLED = False
_BASE_FLOW_METRICS: Any | None = None
_BASE_LIFECYCLE_NAV: Any | None = None


def _finite_number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return number


def _decision_cutoff(swaps: list[Any], explicit: float | None = None) -> float:
    value = _finite_number(explicit)
    if value is not None and value > 0.0:
        return value
    for raw in reversed(swaps):
        if not isinstance(raw, dict):
            continue
        observed = _finite_number(raw.get("observed_ts"))
        if observed is not None and observed > 0.0:
            return observed
    return time.time()


async def _point_in_time_flow_metrics(
    self: Any,
    swaps: Any,
    *,
    deployer: str = "",
    decision_cutoff_ts: float | None = None,
) -> dict[str, Any]:
    """Compute the existing v5 flow metrics using only evidence knowable at T."""
    items = list(swaps or ())
    cutoff = _decision_cutoff(items, decision_cutoff_ts)

    def age_seconds(raw: Any) -> float | None:
        if not isinstance(raw, dict):
            return None
        observed = _finite_number(raw.get("observed_ts"))
        if observed is None or observed <= 0.0:
            return None
        return cutoff - observed

    current: list[dict[str, Any]] = []
    prior: list[dict[str, Any]] = []
    for raw in items:
        age = age_seconds(raw)
        if age is None or age < 0.0:
            continue
        if age <= 60.0:
            current.append(raw)
        elif age <= 120.0:
            prior.append(raw)

    buys = [s for s in current if s.get("side") == "buy"]
    sells = [s for s in current if s.get("side") == "sell"]
    prior_buys = [s for s in prior if s.get("side") == "buy"]
    actors: list[str] = []
    for swap in buys:
        actor = _clean_address(str(swap.get("actor") or ""))
        if actor and actor not in KNOWN_NON_ACTORS and actor not in actors:
            actors.append(actor)
    actors = actors[-12:]
    anchors = await asyncio.gather(*(self._entity_anchor(actor) for actor in actors)) if actors else []
    if any(anchor is None for anchor in anchors):
        return {
            "state": "entity_resolution_incomplete",
            "entity_resolution_complete": False,
            "trigger_actor": "",
            "trigger_entity": "",
            "independent_entities_60s": 0,
            "buy_sell_quote_ratio": 0.0,
            "buy_count_acceleration": 0.0,
            "buy_quote_wei": 0,
            "sell_quote_wei": 0,
            "creator_sell_quote_wei": 0,
            "decision_cutoff_ts": cutoff,
        }
    mapping = {actor: str(anchor) for actor, anchor in zip(actors, anchors) if anchor}
    deployer = _clean_address(deployer)
    deployer_anchor = await self._entity_anchor(deployer) if deployer else None
    trigger_actor = _clean_address(str(buys[-1].get("actor") or "")) if buys else ""
    trigger_entity = mapping.get(trigger_actor, trigger_actor)
    independent = {anchor for anchor in mapping.values() if anchor and anchor != deployer_anchor}
    buy_quote = sum(int(s.get("quote_amount_wei") or 0) for s in buys)
    sell_quote = sum(int(s.get("quote_amount_wei") or 0) for s in sells)
    creator_sell_quote = sum(
        int(s.get("quote_amount_wei") or 0)
        for s in sells
        if deployer and _clean_address(str(s.get("actor") or "")) == deployer
    )
    ratio = buy_quote / max(1, sell_quote)
    acceleration = len(buys) / max(1, len(prior_buys))
    prices: list[float] = []
    for swap in current:
        price = _finite_number(swap.get("price_eth"))
        if price not in (None, 0.0):
            prices.append(float(price))
    price_change = prices[-1] / prices[0] - 1.0 if len(prices) >= 2 and prices[0] > 0 else 0.0
    if len(buys) >= 4 and len(independent) >= 3 and ratio >= 1.5 and acceleration >= 1.25 and 0.01 <= price_change <= 0.40:
        state = "active_fomo"
    elif len(buys) >= 3 and len(independent) >= 2 and ratio >= 1.15 and price_change <= 0.40:
        state = "pre_fomo"
    elif len(independent) >= 2 and buy_quote > sell_quote:
        state = "entity_accumulation"
    elif sells and sell_quote > buy_quote:
        state = "exhaustion"
    else:
        state = "neutral"
    return {
        "state": state,
        "entity_resolution_complete": True,
        "trigger_actor": trigger_actor,
        "trigger_entity": trigger_entity,
        "trigger_is_creator": bool(deployer_anchor and trigger_entity == deployer_anchor),
        "deployer_entity": deployer_anchor or "",
        "independent_entities_60s": len(independent),
        "buy_count_60s": len(buys),
        "sell_count_60s": len(sells),
        "buy_sell_quote_ratio": ratio,
        "buy_count_acceleration": acceleration,
        "price_change_60s": price_change,
        "buy_quote_wei": buy_quote,
        "sell_quote_wei": sell_quote,
        "creator_sell_quote_wei": creator_sell_quote,
        "creator_sell_pressure": creator_sell_quote / max(1, buy_quote),
        "decision_cutoff_ts": cutoff,
    }


def managed_position_nav_multiplier(rows: Iterable[Any]) -> float:
    """Return realized NAV factor without compounding slices of one position."""
    contributions: "OrderedDict[int, float]" = OrderedDict()
    for raw in rows:
        row = dict(raw) if not isinstance(raw, dict) else raw
        try:
            position_id = int(row.get("position_id"))
        except (TypeError, ValueError):
            continue
        fraction = _finite_number(row.get("position_fraction"))
        net_return = _finite_number(row.get("net_return"))
        if fraction is None or net_return is None or fraction <= 0.0:
            continue
        contributions[position_id] = contributions.get(position_id, 0.0) + fraction * net_return
    multiplier = 1.0
    for contribution in contributions.values():
        multiplier *= max(0.0, 1.0 + contribution)
    return multiplier


def _reconciled_paper_nav(self: Any) -> float:
    """Use one realized interpretation for staged lifecycle settlement and NAV."""
    robinhood_lifecycle._ensure_schema(self)
    with self.store._lock:
        legacy = self.store.db.execute(
            "SELECT o.paper_nav_multiplier FROM robinhood_paper_outcomes o "
            "LEFT JOIN v52_robinhood_position_lots l ON l.trial_id=o.trial_id "
            "WHERE o.paper_only=1 AND l.id IS NULL ORDER BY o.id"
        ).fetchall()
        managed = self.store.db.execute(
            "SELECT id,position_id,position_fraction,net_return FROM v52_robinhood_position_events "
            "WHERE net_return IS NOT NULL ORDER BY id"
        ).fetchall()
    legacy_multiplier = 1.0
    for row in legacy:
        legacy_multiplier *= max(0.0, float(row["paper_nav_multiplier"] or 1.0))
    return float(self.starting_nav_usd) * legacy_multiplier * managed_position_nav_multiplier(managed)


def install_v52_operational_integrity_repair() -> None:
    global _INSTALLED, _BASE_FLOW_METRICS, _BASE_LIFECYCLE_NAV
    if _INSTALLED:
        return
    _BASE_FLOW_METRICS = RobinhoodProfitMaximizerMixin._v5_flow_metrics
    _BASE_LIFECYCLE_NAV = robinhood_lifecycle._paper_nav_with_lifecycle
    setattr(_point_in_time_flow_metrics, "__wrapped__", _BASE_FLOW_METRICS)
    setattr(_point_in_time_flow_metrics, "_roi_v52_point_in_time_flow", True)
    RobinhoodProfitMaximizerMixin._v5_flow_metrics = _point_in_time_flow_metrics  # type: ignore[method-assign]
    setattr(_reconciled_paper_nav, "__wrapped__", _BASE_LIFECYCLE_NAV)
    setattr(_reconciled_paper_nav, "_roi_v52_staged_nav_reconciliation", True)
    robinhood_lifecycle._paper_nav_with_lifecycle = _reconciled_paper_nav
    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "version": REPAIR_VERSION,
        "installed": _INSTALLED,
        "point_in_time_flow_cutoff": True,
        "future_flow_allowed": False,
        "staged_exit_slices_compound_independently": False,
        "staged_exit_contributions_add_within_position": True,
        "changes_strategy_thresholds": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


__all__ = [
    "REPAIR_VERSION",
    "install_v52_operational_integrity_repair",
    "managed_position_nav_multiplier",
    "status",
]
