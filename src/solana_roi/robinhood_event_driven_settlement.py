from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from functools import wraps
from typing import Any, Awaitable, Callable

from . import robinhood_chain_runtime as runtime
from . import robinhood_production_ws_transport as transport
from . import robinhood_usage_bounded_transport as bounded


SETTLEMENT_VERSION = "robinhood-event-driven-settlement-v1"
SCHEDULER_SCAN_SECONDS = 0.25
MIN_EXACT_QUOTE_INTERVAL_SECONDS = 1.0
MAX_HOLD_RETRY_SECONDS = 30.0

_INSTALLED = False
_ORIGINAL_PROCESS_BLOCK: Callable[..., Awaitable[None]] | None = None


def _dirty_markets(self: Any) -> set[str]:
    value = getattr(self, "_roi_settlement_dirty_markets", None)
    if not isinstance(value, set):
        value = set()
        setattr(self, "_roi_settlement_dirty_markets", value)
    return value


def _last_attempts(self: Any) -> dict[str, float]:
    value = getattr(self, "_roi_settlement_last_attempt_monotonic", None)
    if not isinstance(value, dict):
        value = {}
        setattr(self, "_roi_settlement_last_attempt_monotonic", value)
    return value


def _bump(self: Any, name: str, amount: int = 1) -> None:
    current = int(getattr(self, name, 0) or 0)
    setattr(self, name, current + int(amount))


def _mark_authoritative_market_events(self: Any, items: list[dict[str, Any]], *, generation: int) -> None:
    now = time.monotonic()
    dirty = _dirty_markets(self)
    for item in items:
        if not bool(item.get("live_authority", False)):
            continue
        if int(item.get("generation", -1)) != int(generation):
            continue
        received = float(item.get("received_monotonic", now))
        if now - received > transport.canonical_latency_hard_max_seconds():
            continue
        log = item.get("log") or {}
        address = runtime._clean_address(log.get("address"))
        if not address or address in bounded.DISCOVERY_ADDRESSES:
            continue
        topics = list(log.get("topics") or ())
        topic0 = str(topics[0]).lower() if topics else ""
        if topic0 not in bounded.MARKET_TOPICS:
            continue
        dirty.add(address)
        _bump(self, "_roi_settlement_authoritative_market_events")


async def _event_marking_process_block(self: Any, items: list[dict[str, Any]], *, generation: int) -> None:
    if _ORIGINAL_PROCESS_BLOCK is None:
        return
    await _ORIGINAL_PROCESS_BLOCK(self, items, generation=generation)
    _mark_authoritative_market_events(self, items, generation=generation)


def _open_trials(self: Any) -> list[dict[str, Any]]:
    with self.store._lock:
        rows = self.store.db.execute(
            "SELECT t.* FROM robinhood_paper_trials t "
            "LEFT JOIN robinhood_paper_outcomes o ON o.trial_id=t.id "
            "WHERE t.release_commit=? AND o.id IS NULL ORDER BY t.id",
            (self.release_commit,),
        ).fetchall()
    return [dict(row) for row in rows]


def _elapsed_seconds(trial: dict[str, Any]) -> float:
    try:
        opened = datetime.fromisoformat(str(trial["opened_at"]))
        if opened.tzinfo is None:
            opened = opened.replace(tzinfo=timezone.utc)
        return max(0.0, (datetime.now(timezone.utc) - opened).total_seconds())
    except Exception:
        return 0.0


async def _settle_due_positions(self: Any) -> None:
    dirty = _dirty_markets(self)
    if not dirty:
        # A local SQLite scan is intentionally cheap and provider-free. It exists only
        # to enforce the max-hold clock when a market has gone completely quiet.
        trials = _open_trials(self)
        if not trials:
            return
    else:
        trials = _open_trials(self)

    now = time.monotonic()
    last_attempt = _last_attempts(self)
    dirty_snapshot = set(dirty)
    for trial in trials:
        market = runtime._clean_address(trial.get("market"))
        if not market:
            continue
        elapsed = _elapsed_seconds(trial)
        event_due = market in dirty_snapshot
        max_hold_due = elapsed >= runtime.MAX_HOLD_SECONDS
        if not event_due and not max_hold_due:
            continue

        previous = float(last_attempt.get(market, 0.0) or 0.0)
        minimum_gap = MIN_EXACT_QUOTE_INTERVAL_SECONDS if event_due else MAX_HOLD_RETRY_SECONDS
        if previous and now - previous < minimum_gap:
            # Preserve a coalesced event for the next scheduler pass rather than paying
            # for multiple exact quotes inside the old one-second resolution boundary.
            if event_due:
                dirty.add(market)
            continue

        last_attempt[market] = now
        dirty.discard(market)
        try:
            await self._settle_one(trial)
            _bump(self, "_roi_settlement_exact_attempts")
            if event_due:
                _bump(self, "_roi_settlement_event_driven_attempts")
            if max_hold_due:
                _bump(self, "_roi_settlement_max_hold_attempts")
        except Exception:
            _bump(self, "_roi_settlement_exact_failures")
            # Do not restore an event-dirty flag after a provider failure: that would
            # recreate the old idle polling loop. A later market event can retry; an
            # overdue max-hold position gets a bounded retry every 30 seconds.

    # Remove dirty markers for markets that are not open positions. This prevents a
    # pre-entry event from becoming retrospective settlement authority later.
    open_markets = {runtime._clean_address(trial.get("market")) for trial in trials}
    dirty.intersection_update({market for market in open_markets if market})


async def _settlement_scheduler(self: Any, stop: asyncio.Event) -> None:
    while not stop.is_set():
        await _settle_due_positions(self)
        try:
            await asyncio.wait_for(stop.wait(), timeout=SCHEDULER_SCAN_SECONDS)
        except asyncio.TimeoutError:
            pass


def _run_wrapper(original: Callable[[Any, asyncio.Event], Awaitable[None]]) -> Callable[[Any, asyncio.Event], Awaitable[None]]:
    @wraps(original)
    async def wrapped(self: Any, stop: asyncio.Event) -> None:
        if not transport.production_provider_configured() or not bool(getattr(self, "enabled", False)):
            await original(self, stop)
            return

        had_instance_override = "_settle_open_positions" in getattr(self, "__dict__", {})
        prior_instance_override = getattr(self, "__dict__", {}).get("_settle_open_positions")

        async def suppress_idle_full_portfolio_poll() -> None:
            _bump(self, "_roi_settlement_idle_full_poll_suppressed")

        setattr(self, "_settle_open_positions", suppress_idle_full_portfolio_poll)
        scheduler = asyncio.create_task(
            _settlement_scheduler(self, stop),
            name="robinhood-event-driven-settlement",
        )
        try:
            await original(self, stop)
        finally:
            scheduler.cancel()
            try:
                await scheduler
            except asyncio.CancelledError:
                pass
            if had_instance_override:
                setattr(self, "_settle_open_positions", prior_instance_override)
            else:
                try:
                    delattr(self, "_settle_open_positions")
                except AttributeError:
                    pass

    setattr(wrapped, "_roi_robinhood_event_driven_settlement", True)
    return wrapped


def install_robinhood_event_driven_settlement(plane_cls: type[Any]) -> None:
    """Replace idle exact-quote polling with live-event-driven paper settlement.

    The bounded WebSocket still provides the authoritative market event. Exact exit
    quotes still use the configured production RPC. This patch only removes repeated
    exact quotes when no authoritative market state changed; max-hold remains enforced
    by a provider-free local clock scan and bounded exact-quote backstop.
    """
    global _INSTALLED, _ORIGINAL_PROCESS_BLOCK
    if _INSTALLED:
        return

    _ORIGINAL_PROCESS_BLOCK = bounded._process_block
    bounded._process_block = _event_marking_process_block

    current_run = plane_cls.run
    if not bool(getattr(current_run, "_roi_robinhood_event_driven_settlement", False)):
        plane_cls.run = _run_wrapper(current_run)  # type: ignore[method-assign]

    setattr(plane_cls, "_roi_robinhood_event_driven_settlement_version", SETTLEMENT_VERSION)
    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "version": SETTLEMENT_VERSION,
        "installed": _INSTALLED,
        "settlement_trigger": "authoritative_live_market_event_or_max_hold_deadline",
        "idle_exact_quote_polling": False,
        "minimum_event_quote_interval_seconds": MIN_EXACT_QUOTE_INTERVAL_SECONDS,
        "max_hold_retry_seconds": MAX_HOLD_RETRY_SECONDS,
        "entry_authority_changed": False,
        "exit_economics_changed": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


__all__ = [
    "SETTLEMENT_VERSION",
    "install_robinhood_event_driven_settlement",
    "status",
]
