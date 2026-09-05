from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from functools import wraps
from typing import Any

from . import robinhood_chain_runtime as runtime
from .robinhood_catchup_capacity_repair import (
    _fetch_market_logs,
    _logs_with_resilient_range,
)


REPAIR_VERSION = "robinhood-verified-live-epoch-v2"
MAX_LIVE_FRONTIER_GAP_BLOCKS = 64

_FACTORY_ADDRESSES = [
    runtime.UNISWAP_V3_FACTORY,
    runtime.PONS_V1_ACTIVE_FACTORY,
    runtime.PONS_V1_LEGACY_FACTORY,
    runtime.PONS_V2_FACTORY,
]


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _inc(self: Any, name: str) -> None:
    attr = f"_roi_live_frontier_{name}"
    setattr(self, attr, int(getattr(self, attr, 0) or 0) + 1)


def _historical_cursor(self: Any) -> int | None:
    cursor = getattr(self, "_cursor", None)
    return int(cursor) if cursor is not None else None


def _live_cursor(self: Any) -> int | None:
    cursor = getattr(self, "_roi_live_epoch_cursor", None)
    return int(cursor) if cursor is not None else None


def _live_epoch_active(self: Any) -> bool:
    return _live_cursor(self) is not None


def _paper_transport_ready(self: Any) -> bool:
    """Return current paper-entry transport authority without conflating backfill."""
    if bool(getattr(self, "_roi_live_epoch_suppress_entries", False)):
        return False
    if _live_epoch_active(self):
        return bool(getattr(self, "_roi_live_epoch_ready", False))
    return bool(getattr(self, "_caught_up", False))


def _clear_epoch_flow_state(self: Any) -> None:
    """Do not let pre-anchor flow contribute to a new prospective decision epoch."""
    for pool in getattr(self, "v3_pools", {}).values():
        recent = getattr(pool, "recent_swaps", None)
        if recent is not None:
            recent.clear()
        if hasattr(pool, "first_price_eth"):
            pool.first_price_eth = None
        if hasattr(pool, "first_live_observed_at"):
            pool.first_live_observed_at = None
    for curve in getattr(self, "v2_curves", {}).values():
        recent = getattr(curve, "recent_swaps", None)
        if recent is not None:
            recent.clear()


def _ensure_epoch_schema(self: Any) -> None:
    store = getattr(self, "store", None)
    if store is None:
        return
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS robinhood_live_epochs ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "release_commit TEXT NOT NULL, anchor_block INTEGER NOT NULL, "
            "historical_cursor_at_anchor INTEGER, started_at TEXT NOT NULL, "
            "reason TEXT NOT NULL, paper_only INTEGER NOT NULL, "
            "live_money_authority INTEGER NOT NULL)"
        )


def _record_epoch(self: Any, *, anchor_block: int, reason: str) -> None:
    store = getattr(self, "store", None)
    if store is None:
        return
    try:
        _ensure_epoch_schema(self)
        with store._lock, store.db:
            store.db.execute(
                "INSERT INTO robinhood_live_epochs("
                "release_commit,anchor_block,historical_cursor_at_anchor,started_at,reason,"
                "paper_only,live_money_authority) VALUES (?,?,?,?,?,1,0)",
                (
                    str(getattr(self, "release_commit", "unknown")),
                    int(anchor_block),
                    _historical_cursor(self),
                    _utcnow(),
                    reason,
                ),
            )
    except Exception:
        # Audit persistence failure must not create alternate execution authority.
        # The in-process epoch remains fail-closed until prospective coverage succeeds.
        setattr(self, "_roi_live_frontier_epoch_audit_error", True)


async def _sync_factory_state(self: Any, *, from_block: int, to_block: int) -> int:
    """Reconstruct the market universe through the live anchor without replaying swaps."""
    if from_block > to_block:
        return 0
    rows = await _logs_with_resilient_range(
        self,
        from_block=from_block,
        to_block=to_block,
        addresses=_FACTORY_ADDRESSES,
        topics=None,
    )
    rows.sort(
        key=lambda log: (
            int(str(log.get("blockNumber") or "0x0"), 16),
            int(str(log.get("transactionIndex") or "0x0"), 16),
            int(str(log.get("logIndex") or "0x0"), 16),
        )
    )
    for log in rows:
        await self._process_factory_log(log)
    return len(rows)


def _start_epoch(self: Any, *, anchor_block: int, reason: str) -> None:
    _clear_epoch_flow_state(self)
    setattr(self, "_roi_live_epoch_anchor_block", int(anchor_block))
    setattr(self, "_roi_live_epoch_cursor", int(anchor_block))
    setattr(self, "_roi_live_epoch_ready", False)
    setattr(self, "_roi_live_epoch_started_at", _utcnow())
    setattr(self, "_roi_live_epoch_last_success_at", None)
    setattr(self, "_roi_live_epoch_last_error_type", None)
    setattr(self, "_roi_live_epoch_factory_verified_through", int(anchor_block))
    setattr(self, "_roi_live_epoch_historical_cursor_at_anchor", _historical_cursor(self))
    setattr(self, "_roi_live_epoch_reason", reason)
    _record_epoch(self, anchor_block=int(anchor_block), reason=reason)
    _inc(self, "epochs_started")


async def _establish_epoch(self: Any, *, latest: int, reason: str) -> None:
    historical = _historical_cursor(self)
    if historical is None:
        return
    # The historical gap may contain factory events for markets that are still active
    # at the new live frontier. Recover only those cheap market-definition events
    # before anchoring; swap history remains a separate, non-authoritative backfill.
    start = historical + 1
    await _sync_factory_state(self, from_block=start, to_block=int(latest))
    _start_epoch(self, anchor_block=int(latest), reason=reason)


async def _advance_live_epoch(self: Any) -> None:
    """Maintain a prospective, contiguous live lane while historical backfill continues."""
    _inc(self, "polls")
    latest = int(await self.rpc.block_number())
    self._latest_block = latest
    historical = _historical_cursor(self)
    if historical is None:
        return

    historical_lag = max(0, latest - historical)
    if not _live_epoch_active(self):
        if historical_lag <= runtime.LIVE_LAG_BLOCKS:
            return
        await _establish_epoch(self, latest=latest, reason="historical_backfill_decoupled")
        return

    live_cursor = _live_cursor(self)
    assert live_cursor is not None

    if latest < live_cursor:
        await _establish_epoch(self, latest=latest, reason="chain_head_regressed")
        _inc(self, "epoch_resets")
        return

    gap = latest - live_cursor
    if gap > MAX_LIVE_FRONTIER_GAP_BLOCKS:
        # A large live-lane outage is never replayed as if it were current. Rebuild
        # market definitions through the new head and start a fresh prospective epoch.
        await _sync_factory_state(self, from_block=live_cursor + 1, to_block=latest)
        _start_epoch(self, anchor_block=latest, reason="live_frontier_gap_reanchored")
        _inc(self, "epoch_resets")
        return

    if gap == 0:
        setattr(self, "_roi_live_epoch_ready", True)
        setattr(self, "_roi_live_epoch_last_success_at", _utcnow())
        return

    from_block = live_cursor + 1
    to_block = latest
    observed_at = _utcnow()

    # Factory events are applied first so markets created in this exact prospective
    # range are included in the market-log acquisition for the same range.
    await _sync_factory_state(self, from_block=from_block, to_block=to_block)
    market_logs = await _fetch_market_logs(self, from_block=from_block, to_block=to_block)

    touched_v3: dict[str, tuple[Any, int]] = {}
    touched_v2: dict[str, Any] = {}
    setattr(self, "_roi_live_epoch_suppress_entries", True)
    try:
        for kind, market, log in market_logs:
            block = int(str(log.get("blockNumber") or "0x0"), 16)
            if kind == "v3":
                await self._process_v3_swap(market, log, live=True, observed_at=observed_at)
                previous = touched_v3.get(market.pool)
                if previous is None or block > previous[1]:
                    touched_v3[market.pool] = (market, block)
            else:
                await self._process_v2_curve_log(market, log, live=True, observed_at=observed_at)
                touched_v2[market.curve] = market
    finally:
        setattr(self, "_roi_live_epoch_suppress_entries", False)

    # Only after the entire contiguous range has been acquired and processed can it
    # become decision-authoritative. Historical `_cursor` is deliberately untouched.
    setattr(self, "_roi_live_epoch_cursor", to_block)
    setattr(self, "_roi_live_epoch_factory_verified_through", to_block)
    setattr(self, "_roi_live_epoch_ready", True)
    setattr(self, "_roi_live_epoch_last_success_at", _utcnow())
    setattr(self, "_roi_live_epoch_last_error_type", None)
    _inc(self, "ranges_completed")
    setattr(
        self,
        "_roi_live_epoch_last_range",
        {"from_block": from_block, "to_block": to_block, "market_logs": len(market_logs)},
    )

    # Decisions are evaluated only after the full live range is complete. All flow
    # now in recent_swaps was observed prospectively after this epoch's anchor.
    for market, block in touched_v3.values():
        await self._maybe_open_v3(market, current_block=block)
    for market in touched_v2.values():
        await self._maybe_open_v2(market)


def _poll_with_live_epoch(
    original: Callable[..., Awaitable[Any]],
) -> Callable[..., Awaitable[Any]]:
    @wraps(original)
    async def wrapped(self: Any, *args: Any, **kwargs: Any) -> Any:
        try:
            await _advance_live_epoch(self)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            setattr(self, "_roi_live_epoch_ready", False)
            setattr(self, "_roi_live_epoch_last_error_type", type(exc).__name__)
            _inc(self, "failures")
        # Historical catch-up remains lossless and keeps its durable cursor. A live
        # lane failure must never erase or skip that backlog.
        return await original(self, *args, **kwargs)

    setattr(wrapped, "_roi_verified_live_epoch_poll", True)
    return wrapped


async def _fresh_head_ready(self: Any) -> bool:
    """Require a just-read chain head before any Robinhood paper entry."""
    if bool(getattr(self, "_roi_live_epoch_suppress_entries", False)):
        return False

    _inc(self, "checks")
    try:
        fresh_latest = int(await self.rpc.block_number())
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        _inc(self, "failures")
        setattr(self, "_roi_live_frontier_last_error_type", type(exc).__name__)
        setattr(self, "_roi_live_frontier_last_checked_at", _utcnow())
        setattr(self, "_roi_live_epoch_ready", False)
        if not _live_epoch_active(self):
            self._caught_up = False
        return False

    decision_cursor = _live_cursor(self) if _live_epoch_active(self) else _historical_cursor(self)
    if decision_cursor is None:
        _inc(self, "missing_cursor")
        setattr(self, "_roi_live_frontier_last_error_type", "MissingDecisionCursor")
        setattr(self, "_roi_live_frontier_last_checked_at", _utcnow())
        setattr(self, "_roi_live_epoch_ready", False)
        if not _live_epoch_active(self):
            self._caught_up = False
        self._latest_block = fresh_latest
        return False

    lag = max(0, fresh_latest - int(decision_cursor))
    self._latest_block = fresh_latest
    setattr(self, "_roi_live_frontier_last_lag", lag)
    setattr(self, "_roi_live_frontier_last_checked_at", _utcnow())
    setattr(self, "_roi_live_frontier_last_error_type", None)

    ready = _paper_transport_ready(self) and lag <= runtime.LIVE_LAG_BLOCKS
    if not ready:
        if _live_epoch_active(self):
            setattr(self, "_roi_live_epoch_ready", False)
        elif bool(getattr(self, "_caught_up", False)) and lag > runtime.LIVE_LAG_BLOCKS:
            _inc(self, "stale_ready_corrections")
            self._caught_up = False
        return False

    _inc(self, "ready_checks")
    return True


def _entry_guard(original: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
    @wraps(original)
    async def guarded(self: Any, *args: Any, **kwargs: Any) -> Any:
        if not await _fresh_head_ready(self):
            return None
        return await original(self, *args, **kwargs)

    setattr(guarded, "_roi_fresh_live_frontier_entry_guard", True)
    return guarded


def _status_with_frontier_verification(
    original: Callable[[Any], dict[str, Any]],
) -> Callable[[Any], dict[str, Any]]:
    @wraps(original)
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        historical_cursor = _historical_cursor(self)
        latest = getattr(self, "_latest_block", None)
        historical_lag = (
            max(0, int(latest) - historical_cursor)
            if latest is not None and historical_cursor is not None
            else None
        )
        live_ready = _paper_transport_ready(self)
        live_cursor = _live_cursor(self)
        live_lag = (
            max(0, int(latest) - live_cursor)
            if latest is not None and live_cursor is not None
            else None
        )

        payload["historical_caught_up"] = bool(getattr(self, "_caught_up", False))
        payload["historical_block_lag"] = historical_lag
        payload["caught_up_for_paper_decisions"] = live_ready
        payload["paper_decision_transport_ready"] = live_ready

        catchup = payload.get("catchup_capacity")
        if isinstance(catchup, dict):
            catchup["paper_entries_allowed_during_catchup"] = live_ready
            catchup["historical_backfill_blocks_paper_entries"] = False
            catchup["live_epoch_required_for_entries_during_backfill"] = True

        payload["live_frontier_verification"] = {
            "repair_version": REPAIR_VERSION,
            "verified_live_epoch": _live_epoch_active(self),
            "live_epoch_ready": live_ready,
            "live_epoch_anchor_block": getattr(self, "_roi_live_epoch_anchor_block", None),
            "live_epoch_cursor_block": live_cursor,
            "live_epoch_lag_blocks": live_lag,
            "live_epoch_started_at": getattr(self, "_roi_live_epoch_started_at", None),
            "live_epoch_last_success_at": getattr(self, "_roi_live_epoch_last_success_at", None),
            "live_epoch_reason": getattr(self, "_roi_live_epoch_reason", None),
            "factory_state_verified_through": getattr(
                self, "_roi_live_epoch_factory_verified_through", None
            ),
            "historical_cursor_at_anchor": getattr(
                self, "_roi_live_epoch_historical_cursor_at_anchor", None
            ),
            "historical_cursor_block": historical_cursor,
            "historical_block_lag": historical_lag,
            "historical_backfill_preserved": True,
            "historical_backfill_can_authorize_entries": False,
            "retrospective_entry_authority": False,
            "prospective_flow_only": True,
            "fresh_block_number_required_before_paper_entry": True,
            "live_lag_blocks": runtime.LIVE_LAG_BLOCKS,
            "max_live_frontier_gap_before_reanchor": MAX_LIVE_FRONTIER_GAP_BLOCKS,
            "checks": int(getattr(self, "_roi_live_frontier_checks", 0) or 0),
            "ready_checks": int(getattr(self, "_roi_live_frontier_ready_checks", 0) or 0),
            "failures": int(getattr(self, "_roi_live_frontier_failures", 0) or 0),
            "epochs_started": int(getattr(self, "_roi_live_frontier_epochs_started", 0) or 0),
            "epoch_resets": int(getattr(self, "_roi_live_frontier_epoch_resets", 0) or 0),
            "ranges_completed": int(
                getattr(self, "_roi_live_frontier_ranges_completed", 0) or 0
            ),
            "last_range": getattr(self, "_roi_live_epoch_last_range", None),
            "last_lag": getattr(self, "_roi_live_frontier_last_lag", None),
            "last_checked_at": getattr(self, "_roi_live_frontier_last_checked_at", None),
            "last_error_type": getattr(self, "_roi_live_epoch_last_error_type", None)
            or getattr(self, "_roi_live_frontier_last_error_type", None),
            "strategy_thresholds_changed": False,
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        }
        return payload

    setattr(status, "_roi_fresh_live_frontier_status", True)
    return status


def install_robinhood_live_frontier_verification_repair(plane_cls: type[Any]) -> None:
    current_poll = getattr(plane_cls, "_poll_once", None)
    if current_poll is not None and not bool(
        getattr(current_poll, "_roi_verified_live_epoch_poll", False)
    ):
        plane_cls._poll_once = _poll_with_live_epoch(current_poll)  # type: ignore[method-assign]

    for name in ("_maybe_open_v3", "_maybe_open_v2"):
        current = getattr(plane_cls, name, None)
        if current is None:
            continue
        if not bool(getattr(current, "_roi_fresh_live_frontier_entry_guard", False)):
            setattr(plane_cls, name, _entry_guard(current))

    current_status = getattr(plane_cls, "status", None)
    if current_status is not None and not bool(
        getattr(current_status, "_roi_fresh_live_frontier_status", False)
    ):
        plane_cls.status = _status_with_frontier_verification(current_status)  # type: ignore[method-assign]

    setattr(plane_cls, "_roi_live_frontier_verification_installed", True)
    setattr(plane_cls, "_roi_live_frontier_verification_version", REPAIR_VERSION)


__all__ = [
    "MAX_LIVE_FRONTIER_GAP_BLOCKS",
    "REPAIR_VERSION",
    "_advance_live_epoch",
    "_entry_guard",
    "_fresh_head_ready",
    "_paper_transport_ready",
    "_status_with_frontier_verification",
    "install_robinhood_live_frontier_verification_repair",
]
