from __future__ import annotations

import asyncio
import os
from collections.abc import Awaitable, Callable
from functools import wraps
from typing import Any

from . import robinhood_chain_runtime as runtime
from . import robinhood_live_frontier_verification_repair as frontier


REPAIR_VERSION = "robinhood-forward-only-runtime-v1"
DEFAULT_LIVE_POLL_SECONDS = 1.0
MIN_LIVE_POLL_SECONDS = 0.5
MAX_LIVE_POLL_SECONDS = 5.0
METADATA_RECOVERY_BLOCKS = 64


def _live_poll_seconds() -> float:
    try:
        value = float(os.getenv("ROBINHOOD_LIVE_POLL_SECONDS", str(DEFAULT_LIVE_POLL_SECONDS)))
    except (TypeError, ValueError):
        value = DEFAULT_LIVE_POLL_SECONDS
    return max(MIN_LIVE_POLL_SECONDS, min(MAX_LIVE_POLL_SECONDS, value))


def _bounded_metadata_start(*, latest: int, previous_live_cursor: int | None) -> int:
    if previous_live_cursor is not None and 0 <= latest - previous_live_cursor <= METADATA_RECOVERY_BLOCKS:
        return previous_live_cursor + 1
    return max(runtime.PONS_V1_LEGACY_START_BLOCK, latest - METADATA_RECOVERY_BLOCKS + 1)


async def _sync_bounded_metadata(
    self: Any,
    *,
    latest: int,
    previous_live_cursor: int | None,
    reason: str,
) -> None:
    start = _bounded_metadata_start(latest=latest, previous_live_cursor=previous_live_cursor)
    count = 0
    if start <= latest:
        count = await frontier._sync_factory_state(self, from_block=start, to_block=latest)
    setattr(
        self,
        "_roi_forward_only_last_metadata_recovery",
        {
            "from_block": start,
            "to_block": latest,
            "factory_logs": int(count),
            "reason": reason,
            "swap_backfill_performed": False,
        },
    )


async def _forward_only_advance_live_epoch(self: Any) -> None:
    """Maintain only the current actionable Robinhood frontier.

    Existing historical data and the old durable cursor remain untouched for audit,
    but no runtime work is spent replaying historical swaps to close that cursor.
    """
    frontier._inc(self, "polls")

    if not bool(getattr(self, "_roi_forward_only_chain_id_verified", False)):
        chain_id = await self.rpc.chain_id()
        if chain_id != runtime.ROBINHOOD_CHAIN_ID:
            raise RuntimeError(f"wrong Robinhood chain id: {chain_id}")
        setattr(self, "_roi_forward_only_chain_id_verified", True)

    latest = int(await self.rpc.block_number())
    self._latest_block = latest

    if not frontier._live_epoch_active(self):
        await _sync_bounded_metadata(
            self,
            latest=latest,
            previous_live_cursor=None,
            reason="startup_current_frontier_metadata",
        )
        frontier._start_epoch(self, anchor_block=latest, reason="forward_only_current_head")
        return

    live_cursor = frontier._live_cursor(self)
    assert live_cursor is not None

    if latest < live_cursor:
        await _sync_bounded_metadata(
            self,
            latest=latest,
            previous_live_cursor=None,
            reason="chain_head_regressed_reanchor",
        )
        frontier._start_epoch(self, anchor_block=latest, reason="chain_head_regressed")
        frontier._inc(self, "epoch_resets")
        return

    gap = latest - live_cursor
    if gap > frontier.MAX_LIVE_FRONTIER_GAP_BLOCKS:
        # A long outage is intentionally not replayed. Recover only bounded market
        # definitions near the new head, clear old flow, and resume prospectively.
        await _sync_bounded_metadata(
            self,
            latest=latest,
            previous_live_cursor=live_cursor,
            reason="long_outage_bounded_metadata_only",
        )
        frontier._start_epoch(self, anchor_block=latest, reason="live_frontier_gap_reanchored_forward_only")
        frontier._inc(self, "epoch_resets")
        return

    if gap == 0:
        setattr(self, "_roi_live_epoch_ready", True)
        setattr(self, "_roi_live_epoch_last_success_at", frontier._utcnow())
        return

    from_block = live_cursor + 1
    to_block = latest
    observed_at = frontier._utcnow()

    # This is current forward ingestion, not historical backfill. Factory events are
    # processed first so markets created since the previous live poll participate in
    # the exact same prospective range.
    await frontier._sync_factory_state(self, from_block=from_block, to_block=to_block)
    market_logs = await frontier._fetch_market_logs(self, from_block=from_block, to_block=to_block)

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

    setattr(self, "_roi_live_epoch_cursor", to_block)
    setattr(self, "_roi_live_epoch_factory_verified_through", to_block)
    setattr(self, "_roi_live_epoch_ready", True)
    setattr(self, "_roi_live_epoch_last_success_at", frontier._utcnow())
    setattr(self, "_roi_live_epoch_last_error_type", None)
    frontier._inc(self, "ranges_completed")
    setattr(
        self,
        "_roi_live_epoch_last_range",
        {
            "from_block": from_block,
            "to_block": to_block,
            "market_logs": len(market_logs),
            "forward_only": True,
        },
    )

    for market, block in touched_v3.values():
        await self._maybe_open_v3(market, current_block=block)
    for market in touched_v2.values():
        await self._maybe_open_v2(market)


def _forward_only_base_poll(
    original: Callable[..., Awaitable[Any]],
) -> Callable[..., Awaitable[Any]]:
    @wraps(original)
    async def wrapped(self: Any, *args: Any, **kwargs: Any) -> None:
        # Chain ingestion is exclusively owned by the verified live-frontier wrapper.
        # The historical scanner captured in `original` is deliberately never called.
        self._last_poll_at = frontier._utcnow()
        self._caught_up = False
        await self._settle_open_positions()
        self._last_success_at = frontier._utcnow()
        self._last_error = None
        setattr(self, "_roi_forward_only_cycles", int(getattr(self, "_roi_forward_only_cycles", 0) or 0) + 1)

    setattr(wrapped, "_roi_robinhood_forward_only_base_poll", True)
    return wrapped


def _forward_only_run(
    original: Callable[..., Awaitable[Any]],
) -> Callable[..., Awaitable[Any]]:
    @wraps(original)
    async def wrapped(self: Any, stop: asyncio.Event) -> None:
        if not self.enabled:
            return
        while not stop.is_set():
            try:
                await self._poll_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._rpc_failures += 1
                self._last_error = f"{type(exc).__name__}: {exc}"
            try:
                await asyncio.wait_for(stop.wait(), timeout=_live_poll_seconds())
            except TimeoutError:
                pass

    setattr(wrapped, "_roi_robinhood_forward_only_run", True)
    return wrapped


def _forward_only_status(
    original: Callable[[Any], dict[str, Any]],
) -> Callable[[Any], dict[str, Any]]:
    @wraps(original)
    def wrapped(self: Any) -> dict[str, Any]:
        payload = original(self)
        live_cursor = frontier._live_cursor(self)
        latest = getattr(self, "_latest_block", None)
        live_lag = (
            max(0, int(latest) - int(live_cursor))
            if latest is not None and live_cursor is not None
            else None
        )
        archival_cursor = getattr(self, "_cursor", None)
        payload["archival_cursor_block"] = archival_cursor
        payload["block_lag"] = live_lag
        payload["historical_backfill_enabled"] = False
        payload["historical_swap_replay_enabled"] = False
        payload["forward_only_runtime"] = {
            "repair_version": REPAIR_VERSION,
            "enabled": True,
            "live_poll_seconds": _live_poll_seconds(),
            "bounded_metadata_recovery_blocks": METADATA_RECOVERY_BLOCKS,
            "historical_cursor_is_archival_only": True,
            "historical_swap_backfill_enabled": False,
            "historical_data_deleted": False,
            "paper_outcomes_preserved": True,
            "wallet_intelligence_preserved": True,
            "durable_market_metadata_preserved": True,
            "retrospective_entry_authority": False,
            "last_metadata_recovery": getattr(self, "_roi_forward_only_last_metadata_recovery", None),
            "cycles": int(getattr(self, "_roi_forward_only_cycles", 0) or 0),
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        }
        catchup = payload.get("catchup_capacity")
        if isinstance(catchup, dict):
            catchup["catchup_mode"] = False
            catchup["catchup_stalled"] = False
            catchup["estimated_batches_to_live"] = None
            catchup["historical_backfill_enabled"] = False
            catchup["historical_swap_replay_enabled"] = False
            catchup["archival_cursor_block"] = archival_cursor
            catchup["paper_entries_allowed_during_catchup"] = False
        return payload

    setattr(wrapped, "_roi_robinhood_forward_only_status", True)
    return wrapped


def _patch_frontier_status_factory() -> None:
    current = frontier._status_with_frontier_verification
    if bool(getattr(current, "_roi_forward_only_frontier_status_factory", False)):
        return

    @wraps(current)
    def factory(original: Callable[[Any], dict[str, Any]]) -> Callable[[Any], dict[str, Any]]:
        base_status = current(original)

        @wraps(base_status)
        def status(self: Any) -> dict[str, Any]:
            payload = base_status(self)
            live = payload.get("live_frontier_verification")
            archival_cursor = getattr(self, "_cursor", None)
            if isinstance(live, dict):
                live["historical_backfill_preserved"] = False
                live["historical_backfill_enabled"] = False
                live["historical_data_preserved"] = True
                live["historical_cursor_block"] = archival_cursor
                live["historical_cursor_is_archival_only"] = True
                live["bounded_metadata_recovery_blocks"] = METADATA_RECOVERY_BLOCKS
                live["forward_only_runtime"] = True
            payload["historical_caught_up"] = False
            payload["historical_block_lag"] = None
            payload["historical_backfill_enabled"] = False
            live_cursor = frontier._live_cursor(self)
            latest = getattr(self, "_latest_block", None)
            payload["block_lag"] = (
                max(0, int(latest) - int(live_cursor))
                if latest is not None and live_cursor is not None
                else None
            )
            catchup = payload.get("catchup_capacity")
            if isinstance(catchup, dict):
                catchup["catchup_mode"] = False
                catchup["catchup_stalled"] = False
                catchup["estimated_batches_to_live"] = None
                catchup["historical_backfill_enabled"] = False
                catchup["historical_backfill_blocks_paper_entries"] = False
                catchup["live_epoch_required_for_entries_during_backfill"] = False
            return payload

        setattr(status, "_roi_forward_only_frontier_status", True)
        return status

    setattr(factory, "_roi_forward_only_frontier_status_factory", True)
    frontier._status_with_frontier_verification = factory  # type: ignore[assignment]


def install_robinhood_forward_only_runtime_repair(plane_cls: type[Any]) -> None:
    # Replace the live-frontier implementation before it is installed onto the final
    # class. The wrapper will therefore use forward-only semantics at runtime.
    frontier._advance_live_epoch = _forward_only_advance_live_epoch  # type: ignore[assignment]
    _patch_frontier_status_factory()

    current_poll = getattr(plane_cls, "_poll_once", None)
    if current_poll is not None and not bool(
        getattr(current_poll, "_roi_robinhood_forward_only_base_poll", False)
    ):
        plane_cls._poll_once = _forward_only_base_poll(current_poll)  # type: ignore[method-assign]

    current_run = getattr(plane_cls, "run", None)
    if current_run is not None and not bool(getattr(current_run, "_roi_robinhood_forward_only_run", False)):
        plane_cls.run = _forward_only_run(current_run)  # type: ignore[method-assign]

    current_status = getattr(plane_cls, "status", None)
    if current_status is not None and not bool(
        getattr(current_status, "_roi_robinhood_forward_only_status", False)
    ):
        plane_cls.status = _forward_only_status(current_status)  # type: ignore[method-assign]

    setattr(plane_cls, "_roi_robinhood_forward_only_runtime_installed", True)
    setattr(plane_cls, "_roi_robinhood_forward_only_runtime_version", REPAIR_VERSION)


__all__ = [
    "DEFAULT_LIVE_POLL_SECONDS",
    "METADATA_RECOVERY_BLOCKS",
    "REPAIR_VERSION",
    "_bounded_metadata_start",
    "_forward_only_advance_live_epoch",
    "_live_poll_seconds",
    "install_robinhood_forward_only_runtime_repair",
]
