from __future__ import annotations

import asyncio
import json
import os
import time
from functools import wraps
from typing import Any, Callable

import websockets

from . import post177_forward_pipeline_bottleneck_repair as post177
from . import robinhood_chain_runtime as runtime
from . import robinhood_forward_only_runtime_repair as forward
from . import robinhood_live_frontier_verification_repair as frontier


REPAIR_VERSION = "robinhood-sequencer-frontier-v1"
DEFAULT_FEED_URL = "wss://feed.mainnet.chain.robinhood.com"
FEED_STALE_SECONDS = 0.30
FEED_RECONNECT_SECONDS = 0.20
FEED_RECV_TIMEOUT_SECONDS = 1.0
FEED_MAX_MESSAGE_BYTES = 8 * 1024 * 1024
DECISION_LAG_BLOCKS = int(runtime.LIVE_LAG_BLOCKS)

_FACTORY_ADDRESSES = {
    runtime.UNISWAP_V3_FACTORY,
    runtime.PONS_V1_ACTIVE_FACTORY,
    runtime.PONS_V1_LEGACY_FACTORY,
    runtime.PONS_V2_FACTORY,
}
_EVENT_TOPICS = (
    runtime.V3_POOL_CREATED_TOPIC,
    runtime.V3_SWAP_TOPIC,
    runtime.PONS_V1_TOKEN_LAUNCHED_TOPIC,
    runtime.PONS_V2_TOKEN_LAUNCHED_TOPIC,
    runtime.PONS_V2_CURVE_BUY_TOPIC,
    runtime.PONS_V2_CURVE_SELL_TOPIC,
)
_INSTALLED = False


def _feed_url() -> str:
    return os.getenv("ROBINHOOD_SEQUENCER_FEED_URL", DEFAULT_FEED_URL).strip() or DEFAULT_FEED_URL


def _inc(self: Any, name: str, amount: int = 1) -> None:
    attr = f"_roi_sequencer_{name}"
    setattr(self, attr, int(getattr(self, attr, 0) or 0) + int(amount))


def _event(self: Any) -> asyncio.Event:
    value = getattr(self, "_roi_sequencer_event", None)
    if not isinstance(value, asyncio.Event):
        value = asyncio.Event()
        setattr(self, "_roi_sequencer_event", value)
    return value


def _feed_generation(self: Any) -> int:
    return int(getattr(self, "_roi_sequencer_generation", 0) or 0)


def _feed_fresh(self: Any) -> bool:
    last = float(getattr(self, "_roi_sequencer_last_monotonic", 0.0) or 0.0)
    return bool(
        getattr(self, "_roi_sequencer_synchronized", False)
        and getattr(self, "_roi_sequencer_continuity_ok", False)
        and last > 0.0
        and time.monotonic() - last <= FEED_STALE_SECONDS
    )


def _set_feed_head(self: Any, head: int) -> None:
    now = time.monotonic()
    setattr(self, "_roi_sequencer_head_block", int(head))
    setattr(self, "_roi_sequencer_last_monotonic", now)
    setattr(self, "_roi_sequencer_last_success_at", frontier._utcnow())
    setattr(self, "_roi_sequencer_last_error_type", None)
    setattr(self, "_roi_post177_head_observed_block", int(head))
    setattr(self, "_roi_post177_head_observer_last_success_monotonic", now)
    setattr(self, "_roi_post177_head_observer_last_success_at", frontier._utcnow())
    setattr(self, "_roi_post177_head_observer_last_error_type", None)
    setattr(self, "_roi_post177_head_observer_continuity_ok", True)
    self._latest_block = int(head)
    _event(self).set()


def _sequence_numbers(payload: Any) -> list[int]:
    if not isinstance(payload, dict):
        return []
    result: list[int] = []
    for message in payload.get("messages") or ():
        if not isinstance(message, dict):
            continue
        try:
            result.append(int(message.get("sequenceNumber")))
        except (TypeError, ValueError):
            continue
    return result


async def _sequencer_reader(self: Any, stop: asyncio.Event) -> None:
    """Continuously drain the public sequencer feed and expose only a synchronized head."""
    while not stop.is_set():
        check_task: asyncio.Task[int] | None = None
        sync_target: int | None = None
        try:
            setattr(self, "_roi_sequencer_synchronized", False)
            setattr(self, "_roi_sequencer_continuity_ok", False)
            check_task = asyncio.create_task(self.rpc.block_number())
            async with websockets.connect(
                _feed_url(),
                open_timeout=10,
                close_timeout=3,
                ping_interval=20,
                max_size=FEED_MAX_MESSAGE_BYTES,
            ) as ws:
                _inc(self, "connections")
                setattr(self, "_roi_sequencer_continuity_ok", True)
                while not stop.is_set():
                    raw = await asyncio.wait_for(ws.recv(), timeout=FEED_RECV_TIMEOUT_SECONDS)
                    payload = json.loads(raw)
                    values = _sequence_numbers(payload)
                    if not values:
                        continue
                    head = max(values)
                    _inc(self, "messages", len(values))
                    _set_feed_head(self, head)

                    if check_task is not None and check_task.done():
                        fresh_head = int(check_task.result())
                        check_task = None
                        if head >= fresh_head - DECISION_LAG_BLOCKS:
                            setattr(self, "_roi_sequencer_synchronized", True)
                            setattr(self, "_roi_sequencer_sync_target", fresh_head)
                            _inc(self, "synchronizations")
                        else:
                            sync_target = fresh_head
                            setattr(self, "_roi_sequencer_sync_target", fresh_head)
                    if (
                        not bool(getattr(self, "_roi_sequencer_synchronized", False))
                        and check_task is None
                        and sync_target is not None
                        and head >= sync_target
                    ):
                        # Do not pause feed draining for this check. A new HTTP head is
                        # sampled only after the feed catches the prior target.
                        check_task = asyncio.create_task(self.rpc.block_number())
                        sync_target = None
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _inc(self, "disconnects")
            setattr(self, "_roi_sequencer_last_error_type", type(exc).__name__)
            setattr(self, "_roi_sequencer_synchronized", False)
            setattr(self, "_roi_sequencer_continuity_ok", False)
            setattr(self, "_roi_live_epoch_ready", False)
            setattr(self, "_roi_post177_head_observer_continuity_ok", False)
            setattr(self, "_roi_post177_head_observer_last_gap_reason", "sequencer_feed_disconnect")
            setattr(self, "_roi_sequencer_generation", _feed_generation(self) + 1)
            _event(self).set()
        finally:
            if check_task is not None and not check_task.done():
                check_task.cancel()
                await asyncio.gather(check_task, return_exceptions=True)
        if not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=FEED_RECONNECT_SECONDS)
            except asyncio.TimeoutError:
                pass


async def _fresh_feed_head_ready(self: Any) -> bool:
    """Apply the unchanged two-block gate against the continuously drained feed head."""
    frontier._inc(self, "checks")
    if bool(getattr(self, "_roi_live_epoch_suppress_entries", False)) or not _feed_fresh(self):
        setattr(self, "_roi_live_epoch_ready", False)
        setattr(self, "_roi_live_frontier_last_error_type", "SequencerFeedNotFresh")
        setattr(self, "_roi_live_frontier_last_checked_at", frontier._utcnow())
        return False
    head = getattr(self, "_roi_sequencer_head_block", None)
    cursor = frontier._live_cursor(self)
    if head is None or cursor is None:
        setattr(self, "_roi_live_epoch_ready", False)
        return False
    lag = max(0, int(head) - int(cursor))
    self._latest_block = int(head)
    setattr(self, "_roi_live_frontier_last_lag", lag)
    setattr(self, "_roi_live_frontier_last_checked_at", frontier._utcnow())
    setattr(self, "_roi_live_frontier_last_error_type", None)
    ready = bool(frontier._paper_transport_ready(self) and lag <= DECISION_LAG_BLOCKS)
    if ready:
        frontier._inc(self, "ready_checks")
    else:
        setattr(self, "_roi_live_epoch_ready", False)
    return ready


def _sort_logs(rows: list[dict[str, Any]]) -> None:
    rows.sort(
        key=lambda log: (
            int(str(log.get("blockNumber") or "0x0"), 16),
            int(str(log.get("transactionIndex") or "0x0"), 16),
            int(str(log.get("logIndex") or "0x0"), 16),
        )
    )


async def _exact_block_events(self: Any, block: int) -> list[dict[str, Any]]:
    rows = await self.rpc.get_logs(
        from_block=int(block),
        to_block=int(block),
        topics=[list(_EVENT_TOPICS)],
    )
    _sort_logs(rows)
    return rows


async def _advance_sequencer_frontier(self: Any) -> None:
    """Process only the latest synchronized feed block; stale blocks never gain entry authority."""
    frontier._inc(self, "polls")
    post177._schedule_rwa_refresh(self)
    if not _feed_fresh(self):
        setattr(self, "_roi_live_epoch_ready", False)
        return
    if not bool(getattr(self, "_roi_forward_only_chain_id_verified", False)):
        chain_id = await self.rpc.chain_id()
        if chain_id != runtime.ROBINHOOD_CHAIN_ID:
            raise RuntimeError(f"wrong Robinhood chain id: {chain_id}")
        setattr(self, "_roi_forward_only_chain_id_verified", True)

    latest = int(getattr(self, "_roi_sequencer_head_block"))
    self._latest_block = latest
    generation = _feed_generation(self)
    processed_generation = getattr(self, "_roi_sequencer_processed_generation", None)
    live_cursor = frontier._live_cursor(self)

    if live_cursor is None or processed_generation != generation:
        await forward._sync_bounded_metadata(
            self,
            latest=latest,
            previous_live_cursor=live_cursor,
            reason="sequencer_feed_anchor_metadata_only",
        )
        post177._start_observed_epoch(self, latest=latest, reason="sequencer_feed_anchor")
        setattr(self, "_roi_sequencer_processed_generation", generation)
        setattr(self, "_roi_live_epoch_ready", False)
        return

    if latest < int(live_cursor):
        setattr(self, "_roi_sequencer_generation", generation + 1)
        setattr(self, "_roi_live_epoch_ready", False)
        return

    gap = max(0, latest - int(live_cursor))
    if gap == 0:
        setattr(self, "_roi_live_epoch_ready", True)
        ready = await _fresh_feed_head_ready(self)
        setattr(self, "_roi_live_epoch_ready", ready)
        if ready:
            setattr(self, "_roi_live_epoch_last_success_at", frontier._utcnow())
        return

    # Preserve market definitions from skipped processing blocks, but never replay
    # their swaps. In the normal steady state gap is one block, so this call is absent.
    stale_skipped = max(0, gap - 1)
    if stale_skipped:
        post177._clear_pending_markets(self)
        metadata_from = max(int(live_cursor) + 1, latest - forward.METADATA_RECOVERY_BLOCKS + 1)
        if metadata_from < latest:
            await frontier._sync_factory_state(self, from_block=metadata_from, to_block=latest - 1)
        _inc(self, "stale_trade_blocks_skipped", stale_skipped)
        _inc(self, "stale_trade_windows_skipped")

    rows = await _exact_block_events(self, latest)
    observed_at = frontier._utcnow()

    # Factory logs are applied first even if their transaction ordering is later in
    # the block, so a market created and traded in this same current block is known.
    for log in rows:
        address = runtime._clean_address(log.get("address"))
        if address in _FACTORY_ADDRESSES:
            await self._process_factory_log(log)

    touched_v3: dict[str, tuple[Any, int]] = {}
    touched_v2: dict[str, Any] = {}
    setattr(self, "_roi_live_epoch_suppress_entries", True)
    try:
        for log in rows:
            address = runtime._clean_address(log.get("address"))
            topics = list(log.get("topics") or ())
            topic0 = str(topics[0]).lower() if topics else ""
            if topic0 == runtime.V3_SWAP_TOPIC.lower():
                market = getattr(self, "v3_pools", {}).get(address)
                if market is not None:
                    await self._process_v3_swap(market, log, live=True, observed_at=observed_at)
                    touched_v3[address] = (market, latest)
            elif topic0 in {runtime.PONS_V2_CURVE_BUY_TOPIC.lower(), runtime.PONS_V2_CURVE_SELL_TOPIC.lower()}:
                market = getattr(self, "v2_curves", {}).get(address)
                if market is not None:
                    await self._process_v2_curve_log(market, log, live=True, observed_at=observed_at)
                    touched_v2[address] = market
    finally:
        setattr(self, "_roi_live_epoch_suppress_entries", False)

    setattr(self, "_roi_live_epoch_cursor", latest)
    setattr(self, "_roi_live_epoch_factory_verified_through", latest)
    setattr(self, "_roi_live_epoch_last_success_at", frontier._utcnow())
    setattr(self, "_roi_live_epoch_last_error_type", None)
    frontier._inc(self, "ranges_completed")
    setattr(
        self,
        "_roi_live_epoch_last_range",
        {
            "from_block": latest,
            "to_block": latest,
            "market_logs": len(rows),
            "decision_tail_blocks": DECISION_LAG_BLOCKS,
            "metadata_window_blocks": int(forward.METADATA_RECOVERY_BLOCKS),
            "stale_trade_blocks_skipped": stale_skipped,
            "stale_trade_blocks_have_retrospective_entry_authority": False,
            "head_source": "robinhood_public_sequencer_feed",
            "log_acquisition": "single_exact_current_block_topic_filter",
            "forward_only": True,
        },
    )

    setattr(self, "_roi_live_epoch_ready", True)
    ready = await _fresh_feed_head_ready(self)
    setattr(self, "_roi_live_epoch_ready", ready)
    if not ready:
        return
    for market, block in touched_v3.values():
        await self._maybe_open_v3(market, current_block=block)
    for market in touched_v2.values():
        await self._maybe_open_v2(market)


async def _sequencer_run(self: Any, stop: asyncio.Event) -> None:
    """Drive the existing final `_poll_once` graph from feed-head changes, not a 0.5-5s timer."""
    if not self.enabled:
        return
    feed_task = asyncio.create_task(_sequencer_reader(self, stop), name="robinhood-sequencer-feed")
    try:
        while not stop.is_set():
            event = _event(self)
            try:
                await asyncio.wait_for(event.wait(), timeout=FEED_STALE_SECONDS)
            except asyncio.TimeoutError:
                if not _feed_fresh(self):
                    setattr(self, "_roi_live_epoch_ready", False)
                continue
            event.clear()
            if not _feed_fresh(self):
                continue
            try:
                await self._poll_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._rpc_failures += 1
                self._last_error = f"{type(exc).__name__}: {exc}"
                setattr(self, "_roi_live_epoch_ready", False)
    finally:
        feed_task.cancel()
        await asyncio.gather(feed_task, return_exceptions=True)


setattr(_sequencer_run, "_roi_robinhood_sequencer_frontier", True)
setattr(_advance_sequencer_frontier, "_roi_robinhood_sequencer_frontier", True)
setattr(_fresh_feed_head_ready, "_roi_robinhood_sequencer_frontier", True)


def _status_with_sequencer(original: Callable[[Any], dict[str, Any]]) -> Callable[[Any], dict[str, Any]]:
    @wraps(original)
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        head = getattr(self, "_roi_sequencer_head_block", None)
        cursor = frontier._live_cursor(self)
        lag = max(0, int(head) - int(cursor)) if head is not None and cursor is not None else None
        payload["sequencer_frontier"] = {
            "repair_version": REPAIR_VERSION,
            "feed_endpoint_kind": "official_public_sequencer_feed",
            "feed_url_exposed": False,
            "connected": bool(getattr(self, "_roi_sequencer_continuity_ok", False)),
            "synchronized": bool(getattr(self, "_roi_sequencer_synchronized", False)),
            "fresh": _feed_fresh(self),
            "feed_stale_seconds": FEED_STALE_SECONDS,
            "head_block": head,
            "processing_cursor_block": cursor,
            "processing_lag_blocks": lag,
            "generation": _feed_generation(self),
            "connections": int(getattr(self, "_roi_sequencer_connections", 0) or 0),
            "disconnects": int(getattr(self, "_roi_sequencer_disconnects", 0) or 0),
            "messages": int(getattr(self, "_roi_sequencer_messages", 0) or 0),
            "synchronizations": int(getattr(self, "_roi_sequencer_synchronizations", 0) or 0),
            "last_success_at": getattr(self, "_roi_sequencer_last_success_at", None),
            "last_error_type": getattr(self, "_roi_sequencer_last_error_type", None),
            "decision_lag_blocks": DECISION_LAG_BLOCKS,
            "decision_lag_blocks_changed": False,
            "stale_blocks_have_retrospective_entry_authority": False,
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        }
        return payload

    setattr(status, "_roi_robinhood_sequencer_frontier", True)
    return status


def install_robinhood_sequencer_frontier(plane_cls: type[Any]) -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    # The verified-live poll wrapper resolves these globals dynamically, so replacing
    # them here changes transport acquisition only, after all strategy composition.
    frontier._advance_live_epoch = _advance_sequencer_frontier  # type: ignore[assignment]
    frontier._fresh_head_ready = _fresh_feed_head_ready  # type: ignore[assignment]
    current_run = getattr(plane_cls, "run")
    plane_cls.run = _sequencer_run  # type: ignore[method-assign]
    current_status = getattr(plane_cls, "status")
    if not bool(getattr(current_status, "_roi_robinhood_sequencer_frontier", False)):
        plane_cls.status = _status_with_sequencer(current_status)  # type: ignore[method-assign]
    setattr(plane_cls, "_roi_pre_sequencer_run", current_run)
    setattr(plane_cls, "_roi_robinhood_sequencer_frontier_version", REPAIR_VERSION)
    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "repair_version": REPAIR_VERSION,
        "installed": _INSTALLED,
        "head_transport": "official_public_sequencer_feed",
        "log_transport": "single_exact_current_block_eth_getLogs_topic_filter",
        "decision_lag_blocks": DECISION_LAG_BLOCKS,
        "decision_lag_blocks_changed": False,
        "retrospective_entry_authority": False,
        "changes_strategy_authority": False,
        "changes_economic_thresholds": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


__all__ = [
    "DECISION_LAG_BLOCKS",
    "FEED_STALE_SECONDS",
    "REPAIR_VERSION",
    "_advance_sequencer_frontier",
    "_feed_fresh",
    "_fresh_feed_head_ready",
    "_sequence_numbers",
    "install_robinhood_sequencer_frontier",
    "status",
]
