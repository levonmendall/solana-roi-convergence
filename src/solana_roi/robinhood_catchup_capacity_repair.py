from __future__ import annotations

import asyncio
import math
import os
import time
from collections import deque
from collections.abc import Callable
from typing import Any

from . import robinhood_chain_runtime as runtime
from .robinhood_chain_runtime import RobinhoodRuntimeMixin


REPAIR_VERSION = "robinhood-catchup-capacity-v1"
DEFAULT_CATCHUP_MAX_BLOCKS = 800
MAX_CATCHUP_MAX_BLOCKS = 2000
DEFAULT_CATCHUP_POLL_SECONDS = 0.25
DEFAULT_QUERY_CONCURRENCY = 2
MAX_QUERY_CONCURRENCY = 4
STALL_OBSERVATION_SECONDS = 30.0

_ORIGINAL_STATUS: Callable[[Any], dict[str, Any]] | None = None


def _catchup_max_blocks() -> int:
    try:
        value = int(os.getenv("ROBINHOOD_CATCHUP_MAX_BLOCKS_PER_POLL", str(DEFAULT_CATCHUP_MAX_BLOCKS)))
    except (TypeError, ValueError):
        value = DEFAULT_CATCHUP_MAX_BLOCKS
    return max(runtime.MAX_BLOCKS_PER_POLL, min(MAX_CATCHUP_MAX_BLOCKS, value))


def _catchup_poll_seconds() -> float:
    try:
        value = float(os.getenv("ROBINHOOD_CATCHUP_POLL_SECONDS", str(DEFAULT_CATCHUP_POLL_SECONDS)))
    except (TypeError, ValueError):
        value = DEFAULT_CATCHUP_POLL_SECONDS
    return max(0.05, min(2.0, value))


def _query_concurrency() -> int:
    try:
        value = int(os.getenv("ROBINHOOD_CATCHUP_QUERY_CONCURRENCY", str(DEFAULT_QUERY_CONCURRENCY)))
    except (TypeError, ValueError):
        value = DEFAULT_QUERY_CONCURRENCY
    return max(1, min(MAX_QUERY_CONCURRENCY, value))


def _ensure_metrics(self: Any) -> deque[tuple[float, int, int]]:
    history = getattr(self, "_roi_catchup_history", None)
    if not isinstance(history, deque):
        history = deque(maxlen=24)
        setattr(self, "_roi_catchup_history", history)
    if not hasattr(self, "_roi_blocks_scanned_total"):
        setattr(self, "_roi_blocks_scanned_total", 0)
    if not hasattr(self, "_roi_catchup_started_at"):
        setattr(self, "_roi_catchup_started_at", None)
    if not hasattr(self, "_roi_last_batch_blocks"):
        setattr(self, "_roi_last_batch_blocks", 0)
    if not hasattr(self, "_roi_last_batch_seconds"):
        setattr(self, "_roi_last_batch_seconds", None)
    if not hasattr(self, "_roi_last_batch_size_limit"):
        setattr(self, "_roi_last_batch_size_limit", runtime.MAX_BLOCKS_PER_POLL)
    if not hasattr(self, "_roi_catchup_mode"):
        setattr(self, "_roi_catchup_mode", False)
    return history


def _select_batch_limit(lag: int) -> int:
    # Stay on the original 200-block shape near the live frontier. A larger batch
    # is used only while paper decisions are disabled by catch-up.
    if lag <= runtime.MAX_BLOCKS_PER_POLL + runtime.LIVE_LAG_BLOCKS:
        return runtime.MAX_BLOCKS_PER_POLL
    return _catchup_max_blocks()


async def _logs_with_resilient_range(
    self: Any,
    *,
    from_block: int,
    to_block: int,
    addresses: list[str],
    topics: list[Any] | None,
) -> list[dict[str, Any]]:
    last_error: BaseException | None = None
    for attempt in range(3):
        try:
            return await self.rpc.get_logs(
                from_block=from_block,
                to_block=to_block,
                addresses=addresses,
                topics=topics,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            last_error = exc
            if attempt < 2:
                await asyncio.sleep(0.15 * (2**attempt))

    # Some public EVM RPCs enforce smaller eth_getLogs ranges under load. Split a
    # catch-up range only after retrying it; never skip the failed interval.
    if to_block - from_block + 1 > runtime.MAX_BLOCKS_PER_POLL:
        midpoint = (from_block + to_block) // 2
        left = await _logs_with_resilient_range(
            self,
            from_block=from_block,
            to_block=midpoint,
            addresses=addresses,
            topics=topics,
        )
        right = await _logs_with_resilient_range(
            self,
            from_block=midpoint + 1,
            to_block=to_block,
            addresses=addresses,
            topics=topics,
        )
        return left + right
    assert last_error is not None
    raise last_error


async def _fetch_market_logs(
    self: Any,
    *,
    from_block: int,
    to_block: int,
) -> list[tuple[str, dict[str, Any], dict[str, Any]]]:
    jobs: list[tuple[str, list[Any], dict[str, Any]]] = []
    pools = list(self.v3_pools.values())
    for index in range(0, len(pools), 32):
        batch = pools[index : index + 32]
        jobs.append(
            (
                "v3",
                batch,
                {
                    "addresses": [pool.pool for pool in batch],
                    "topics": [runtime.V3_SWAP_TOPIC],
                },
            )
        )
    curves = list(self.v2_curves.values())
    for index in range(0, len(curves), 32):
        batch = curves[index : index + 32]
        jobs.append(
            (
                "v2",
                batch,
                {
                    "addresses": [curve.curve for curve in batch],
                    "topics": [[runtime.PONS_V2_CURVE_BUY_TOPIC, runtime.PONS_V2_CURVE_SELL_TOPIC]],
                },
            )
        )

    gate = asyncio.Semaphore(_query_concurrency())

    async def fetch(kind: str, batch: list[Any], query: dict[str, Any]) -> tuple[str, list[Any], list[dict[str, Any]]]:
        async with gate:
            rows = await _logs_with_resilient_range(
                self,
                from_block=from_block,
                to_block=to_block,
                addresses=query["addresses"],
                topics=query["topics"],
            )
        return kind, batch, rows

    results = await asyncio.gather(*(fetch(kind, batch, query) for kind, batch, query in jobs))
    flattened: list[tuple[str, dict[str, Any], dict[str, Any]]] = []
    for kind, batch, rows in results:
        by_market = {
            (item.pool if kind == "v3" else item.curve): item
            for item in batch
        }
        for log in rows:
            key = runtime._clean_address(log.get("address"))
            market = by_market.get(key)
            if market is not None:
                flattened.append((kind, market, log))
    # Deterministic chain order makes concurrent reads observationally equivalent to
    # the prior sequential reads.
    flattened.sort(
        key=lambda row: (
            int(str(row[2].get("blockNumber") or "0x0"), 16),
            int(str(row[2].get("transactionIndex") or "0x0"), 16),
            int(str(row[2].get("logIndex") or "0x0"), 16),
        )
    )
    return flattened


async def _capacity_poll_once(self: Any) -> None:
    _ensure_metrics(self)
    poll_started = time.monotonic()
    self._last_poll_at = runtime._utcnow()
    chain_id = await self.rpc.chain_id()
    if chain_id != runtime.ROBINHOOD_CHAIN_ID:
        raise RuntimeError(f"wrong Robinhood chain id: {chain_id}")
    latest = await self.rpc.block_number()
    self._latest_block = latest
    if self._cursor is None:
        lookback = max(10, int(os.getenv("ROBINHOOD_BOOTSTRAP_BLOCK_LOOKBACK", "1200")))
        self._cursor = max(runtime.PONS_V1_LEGACY_START_BLOCK, latest - lookback)

    if self._cursor >= latest:
        self._caught_up = True
        self._roi_catchup_mode = False
        self._roi_last_batch_blocks = 0
        await self._settle_open_positions()
        self._last_success_at = runtime._utcnow()
        self._last_error = None
        history = _ensure_metrics(self)
        history.append((time.monotonic(), 0, int(self._roi_blocks_scanned_total)))
        return

    start_cursor = int(self._cursor)
    lag_before = max(0, latest - start_cursor)
    batch_limit = _select_batch_limit(lag_before)
    self._roi_last_batch_size_limit = batch_limit
    self._roi_catchup_mode = lag_before > runtime.LIVE_LAG_BLOCKS
    if self._roi_catchup_mode and self._roi_catchup_started_at is None:
        self._roi_catchup_started_at = runtime._utcnow()

    to_block = min(latest, start_cursor + batch_limit)
    from_block = start_cursor + 1
    live = latest - to_block <= runtime.LIVE_LAG_BLOCKS
    observed_at = runtime._utcnow()

    # Factory events are processed first so markets created inside this exact range
    # are included in the same range's swap acquisition below.
    factory_logs = await _logs_with_resilient_range(
        self,
        from_block=from_block,
        to_block=to_block,
        addresses=[
            runtime.UNISWAP_V3_FACTORY,
            runtime.PONS_V1_ACTIVE_FACTORY,
            runtime.PONS_V1_LEGACY_FACTORY,
            runtime.PONS_V2_FACTORY,
        ],
        topics=None,
    )
    for log in factory_logs:
        await self._process_factory_log(log)

    market_logs = await _fetch_market_logs(self, from_block=from_block, to_block=to_block)
    for kind, market, log in market_logs:
        if kind == "v3":
            await self._process_v3_swap(market, log, live=live, observed_at=observed_at)
        else:
            await self._process_v2_curve_log(market, log, live=live, observed_at=observed_at)

    self._set_cursor(to_block)
    self._caught_up = latest - to_block <= runtime.LIVE_LAG_BLOCKS
    if self._caught_up:
        self._roi_catchup_mode = False
        await self._settle_open_positions()
    self._last_success_at = runtime._utcnow()
    self._last_error = None

    scanned = max(0, to_block - start_cursor)
    self._roi_last_batch_blocks = scanned
    self._roi_blocks_scanned_total = int(self._roi_blocks_scanned_total) + scanned
    self._roi_last_batch_seconds = max(0.0, time.monotonic() - poll_started)
    lag_after = max(0, latest - to_block)
    history = _ensure_metrics(self)
    history.append((time.monotonic(), lag_after, int(self._roi_blocks_scanned_total)))


async def _capacity_run(self: Any, stop: asyncio.Event) -> None:
    if not self.enabled:
        return
    while not stop.is_set():
        succeeded = False
        try:
            await self._poll_once()
            succeeded = True
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._rpc_failures += 1
            self._last_error = f"{type(exc).__name__}: {exc}"
        delay = (
            _catchup_poll_seconds()
            if succeeded and not bool(self._caught_up)
            else float(self.poll_seconds)
        )
        try:
            await asyncio.wait_for(stop.wait(), timeout=max(0.05, delay))
        except TimeoutError:
            pass


def _status_with_catchup_capacity(original: Callable[[Any], dict[str, Any]]) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        history = _ensure_metrics(self)
        cursor = self._cursor
        latest = self._latest_block
        lag = max(0, int(latest) - int(cursor)) if cursor is not None and latest is not None else None
        net_catchup_per_minute: float | None = None
        scanned_per_minute: float | None = None
        observation_seconds = 0.0
        if len(history) >= 2:
            first_t, first_lag, first_scanned = history[0]
            last_t, last_lag, last_scanned = history[-1]
            observation_seconds = max(0.0, last_t - first_t)
            if observation_seconds > 0:
                net_catchup_per_minute = (first_lag - last_lag) / observation_seconds * 60.0
                scanned_per_minute = (last_scanned - first_scanned) / observation_seconds * 60.0
        current_limit = int(getattr(self, "_roi_last_batch_size_limit", runtime.MAX_BLOCKS_PER_POLL))
        estimated_batches = None
        if lag is not None:
            remaining = max(0, lag - runtime.LIVE_LAG_BLOCKS)
            estimated_batches = math.ceil(remaining / max(1, current_limit))
        stalled = bool(
            lag is not None
            and lag > runtime.LIVE_LAG_BLOCKS
            and observation_seconds >= STALL_OBSERVATION_SECONDS
            and net_catchup_per_minute is not None
            and net_catchup_per_minute <= 0.0
        )
        payload["block_lag"] = lag
        payload["paper_decision_transport_ready"] = bool(self._caught_up)
        payload["catchup_capacity"] = {
            "repair_version": REPAIR_VERSION,
            "catchup_mode": bool(getattr(self, "_roi_catchup_mode", False)),
            "live_lag_blocks": runtime.LIVE_LAG_BLOCKS,
            "live_batch_limit_blocks": runtime.MAX_BLOCKS_PER_POLL,
            "catchup_batch_limit_blocks": _catchup_max_blocks(),
            "current_batch_limit_blocks": current_limit,
            "last_batch_blocks": int(getattr(self, "_roi_last_batch_blocks", 0)),
            "last_batch_seconds": getattr(self, "_roi_last_batch_seconds", None),
            "blocks_scanned_total_this_process": int(getattr(self, "_roi_blocks_scanned_total", 0)),
            "blocks_scanned_per_minute": scanned_per_minute,
            "net_catchup_rate_blocks_per_minute": net_catchup_per_minute,
            "estimated_batches_to_live": estimated_batches,
            "catchup_poll_seconds": _catchup_poll_seconds(),
            "query_concurrency": _query_concurrency(),
            "catchup_started_at": getattr(self, "_roi_catchup_started_at", None),
            "observation_window_seconds": observation_seconds,
            "catchup_stalled": stalled,
            "factory_logs_processed_before_parallel_market_reads": True,
            "failed_large_ranges_split_without_skipping_blocks": True,
            "paper_entries_allowed_during_catchup": False,
            "strategy_thresholds_changed": False,
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        }
        return payload

    try:
        status.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(status, "_roi_catchup_capacity", True)
    return status


def install_robinhood_catchup_capacity_repair() -> None:
    global _ORIGINAL_STATUS
    if not bool(getattr(RobinhoodRuntimeMixin._poll_once, "_roi_catchup_capacity", False)):
        setattr(_capacity_poll_once, "_roi_catchup_capacity", True)
        RobinhoodRuntimeMixin._poll_once = _capacity_poll_once  # type: ignore[method-assign]
    if not bool(getattr(RobinhoodRuntimeMixin.run, "_roi_catchup_capacity", False)):
        setattr(_capacity_run, "_roi_catchup_capacity", True)
        RobinhoodRuntimeMixin.run = _capacity_run  # type: ignore[method-assign]
    current_status = RobinhoodRuntimeMixin.status
    if not bool(getattr(current_status, "_roi_catchup_capacity", False)):
        _ORIGINAL_STATUS = current_status
        RobinhoodRuntimeMixin.status = _status_with_catchup_capacity(current_status)  # type: ignore[method-assign]


__all__ = [
    "DEFAULT_CATCHUP_MAX_BLOCKS",
    "REPAIR_VERSION",
    "_catchup_max_blocks",
    "_catchup_poll_seconds",
    "_query_concurrency",
    "_select_batch_limit",
    "install_robinhood_catchup_capacity_repair",
]
