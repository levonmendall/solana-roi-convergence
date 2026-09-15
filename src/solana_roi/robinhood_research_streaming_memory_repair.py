from __future__ import annotations

import time
from collections import deque
from functools import wraps
from typing import Any, Callable

from . import robinhood_chain_runtime as runtime
from . import robinhood_provider_budget_transport as provider_budget
from . import robinhood_production_ws_transport as transport


REPAIR_VERSION = "robinhood-research-streaming-memory-v1"
_PENDING_PER_MARKET_MAX = 256
_INSTALLED = False
_ORIGINAL_STATUS: Callable[[Any], dict[str, Any]] | None = None


def _stage_logs(
    universe: dict[str, dict[str, Any]],
    logs: list[dict[str, Any]],
    pending: dict[str, deque[tuple[str, int, str]]],
) -> None:
    """Reduce raw provider logs immediately to the bounded research signal surface."""
    for log in logs:
        address = runtime._clean_address(log.get("address"))
        descriptor = universe.get(address)
        if descriptor is None:
            continue
        signal = provider_budget._research_log_signal(descriptor, log)
        if signal is None:
            continue
        side, quote, actor = signal
        queue = pending.get(address)
        if queue is None:
            queue = deque(maxlen=_PENDING_PER_MARKET_MAX)
            pending[address] = queue
        queue.append((side, int(quote), actor))


def _commit_pending(self: Any, pending: dict[str, deque[tuple[str, int, str]]]) -> int:
    committed = 0
    for address, signals in pending.items():
        for side, quote, actor in signals:
            provider_budget._record_research_event(
                self,
                address=address,
                side=side,
                quote_amount=quote,
                actor=actor,
            )
            committed += 1
    return committed


async def _streaming_research_pass(self: Any, rpc: runtime.RobinhoodRpc) -> None:
    """Run the full research pass without retaining all raw logs simultaneously.

    Coverage, request count, block range and cursor semantics are identical to the
    canonical provider-budget research pass. Each provider batch is decoded as soon as
    it arrives into a compact per-market deque capped at the same 256-event retention
    already enforced by ``_record_research_event``. Nothing is committed to research
    state until every batch succeeds, so a mid-pass provider failure preserves the
    prior cursor and prior research-event state exactly.
    """
    universe = provider_budget._candidate_universe(self)
    latest = await rpc.block_number()
    state = provider_budget._research_state(self)
    cursor = state.get("cursor_block")
    if not isinstance(cursor, int):
        provider_budget._update_research_state(
            self,
            ready=True,
            cursor_block=latest,
            last_success_monotonic=time.monotonic(),
            last_success_at=transport._utcnow(),
            last_error_type=None,
            universe_size=len(universe),
            passes=int(state.get("passes", 0) or 0) + 1,
            research_memory_repair_version=REPAIR_VERSION,
            raw_logs_retained_across_batches=False,
            max_raw_batch_logs_last_pass=0,
            compact_pending_markets_last_pass=0,
            compact_pending_signals_last_pass=0,
        )
        return
    if latest <= cursor:
        provider_budget._update_research_state(
            self,
            ready=True,
            last_success_monotonic=time.monotonic(),
            last_success_at=transport._utcnow(),
            last_error_type=None,
            universe_size=len(universe),
            passes=int(state.get("passes", 0) or 0) + 1,
            research_memory_repair_version=REPAIR_VERSION,
            raw_logs_retained_across_batches=False,
            max_raw_batch_logs_last_pass=0,
            compact_pending_markets_last_pass=0,
            compact_pending_signals_last_pass=0,
        )
        return

    to_block = min(latest, cursor + provider_budget.RESEARCH_MAX_BLOCKS_PER_PASS)
    v3 = [address for address, descriptor in universe.items() if descriptor["kind"] == "v3"]
    v2 = [address for address, descriptor in universe.items() if descriptor["kind"] == "v2"]
    pending: dict[str, deque[tuple[str, int, str]]] = {}
    logs_seen = 0
    max_raw_batch_logs = 0

    async def consume_batch(
        *, addresses: list[str], topics: list[Any]
    ) -> None:
        nonlocal logs_seen, max_raw_batch_logs
        batch_logs = await rpc.get_logs(
            from_block=cursor + 1,
            to_block=to_block,
            addresses=addresses,
            topics=topics,
        )
        batch_size = len(batch_logs)
        logs_seen += batch_size
        max_raw_batch_logs = max(max_raw_batch_logs, batch_size)
        _stage_logs(universe, batch_logs, pending)
        # Do not retain the provider response after it has been reduced to bounded
        # normalized signals. The next request therefore cannot accumulate prior raw
        # batches in process memory.
        batch_logs.clear()

    for batch in provider_budget._chunks(v3, provider_budget.RESEARCH_BATCH_SIZE):
        await consume_batch(addresses=batch, topics=[runtime.V3_SWAP_TOPIC])
    for batch in provider_budget._chunks(v2, provider_budget.RESEARCH_BATCH_SIZE):
        await consume_batch(
            addresses=batch,
            topics=[[runtime.PONS_V2_CURVE_BUY_TOPIC, runtime.PONS_V2_CURVE_SELL_TOPIC]],
        )

    pending_signals = sum(len(signals) for signals in pending.values())
    _commit_pending(self, pending)

    provider_budget._update_research_state(
        self,
        ready=True,
        cursor_block=to_block,
        last_success_monotonic=time.monotonic(),
        last_success_at=transport._utcnow(),
        last_error_type=None,
        universe_size=len(universe),
        logs_seen=int(state.get("logs_seen", 0) or 0) + logs_seen,
        passes=int(state.get("passes", 0) or 0) + 1,
        research_memory_repair_version=REPAIR_VERSION,
        raw_logs_retained_across_batches=False,
        max_raw_batch_logs_last_pass=max_raw_batch_logs,
        compact_pending_markets_last_pass=len(pending),
        compact_pending_signals_last_pass=pending_signals,
        cursor_commit_requires_complete_pass=True,
    )


setattr(_streaming_research_pass, "_roi_robinhood_research_streaming_memory", True)


def _status_wrapper(original: Callable[[Any], dict[str, Any]]) -> Callable[[Any], dict[str, Any]]:
    @wraps(original)
    def wrapped(self: Any) -> dict[str, Any]:
        payload = original(self)
        state = provider_budget._research_state(self)
        payload["robinhood_research_memory"] = {
            "version": REPAIR_VERSION,
            "installed": _INSTALLED,
            "raw_logs_retained_across_batches": bool(state.get("raw_logs_retained_across_batches", False)),
            "max_raw_batch_logs_last_pass": int(state.get("max_raw_batch_logs_last_pass", 0) or 0),
            "compact_pending_markets_last_pass": int(state.get("compact_pending_markets_last_pass", 0) or 0),
            "compact_pending_signals_last_pass": int(state.get("compact_pending_signals_last_pass", 0) or 0),
            "cursor_commit_requires_complete_pass": True,
            "research_block_cap": int(provider_budget.RESEARCH_MAX_BLOCKS_PER_PASS),
            "research_address_batch_size": int(provider_budget.RESEARCH_BATCH_SIZE),
            "candidate_universe_reduced": False,
            "request_budget_changed": False,
            "strategy_thresholds_changed": False,
            "paper_only": True,
            "live_money_authority": False,
        }
        return payload

    setattr(wrapped, "_roi_robinhood_research_streaming_memory_status", True)
    return wrapped


def install_robinhood_research_streaming_memory_repair(plane_cls: type[Any]) -> None:
    global _INSTALLED, _ORIGINAL_STATUS
    provider_budget._research_pass = _streaming_research_pass
    current = getattr(plane_cls, "status", None)
    if current is not None and not bool(getattr(current, "_roi_robinhood_research_streaming_memory_status", False)):
        _ORIGINAL_STATUS = current
        plane_cls.status = _status_wrapper(current)  # type: ignore[method-assign]
    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "version": REPAIR_VERSION,
        "installed": _INSTALLED,
        "raw_provider_batches_streamed": True,
        "pending_signals_per_market_max": _PENDING_PER_MARKET_MAX,
        "cursor_commit_requires_complete_pass": True,
        "candidate_universe_reduced": False,
        "research_block_cap_changed": False,
        "request_budget_changed": False,
        "strategy_thresholds_changed": False,
        "paper_only": True,
        "live_money_authority": False,
    }


__all__ = [
    "REPAIR_VERSION",
    "_streaming_research_pass",
    "install_robinhood_research_streaming_memory_repair",
    "status",
]
