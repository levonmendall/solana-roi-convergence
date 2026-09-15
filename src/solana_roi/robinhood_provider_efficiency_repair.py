from __future__ import annotations

import asyncio
from typing import Any

from . import robinhood_catchup_capacity_repair as catchup
from . import robinhood_chain_runtime as runtime
from . import robinhood_live_frontier_verification_repair as frontier


EFFICIENCY_REPAIR_VERSION = "robinhood-provider-efficiency-v3-aligned-frontier-dedup"
_INSTALLED = False
_ORIGINAL_FETCH_MARKET_LOGS = catchup._fetch_market_logs
_ORIGINAL_FRONTIER_FETCH_MARKET_LOGS = frontier._fetch_market_logs
_ORIGINAL_ADVANCE_LIVE_EPOCH = frontier._advance_live_epoch


def _topic_zero(log: dict[str, Any]) -> str | None:
    topics = log.get("topics")
    if not isinstance(topics, list) or not topics or topics[0] is None:
        return None
    return str(topics[0]).lower()


async def _combined_fetch_market_logs(
    self: Any,
    *,
    from_block: int,
    to_block: int,
) -> list[tuple[str, dict[str, Any], dict[str, Any]]]:
    pools = list(self.v3_pools.values())
    curves = list(self.v2_curves.values())
    v3_by_market = {pool.pool: pool for pool in pools}
    v2_by_market = {curve.curve: curve for curve in curves}
    if set(v3_by_market).intersection(v2_by_market):
        raise RuntimeError("robinhood_market_address_classification_collision")

    ordered_addresses = [pool.pool for pool in pools] + [curve.curve for curve in curves]
    batch_size = runtime._combined_log_address_batch_size()
    jobs = [
        ordered_addresses[index : index + batch_size]
        for index in range(0, len(ordered_addresses), batch_size)
    ]
    gate = asyncio.Semaphore(catchup._query_concurrency())

    async def fetch(addresses: list[str]) -> list[dict[str, Any]]:
        async with gate:
            return await catchup._logs_with_resilient_range(
                self,
                from_block=from_block,
                to_block=to_block,
                addresses=addresses,
                topics=[[
                    runtime.V3_SWAP_TOPIC,
                    runtime.PONS_V2_CURVE_BUY_TOPIC,
                    runtime.PONS_V2_CURVE_SELL_TOPIC,
                ]],
            )

    results = await asyncio.gather(*(fetch(addresses) for addresses in jobs))
    v3_topic = runtime.V3_SWAP_TOPIC.lower()
    v2_topics = {
        runtime.PONS_V2_CURVE_BUY_TOPIC.lower(),
        runtime.PONS_V2_CURVE_SELL_TOPIC.lower(),
    }
    flattened: list[tuple[str, dict[str, Any], dict[str, Any]]] = []
    for rows in results:
        for log in rows:
            key = runtime._clean_address(log.get("address"))
            topic_zero = _topic_zero(log)
            if key in v3_by_market and topic_zero == v3_topic:
                flattened.append(("v3", v3_by_market[key], log))
            elif key in v2_by_market and topic_zero in v2_topics:
                flattened.append(("v2", v2_by_market[key], log))

    flattened.sort(
        key=lambda row: (
            int(str(row[2].get("blockNumber") or "0x0"), 16),
            int(str(row[2].get("transactionIndex") or "0x0"), 16),
            int(str(row[2].get("logIndex") or "0x0"), 16),
        )
    )

    legacy_equivalent = (len(pools) + 31) // 32 + (len(curves) + 31) // 32
    self._roi_market_log_legacy_equivalent_requests = int(
        getattr(self, "_roi_market_log_legacy_equivalent_requests", 0) or 0
    ) + legacy_equivalent
    self._roi_market_log_actual_requests = int(
        getattr(self, "_roi_market_log_actual_requests", 0) or 0
    ) + len(jobs)
    return flattened


async def _advance_live_epoch_with_aligned_history_reuse(self: Any) -> None:
    """Reuse a fully acquired live range for the aligned lossless cursor.

    The live-frontier wrapper runs before historical catch-up on every poll. Once the
    historical cursor has caught up exactly to the live cursor, a normal live advance
    acquires factory and market logs for the same next range that historical catch-up
    would immediately request again. After (and only after) the live lane reports a
    successfully completed contiguous range, that acquisition is semantically the
    same lossless range needed by the historical cursor, so persist the historical
    cursor to the completed live frontier and let the following catch-up poll observe
    that it is already current.

    Backlog, initial anchoring, large-gap re-anchors, chain-head regression and failed
    live ranges never take this path and therefore cannot skip historical evidence.
    """
    historical_before = frontier._historical_cursor(self)
    live_before = frontier._live_cursor(self)
    completed_before = int(getattr(self, "_roi_live_frontier_ranges_completed", 0) or 0)

    await _ORIGINAL_ADVANCE_LIVE_EPOCH(self)

    live_after = frontier._live_cursor(self)
    completed_after = int(getattr(self, "_roi_live_frontier_ranges_completed", 0) or 0)
    last_range = getattr(self, "_roi_live_epoch_last_range", None)

    reusable = bool(
        historical_before is not None
        and live_before is not None
        and historical_before == live_before
        and live_after is not None
        and live_after > live_before
        and completed_after == completed_before + 1
        and bool(getattr(self, "_roi_live_epoch_ready", False))
        and isinstance(last_range, dict)
        and int(last_range.get("from_block", -1)) == int(live_before) + 1
        and int(last_range.get("to_block", -1)) == int(live_after)
    )
    if not reusable:
        return

    self._set_cursor(int(live_after))
    reused_blocks = int(live_after) - int(live_before)
    self._roi_live_frontier_ranges_reused_for_historical = int(
        getattr(self, "_roi_live_frontier_ranges_reused_for_historical", 0) or 0
    ) + 1
    self._roi_live_frontier_blocks_reused_for_historical = int(
        getattr(self, "_roi_live_frontier_blocks_reused_for_historical", 0) or 0
    ) + reused_blocks
    self._roi_live_frontier_last_reused_range = {
        "from_block": int(live_before) + 1,
        "to_block": int(live_after),
        "blocks": reused_blocks,
        "reason": "aligned_live_range_already_fully_acquired",
    }
    print(
        "ROBINHOOD_FRONTIER_RANGE_REUSED_FOR_HISTORY "
        f"from_block={int(live_before) + 1} to_block={int(live_after)} "
        f"blocks={reused_blocks}",
        flush=True,
    )


setattr(
    _advance_live_epoch_with_aligned_history_reuse,
    "_roi_robinhood_aligned_frontier_dedup",
    True,
)


def install_robinhood_provider_efficiency_repair() -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    catchup._fetch_market_logs = _combined_fetch_market_logs
    frontier._fetch_market_logs = _combined_fetch_market_logs
    current_advance = frontier._advance_live_epoch
    if not bool(getattr(current_advance, "_roi_robinhood_aligned_frontier_dedup", False)):
        frontier._advance_live_epoch = _advance_live_epoch_with_aligned_history_reuse
    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "version": EFFICIENCY_REPAIR_VERSION,
        "installed": _INSTALLED,
        "composed_catchup_market_log_batching": catchup._fetch_market_logs is _combined_fetch_market_logs,
        "composed_live_frontier_market_log_batching": frontier._fetch_market_logs is _combined_fetch_market_logs,
        "aligned_live_range_reused_for_historical_cursor": bool(
            getattr(frontier._advance_live_epoch, "_roi_robinhood_aligned_frontier_dedup", False)
        ),
        "historical_backlog_skipped": False,
        "large_gap_reanchor_reused_as_history": False,
        "combined_address_batch_size": runtime._combined_log_address_batch_size(),
        "factory_discovery_unchanged": True,
        "block_coverage_reduced": False,
        "market_coverage_reduced": False,
        "topic_zero_or_filter_preserves_v3_and_v2_events": True,
        "venue_specific_topic_zero_revalidated_locally": True,
        "preexisting_chain_order_preserved": True,
        "bounded_retry_and_range_split_preserved": True,
        "strategy_thresholds_changed": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


__all__ = [
    "EFFICIENCY_REPAIR_VERSION",
    "_advance_live_epoch_with_aligned_history_reuse",
    "install_robinhood_provider_efficiency_repair",
    "status",
]
