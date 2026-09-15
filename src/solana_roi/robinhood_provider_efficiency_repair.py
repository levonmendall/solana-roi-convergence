from __future__ import annotations

import asyncio
from typing import Any

from . import robinhood_catchup_capacity_repair as catchup
from . import robinhood_chain_runtime as runtime
from . import robinhood_live_frontier_verification_repair as frontier


EFFICIENCY_REPAIR_VERSION = "robinhood-provider-efficiency-v2-live-frontier-composed-market-log-batching"
_INSTALLED = False
_ORIGINAL_FETCH_MARKET_LOGS = catchup._fetch_market_logs
_ORIGINAL_FRONTIER_FETCH_MARKET_LOGS = frontier._fetch_market_logs


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
            # The combined provider query ORs all three topic[0] signatures. Recheck
            # the venue-specific signature locally so this remains observationally
            # identical to the prior separate V3 and V2 provider filters even if a
            # contract emits an unexpected cross-family signature.
            if key in v3_by_market and topic_zero == v3_topic:
                flattened.append(("v3", v3_by_market[key], log))
            elif key in v2_by_market and topic_zero in v2_topics:
                flattened.append(("v2", v2_by_market[key], log))

    # Preserve the exact catch-up/runtime deterministic chain order.
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


def install_robinhood_provider_efficiency_repair() -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    # Both seams matter. The verified live-frontier module imports the catch-up helper
    # by value, so replacing only catchup._fetch_market_logs leaves the actual
    # forward-only production lane on the legacy two-query path. Bind the composed
    # helper to both aliases before the production worker starts.
    catchup._fetch_market_logs = _combined_fetch_market_logs
    frontier._fetch_market_logs = _combined_fetch_market_logs
    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "version": EFFICIENCY_REPAIR_VERSION,
        "installed": _INSTALLED,
        "composed_catchup_market_log_batching": catchup._fetch_market_logs is _combined_fetch_market_logs,
        "composed_live_frontier_market_log_batching": frontier._fetch_market_logs is _combined_fetch_market_logs,
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
    "install_robinhood_provider_efficiency_repair",
    "status",
]
