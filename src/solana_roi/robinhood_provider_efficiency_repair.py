from __future__ import annotations

import asyncio
from typing import Any

from . import robinhood_catchup_capacity_repair as catchup
from . import robinhood_chain_runtime as runtime


EFFICIENCY_REPAIR_VERSION = "robinhood-provider-efficiency-v1-composed-market-log-batching"
_INSTALLED = False
_ORIGINAL_FETCH_MARKET_LOGS = catchup._fetch_market_logs


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
    flattened: list[tuple[str, dict[str, Any], dict[str, Any]]] = []
    for rows in results:
        for log in rows:
            key = runtime._clean_address(log.get("address"))
            if key in v3_by_market:
                flattened.append(("v3", v3_by_market[key], log))
            elif key in v2_by_market:
                flattened.append(("v2", v2_by_market[key], log))

    # Preserve the exact catch-up runtime's pre-existing deterministic chain order.
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
    catchup._fetch_market_logs = _combined_fetch_market_logs
    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "version": EFFICIENCY_REPAIR_VERSION,
        "installed": _INSTALLED,
        "composed_catchup_market_log_batching": catchup._fetch_market_logs is _combined_fetch_market_logs,
        "combined_address_batch_size": runtime._combined_log_address_batch_size(),
        "factory_discovery_unchanged": True,
        "block_coverage_reduced": False,
        "market_coverage_reduced": False,
        "topic_zero_or_filter_preserves_v3_and_v2_events": True,
        "preexisting_catchup_chain_order_preserved": True,
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
