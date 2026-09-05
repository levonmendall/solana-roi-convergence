from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from functools import wraps
from typing import Any

from . import robinhood_catchup_capacity_repair as catchup
from . import robinhood_live_frontier_verification_repair as frontier


REPAIR_VERSION = "robinhood-live-getlogs-resilience-v1"
_INSTALLED = False
_ORIGINAL: Callable[..., Awaitable[list[dict[str, Any]]]] | None = None


async def _resilient_live_logs(
    self: Any,
    *,
    from_block: int,
    to_block: int,
    addresses: list[str],
    topics: list[Any] | None,
) -> list[dict[str, Any]]:
    """Acquire every requested log without treating a 64-block timeout as terminal.

    The prior helper retried a failing live request but only split ranges larger than
    the legacy 200-block scanner batch. Production's authoritative live window is 64
    blocks, so repeated ReadTimeouts aborted the whole current-window cycle. Split any
    multi-block range after bounded retries; if one exact block still fails and the
    address set is compound, split the addresses. No block/address is skipped and no
    failed interval gains paper-entry authority.
    """
    last_error: BaseException | None = None
    for attempt in range(3):
        try:
            return await self.rpc.get_logs(
                from_block=int(from_block),
                to_block=int(to_block),
                addresses=list(addresses),
                topics=topics,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            last_error = exc
            if attempt < 2:
                await asyncio.sleep(0.10 * (2**attempt))

    if int(from_block) < int(to_block):
        midpoint = (int(from_block) + int(to_block)) // 2
        left = await _resilient_live_logs(
            self,
            from_block=int(from_block),
            to_block=midpoint,
            addresses=addresses,
            topics=topics,
        )
        right = await _resilient_live_logs(
            self,
            from_block=midpoint + 1,
            to_block=int(to_block),
            addresses=addresses,
            topics=topics,
        )
        return left + right

    if len(addresses) > 1:
        midpoint = len(addresses) // 2
        left_addresses = list(addresses[:midpoint])
        right_addresses = list(addresses[midpoint:])
        left = await _resilient_live_logs(
            self,
            from_block=int(from_block),
            to_block=int(to_block),
            addresses=left_addresses,
            topics=topics,
        )
        right = await _resilient_live_logs(
            self,
            from_block=int(from_block),
            to_block=int(to_block),
            addresses=right_addresses,
            topics=topics,
        )
        return left + right

    assert last_error is not None
    raise last_error


setattr(_resilient_live_logs, "_roi_live_getlogs_resilience", True)


def install_robinhood_live_getlogs_resilience() -> None:
    global _INSTALLED, _ORIGINAL
    if _INSTALLED:
        return
    current = catchup._logs_with_resilient_range
    if not bool(getattr(current, "_roi_live_getlogs_resilience", False)):
        _ORIGINAL = current
        try:
            _resilient_live_logs.__dict__.update(getattr(current, "__dict__", {}))
        except Exception:
            pass
        catchup._logs_with_resilient_range = _resilient_live_logs
        # frontier imported the old helper by value, so update that reference too.
        frontier._logs_with_resilient_range = _resilient_live_logs
    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "repair_version": REPAIR_VERSION,
        "installed": _INSTALLED,
        "splits_failed_live_ranges_below_legacy_200_block_batch": True,
        "single_block_address_split_fallback": True,
        "skips_failed_blocks": False,
        "changes_strategy_thresholds": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


__all__ = [
    "REPAIR_VERSION",
    "_resilient_live_logs",
    "install_robinhood_live_getlogs_resilience",
    "status",
]
