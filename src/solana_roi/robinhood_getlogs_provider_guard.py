from __future__ import annotations

import os
from collections.abc import Awaitable, Callable
from functools import wraps
from typing import Any
from urllib.parse import urlparse

from . import robinhood_chain_core as core


REPAIR_VERSION = "robinhood-getlogs-provider-guard-v1"
ALCHEMY_SAFE_MAX_BLOCKS = 10
MAX_CONFIGURED_BLOCKS = 10_000
ENV_MAX_BLOCKS = "ROBINHOOD_ETH_GET_LOGS_MAX_BLOCKS"

_INSTALLED = False
_ORIGINAL_GET_LOGS: Callable[..., Awaitable[list[dict[str, Any]]]] | None = None


def _is_alchemy_endpoint(rpc_url: str) -> bool:
    try:
        host = (urlparse(str(rpc_url)).hostname or "").lower()
    except Exception:
        return False
    return bool(
        host == "alchemy.com"
        or host.endswith(".alchemy.com")
        or host == "alchemyapi.io"
        or host.endswith(".alchemyapi.io")
    )


def _explicit_max_blocks() -> int | None:
    raw = str(os.getenv(ENV_MAX_BLOCKS) or "").strip()
    if not raw:
        return None
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return None
    if value <= 0:
        return None
    return min(MAX_CONFIGURED_BLOCKS, value)


def _provider_max_blocks(self: Any) -> int | None:
    explicit = _explicit_max_blocks()
    if explicit is not None:
        return explicit
    if _is_alchemy_endpoint(str(getattr(self, "rpc_url", "") or "")):
        # The connected Robinhood production app is on the Alchemy Free tier, whose
        # eth_getLogs range is capped at ten inclusive blocks. Keep ten as the safe
        # automatic default even if the account is later upgraded; operators can
        # raise the cap explicitly through ENV_MAX_BLOCKS after validating the plan.
        return ALCHEMY_SAFE_MAX_BLOCKS
    return None


def _inc(self: Any, name: str, amount: int = 1) -> None:
    attr = f"_roi_getlogs_guard_{name}"
    setattr(self, attr, int(getattr(self, attr, 0) or 0) + int(amount))


def _set_max(self: Any, name: str, value: int) -> None:
    attr = f"_roi_getlogs_guard_{name}"
    setattr(self, attr, max(int(getattr(self, attr, 0) or 0), int(value)))


async def _provider_bounded_get_logs(
    self: Any,
    *,
    from_block: int,
    to_block: int,
    addresses: list[str] | tuple[str, ...] | None = None,
    topics: list[Any] | None = None,
) -> list[dict[str, Any]]:
    if _ORIGINAL_GET_LOGS is None:
        raise RuntimeError("Robinhood eth_getLogs provider guard is not installed")

    start = int(from_block)
    end = int(to_block)
    if end < start:
        return await _ORIGINAL_GET_LOGS(
            self,
            from_block=start,
            to_block=end,
            addresses=addresses,
            topics=topics,
        )

    requested_blocks = end - start + 1
    _set_max(self, "max_requested_blocks", requested_blocks)
    limit = _provider_max_blocks(self)
    if limit is None or requested_blocks <= limit:
        _set_max(self, "max_sent_blocks", requested_blocks)
        return await _ORIGINAL_GET_LOGS(
            self,
            from_block=start,
            to_block=end,
            addresses=addresses,
            topics=topics,
        )

    _inc(self, "ranges_chunked")
    rows: list[dict[str, Any]] = []
    cursor = start
    while cursor <= end:
        chunk_end = min(end, cursor + limit - 1)
        chunk_blocks = chunk_end - cursor + 1
        _inc(self, "provider_requests")
        _set_max(self, "max_sent_blocks", chunk_blocks)
        rows.extend(
            await _ORIGINAL_GET_LOGS(
                self,
                from_block=cursor,
                to_block=chunk_end,
                addresses=addresses,
                topics=topics,
            )
        )
        cursor = chunk_end + 1
    return rows


setattr(_provider_bounded_get_logs, "_roi_robinhood_getlogs_provider_guard", True)


def install_robinhood_getlogs_provider_guard() -> None:
    global _INSTALLED, _ORIGINAL_GET_LOGS
    current = core.RobinhoodRpc.get_logs
    if bool(getattr(current, "_roi_robinhood_getlogs_provider_guard", False)):
        _INSTALLED = True
        return
    _ORIGINAL_GET_LOGS = current
    wrapped = wraps(current)(_provider_bounded_get_logs)
    setattr(wrapped, "_roi_robinhood_getlogs_provider_guard", True)
    core.RobinhoodRpc.get_logs = wrapped  # type: ignore[method-assign]
    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "repair_version": REPAIR_VERSION,
        "installed": _INSTALLED,
        "alchemy_detected_max_blocks": ALCHEMY_SAFE_MAX_BLOCKS,
        "configured_max_blocks_env": ENV_MAX_BLOCKS,
        "prevents_oversized_provider_requests": True,
        "inclusive_block_range_accounting": True,
        "changes_strategy_thresholds": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


__all__ = [
    "ALCHEMY_SAFE_MAX_BLOCKS",
    "ENV_MAX_BLOCKS",
    "REPAIR_VERSION",
    "_is_alchemy_endpoint",
    "_provider_bounded_get_logs",
    "_provider_max_blocks",
    "install_robinhood_getlogs_provider_guard",
    "status",
]
