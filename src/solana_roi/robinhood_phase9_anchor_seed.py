from __future__ import annotations

import asyncio
from functools import wraps
from typing import Any, Awaitable, Callable

from . import robinhood_forward_only_runtime_repair as forward_only
from . import robinhood_production_ws_transport as production_transport


ANCHOR_VERSION = "robinhood-phase9-latest-seed-reorg-insurance-v1"
_INSTALLED = False
_ORIGINAL_RUN: Callable[..., Awaitable[Any]] | None = None
_ORIGINAL_STATUS: Callable[[Any], dict[str, Any]] | None = None


async def _seed_current_anchor(self: Any) -> None:
    latest = int(await self.rpc.block_number())
    self._latest_block = latest
    await forward_only._sync_bounded_metadata(
        self,
        latest=latest,
        previous_live_cursor=None,
        reason="phase9_latest_seed_plus_reorg_insurance",
    )
    setattr(
        self,
        "_roi_phase9_anchor_seed",
        {
            "anchor_version": ANCHOR_VERSION,
            "latest_block": latest,
            "metadata_recovery_blocks": forward_only.METADATA_RECOVERY_BLOCKS,
            "factory_metadata_only": True,
            "historical_swap_replay": False,
            "historical_cursor_readiness_authority": False,
            "catchup_mode": "latest_seed_plus_reorg_insurance",
        },
    )


def _run_wrapper(original: Callable[[Any, asyncio.Event], Awaitable[None]]) -> Callable[[Any, asyncio.Event], Awaitable[None]]:
    @wraps(original)
    async def wrapped(self: Any, stop: asyncio.Event) -> None:
        if production_transport.production_provider_configured():
            # The production WebSocket runner intentionally does not replay historical
            # swaps. Before subscribing, recover only bounded near-head factory
            # metadata so a restart/reorg cannot hide a just-created market.
            await _seed_current_anchor(self)
        await original(self, stop)

    setattr(wrapped, "_roi_robinhood_phase9_anchor_seed", True)
    return wrapped


def _status_wrapper(original: Callable[[Any], dict[str, Any]]) -> Callable[[Any], dict[str, Any]]:
    @wraps(original)
    def wrapped(self: Any) -> dict[str, Any]:
        payload = original(self)
        payload["phase9_anchor_seed"] = getattr(
            self,
            "_roi_phase9_anchor_seed",
            {
                "anchor_version": ANCHOR_VERSION,
                "latest_block": None,
                "metadata_recovery_blocks": forward_only.METADATA_RECOVERY_BLOCKS,
                "factory_metadata_only": True,
                "historical_swap_replay": False,
                "historical_cursor_readiness_authority": False,
                "catchup_mode": "latest_seed_plus_reorg_insurance",
            },
        )
        return payload

    setattr(wrapped, "_roi_robinhood_phase9_anchor_seed", True)
    return wrapped


def install_robinhood_phase9_anchor_seed(plane_cls: type[Any]) -> None:
    global _INSTALLED, _ORIGINAL_RUN, _ORIGINAL_STATUS
    if _INSTALLED:
        return
    current_run = plane_cls.run
    if not bool(getattr(current_run, "_roi_robinhood_phase9_anchor_seed", False)):
        _ORIGINAL_RUN = current_run
        plane_cls.run = _run_wrapper(current_run)  # type: ignore[method-assign]
    current_status = plane_cls.status
    if not bool(getattr(current_status, "_roi_robinhood_phase9_anchor_seed", False)):
        _ORIGINAL_STATUS = current_status
        plane_cls.status = _status_wrapper(current_status)  # type: ignore[method-assign]
    setattr(plane_cls, "_roi_robinhood_phase9_anchor_seed_version", ANCHOR_VERSION)
    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "anchor_version": ANCHOR_VERSION,
        "installed": _INSTALLED,
        "catchup_mode": "latest_seed_plus_reorg_insurance",
        "bounded_metadata_recovery_blocks": forward_only.METADATA_RECOVERY_BLOCKS,
        "factory_metadata_only": True,
        "historical_swap_replay": False,
        "historical_cursor_readiness_authority": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


__all__ = [
    "ANCHOR_VERSION",
    "install_robinhood_phase9_anchor_seed",
    "status",
]
