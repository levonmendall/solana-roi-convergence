from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import Any

from . import robinhood_chain_runtime as runtime


REPAIR_VERSION = "robinhood-live-frontier-verification-v1"


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _inc(self: Any, name: str) -> None:
    attr = f"_roi_live_frontier_{name}"
    setattr(self, attr, int(getattr(self, attr, 0) or 0) + 1)


async def _fresh_head_ready(self: Any) -> bool:
    """Require a just-read chain head before any Robinhood paper entry.

    Catch-up batches can take several seconds on the public RPC. A `latest` value
    read before processing that batch is therefore not sufficient proof that the
    persisted cursor is still within the existing <=2-block live boundary at the
    moment an entry is considered. Re-read only the block number on the candidate
    path, fail closed on RPC error, and never change the strategy threshold itself.
    """

    _inc(self, "checks")
    try:
        fresh_latest = int(await self.rpc.block_number())
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        _inc(self, "failures")
        setattr(self, "_roi_live_frontier_last_error_type", type(exc).__name__)
        setattr(self, "_roi_live_frontier_last_checked_at", _utcnow())
        self._caught_up = False
        return False

    cursor = getattr(self, "_cursor", None)
    if cursor is None:
        _inc(self, "missing_cursor")
        setattr(self, "_roi_live_frontier_last_error_type", "MissingCursor")
        setattr(self, "_roi_live_frontier_last_checked_at", _utcnow())
        self._caught_up = False
        self._latest_block = fresh_latest
        return False

    lag = max(0, fresh_latest - int(cursor))
    self._latest_block = fresh_latest
    setattr(self, "_roi_live_frontier_last_lag", lag)
    setattr(self, "_roi_live_frontier_last_checked_at", _utcnow())
    setattr(self, "_roi_live_frontier_last_error_type", None)

    ready = bool(getattr(self, "_caught_up", False)) and lag <= runtime.LIVE_LAG_BLOCKS
    if not ready:
        if bool(getattr(self, "_caught_up", False)) and lag > runtime.LIVE_LAG_BLOCKS:
            _inc(self, "stale_ready_corrections")
        self._caught_up = False
        if hasattr(self, "_roi_catchup_mode"):
            self._roi_catchup_mode = True
        return False

    _inc(self, "ready_checks")
    return True


def _entry_guard(original: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
    async def guarded(self: Any, *args: Any, **kwargs: Any) -> Any:
        if not await _fresh_head_ready(self):
            return None
        return await original(self, *args, **kwargs)

    try:
        guarded.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(guarded, "_roi_fresh_live_frontier_entry_guard", True)
    return guarded


def _status_with_frontier_verification(
    original: Callable[[Any], dict[str, Any]],
) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        payload["live_frontier_verification"] = {
            "repair_version": REPAIR_VERSION,
            "fresh_block_number_required_before_paper_entry": True,
            "live_lag_blocks": runtime.LIVE_LAG_BLOCKS,
            "checks": int(getattr(self, "_roi_live_frontier_checks", 0) or 0),
            "ready_checks": int(getattr(self, "_roi_live_frontier_ready_checks", 0) or 0),
            "failures": int(getattr(self, "_roi_live_frontier_failures", 0) or 0),
            "missing_cursor": int(getattr(self, "_roi_live_frontier_missing_cursor", 0) or 0),
            "stale_ready_corrections": int(
                getattr(self, "_roi_live_frontier_stale_ready_corrections", 0) or 0
            ),
            "last_lag": getattr(self, "_roi_live_frontier_last_lag", None),
            "last_checked_at": getattr(self, "_roi_live_frontier_last_checked_at", None),
            "last_error_type": getattr(self, "_roi_live_frontier_last_error_type", None),
            "strategy_thresholds_changed": False,
            "paper_decision_lag_boundary_changed": False,
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
    setattr(status, "_roi_fresh_live_frontier_status", True)
    return status


def install_robinhood_live_frontier_verification_repair(plane_cls: type[Any]) -> None:
    for name in ("_maybe_open_v3", "_maybe_open_v2"):
        current = getattr(plane_cls, name, None)
        if current is None:
            continue
        if not bool(getattr(current, "_roi_fresh_live_frontier_entry_guard", False)):
            setattr(plane_cls, name, _entry_guard(current))

    current_status = getattr(plane_cls, "status", None)
    if current_status is not None and not bool(
        getattr(current_status, "_roi_fresh_live_frontier_status", False)
    ):
        plane_cls.status = _status_with_frontier_verification(current_status)  # type: ignore[method-assign]

    setattr(plane_cls, "_roi_live_frontier_verification_installed", True)
    setattr(plane_cls, "_roi_live_frontier_verification_version", REPAIR_VERSION)


__all__ = [
    "REPAIR_VERSION",
    "_entry_guard",
    "_fresh_head_ready",
    "install_robinhood_live_frontier_verification_repair",
]
