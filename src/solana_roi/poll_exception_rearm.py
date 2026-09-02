from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

from . import live_poll_redundancy as live_poll
from . import poll_recoverability_lease as lease
from . import poll_watermark_repair as watermark
from .direct_solana import DirectSolanaIngestionPlane, WatchTarget


_ORIGINAL_FETCH_DELTA = watermark._slot_fetch_delta


async def _exception_rearm_fetch_delta(
    self: Any,
    target: WatchTarget,
    cursor_slot: int,
) -> tuple[list[dict[str, Any]], bool, str | None, float | None]:
    """Route recoverable delta exceptions through the existing standby re-arm path.

    A multi-page live poll can fail transiently even while the same target has
    remained continuously covered by the real WebSocket provider union. In that
    case no prospective observation interval has been lost: the WebSocket path is
    still authoritative, so the poll lane may be re-baselined prospectively from
    the current confirmed head exactly as it already is after a bounded overflow.

    Return an incomplete delta only when the WebSocket coverage generation is
    unchanged. The existing recoverability worker will then invoke the existing
    same-target standby re-arm helper. If that head read also fails, the exception
    still reaches the normal lease path. If any real zero-coverage generation has
    occurred, re-raise immediately so the exact-release fail-closed semantics are
    unchanged.
    """

    try:
        return await _ORIGINAL_FETCH_DELTA(self, target, cursor_slot)
    except asyncio.CancelledError:
        raise
    except Exception:
        key = live_poll._poll_target_key(target)
        runtime = lease._runtime(self).get(key, {})
        current_generation = lease._current_ws_generation(self, target)
        cursor_generation = int(runtime.get("cursor_ws_generation", current_generation) or 0)
        websocket_continuous = (
            live_poll._ws_target_covered(self, target)
            and current_generation == cursor_generation
        )
        if not websocket_continuous:
            raise
        return [], False, None, None


def _status_with_exception_rearm(
    original: Callable[[Any], dict[str, Any]],
) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        poll = payload.get("live_poll_redundancy")
        if isinstance(poll, dict):
            poll["transient_delta_exception_standby_rearm_enabled"] = True
            poll["exception_rearm_requires_continuous_same_target_websocket"] = True
            poll["exception_rearm_can_restore_irrecoverable_gap"] = False
        policy = payload.setdefault("provider_runtime_policy", {})
        if isinstance(policy, dict):
            policy.update(
                {
                    "live_poll_delta_exception_can_rearm_standby": True,
                    "live_poll_exception_rearm_requires_unchanged_ws_generation": True,
                    "live_poll_exception_rearm_can_restore_gap": False,
                    "live_poll_true_delta_bound_unchanged": True,
                }
            )
        return payload

    try:
        status.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(status, "_roi_poll_exception_rearm", True)
    return status


def install_poll_exception_rearm() -> None:
    watermark._slot_fetch_delta = _exception_rearm_fetch_delta  # type: ignore[assignment]
    live_poll._fetch_delta = _exception_rearm_fetch_delta  # type: ignore[assignment]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_poll_exception_rearm", False)):
        DirectSolanaIngestionPlane.status = _status_with_exception_rearm(current_status)  # type: ignore[method-assign]


__all__ = [
    "install_poll_exception_rearm",
    "_exception_rearm_fetch_delta",
]
