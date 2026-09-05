from __future__ import annotations

import asyncio
from typing import Any, Callable

from .direct_solana import DirectSolanaIngestionPlane
from .handshake_pump import MAX_INFLIGHT_NOTIFICATION_HANDLERS
from . import public_ws_shard_transport_repair as public_shards


REPAIR_VERSION = "notification-dispatch-bounded-drain-v1"

_STATS: dict[str, int] = {
    "saturation_wait_events": 0,
    "handler_completions_during_wait": 0,
    "handler_failures_during_wait": 0,
    "max_inflight_observed": 0,
}


async def _bounded_drain_dispatch_capacity(tasks: set[asyncio.Task[Any]]) -> None:
    """Apply bounded backpressure without converting saturation into a disconnect.

    The public shard reader still owns the only websocket ``recv`` loop and handler
    concurrency remains capped at the existing constant. When every handler slot is
    occupied, stop receiving long enough for at least one already-owned handler to
    finish. The websocket library and TCP receive window provide the next bounded
    buffer, so no raw notification is dropped and ordinary saturation no longer
    tears down all accepted subscriptions on the shard.
    """

    active = len(tasks)
    _STATS["max_inflight_observed"] = max(_STATS["max_inflight_observed"], active)
    while len(tasks) >= MAX_INFLIGHT_NOTIFICATION_HANDLERS:
        _STATS["saturation_wait_events"] += 1
        snapshot = tuple(tasks)
        if not snapshot:
            return
        done, _ = await asyncio.wait(snapshot, return_when=asyncio.FIRST_COMPLETED)
        _STATS["handler_completions_during_wait"] += len(done)
        for task in done:
            tasks.discard(task)
            if task.cancelled():
                continue
            exc = task.exception()
            if exc is not None:
                _STATS["handler_failures_during_wait"] += 1
                raise exc
        await asyncio.sleep(0)


def _status_with_bounded_dispatch(
    original: Callable[[Any], dict[str, Any]]
) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        policy = payload.setdefault("provider_runtime_policy", {})
        if isinstance(policy, dict):
            policy.update(
                {
                    "public_notification_dispatch_path": "bounded-concurrent-handlers-with-drain-backpressure",
                    "public_notification_saturation_disconnects": False,
                    "public_notification_saturation_policy": "wait-for-owned-handler-completion",
                    "public_notification_max_inflight_unchanged": MAX_INFLIGHT_NOTIFICATION_HANDLERS,
                    "raw_receipt_drops_allowed": False,
                    "target_quorum_semantics_unchanged": True,
                    "live_poll_recoverability_lease_seconds_unchanged": 12.0,
                }
            )
        payload["notification_dispatch_capacity_repair"] = {
            "repair_version": REPAIR_VERSION,
            "enabled": True,
            "saturation_policy": "bounded-drain-not-reconnect",
            "max_inflight_handlers": MAX_INFLIGHT_NOTIFICATION_HANDLERS,
            "max_inflight_limit_changed": False,
            "saturation_wait_events": int(_STATS["saturation_wait_events"]),
            "handler_completions_during_wait": int(_STATS["handler_completions_during_wait"]),
            "handler_failures_during_wait": int(_STATS["handler_failures_during_wait"]),
            "max_inflight_observed": int(_STATS["max_inflight_observed"]),
            "drops_allowed": False,
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
    setattr(status, "_roi_notification_dispatch_bounded_drain", True)
    return status


def install_notification_dispatch_backpressure_repair() -> None:
    current_capacity = public_shards._cooperative_dispatch_capacity
    if not bool(getattr(current_capacity, "_roi_bounded_drain", False)):
        setattr(_bounded_drain_dispatch_capacity, "_roi_bounded_drain", True)
        setattr(_bounded_drain_dispatch_capacity, "_roi_bounded_drain_version", REPAIR_VERSION)
        public_shards._cooperative_dispatch_capacity = _bounded_drain_dispatch_capacity

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_notification_dispatch_bounded_drain", False)):
        DirectSolanaIngestionPlane.status = _status_with_bounded_dispatch(current_status)  # type: ignore[method-assign]


__all__ = [
    "REPAIR_VERSION",
    "_bounded_drain_dispatch_capacity",
    "install_notification_dispatch_backpressure_repair",
]
