from __future__ import annotations

import asyncio
from typing import Any, Callable

from .direct_solana import DirectSolanaIngestionPlane, WatchTarget
from .handshake_pump import MAX_INFLIGHT_NOTIFICATION_HANDLERS
from . import public_ws_shard_transport_repair as public_shards


REPAIR_VERSION = "notification-dispatch-bounded-drain-v2"

_STATS: dict[str, int] = {
    "saturation_wait_events": 0,
    "handler_completions_during_wait": 0,
    "handler_failures_during_wait": 0,
    "max_inflight_observed": 0,
}


def _strategy_critical_target_shards(
    targets: tuple[WatchTarget, ...],
    provider: str,
    targets_per_socket: int | None = None,
) -> tuple[tuple[WatchTarget, ...], ...]:
    """Keep scout continuity off the high-volume program-firehose sockets.

    The frozen production scope has three scouts and seven program targets with a
    four-target socket bound, so isolation still uses exactly three public sockets
    per provider: one scout-only shard plus two program-only shards. Program order is
    independently provider-rotated to retain the existing cross-provider correlation
    reduction. No target is removed and no subscription authority changes.
    """

    size = targets_per_socket or public_shards._targets_per_socket()
    scouts = tuple(target for target in targets if target.kind == "scout")
    programs = tuple(target for target in targets if target.kind != "scout")
    shards: list[tuple[WatchTarget, ...]] = []

    if scouts:
        ordered_scouts = public_shards._rotated_targets(scouts, f"{provider}:scout")
        shards.extend(
            tuple(ordered_scouts[index : index + size])
            for index in range(0, len(ordered_scouts), size)
        )
    if programs:
        ordered_programs = public_shards._rotated_targets(programs, f"{provider}:program")
        shards.extend(
            tuple(ordered_programs[index : index + size])
            for index in range(0, len(ordered_programs), size)
        )
    return tuple(shards)


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
                    "strategy_critical_scout_shards_isolated": True,
                    "program_firehose_shares_scout_socket": False,
                    "scout_target_scope_unchanged": True,
                    "program_target_scope_unchanged": True,
                    "raw_receipt_drops_allowed": False,
                    "target_quorum_semantics_unchanged": True,
                    "live_poll_recoverability_lease_seconds_unchanged": 12.0,
                }
            )
        payload["notification_dispatch_capacity_repair"] = {
            "repair_version": REPAIR_VERSION,
            "enabled": True,
            "saturation_policy": "bounded-drain-not-reconnect",
            "shard_policy": "scout-only-strategy-critical-shards-plus-program-only-firehose-shards",
            "max_inflight_handlers": MAX_INFLIGHT_NOTIFICATION_HANDLERS,
            "max_inflight_limit_changed": False,
            "saturation_wait_events": int(_STATS["saturation_wait_events"]),
            "handler_completions_during_wait": int(_STATS["handler_completions_during_wait"]),
            "handler_failures_during_wait": int(_STATS["handler_failures_during_wait"]),
            "max_inflight_observed": int(_STATS["max_inflight_observed"]),
            "drops_allowed": False,
            "target_scope_reduced": False,
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

    current_shards = public_shards._target_shards
    if not bool(getattr(current_shards, "_roi_strategy_critical_isolated", False)):
        setattr(_strategy_critical_target_shards, "_roi_strategy_critical_isolated", True)
        setattr(_strategy_critical_target_shards, "_roi_strategy_critical_isolated_version", REPAIR_VERSION)
        public_shards._target_shards = _strategy_critical_target_shards

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_notification_dispatch_bounded_drain", False)):
        DirectSolanaIngestionPlane.status = _status_with_bounded_dispatch(current_status)  # type: ignore[method-assign]


__all__ = [
    "REPAIR_VERSION",
    "_bounded_drain_dispatch_capacity",
    "_strategy_critical_target_shards",
    "install_notification_dispatch_backpressure_repair",
]
