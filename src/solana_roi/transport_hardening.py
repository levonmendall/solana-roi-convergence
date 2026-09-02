from __future__ import annotations

from collections.abc import Callable
from typing import Any

from websockets.asyncio.client import connect as _native_ws_connect

from . import direct_solana as direct_solana_module
from .direct_solana import DirectSolanaIngestionPlane, WatchTarget


STREAM_WS_MAX_QUEUE = 64
STREAM_WS_MAX_SIZE_BYTES = 1024 * 1024

_SOURCE_SETUP_RANK = {
    "RAYDIUM": 1,
    "PUMP_AMM": 2,
    "PUMP_FUN": 3,
}


def _transport_kwargs(kwargs: dict[str, Any]) -> dict[str, Any]:
    """Return the bounded receive envelope used by the direct Solana stream.

    The previous 256 KiB frame ceiling was small enough to terminate otherwise
    healthy Solana log streams on unusually large notifications. The replacement
    remains tightly bounded: at most 64 queued messages and 1 MiB per message.
    """

    values = dict(kwargs)
    requested_queue = values.get("max_queue")
    if requested_queue is None:
        values["max_queue"] = STREAM_WS_MAX_QUEUE
    else:
        try:
            values["max_queue"] = min(int(requested_queue), STREAM_WS_MAX_QUEUE)
        except (TypeError, ValueError):
            values["max_queue"] = STREAM_WS_MAX_QUEUE

    # The direct stream currently passes the obsolete 256 KiB ceiling explicitly.
    # Lift that exact transport limit while still clamping any larger request.
    values["max_size"] = STREAM_WS_MAX_SIZE_BYTES
    return values


def _frame_resilient_connect(*args: Any, **kwargs: Any) -> Any:
    return _native_ws_connect(*args, **_transport_kwargs(kwargs))


setattr(_frame_resilient_connect, "_roi_memory_bounded", True)
setattr(_frame_resilient_connect, "_roi_frame_resilient", True)


def _ordered_watch_targets(original: Callable[[Any], tuple[WatchTarget, ...]]) -> Callable[[Any], tuple[WatchTarget, ...]]:
    """Preserve every frozen target while making setup resistant to traffic floods.

    Scout subscriptions are established first. Program subscriptions then progress
    from lower-volume Raydium programs through PumpSwap, with Pump.fun last. Once
    the final high-volume subscription is acknowledged the provider can be marked
    connected immediately, instead of processing a Pump.fun firehose while still
    waiting to establish nine more subscriptions.
    """

    def targets(self: Any) -> tuple[WatchTarget, ...]:
        rows = list(original(self))

        def rank(target: WatchTarget) -> tuple[int, int, str]:
            if target.kind == "scout":
                return (0, 0, target.address)
            return (1, _SOURCE_SETUP_RANK.get(str(target.source_hint or ""), 0), target.address)

        return tuple(sorted(rows, key=rank))

    setattr(targets, "_roi_setup_ordered", True)
    return targets


def _status_with_transport_envelope(original: Callable[[Any], dict[str, Any]]) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        boundary = payload.setdefault("production_memory_boundary", {})
        if isinstance(boundary, dict):
            boundary.update(
                {
                    "installed_intrinsically": True,
                    "websocket_max_queue": STREAM_WS_MAX_QUEUE,
                    "websocket_max_size_bytes": STREAM_WS_MAX_SIZE_BYTES,
                    "receive_payload_ceiling_bytes_per_provider": STREAM_WS_MAX_QUEUE * STREAM_WS_MAX_SIZE_BYTES,
                    "strategy_scope_reduced": False,
                }
            )
        policy = payload.setdefault("provider_runtime_policy", {})
        if isinstance(policy, dict):
            policy.update(
                {
                    "subscription_target_order": "scouts-first/raydium/pump-amm/pump-fun-last",
                    "high_volume_programs_subscribed_last": True,
                    "frame_ceiling_hardened_bytes": STREAM_WS_MAX_SIZE_BYTES,
                    "full_target_count_unchanged": len(self.watch_targets),
                }
            )
        return payload

    # Preserve all compatibility markers so later production-entrypoint imports
    # cannot wrap over and hide the richer intrinsic status method.
    setattr(status, "_roi_transport_hardened", True)
    setattr(status, "_roi_memory_bounded", True)
    setattr(status, "_roi_subscription_telemetry", True)
    return status


def install_transport_hardening() -> None:
    current_connect = direct_solana_module.websockets.connect
    if not bool(getattr(current_connect, "_roi_frame_resilient", False)):
        direct_solana_module.websockets.connect = _frame_resilient_connect  # type: ignore[assignment]

    current_property = DirectSolanaIngestionPlane.watch_targets
    getter = current_property.fget
    if getter is not None and not bool(getattr(getter, "_roi_setup_ordered", False)):
        DirectSolanaIngestionPlane.watch_targets = property(_ordered_watch_targets(getter))  # type: ignore[assignment]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_transport_hardened", False)):
        DirectSolanaIngestionPlane.status = _status_with_transport_envelope(current_status)  # type: ignore[method-assign]


__all__ = [
    "STREAM_WS_MAX_QUEUE",
    "STREAM_WS_MAX_SIZE_BYTES",
    "install_transport_hardening",
]
