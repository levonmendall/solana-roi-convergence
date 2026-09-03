from __future__ import annotations

import asyncio
import itertools
import time
from collections import deque
from collections.abc import Callable
from contextvars import ContextVar
from datetime import datetime
from typing import Any

from . import direct_solana as direct_solana_module
from . import launch_ws_frontier_timing_repair as frontier
from .direct_solana import DirectSolanaIngestionPlane


RAW_RECEIPT_QUEUE_MAX = 4096
RAW_RECEIPT_DELAY_WINDOW = 512
_LAUNCH_SENTINEL = "__roi_launch_like__"

_ORIGINAL_UTCNOW = direct_solana_module.utcnow
_ORIGINAL_LAUNCH_LIKE = DirectSolanaIngestionPlane._launch_like
_RECEIPT_WALL_TIME: ContextVar[datetime | None] = ContextVar(
    "roi_raw_receipt_wall_time",
    default=None,
)


def _receipt_aware_utcnow() -> datetime:
    """Return the socket-read wall time only while dispatching that exact receipt.

    The direct-Solana canonical handler historically stamped `received_at` when it
    entered the synchronous SQLite path. Under bursty program traffic that makes
    database scheduling delay indistinguishable from provider/network arrival
    delay. A ContextVar keeps the change local to one queued receipt while every
    unrelated runtime call retains the original clock.
    """

    bound = _RECEIPT_WALL_TIME.get()
    return bound if isinstance(bound, datetime) else _ORIGINAL_UTCNOW()


def _launch_like_with_sentinel(logs: Any) -> bool:
    if isinstance(logs, list) and _LAUNCH_SENTINEL in logs:
        return True
    return bool(_ORIGINAL_LAUNCH_LIKE(logs))


def _dispatch_queue(self: Any) -> asyncio.PriorityQueue[Any] | None:
    value = getattr(self, "_roi_raw_receipt_dispatch_queue", None)
    return value if isinstance(value, asyncio.PriorityQueue) else None


def _delay_window(self: Any) -> deque[float]:
    value = getattr(self, "_roi_raw_receipt_dispatch_delays_ms", None)
    if isinstance(value, deque):
        return value
    value = deque(maxlen=RAW_RECEIPT_DELAY_WINDOW)
    setattr(self, "_roi_raw_receipt_dispatch_delays_ms", value)
    return value


def _increment(self: Any, name: str, amount: int = 1) -> None:
    attr = f"_roi_raw_receipt_dispatch_{name}"
    setattr(self, attr, int(getattr(self, attr, 0) or 0) + int(amount))


def _priority(target: Any, launch_like: bool) -> int:
    if launch_like:
        return 0
    if str(getattr(target, "kind", "")) == "scout":
        return 1
    return 10


def _compact_notification(
    message: dict[str, Any],
    *,
    launch_like: bool,
    received_at: datetime,
    received_monotonic: float,
) -> dict[str, Any]:
    """Keep only fields consumed by the canonical direct-Solana handler.

    Raw log arrays can be very large. The reader determines launch-likeness before
    enqueueing and stores one sentinel instead of retaining the raw log payload,
    so a bounded queue cannot multiply the one-megabyte transport frame ceiling.
    """

    params = message.get("params")
    result = params.get("result") if isinstance(params, dict) else None
    context = result.get("context") if isinstance(result, dict) else None
    value = result.get("value") if isinstance(result, dict) else None
    compact_value = {
        "signature": str(value.get("signature") or "") if isinstance(value, dict) else "",
        "err": value.get("err") if isinstance(value, dict) else None,
        "logs": [_LAUNCH_SENTINEL] if launch_like else [],
    }
    return {
        "method": "logsNotification",
        "params": {
            "subscription": params.get("subscription") if isinstance(params, dict) else None,
            "result": {
                "context": {"slot": context.get("slot") if isinstance(context, dict) else None},
                "value": compact_value,
            },
        },
        "_roi_raw_received_at": received_at.isoformat(),
        "_roi_raw_received_monotonic": float(received_monotonic),
        "_roi_frontier_precaptured": True,
    }


def _parse_notification(
    self: Any,
    subscription_targets: dict[int, Any],
    message: dict[str, Any],
) -> tuple[Any, int, str, bool, bool] | None:
    if message.get("method") != "logsNotification":
        return None
    params = message.get("params")
    if not isinstance(params, dict):
        return None
    try:
        subscription = int(params["subscription"])
        result = params["result"]
        slot = int(result["context"]["slot"])
        value = result["value"]
        signature = str(value["signature"])
    except (KeyError, TypeError, ValueError):
        return None
    target = subscription_targets.get(subscription)
    if target is None or not signature or slot <= 0 or not isinstance(value, dict):
        return None
    launch_like = bool(self._launch_like(value.get("logs") or []))
    failed = value.get("err") is not None
    return target, slot, signature, launch_like, failed


async def _dispatch_worker(
    self: Any,
    stop: asyncio.Event,
    handler: Callable[[Any, str, dict[int, Any], dict[str, Any]], Any],
) -> None:
    queue = _dispatch_queue(self)
    if queue is None:
        return
    while not stop.is_set():
        try:
            item = await asyncio.wait_for(queue.get(), timeout=0.5)
        except asyncio.TimeoutError:
            continue
        priority, received_monotonic, _sequence, received_at, provider, targets, message = item
        token = _RECEIPT_WALL_TIME.set(received_at)
        try:
            delay_ms = max(0.0, (time.monotonic() - float(received_monotonic)) * 1000.0)
            _delay_window(self).append(delay_ms)
            setattr(self, "_roi_raw_receipt_dispatch_last_delay_ms", delay_ms)
            setattr(
                self,
                "_roi_raw_receipt_dispatch_max_delay_ms",
                max(float(getattr(self, "_roi_raw_receipt_dispatch_max_delay_ms", 0.0) or 0.0), delay_ms),
            )
            await handler(self, provider, targets, message)
            _increment(self, "completed")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _increment(self, "failed")
            setattr(self, "_roi_raw_receipt_dispatch_last_error_type", type(exc).__name__)
            setattr(self, "_roi_raw_receipt_dispatch_fatal", exc)
            # The old inline path would have torn down the WebSocket on a handler
            # exception. Preserve fail-closed behavior: future reads raise through
            # the lightweight wrapper instead of silently dropping observations.
            return
        finally:
            _RECEIPT_WALL_TIME.reset(token)
            queue.task_done()



def _queued_handler(
    original: Callable[[Any, str, dict[int, Any], dict[str, Any]], Any],
) -> Callable[[Any, str, dict[int, Any], dict[str, Any]], Any]:
    async def handle(
        self: Any,
        provider: str,
        subscription_targets: dict[int, Any],
        message: dict[str, Any],
    ) -> None:
        queue = _dispatch_queue(self)
        if queue is None:
            await original(self, provider, subscription_targets, message)
            return
        fatal = getattr(self, "_roi_raw_receipt_dispatch_fatal", None)
        if fatal is not None:
            raise RuntimeError("raw receipt dispatcher failed closed") from fatal

        parsed = _parse_notification(self, subscription_targets, message)
        if parsed is None:
            await original(self, provider, subscription_targets, message)
            return
        target, slot, signature, launch_like, failed = parsed
        received_at = _ORIGINAL_UTCNOW()
        received_monotonic = time.monotonic()

        # Update the v7 chain frontier at the socket-read boundary, before SQLite,
        # hydration, or any queued durable work. The launch receipt snapshots the
        # previously observed frontier first so it can never prove its own timing.
        try:
            if launch_like and not failed:
                frontier._capture_preexisting_frontier(
                    self,
                    signature,
                    slot,
                    received_monotonic,
                )
            frontier._observe_frontier(self, provider, slot, received_monotonic)
        except Exception:
            # The canonical launch gate remains fail-closed if timing proof cannot
            # be persisted. Receipt transport itself must continue to the durable
            # dispatcher so a timing-diagnostic failure cannot drop market data.
            _increment(self, "frontier_precapture_errors")

        compact = _compact_notification(
            message,
            launch_like=launch_like,
            received_at=received_at,
            received_monotonic=received_monotonic,
        )
        sequence = next(getattr(self, "_roi_raw_receipt_dispatch_sequence"))
        item = (
            _priority(target, launch_like),
            received_monotonic,
            sequence,
            received_at,
            str(provider),
            dict(subscription_targets),
            compact,
        )
        _increment(self, "received")
        if launch_like:
            _increment(self, "launch_received")
        if queue.full():
            _increment(self, "queue_saturation_events")
        await queue.put(item)
        depth = queue.qsize()
        setattr(
            self,
            "_roi_raw_receipt_dispatch_max_queue_depth",
            max(int(getattr(self, "_roi_raw_receipt_dispatch_max_queue_depth", 0) or 0), depth),
        )

    try:
        handle.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(handle, "_roi_raw_receipt_dispatch", True)
    return handle


def _run_with_dispatch(
    original: Callable[[Any, asyncio.Event], Any],
    handler: Callable[[Any, str, dict[int, Any], dict[str, Any]], Any],
) -> Callable[[Any, asyncio.Event], Any]:
    async def run(self: Any, stop: asyncio.Event) -> None:
        setattr(self, "_roi_raw_receipt_dispatch_queue", asyncio.PriorityQueue(maxsize=RAW_RECEIPT_QUEUE_MAX))
        setattr(self, "_roi_raw_receipt_dispatch_sequence", itertools.count())
        setattr(self, "_roi_raw_receipt_dispatch_fatal", None)
        _delay_window(self).clear()
        worker = asyncio.create_task(
            _dispatch_worker(self, stop, handler),
            name="direct-solana-raw-receipt-dispatch",
        )
        setattr(self, "_roi_raw_receipt_dispatch_worker", worker)
        try:
            await original(self, stop)
        finally:
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
            setattr(self, "_roi_raw_receipt_dispatch_worker", None)

    try:
        run.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(run, "_roi_raw_receipt_dispatch", True)
    return run


def _percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    rows = sorted(float(value) for value in values)
    index = max(0, min(len(rows) - 1, int((len(rows) - 1) * fraction)))
    return rows[index]


def _status_with_dispatch(original: Callable[[Any], dict[str, Any]]) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        queue = _dispatch_queue(self)
        delays = list(_delay_window(self))
        worker = getattr(self, "_roi_raw_receipt_dispatch_worker", None)
        payload["raw_receipt_dispatch"] = {
            "enabled": True,
            "socket_read_time_is_authoritative_arrival": True,
            "launch_frontier_updated_before_durable_dispatch": True,
            "durable_dispatch_decoupled_from_websocket_reader": True,
            "launches_prioritized_over_background_receipts": True,
            "compact_log_payload": True,
            "queue_max": RAW_RECEIPT_QUEUE_MAX,
            "queue_depth": queue.qsize() if queue is not None else 0,
            "max_queue_depth": int(getattr(self, "_roi_raw_receipt_dispatch_max_queue_depth", 0) or 0),
            "queue_saturation_events": int(getattr(self, "_roi_raw_receipt_dispatch_queue_saturation_events", 0) or 0),
            "received": int(getattr(self, "_roi_raw_receipt_dispatch_received", 0) or 0),
            "launch_received": int(getattr(self, "_roi_raw_receipt_dispatch_launch_received", 0) or 0),
            "completed": int(getattr(self, "_roi_raw_receipt_dispatch_completed", 0) or 0),
            "failed": int(getattr(self, "_roi_raw_receipt_dispatch_failed", 0) or 0),
            "frontier_precapture_errors": int(getattr(self, "_roi_raw_receipt_dispatch_frontier_precapture_errors", 0) or 0),
            "last_error_type": getattr(self, "_roi_raw_receipt_dispatch_last_error_type", None),
            "worker_alive": bool(worker is not None and not worker.done()),
            "dispatch_delay_sample_count": len(delays),
            "p50_dispatch_delay_ms": _percentile(delays, 0.50),
            "p95_dispatch_delay_ms": _percentile(delays, 0.95),
            "p99_dispatch_delay_ms": _percentile(delays, 0.99),
            "max_dispatch_delay_ms": float(getattr(self, "_roi_raw_receipt_dispatch_max_delay_ms", 0.0) or 0.0),
            "drops_allowed": False,
        }
        bridge = payload.get("launch_coverage_bridge")
        if isinstance(bridge, dict):
            bridge.update(
                {
                    "near_creation_socket_read_timestamp": True,
                    "near_creation_frontier_socket_read_timestamp": True,
                    "near_creation_durable_dispatch_delay_excluded": True,
                    "near_creation_threshold_unchanged": True,
                }
            )
        policy = payload.setdefault("provider_runtime_policy", {})
        if isinstance(policy, dict):
            policy.update(
                {
                    "websocket_reader_waits_for_sqlite_handler": False,
                    "raw_receipt_dispatch_bounded": True,
                    "raw_receipt_dispatch_queue_max": RAW_RECEIPT_QUEUE_MAX,
                    "raw_receipt_dispatch_drops_allowed": False,
                    "launch_receipt_priority_over_background": True,
                    "launch_near_creation_uses_socket_read_boundary": True,
                }
            )
        return payload

    try:
        status.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(status, "_roi_raw_receipt_dispatch", True)
    return status


def install_raw_receipt_dispatch_repair() -> None:
    """Separate raw network observation from durable processing without weakening gates."""

    direct_solana_module.utcnow = _receipt_aware_utcnow  # type: ignore[assignment]
    DirectSolanaIngestionPlane._launch_like = staticmethod(_launch_like_with_sentinel)  # type: ignore[method-assign]

    current_handler = DirectSolanaIngestionPlane._handle_notification
    if not bool(getattr(current_handler, "_roi_raw_receipt_dispatch", False)):
        queued = _queued_handler(current_handler)
        DirectSolanaIngestionPlane._handle_notification = queued  # type: ignore[method-assign]
    else:
        queued = current_handler

    current_run = DirectSolanaIngestionPlane.run
    if not bool(getattr(current_run, "_roi_raw_receipt_dispatch", False)):
        DirectSolanaIngestionPlane.run = _run_with_dispatch(current_run, current_handler)  # type: ignore[method-assign]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_raw_receipt_dispatch", False)):
        DirectSolanaIngestionPlane.status = _status_with_dispatch(current_status)  # type: ignore[method-assign]


__all__ = [
    "RAW_RECEIPT_QUEUE_MAX",
    "RAW_RECEIPT_DELAY_WINDOW",
    "install_raw_receipt_dispatch_repair",
    "_compact_notification",
    "_receipt_aware_utcnow",
]
