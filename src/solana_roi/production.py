from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

from . import direct_solana as direct_solana_module
from .direct_solana import DirectSolanaIngestionPlane


NotificationHandler = Callable[[Any, str, dict[int, Any], dict[str, Any]], Awaitable[None]]
ContextPrefill = Callable[[Any, Any], Awaitable[bool]]

# These are hard production memory ceilings, not strategy/sample ceilings.
# The raw Solana feed remains full-scope; when a provider outruns the process,
# WebSocket/TCP backpressure or a reconnect invokes the existing durable gap
# recovery path instead of accumulating an effectively unbounded receive buffer.
DIRECT_WS_MAX_QUEUE = 64
DIRECT_WS_MAX_SIZE_BYTES = 256 * 1024
DIRECT_CANDIDATE_CONTEXT_SLOTS = 3
DIRECT_BACKGROUND_CONTEXT_SLOTS = 1


def _cooperative_handler(original: NotificationHandler) -> NotificationHandler:
    """Force a scheduler handoff after every raw Solana notification.

    The direct feed intentionally observes the complete frozen seven-program
    universe. During bursts, ``websockets.recv()`` can remain immediately ready
    for long stretches, while the notification handler performs synchronous
    SQLite journaling. An explicit ``sleep(0)`` preserves that scope and receipt
    depth while preventing the single Uvicorn event loop from being starved.
    """

    async def handle(
        self: Any,
        provider: str,
        subscription_targets: dict[int, Any],
        message: dict[str, Any],
    ) -> None:
        await original(self, provider, subscription_targets, message)
        await asyncio.sleep(0)

    setattr(handle, "_roi_cooperative_yield", True)
    return handle


def _bounded_ws_connect(original: Callable[..., Any]) -> Callable[..., Any]:
    """Clamp receive buffering without narrowing subscriptions or dropping data."""

    def connect(*args: Any, **kwargs: Any) -> Any:
        requested_queue = kwargs.get("max_queue")
        requested_size = kwargs.get("max_size")
        kwargs["max_queue"] = DIRECT_WS_MAX_QUEUE if requested_queue is None else min(
            int(requested_queue), DIRECT_WS_MAX_QUEUE
        )
        kwargs["max_size"] = DIRECT_WS_MAX_SIZE_BYTES if requested_size is None else min(
            int(requested_size), DIRECT_WS_MAX_SIZE_BYTES
        )
        return original(*args, **kwargs)

    setattr(connect, "_roi_memory_bounded", True)
    return connect


def _bounded_context_prefill(original: ContextPrefill) -> ContextPrefill:
    """Prevent twelve hydrators from multiplying full context fanout concurrently.

    Candidate/scout work has dedicated capacity so background certification can
    never consume the candidate fast path. The original per-context behavior is
    unchanged: up to 600 signatures, 24 inner RPC operations, and the existing
    three-second context deadline remain authoritative.
    """

    async def prefill(self: Any, candidate: Any) -> bool:
        critical = False
        try:
            profile = self.service.registry.get(candidate.wallet)
            critical = bool(profile is not None and str(candidate.side).lower() == "buy")
        except Exception:
            critical = False

        attribute = "_roi_candidate_context_gate" if critical else "_roi_background_context_gate"
        slots = DIRECT_CANDIDATE_CONTEXT_SLOTS if critical else DIRECT_BACKGROUND_CONTEXT_SLOTS
        gate = getattr(self, attribute, None)
        if gate is None:
            gate = asyncio.Semaphore(slots)
            setattr(self, attribute, gate)
        async with gate:
            return await original(self, candidate)

    setattr(prefill, "_roi_memory_bounded", True)
    return prefill


def _bounded_status(original: Callable[[Any], dict[str, Any]]) -> Callable[[Any], dict[str, Any]]:
    """Expose the installed memory envelope in direct-Solana telemetry."""

    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        payload["production_memory_boundary"] = {
            "websocket_max_queue": DIRECT_WS_MAX_QUEUE,
            "websocket_max_size_bytes": DIRECT_WS_MAX_SIZE_BYTES,
            "candidate_context_slots": DIRECT_CANDIDATE_CONTEXT_SLOTS,
            "background_context_slots": DIRECT_BACKGROUND_CONTEXT_SLOTS,
            "strategy_scope_reduced": False,
            "context_signature_limit_unchanged": int(self.candidate_context_max_signatures),
            "hydration_worker_count_unchanged": int(self.worker_count),
        }
        return payload

    setattr(status, "_roi_memory_bounded", True)
    return status


def install_direct_stream_fairness() -> None:
    """Install the production scheduling guard exactly once."""

    current = DirectSolanaIngestionPlane._handle_notification
    if bool(getattr(current, "_roi_cooperative_yield", False)):
        return
    DirectSolanaIngestionPlane._handle_notification = _cooperative_handler(current)  # type: ignore[method-assign]


def install_direct_stream_memory_bounds() -> None:
    """Install bounded buffering/fanout guards exactly once."""

    current_connect = direct_solana_module.websockets.connect
    if not bool(getattr(current_connect, "_roi_memory_bounded", False)):
        direct_solana_module.websockets.connect = _bounded_ws_connect(current_connect)  # type: ignore[assignment]

    current_prefill = DirectSolanaIngestionPlane._prefill_launch_context
    if not bool(getattr(current_prefill, "_roi_memory_bounded", False)):
        DirectSolanaIngestionPlane._prefill_launch_context = _bounded_context_prefill(current_prefill)  # type: ignore[method-assign]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_memory_bounded", False)):
        DirectSolanaIngestionPlane.status = _bounded_status(current_status)  # type: ignore[method-assign]


install_direct_stream_fairness()
install_direct_stream_memory_bounds()

from . import runtime as runtime_module
from .config import BASELINE

runtime_module.BASELINE = BASELINE

from .production_capacity_repair import install_production_capacity_repair

install_production_capacity_repair()

from .certification_hotpath_repair import install_certification_hotpath_repair

install_certification_hotpath_repair()

from .wallet_realtime_tracking_repair import install_wallet_realtime_tracking_repair

install_wallet_realtime_tracking_repair()

# Fresh prospective wallet receipts must never sit behind historical catch-up.
# Reserve hydration workers for receipts still inside the unchanged 20-second
# copyability SLA, move stale/recovery work onto a separate backlog lane, bound
# recovery concurrency, and make risk enrichment a claimed multi-worker queue with
# explicit pending-age telemetry. No promotion or strategy threshold is changed.
from .wallet_live_priority_repair import install_wallet_live_priority_repair

install_wallet_live_priority_repair()

from .full_scope_dispatch_capacity_repair import install_full_scope_dispatch_capacity_repair

install_full_scope_dispatch_capacity_repair()

from .wallet_realtime_status_compat import install_wallet_realtime_status_compatibility

install_wallet_realtime_status_compatibility()

from .wallet_realtime_intelligence_boundary import install_wallet_realtime_intelligence_boundary

install_wallet_realtime_intelligence_boundary()

from .wallet_intelligence_startup_repair import install_wallet_intelligence_startup_isolation

install_wallet_intelligence_startup_isolation()

from .wallet_discovery_startup_repair import install_wallet_discovery_startup_isolation

install_wallet_discovery_startup_isolation()

from .api import app as app  # noqa: E402  (installation must happen first)

__all__ = [
    "app",
    "install_direct_stream_fairness",
    "install_direct_stream_memory_bounds",
]
