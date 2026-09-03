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
            # Classification uncertainty is background-only; it must never gain
            # candidate-reserved capacity by accident.
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


# Install before importing the FastAPI runtime so every production instance uses
# fair scheduling and bounded memory without changing strategy, sampling, scope,
# certification thresholds, signing, submission, or paper-only authority.
install_direct_stream_fairness()
install_direct_stream_memory_bounds()

# PR #59 added _wallet_discovery_policy() with BASELINE.max_chase_fraction but did
# not import BASELINE into runtime.py. Render therefore failed synchronously in
# build_runtime() with NameError before any background isolation could take effect.
# Bind the canonical frozen baseline into the runtime module before api.py invokes
# build_runtime(). This changes no strategy value; it only restores the intended
# reference to the already-canonical configuration object.
from . import runtime as runtime_module
from .config import BASELINE

runtime_module.BASELINE = BASELINE

# Production telemetry on the first successful PR #60 deployment proved that the
# official public Solana endpoint was being rate-limited under routine hedging and
# that the no-drop durable receipt queue remained saturated despite provider-copy
# suppression. Install the capacity repair before runtime construction: 429s gain a
# bounded read-only cooldown, api.mainnet.solana.com becomes sequential emergency
# HTTP fallback instead of a proactive hedge, ordinary no-hydration program receipts
# are committed in durable SQLite micro-batches, and broad research yields while
# critical ingestion capacity is degraded. Strategy/certification limits stay
# unchanged and no provider is created by this repair.
from .production_capacity_repair import install_production_capacity_repair

install_production_capacity_repair()

# Exact-release telemetry then isolated two remaining proof hot paths: funding
# provenance could spend the entire launch cycle hydrating history serially, while
# an actual WebSocket gap could spend its unchanged 12-second recovery lease on one
# slow public RPC page. It also showed v7 launch reconstruction producing swaps but
# no final completeness attestations. Overlap only bounded read-only funding calls,
# restore hedging only for the dedicated urgent recovery pool, and carry the existing
# immutable-window attestation through the final v7 timing wrapper. No threshold,
# market scope, signing/submission authority, or paper-only boundary changes.
from .certification_hotpath_repair import install_certification_hotpath_repair

install_certification_hotpath_repair()

# Broad discovery and historical screening are allowed to yield to critical
# capacity, but already-enrolled wallets require their own prospective clock. Move
# those wallets to direct logsSubscribe receipt timing, an independent read-only RPC
# pool, durable small-queue hydration, bounded reconnect recovery and asynchronous
# risk enrichment. Also upgrade ordinary receipt microbatches to grouped/set-based
# writes. No entry/promotion threshold or active cohort authority is changed.
from .wallet_realtime_tracking_repair import install_wallet_realtime_tracking_repair

install_wallet_realtime_tracking_repair()

# Exact-release telemetry on PR #67 proved the ordinary set-based writer was active,
# yet the bounded no-drop queue still stayed ~100% full with ~100-second dispatch
# delay. The remaining per-receipt transactions were launch, scout and bootstrap/
# sample receipts. Batch the complete raw dispatch scope in one durable transaction
# while reproducing the canonical hydration enqueue decisions and priorities. The
# 4096 queue bound, no-drop rule, full market scope, certification thresholds and
# paper-only authority remain unchanged.
from .full_scope_dispatch_capacity_repair import install_full_scope_dispatch_capacity_repair

install_full_scope_dispatch_capacity_repair()

# The outer realtime telemetry wrapper must preserve every marker attached by the
# intrinsic stream/poll/memory repairs below it. Those markers are part of the
# repository's production-composition proof, not cosmetic test metadata.
from .wallet_realtime_status_compat import install_wallet_realtime_status_compatibility

install_wallet_realtime_status_compatibility()

# Keep append-only wallet-intelligence history, but prevent a snapshot from an old
# polling epoch from becoming promotion evidence after the new realtime epoch has
# started. Reads become epoch-aware; historical rows remain intact for audit.
from .wallet_realtime_intelligence_boundary import install_wallet_realtime_intelligence_boundary

install_wallet_realtime_intelligence_boundary()

# PR #59 also moved wallet intelligence itself into build_runtime(). Its schema
# creation is research-only and must not become a synchronous Render startup
# prerequisite. Install its lazy proxy before the discovery proxy and before api.py
# captures runtime.build_runtime.
from .wallet_intelligence_startup_repair import install_wallet_intelligence_startup_isolation

install_wallet_intelligence_startup_isolation()

# Continuous wallet discovery is research-only and has zero paper/live authority.
# Its schema/bootstrap must therefore never be able to terminate the web service.
# Defer that bootstrap into its background task, report any failure through status,
# and retry without changing the active cohort or any certification threshold.
from .wallet_discovery_startup_repair import install_wallet_discovery_startup_isolation

install_wallet_discovery_startup_isolation()

# PR #68 removes the remaining per-critical-receipt commits by writing the complete
# raw dispatch batch in one set-based transaction. Keep that exact full-scope batch,
# but do not execute its FULL-sync persistent-disk commit on the single Uvicorn event
# loop that must satisfy Render's five-second HTTP liveness deadline. The store's
# check_same_thread=False connection and process RLock make the bounded transaction
# safe to run in an asyncio worker thread without changing receipt order, evidence,
# queue bounds, hydration priorities, strategy, certification, or paper-only authority.
from .web_liveness_isolation_repair import install_web_liveness_isolation

install_web_liveness_isolation()

from .api import app as app  # noqa: E402  (installation must happen first)

__all__ = [
    "app",
    "install_direct_stream_fairness",
    "install_direct_stream_memory_bounds",
]
