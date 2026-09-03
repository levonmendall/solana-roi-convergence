from __future__ import annotations

import asyncio
import sqlite3
import time
from collections.abc import Callable
from datetime import datetime
from typing import Any

from . import full_scope_dispatch_capacity_repair as full_scope
from . import production_capacity_repair as capacity
from . import raw_receipt_dispatch_repair as raw_dispatch
from . import runtime as runtime_module
from . import wallet_live_priority_repair as wallet_priority
from .direct_solana import DirectSolanaIngestionPlane
from .ingestion import NormalizedSwap


# A same-release Render restart can briefly overlap the prior process's final
# persistent-disk SQLite teardown. Retry only the narrow SQLite lock/busy class;
# every other startup exception remains fail-fast and visible to Uvicorn/Render.
STARTUP_SQLITE_RETRY_DELAYS_SECONDS = (0.25, 0.75, 1.50)


async def _persist_full_scope_batch_off_loop(self: Any, items: list[Any]) -> int:
    """Run the existing durable full-scope SQLite batch outside Uvicorn's loop.

    The canonical store uses ``check_same_thread=False`` and serializes the one
    SQLite connection with a process-wide ``threading.RLock``. The exact same
    full-scope transaction can therefore run in an asyncio worker thread without
    changing ordering, WAL/FULL durability, hydration enqueue semantics, queue
    bounds, or the no-drop policy.

    Render's HTTP health deadline is five seconds. A slow persistent-disk fsync on
    the single Uvicorn event-loop thread can exceed that deadline even while the
    process is otherwise healthy. This isolates only that synchronous disk batch.
    """

    started = time.perf_counter()
    inserted = await asyncio.to_thread(full_scope._persist_full_scope_batch, self, items)
    elapsed_ms = max(0.0, (time.perf_counter() - started) * 1000.0)
    setattr(
        self,
        "_roi_web_liveness_sqlite_batch_offloads",
        int(getattr(self, "_roi_web_liveness_sqlite_batch_offloads", 0) or 0) + 1,
    )
    setattr(self, "_roi_web_liveness_sqlite_batch_last_ms", elapsed_ms)
    setattr(
        self,
        "_roi_web_liveness_sqlite_batch_max_ms",
        max(float(getattr(self, "_roi_web_liveness_sqlite_batch_max_ms", 0.0) or 0.0), elapsed_ms),
    )
    return int(inserted)


async def _web_safe_full_scope_dispatch_worker(
    self: Any,
    stop: asyncio.Event,
    _handler: Callable[[Any, str, dict[int, Any], dict[str, Any]], Any],
) -> None:
    """Preserve the full-scope dispatcher while moving its commit off-loop."""

    queue = raw_dispatch._dispatch_queue(self)
    if queue is None:
        return

    while not stop.is_set():
        try:
            first = await asyncio.wait_for(queue.get(), timeout=0.5)
        except asyncio.TimeoutError:
            continue

        items = [first]
        while len(items) < full_scope.FULL_SCOPE_BATCH_MAX:
            try:
                items.append(queue.get_nowait())
            except asyncio.QueueEmpty:
                break

        try:
            for item in items:
                capacity._observe_dispatch_delay(self, item)
            await _persist_full_scope_batch_off_loop(self, items)
            raw_dispatch._increment(self, "completed", len(items))
            setattr(
                self,
                "_roi_capacity_dispatch_batch_commits",
                int(getattr(self, "_roi_capacity_dispatch_batch_commits", 0) or 0) + 1,
            )
            setattr(
                self,
                "_roi_capacity_dispatch_batched_receipts",
                int(getattr(self, "_roi_capacity_dispatch_batched_receipts", 0) or 0) + len(items),
            )
            setattr(
                self,
                "_roi_capacity_dispatch_max_batch_size",
                max(
                    int(getattr(self, "_roi_capacity_dispatch_max_batch_size", 0) or 0),
                    len(items),
                ),
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raw_dispatch._increment(self, "failed")
            setattr(self, "_roi_raw_receipt_dispatch_last_error_type", type(exc).__name__)
            setattr(self, "_roi_raw_receipt_dispatch_fatal", exc)
            return
        finally:
            for _item in items:
                queue.task_done()


def _risk_swap_from_row(row: dict[str, Any]) -> NormalizedSwap:
    """Rebuild the exact realtime risk-work swap without a hidden helper dependency.

    PR #73's no-lookahead worker calls ``wallet_live_priority_repair._risk_swap``.
    The priority module never defined that private helper because its original
    worker built the same NormalizedSwap inline. Install this adapter before the
    PR #73 worker is activated so claimed risk rows can actually be evaluated.
    """

    token_amount = float(row["token_amount"])
    wallet_price_sol = float(row["wallet_price_sol"])
    return NormalizedSwap(
        signature=str(row["signature"]),
        slot=0,
        observed_at=datetime.fromisoformat(str(row["observed_at"])),
        received_at=datetime.fromisoformat(str(row["received_at"])),
        wallet=str(row["wallet"]),
        token_mint=str(row["token_mint"]),
        side="buy",
        token_amount=token_amount,
        native_amount_sol=token_amount * wallet_price_sol,
        reference_price_sol=wallet_price_sol,
        source=str(row["source"]),
    )


def _restart_safe_build_runtime(original: Callable[[], Any]) -> Callable[[], Any]:
    """Retry only transient SQLite lock/busy errors during same-release restarts."""

    def build_runtime() -> Any:
        for attempt, delay in enumerate((*STARTUP_SQLITE_RETRY_DELAYS_SECONDS, 0.0), start=1):
            try:
                return original()
            except sqlite3.OperationalError as exc:
                message = str(exc).lower()
                retryable = "locked" in message or "busy" in message
                final_attempt = attempt > len(STARTUP_SQLITE_RETRY_DELAYS_SECONDS)
                if not retryable or final_attempt:
                    raise
                # Startup has not yielded the ASGI lifespan yet, so this bounded
                # synchronous wait cannot starve live HTTP traffic. It only lets a
                # previous same-service process release the persistent SQLite file.
                time.sleep(delay)
        raise RuntimeError("unreachable Render startup retry state")

    setattr(build_runtime, "_roi_render_restart_sqlite_retry", True)
    setattr(build_runtime, "_roi_original_build_runtime", original)
    return build_runtime


def _status_with_web_liveness_isolation(
    original: Callable[[Any], dict[str, Any]],
) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        offloads = int(getattr(self, "_roi_web_liveness_sqlite_batch_offloads", 0) or 0)
        last_ms = float(getattr(self, "_roi_web_liveness_sqlite_batch_last_ms", 0.0) or 0.0)
        max_ms = float(getattr(self, "_roi_web_liveness_sqlite_batch_max_ms", 0.0) or 0.0)
        payload["web_liveness_isolation"] = {
            "installed": True,
            "full_scope_sqlite_batch_off_event_loop": True,
            "sqlite_check_same_thread_false_required": True,
            "sqlite_process_lock_preserved": True,
            "sqlite_wal_preserved": True,
            "sqlite_synchronous_full_preserved": True,
            "full_scope_batch_semantics_preserved": True,
            "receipt_order_preserved": True,
            "hydration_enqueue_semantics_preserved": True,
            "drops_allowed": False,
            "strategy_scope_reduced": False,
            "certification_thresholds_unchanged": True,
            "wallet_risk_row_adapter_installed": callable(
                getattr(wallet_priority, "_risk_swap", None)
            ),
            "same_release_sqlite_restart_retry_installed": bool(
                getattr(runtime_module.build_runtime, "_roi_render_restart_sqlite_retry", False)
            ),
            "same_release_sqlite_restart_retry_delays_seconds": list(
                STARTUP_SQLITE_RETRY_DELAYS_SECONDS
            ),
            "startup_non_lock_errors_fail_fast": True,
            "batch_offloads": offloads,
            "last_batch_ms": last_ms if offloads else None,
            "max_batch_ms": max_ms if offloads else None,
        }
        dispatch = payload.get("raw_receipt_dispatch")
        if isinstance(dispatch, dict):
            dispatch.update(
                {
                    "full_scope_sqlite_commit_off_uvicorn_event_loop": True,
                    "full_scope_set_based_writer_preserved": True,
                    "critical_receipts_batched": True,
                    "critical_per_receipt_commits": False,
                }
            )
        return payload

    try:
        status.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(status, "_roi_web_liveness_isolation", True)
    return status


def install_web_liveness_isolation() -> None:
    """Keep persistent-disk work and restart races from terminating web liveness."""

    raw_dispatch._dispatch_worker = _web_safe_full_scope_dispatch_worker  # type: ignore[assignment]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_web_liveness_isolation", False)):
        DirectSolanaIngestionPlane.status = _status_with_web_liveness_isolation(current_status)  # type: ignore[method-assign]

    current_build_runtime = runtime_module.build_runtime
    if not bool(getattr(current_build_runtime, "_roi_render_restart_sqlite_retry", False)):
        runtime_module.build_runtime = _restart_safe_build_runtime(current_build_runtime)  # type: ignore[assignment]

    # PR #73's point-in-time risk worker intentionally lives in the priority module
    # global so the already-installed run loop picks it up dynamically. Its row
    # reconstruction had previously been inline and therefore had no `_risk_swap`
    # helper. Publish the exact adapter before installing the evidence worker.
    wallet_priority._risk_swap = _risk_swap_from_row  # type: ignore[attr-defined]

    # This is the final production-composition hook before api.py constructs the
    # runtime. Install the wallet evidence repair here so it sees every earlier
    # realtime/priority/status wrapper and remains independent of web liveness
    # behavior itself.
    from .wallet_evidence_rpc_repair import install_wallet_evidence_rpc_repair

    install_wallet_evidence_rpc_repair()


__all__ = [
    "STARTUP_SQLITE_RETRY_DELAYS_SECONDS",
    "_persist_full_scope_batch_off_loop",
    "_restart_safe_build_runtime",
    "_risk_swap_from_row",
    "_web_safe_full_scope_dispatch_worker",
    "install_web_liveness_isolation",
]
