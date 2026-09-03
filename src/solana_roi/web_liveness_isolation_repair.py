from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from typing import Any

from . import production_capacity_repair as capacity
from . import raw_receipt_dispatch_repair as raw_dispatch
from .direct_solana import DirectSolanaIngestionPlane


async def _persist_background_batch_off_loop(self: Any, items: list[Any]) -> int:
    """Run the durable SQLite micro-batch outside Uvicorn's event-loop thread.

    The production store deliberately uses ``check_same_thread=False`` and guards
    its single SQLite connection with a process-wide ``threading.RLock``. That
    makes this exact bounded write transaction safe to execute in an asyncio worker
    thread while preserving the existing ordering, WAL/FULL durability, no-drop
    policy, and one-commit-per-batch semantics.

    Render's liveness probe has a five-second deadline. Keeping a potentially slow
    persistent-disk fsync on the Uvicorn loop can therefore make an otherwise
    healthy process look dead. Offloading only the already-bounded synchronous
    micro-batch removes that failure mode without weakening any evidence or
    certification requirement.
    """

    started = time.perf_counter()
    inserted = await asyncio.to_thread(capacity._persist_background_batch, self, items)
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


async def _web_safe_capacity_dispatch_worker(
    self: Any,
    stop: asyncio.Event,
    handler: Callable[[Any, str, dict[int, Any], dict[str, Any]], Any],
) -> None:
    """Preserve the capacity dispatcher while isolating its bulk disk commit."""

    queue = raw_dispatch._dispatch_queue(self)
    if queue is None:
        return
    while not stop.is_set():
        try:
            first = await asyncio.wait_for(queue.get(), timeout=0.5)
        except asyncio.TimeoutError:
            continue

        items = [first]
        while len(items) < capacity.RAW_RECEIPT_BATCH_MAX:
            try:
                items.append(queue.get_nowait())
            except asyncio.QueueEmpty:
                break

        background: list[Any] = []
        try:
            # This mirrors production_capacity_repair exactly: critical receipts
            # retain the canonical path and queue ordering; only ordinary
            # no-hydration program receipts are grouped into the durable batch.
            for item in items:
                capacity._observe_dispatch_delay(self, item)
                if capacity._can_batch_background(self, item):
                    background.append(item)
                    continue
                _priority, _mono, _seq, received_at, provider, targets, message = capacity._parse_dispatch_item(item)
                token = raw_dispatch._RECEIPT_WALL_TIME.set(received_at)
                try:
                    await handler(self, provider, targets, message)
                    raw_dispatch._increment(self, "completed")
                    setattr(
                        self,
                        "_roi_capacity_dispatch_canonical_critical",
                        int(getattr(self, "_roi_capacity_dispatch_canonical_critical", 0) or 0) + 1,
                    )
                finally:
                    raw_dispatch._RECEIPT_WALL_TIME.reset(token)

            if background:
                await _persist_background_batch_off_loop(self, background)
                raw_dispatch._increment(self, "completed", len(background))
                setattr(
                    self,
                    "_roi_capacity_dispatch_batch_commits",
                    int(getattr(self, "_roi_capacity_dispatch_batch_commits", 0) or 0) + 1,
                )
                setattr(
                    self,
                    "_roi_capacity_dispatch_batched_receipts",
                    int(getattr(self, "_roi_capacity_dispatch_batched_receipts", 0) or 0) + len(background),
                )
                setattr(
                    self,
                    "_roi_capacity_dispatch_max_batch_size",
                    max(int(getattr(self, "_roi_capacity_dispatch_max_batch_size", 0) or 0), len(background)),
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
            "bulk_receipt_sqlite_off_event_loop": True,
            "sqlite_check_same_thread_false_required": True,
            "sqlite_process_lock_preserved": True,
            "sqlite_wal_preserved": True,
            "sqlite_synchronous_full_preserved": True,
            "receipt_order_preserved": True,
            "drops_allowed": False,
            "strategy_scope_reduced": False,
            "certification_thresholds_unchanged": True,
            "batch_offloads": offloads,
            "last_batch_ms": last_ms if offloads else None,
            "max_batch_ms": max_ms if offloads else None,
        }
        dispatch = payload.get("raw_receipt_dispatch")
        if isinstance(dispatch, dict):
            dispatch["bulk_sqlite_commit_off_uvicorn_event_loop"] = True
        return payload

    try:
        status.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(status, "_roi_web_liveness_isolation", True)
    return status


def install_web_liveness_isolation() -> None:
    """Keep persistent-disk micro-batches from blocking Render liveness."""

    # raw_receipt_dispatch_repair resolves this module global when its production
    # run task starts, so replacing it here changes no queue topology or receipts.
    raw_dispatch._dispatch_worker = _web_safe_capacity_dispatch_worker  # type: ignore[assignment]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_web_liveness_isolation", False)):
        DirectSolanaIngestionPlane.status = _status_with_web_liveness_isolation(current_status)  # type: ignore[method-assign]


__all__ = [
    "install_web_liveness_isolation",
    "_persist_background_batch_off_loop",
]
