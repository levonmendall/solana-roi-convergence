from __future__ import annotations

import asyncio
from typing import Any

from . import direct_solana as direct_module
from . import strategy_relevant_continuity as strategy
from .direct_solana import WatchTarget


REPAIR_VERSION = "poll-receipt-offloop-v3"


def _persist_scout_rows_sync(
    self: Any,
    target: WatchTarget,
    rows: list[dict[str, Any]],
) -> int:
    """Persist canonical scout receipts and enqueue triggers in one worker call.

    This is the synchronous body of the intrinsic live-poll recorder for the scout
    path. Keeping receipt insertion and trigger enqueue in the same worker preserves
    their existing ordering while avoiding any nested event loop or dynamic wrapper
    lookup from inside the worker thread.
    """

    inserted_count = 0
    source_key = target.source_hint or f"SCOUT:{target.address}"
    for row in rows:
        signature = str(row.get("signature") or "")
        if not signature:
            continue
        try:
            slot = int(row.get("slot") or 0)
        except (TypeError, ValueError):
            continue
        if slot <= 0:
            continue

        received_at = direct_module.utcnow()
        inserted = self.journal.record_receipt(
            signature=signature,
            source_key=source_key,
            slot=slot,
            received_at=received_at,
            launch_like=False,
        )
        if not inserted or row.get("err") is not None:
            continue

        inserted_count += 1
        self.journal.enqueue(
            signature=signature,
            slot=slot,
            trigger_received_at=received_at,
            source_hint=None,
            priority=0,
            reason="frozen_scout_live_poll_trigger",
        )
    return inserted_count


def _persist_program_rows_sync(
    self: Any,
    target: WatchTarget,
    rows: list[dict[str, Any]],
) -> int:
    """Persist program discovery receipts without granting hydration authority."""

    inserted_count = 0
    source_key = target.source_hint or f"PROGRAM:{target.address}"
    for row in rows:
        signature = str(row.get("signature") or "")
        if not signature:
            continue
        try:
            slot = int(row.get("slot") or 0)
        except (TypeError, ValueError):
            continue
        if slot <= 0:
            continue
        inserted = self.journal.record_receipt(
            signature=signature,
            source_key=source_key,
            slot=slot,
            received_at=direct_module.utcnow(),
            launch_like=False,
        )
        if inserted and row.get("err") is None:
            inserted_count += 1
    return inserted_count


async def _record_poll_rows_scoped_offloop(
    self: Any,
    target: WatchTarget,
    rows: list[dict[str, Any]],
) -> int:
    """Persist poll-recovery receipts outside Uvicorn's event-loop thread.

    Production journal writes are always executed as one synchronous worker call.
    In particular, the scout path no longer enters a worker and then creates a new
    event loop with ``asyncio.run()`` to invoke a dynamically captured wrapper. That
    old shape could recurse after installer re-composition, creating a new default
    executor/thread layer on every pass.

    Partial/non-production fixtures that do not expose ``record_receipt`` retain the
    prior delegate semantics, but the delegate is awaited directly on the existing
    loop so even a bad wrapper composition cannot recursively manufacture workers.
    """

    journal = getattr(self, "journal", None)
    if not hasattr(journal, "record_receipt"):
        original = strategy._ORIGINAL_RECORD_POLL_ROWS
        if original is None:
            raise RuntimeError("strategy continuity repair missing original poll recorder")
        return int(await original(self, target, rows))

    if target.kind == "scout":
        return int(await asyncio.to_thread(_persist_scout_rows_sync, self, target, rows))

    inserted_count = int(await asyncio.to_thread(_persist_program_rows_sync, self, target, rows))
    setattr(
        self,
        "_roi_program_poll_rows_raw_only_total",
        int(getattr(self, "_roi_program_poll_rows_raw_only_total", 0) or 0) + inserted_count,
    )
    return inserted_count


def install_poll_receipt_offloop_repair() -> None:
    current = strategy._record_poll_rows_scoped
    if not bool(getattr(current, "_roi_poll_receipt_offloop", False)):
        setattr(_record_poll_rows_scoped_offloop, "_roi_poll_receipt_offloop", True)
        setattr(_record_poll_rows_scoped_offloop, "_roi_poll_receipt_offloop_version", REPAIR_VERSION)
        strategy._record_poll_rows_scoped = _record_poll_rows_scoped_offloop

    # This installer is the final direct-Solana composition boundary in the legacy
    # package composition. Exact-release telemetry proved the already-bounded public
    # shard dispatcher was treating ordinary 32-handler saturation as a fatal
    # transport failure. Replace only that saturation behavior here, after the shard
    # topology is installed, so the unchanged bound becomes receive backpressure
    # rather than reconnect churn. No scope, continuity lease, strategy threshold or
    # authority changes.
    from .notification_dispatch_backpressure_repair import (
        install_notification_dispatch_backpressure_repair,
    )

    install_notification_dispatch_backpressure_repair()


__all__ = ["REPAIR_VERSION", "install_poll_receipt_offloop_repair"]
