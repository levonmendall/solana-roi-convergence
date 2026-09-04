from __future__ import annotations

import asyncio
from typing import Any

from . import direct_solana as direct_module
from . import strategy_relevant_continuity as strategy
from .direct_solana import WatchTarget


REPAIR_VERSION = "poll-receipt-offloop-v2"


async def _record_poll_rows_scoped_offloop(
    self: Any,
    target: WatchTarget,
    rows: list[dict[str, Any]],
) -> int:
    """Persist poll-recovery receipts outside Uvicorn's event-loop thread.

    This function replaces only the implementation captured later by
    ``install_strategy_relevant_continuity``. It does not reinstall or reorder any
    existing wrapper, so status/continuity composition stays identical.
    """

    original = strategy._ORIGINAL_RECORD_POLL_ROWS
    if target.kind == "scout" or not hasattr(getattr(self, "journal", None), "record_receipt"):
        if original is None:
            raise RuntimeError("strategy continuity repair missing original poll recorder")

        def run_original() -> int:
            return int(asyncio.run(original(self, target, rows)))

        return await asyncio.to_thread(run_original)

    def persist_program_rows() -> int:
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

    inserted_count = await asyncio.to_thread(persist_program_rows)
    setattr(
        self,
        "_roi_program_poll_rows_raw_only_total",
        int(getattr(self, "_roi_program_poll_rows_raw_only_total", 0) or 0) + inserted_count,
    )
    return inserted_count


def install_poll_receipt_offloop_repair() -> None:
    current = strategy._record_poll_rows_scoped
    if bool(getattr(current, "_roi_poll_receipt_offloop", False)):
        return
    setattr(_record_poll_rows_scoped_offloop, "_roi_poll_receipt_offloop", True)
    setattr(_record_poll_rows_scoped_offloop, "_roi_poll_receipt_offloop_version", REPAIR_VERSION)
    strategy._record_poll_rows_scoped = _record_poll_rows_scoped_offloop


__all__ = ["REPAIR_VERSION", "install_poll_receipt_offloop_repair"]
