from __future__ import annotations

from typing import Any

from . import batch9_continuity_frontier_proof_repair as batch9
from . import live_poll_redundancy as live_poll
from . import poll_recoverability_lease as lease
from . import poll_watermark_repair as watermark
from .direct_solana import WatchTarget


REPAIR_VERSION = "batch9-scout-checkpoint-commit-order-v1"
_ORIGINAL_RECORD_ROWS = None
_INSTALLED = False


async def _fetch_with_commit_order(
    self: Any,
    target: WatchTarget,
    cursor_slot: int,
) -> tuple[list[dict[str, Any]], bool, str | None, float | None]:
    if target.kind != "scout":
        if batch9._ORIGINAL_SLOT_FETCH is None:
            raise RuntimeError("Batch 9 original slot fetch unavailable")
        return await batch9._ORIGINAL_SLOT_FETCH(self, target, cursor_slot)

    rows, complete, provider, latency = await batch9._fetch_scout_delta(self, target, cursor_slot)
    if complete and live_poll._ws_target_covered(self, target):
        newest = max((watermark._row_slot(row) for row in rows), default=int(cursor_slot))
        batch9._save_checkpoint(
            self,
            target,
            cursor_slot=max(int(cursor_slot), newest),
            ws_gap_generation=lease._current_ws_generation(self, target),
        )
    # When WebSocket coverage is absent the checkpoint is intentionally not advanced
    # here. _record_rows_then_checkpoint moves it only after the fallback receipts
    # have committed successfully to the canonical journal.
    return rows, complete, provider, latency


async def _record_rows_then_checkpoint(
    self: Any,
    target: WatchTarget,
    rows: list[dict[str, Any]],
) -> int:
    if _ORIGINAL_RECORD_ROWS is None:
        raise RuntimeError("Batch 9 original poll row recorder unavailable")
    inserted = await _ORIGINAL_RECORD_ROWS(self, target, rows)
    if target.kind == "scout" and rows:
        newest = max((watermark._row_slot(row) for row in rows), default=0)
        if newest > 0:
            batch9._save_checkpoint(
                self,
                target,
                cursor_slot=newest,
                ws_gap_generation=lease._current_ws_generation(self, target),
            )
    return int(inserted)


def install_batch9_scout_checkpoint_commit_repair() -> None:
    global _INSTALLED, _ORIGINAL_RECORD_ROWS
    if _INSTALLED:
        return
    _ORIGINAL_RECORD_ROWS = live_poll._record_poll_rows
    watermark._slot_fetch_delta = _fetch_with_commit_order  # type: ignore[assignment]
    live_poll._record_poll_rows = _record_rows_then_checkpoint  # type: ignore[assignment]
    _INSTALLED = True


__all__ = [
    "REPAIR_VERSION",
    "_fetch_with_commit_order",
    "_record_rows_then_checkpoint",
    "install_batch9_scout_checkpoint_commit_repair",
]
