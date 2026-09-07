from __future__ import annotations

import asyncio
from types import SimpleNamespace

from solana_roi import batch9_scout_checkpoint_commit_repair as repair


def test_fallback_checkpoint_moves_only_after_receipt_commit(monkeypatch) -> None:
    target = SimpleNamespace(kind="scout", address="wallet-a", source_hint=None)
    plane = SimpleNamespace()
    rows = [{"signature": "sig-a", "slot": 101}]
    saved: list[int] = []

    async def fetch_delta(_plane, _target, _cursor):
        return rows, True, "provider-a", 1.0

    async def record_rows(_plane, _target, _rows):
        return 1

    monkeypatch.setattr(repair.batch9, "_fetch_scout_delta", fetch_delta)
    monkeypatch.setattr(repair.live_poll, "_ws_target_covered", lambda _plane, _target: False)
    monkeypatch.setattr(repair.lease, "_current_ws_generation", lambda _plane, _target: 3)
    monkeypatch.setattr(
        repair.batch9,
        "_save_checkpoint",
        lambda _plane, _target, *, cursor_slot, ws_gap_generation: saved.append(cursor_slot),
    )
    monkeypatch.setattr(repair, "_ORIGINAL_RECORD_ROWS", record_rows)

    result = asyncio.run(repair._fetch_with_commit_order(plane, target, 100))
    assert result[1] is True
    assert saved == []

    inserted = asyncio.run(repair._record_rows_then_checkpoint(plane, target, rows))
    assert inserted == 1
    assert saved == [101]
