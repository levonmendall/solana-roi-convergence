from __future__ import annotations

import pytest

from solana_roi import robinhood_phase9_anchor_seed as phase9


class Rpc:
    async def block_number(self) -> int:
        return 12_345


class Plane:
    def __init__(self) -> None:
        self.rpc = Rpc()
        self._latest_block = None


@pytest.mark.asyncio
async def test_latest_seed_uses_only_bounded_factory_metadata_insurance(monkeypatch) -> None:
    calls = []

    async def fake_sync(self, *, latest, previous_live_cursor, reason):
        calls.append(
            {
                "latest": latest,
                "previous_live_cursor": previous_live_cursor,
                "reason": reason,
            }
        )

    monkeypatch.setattr(phase9.forward_only, "_sync_bounded_metadata", fake_sync)
    plane = Plane()
    await phase9._seed_current_anchor(plane)

    assert plane._latest_block == 12_345
    assert calls == [
        {
            "latest": 12_345,
            "previous_live_cursor": None,
            "reason": "phase9_latest_seed_plus_reorg_insurance",
        }
    ]
    assert plane._roi_phase9_anchor_seed["factory_metadata_only"] is True
    assert plane._roi_phase9_anchor_seed["historical_swap_replay"] is False
    assert plane._roi_phase9_anchor_seed["historical_cursor_readiness_authority"] is False
    assert plane._roi_phase9_anchor_seed["catchup_mode"] == "latest_seed_plus_reorg_insurance"
