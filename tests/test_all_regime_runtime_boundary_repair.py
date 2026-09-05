from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from solana_roi import all_regime_runtime_boundary_repair as repair
from solana_roi import continuity_recovery_isolation_repair as isolation
from solana_roi import live_poll_redundancy as live_poll
from solana_roi import poll_recoverability_lease as lease
from solana_roi import robinhood_blockscout_pro_repair as blockscout


def test_blockscout_secret_aliases_and_canonical_precedence(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in blockscout.API_KEY_ENV_NAMES:
        monkeypatch.delenv(name, raising=False)

    monkeypatch.setenv("BLOCKSCOUT_PRO_API_KEY", "alias-secret")
    assert blockscout._api_key() == "alias-secret"
    assert blockscout._api_key_source()[0] == "BLOCKSCOUT_PRO_API_KEY"

    monkeypatch.setenv("BLOCKSCOUT_API_KEY", "canonical-secret")
    assert blockscout._api_key() == "canonical-secret"
    assert blockscout._api_key_source()[0] == "BLOCKSCOUT_API_KEY"


def test_render_blueprint_declares_canonical_blockscout_secret() -> None:
    blueprint = Path("render.yaml").read_text()
    assert "- key: BLOCKSCOUT_API_KEY\n        sync: false" in blueprint


def test_robinhood_transport_readiness_fails_closed_on_stale_caught_up_flag() -> None:
    stale = SimpleNamespace(_caught_up=True, _cursor=100, _latest_block=200)
    assert repair._paper_transport_ready(stale) is False

    current = SimpleNamespace(
        _caught_up=True,
        _cursor=198,
        _latest_block=200,
    )
    assert repair._paper_transport_ready(current) is True


def test_urgent_gap_recovery_expands_only_real_gap_capacity() -> None:
    assert repair.URGENT_GAP_RECOVERY_MAX_PAGES > live_poll.POLL_CURSOR_MAX_PAGES
    assert live_poll.POLL_CURSOR_MAX_PAGES == 3
    assert lease.POLL_RECOVERABILITY_LEASE_SECONDS == 12.0


@pytest.mark.asyncio
async def test_dense_scout_gap_can_cross_old_three_page_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    limit = live_poll.POLL_LIMIT
    cursor = 100

    def page(start: int, count: int) -> list[dict[str, object]]:
        return [
            {"signature": f"sig-{slot}", "slot": slot}
            for slot in range(start, start - count, -1)
        ]

    pages = [
        page(4100, limit),
        page(3100, limit),
        page(2100, limit),
        page(1100, limit),
        page(100, 11),
    ]

    class FakeRpc:
        def __init__(self) -> None:
            self.calls = 0

        async def call_with_meta(self, method: str, params: list[object], *, hedge: bool):
            assert method == "getSignaturesForAddress"
            assert hedge is True
            index = self.calls
            self.calls += 1
            return pages[index], "fake", 1.0

    fake = FakeRpc()
    monkeypatch.setattr(isolation, "_recovery_rpc", lambda _self: fake)
    target = SimpleNamespace(address="dense-scout")

    rows, complete, provider, latency, meta = await repair._expanded_gap_fetch_delta(
        SimpleNamespace(), target, cursor
    )

    assert complete is True
    assert provider == "fake"
    assert latency == 1.0
    assert fake.calls == 5
    assert meta["page_count"] == 5
    assert meta["urgent_page_limit"] == repair.URGENT_GAP_RECOVERY_MAX_PAGES
    assert meta["routine_page_limit_unchanged"] == 3
    assert len(rows) == 4000
