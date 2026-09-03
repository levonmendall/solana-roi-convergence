from __future__ import annotations

import asyncio
from types import SimpleNamespace

from solana_roi import continuity_recovery_isolation_repair as isolation
from solana_roi import live_poll_redundancy as live_poll
from solana_roi import poll_recoverability_lease as lease
from solana_roi.direct_solana import WatchTarget


def _target() -> WatchTarget:
    return WatchTarget(kind="program", address="program-a", source_hint="PUMP_FUN")


class OnePagePool:
    async def call_with_meta(self, method, params, *, hedge=False):
        assert method == "getSignaturesForAddress"
        assert hedge is True
        config = params[1]
        assert config["limit"] == 1000
        return ([{"signature": "sig-101", "slot": 101}], "publicnode", 25.0)


class ThreeFullPagesPool:
    def __init__(self):
        self.calls = 0

    async def call_with_meta(self, method, params, *, hedge=False):
        assert method == "getSignaturesForAddress"
        assert hedge is True
        self.calls += 1
        base = 4000 - self.calls * 1000
        rows = [
            {"signature": f"sig-{self.calls}-{index}", "slot": base + index + 1}
            for index in range(1000)
        ]
        return rows, "publicnode", 30.0


def test_isolated_gap_fetch_returns_complete_delta_and_metadata(monkeypatch):
    target = _target()
    plane = SimpleNamespace()
    monkeypatch.setattr(isolation, "_recovery_rpc", lambda _plane: OnePagePool())

    rows, complete, provider, latency, meta = asyncio.run(
        isolation._isolated_gap_fetch_delta(plane, target, 100)
    )

    assert complete is True
    assert provider == "publicnode"
    assert latency == 25.0
    assert rows == [{"signature": "sig-101", "slot": 101}]
    assert meta["page_count"] == 1
    assert meta["recovered_row_count"] == 1
    assert meta["hard_page_limit"] == 3
    assert meta["hard_page_size"] == 1000
    assert live_poll.POLL_CURSOR_MAX_PAGES == 3
    assert live_poll.POLL_LIMIT == 1000


def test_three_full_pages_without_cursor_remain_incomplete_at_unchanged_bound(monkeypatch):
    target = _target()
    plane = SimpleNamespace()
    pool = ThreeFullPagesPool()
    monkeypatch.setattr(isolation, "_recovery_rpc", lambda _plane: pool)

    rows, complete, _provider, _latency, meta = asyncio.run(
        isolation._isolated_gap_fetch_delta(plane, target, 100)
    )

    assert complete is False
    assert rows == []
    assert pool.calls == 3
    assert meta["page_count"] == 3
    assert meta["page_sizes"] == [1000, 1000, 1000]
    assert meta["cursor_reached"] is False
    assert meta["hard_page_limit"] == 3
    assert meta["hard_page_size"] == 1000


def test_successful_isolated_recovery_records_target_generation_and_pages(monkeypatch):
    target = _target()
    key = live_poll._poll_target_key(target)
    plane = SimpleNamespace(
        _roi_real_ws_gap_generations={key: 2},
        _roi_continuity_gap_clocks={key: {"generation": 2, "started_monotonic": 0.0}},
    )

    async def complete(_plane, _target, cursor_slot):
        assert cursor_slot == 100
        return (
            [{"signature": "sig", "slot": 101}],
            True,
            "solana-mainnet",
            12.0,
            {
                "page_count": 1,
                "page_sizes": [1],
                "page_providers": ["solana-mainnet"],
                "page_latencies_ms": [12.0],
                "newest_slot_seen": 101,
                "oldest_slot_seen": 101,
                "cursor_slot": 100,
                "cursor_reached": True,
                "complete": True,
                "recovered_row_count": 1,
                "hard_page_limit": 3,
                "hard_page_size": 1000,
            },
        )

    times = iter([1.0, 1.1])
    monkeypatch.setattr(isolation, "_isolated_gap_fetch_delta", complete)
    monkeypatch.setattr(isolation, "_monotonic", lambda: next(times, 1.1))

    rows, complete_flag, provider, _latency = asyncio.run(
        isolation._recover_with_isolated_rpc(plane, target, 100, 2)
    )
    attribution = isolation._attribution_state(plane)["last_success"]

    assert complete_flag is True
    assert rows[0]["slot"] == 101
    assert provider == "solana-mainnet"
    assert attribution["target"] == key
    assert attribution["generation"] == 2
    assert attribution["page_count"] == 1
    assert attribution["recovered_row_count"] == 1


def test_failed_recovery_attributes_bound_without_widening_lease(monkeypatch):
    target = _target()
    key = live_poll._poll_target_key(target)
    plane = SimpleNamespace(
        _roi_real_ws_gap_generations={key: 1},
        _roi_continuity_gap_clocks={key: {"generation": 1, "started_monotonic": 0.0}},
    )

    async def incomplete(_plane, _target, _cursor_slot):
        return (
            [],
            False,
            "publicnode",
            2500.0,
            {
                "page_count": 3,
                "page_sizes": [1000, 1000, 1000],
                "page_providers": ["publicnode"] * 3,
                "page_latencies_ms": [2500.0] * 3,
                "newest_slot_seen": 4000,
                "oldest_slot_seen": 1001,
                "cursor_slot": 100,
                "cursor_reached": False,
                "complete": False,
                "recovered_row_count": 0,
                "hard_page_limit": 3,
                "hard_page_size": 1000,
            },
        )

    times = iter([1.0, 13.0, 13.0])
    monkeypatch.setattr(isolation, "_isolated_gap_fetch_delta", incomplete)
    monkeypatch.setattr(isolation, "_monotonic", lambda: next(times, 13.0))

    try:
        asyncio.run(isolation._recover_with_isolated_rpc(plane, target, 100, 1))
    except RuntimeError:
        pass
    else:
        raise AssertionError("bounded real-gap failure must remain fail-closed")

    failure = isolation._attribution_state(plane)["last_failure"]
    assert failure["target"] == key
    assert failure["generation"] == 1
    assert failure["page_count"] == 3
    assert failure["reason"] == "bounded_page_limit_exhausted_at_lease"
    assert failure["hard_page_limit"] == 3
    assert failure["hard_page_size"] == 1000
    assert failure["lease_seconds"] == 12.0
    assert lease.POLL_RECOVERABILITY_LEASE_SECONDS == 12.0
