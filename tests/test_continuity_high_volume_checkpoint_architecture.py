from __future__ import annotations

import asyncio
from collections import deque
from types import SimpleNamespace

from solana_roi import continuity_high_volume_checkpoint_architecture as architecture
from solana_roi import continuity_high_volume_poll_affinity_repair as affinity
from solana_roi import continuity_target_frontier_repair as frontier
from solana_roi import live_poll_redundancy as live_poll
from solana_roi import poll_exception_rearm as exception_rearm
from solana_roi import poll_recoverability_lease as lease
from solana_roi import poll_watermark_repair as watermark
from solana_roi.direct_solana import WatchTarget


def _target() -> WatchTarget:
    return WatchTarget(kind="program", address="pump-fun", source_hint="PUMP_FUN")


def test_high_volume_healthy_websocket_uses_confirmed_frontier_checkpoint(monkeypatch):
    target = _target()
    key = live_poll._poll_target_key(target)
    plane = SimpleNamespace(
        _roi_target_ws_frontiers={
            key: deque(
                [
                    {
                        "signature": "sig-500",
                        "slot": 500,
                        "provider": "publicnode",
                        "observed_monotonic": 10.0,
                    }
                ],
                maxlen=16,
            )
        },
        _roi_real_ws_gap_generations={key: 7},
    )

    monkeypatch.setattr(affinity, "_is_high_volume_target", lambda _target: True)
    monkeypatch.setattr(live_poll, "_ws_target_covered", lambda _self, _target: True)
    monkeypatch.setattr(lease, "_current_ws_generation", lambda _self, _target: 7)

    async def confirmed(_self, _target, routine_cursor, generation):
        assert routine_cursor == 100
        assert generation == 7
        return 499, {
            "source": "confirmed-target-websocket-frontier",
            "generation": 7,
            "signature": "sig-500",
            "confirmed_frontier_slot": 500,
            "effective_cursor_slot": 499,
            "confirmation_provider": "publicnode",
            "confirmation_latency_ms": 12.5,
            "same_slot_replay_required": True,
        }

    async def fallback(*args, **kwargs):
        raise AssertionError("healthy high-volume WebSocket should not replay suppressed history")

    monkeypatch.setattr(frontier, "_confirmed_target_frontier_cursor", confirmed)
    monkeypatch.setattr(architecture, "_ORIGINAL_SLOT_FETCH", fallback)

    rows, complete, provider, latency = asyncio.run(
        architecture._checkpointed_slot_fetch_delta(plane, target, 100)
    )

    assert complete is True
    assert provider == "publicnode"
    assert latency == 12.5
    assert rows == [
        {
            "signature": "",
            "slot": 499,
            "err": None,
            "_roi_standby_checkpoint": True,
        }
    ]
    assert architecture._checkpoint_counts(plane)[key] == 1
    checkpoint = architecture._last_checkpoints(plane)[key]
    assert checkpoint["prior_cursor_slot"] == 100
    assert checkpoint["checkpoint_cursor_slot"] == 499
    assert checkpoint["same_slot_replay_required"] is True

    assert lease.POLL_RECOVERABILITY_LEASE_SECONDS == 12.0
    assert live_poll.POLL_CURSOR_MAX_PAGES == 3
    assert live_poll.POLL_LIMIT == 1000


def test_generation_change_during_confirmation_falls_back_to_bounded_delta(monkeypatch):
    target = _target()
    key = live_poll._poll_target_key(target)
    plane = SimpleNamespace(
        _roi_target_ws_frontiers={
            key: deque(
                [
                    {
                        "signature": "sig-500",
                        "slot": 500,
                        "provider": "publicnode",
                        "observed_monotonic": 10.0,
                    }
                ],
                maxlen=16,
            )
        }
    )
    generations = iter([7, 8])

    monkeypatch.setattr(affinity, "_is_high_volume_target", lambda _target: True)
    monkeypatch.setattr(live_poll, "_ws_target_covered", lambda _self, _target: True)
    monkeypatch.setattr(lease, "_current_ws_generation", lambda _self, _target: next(generations))

    async def confirmed(_self, _target, routine_cursor, generation):
        return 499, {
            "source": "confirmed-target-websocket-frontier",
            "generation": generation,
            "signature": "sig-500",
            "confirmed_frontier_slot": 500,
            "effective_cursor_slot": 499,
        }

    calls = []

    async def fallback(_self, _target, cursor):
        calls.append(cursor)
        return ([{"signature": "real-gap-row", "slot": 101}], True, "publicnode", 9.0)

    monkeypatch.setattr(frontier, "_confirmed_target_frontier_cursor", confirmed)
    monkeypatch.setattr(architecture, "_ORIGINAL_SLOT_FETCH", fallback)

    result = asyncio.run(architecture._checkpointed_slot_fetch_delta(plane, target, 100))
    assert calls == [100]
    assert result[0][0]["signature"] == "real-gap-row"
    assert architecture._checkpoint_counts(plane) == {}


def test_non_high_volume_target_keeps_existing_bounded_poll(monkeypatch):
    target = WatchTarget(kind="program", address="raydium", source_hint="RAYDIUM")
    plane = SimpleNamespace()
    monkeypatch.setattr(affinity, "_is_high_volume_target", lambda _target: False)

    calls = []

    async def fallback(_self, _target, cursor):
        calls.append(cursor)
        return ([], True, "publicnode", 4.0)

    monkeypatch.setattr(architecture, "_ORIGINAL_SLOT_FETCH", fallback)
    result = asyncio.run(architecture._checkpointed_slot_fetch_delta(plane, target, 44))
    assert calls == [44]
    assert result == ([], True, "publicnode", 4.0)


def test_outer_checkpoint_proxy_preserves_canonical_poll_contracts():
    architecture.install_high_volume_standby_checkpoint_architecture()

    assert isinstance(lease.watermark, architecture._HighVolumeCheckpointProxy)
    assert watermark._slot_fetch_delta is exception_rearm._exception_rearm_fetch_delta
    assert live_poll._fetch_delta is exception_rearm._exception_rearm_fetch_delta
    assert lease.watermark._base is watermark
    assert lease.watermark._slot_poll_page is watermark._slot_poll_page
