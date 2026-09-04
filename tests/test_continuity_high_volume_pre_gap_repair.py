from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace

from solana_roi import continuity_high_volume_pre_gap_repair as repair
from solana_roi import continuity_immediate_recovery_repair as immediate
from solana_roi import live_poll_redundancy as live_poll
from solana_roi import poll_recoverability_lease as lease
from solana_roi.direct_solana import WatchTarget


def _target() -> WatchTarget:
    return WatchTarget(kind="program", address="pump-program", source_hint="PUMP_FUN")


def _plane(*, cursor: int = 100, generation: int = 7) -> SimpleNamespace:
    target = _target()
    key = live_poll._poll_target_key(target)
    return SimpleNamespace(
        _roi_live_poll_state={
            key: {
                "connected": True,
                "baseline_established": True,
                "cursor_slot": cursor,
            }
        },
        _roi_poll_recoverability_runtime={
            key: {"cursor_ws_generation": generation}
        },
        _roi_real_ws_gap_generations={key: generation},
    )


def test_burst_buffer_preserves_exact_per_target_byte_ceiling():
    assert repair.TARGET_WS_BURST_MAX_QUEUE == 32
    assert repair.TARGET_WS_BURST_MAX_SIZE_BYTES == 256 * 1024
    assert repair.TARGET_WS_BURST_BYTE_CEILING == 8 * 1024 * 1024
    assert repair.TARGET_WS_BURST_BYTE_CEILING == 8 * (1024 * 1024)


def test_proactive_confirmation_publishes_same_generation_slot_minus_one(monkeypatch):
    target = _target()
    plane = _plane(cursor=100, generation=7)

    monkeypatch.setattr(live_poll, "_ws_target_covered", lambda _self, _target: True)

    async def confirmed(_self, _target, routine_cursor_slot, generation):
        assert routine_cursor_slot == 100
        assert generation == 7
        return 149, {
            "source": "confirmed-target-websocket-frontier",
            "generation": 7,
            "confirmed_frontier_slot": 150,
            "confirmation_provider": "publicnode",
            "confirmation_latency_ms": 12.0,
        }

    monkeypatch.setattr(repair.frontier, "_confirmed_target_frontier_cursor", confirmed)
    asyncio.run(repair._confirm_pre_gap_frontier(plane, target))

    row = repair._cache(plane)[live_poll._poll_target_key(target)]
    assert row["generation"] == 7
    assert row["checkpoint_cursor_slot"] == 149
    assert row["confirmed_frontier_slot"] == 150
    assert row["same_slot_replay_required"] is True
    assert plane._roi_pre_gap_frontier_confirmed_checkpoints == 1


def test_proactive_confirmation_refuses_unrecovered_generation(monkeypatch):
    target = _target()
    plane = _plane(cursor=100, generation=7)
    key = live_poll._poll_target_key(target)
    plane._roi_poll_recoverability_runtime[key]["cursor_ws_generation"] = 6
    called = False

    monkeypatch.setattr(live_poll, "_ws_target_covered", lambda _self, _target: True)

    async def confirmed(*_args, **_kwargs):
        nonlocal called
        called = True
        return 149, {}

    monkeypatch.setattr(repair.frontier, "_confirmed_target_frontier_cursor", confirmed)
    asyncio.run(repair._confirm_pre_gap_frontier(plane, target))

    assert called is False
    assert repair._cache(plane) == {}
    assert plane._roi_pre_gap_frontier_blocked_unrecovered_generation == 1


def test_cached_checkpoint_advances_healthy_poll_without_bypassing_generation_guard(monkeypatch):
    target = _target()
    plane = _plane(cursor=100, generation=7)
    key = live_poll._poll_target_key(target)
    repair._cache(plane)[key] = {
        "generation": 7,
        "checkpoint_cursor_slot": 149,
        "confirmed_frontier_slot": 150,
        "confirmation_provider": "publicnode",
        "confirmation_latency_ms": 9.0,
        "captured_monotonic": time.monotonic(),
    }
    delegated: list[int] = []

    async def fallback(_self, _target, cursor):
        delegated.append(cursor)
        return [], True, "fallback", 1.0

    monkeypatch.setattr(repair, "_ORIGINAL_CHECKPOINT_FETCH", fallback)
    monkeypatch.setattr(live_poll, "_ws_target_covered", lambda _self, _target: True)

    result = asyncio.run(repair._checkpoint_fetch_with_pre_gap_cache(plane, target, 100))
    assert result[0][0]["slot"] == 149
    assert result[0][0]["signature"] == ""
    assert result[1] is True
    assert delegated == []

    # If the runtime cursor still belongs to the previous generation, the cache is
    # forbidden and the PR99 generation-safe delegate must remain authoritative.
    plane._roi_poll_recoverability_runtime[key]["cursor_ws_generation"] = 6
    result = asyncio.run(repair._checkpoint_fetch_with_pre_gap_cache(plane, target, 100))
    assert result == ([], True, "fallback", 1.0)
    assert delegated == [100]


def test_real_gap_kick_uses_only_fresh_immediately_previous_generation_checkpoint(monkeypatch):
    target = _target()
    plane = _plane(cursor=100, generation=8)
    key = live_poll._poll_target_key(target)
    # State/runtime cursor belongs to healthy generation 7 at the instant generation
    # 8 is opened by a true zero-WebSocket-coverage transition.
    plane._roi_poll_recoverability_runtime[key]["cursor_ws_generation"] = 7
    repair._cache(plane)[key] = {
        "generation": 7,
        "checkpoint_cursor_slot": 149,
        "confirmed_frontier_slot": 150,
        "captured_monotonic": time.monotonic(),
    }
    seen: list[int] = []

    def original(_self, _target, generation):
        assert generation == 8
        seen.append(int(_self._roi_live_poll_state[key]["cursor_slot"]))

    monkeypatch.setattr(repair, "_ORIGINAL_KICK", original)
    repair._kick_with_pre_gap_checkpoint(plane, target, 8)

    assert seen == [149]
    assert plane._roi_live_poll_state[key]["pre_gap_checkpoint_applied"] is True
    assert plane._roi_pre_gap_frontier_recovery_cursor_upgrades == 1

    # A checkpoint from anything other than generation N-1 cannot move the cursor.
    plane._roi_live_poll_state[key]["cursor_slot"] = 100
    repair._cache(plane)[key]["generation"] = 6
    seen.clear()
    repair._kick_with_pre_gap_checkpoint(plane, target, 8)
    assert seen == [100]


def test_fixed_continuity_and_candidate_boundaries_remain_unchanged():
    from solana_roi import forward_evidence_runtime_repair as forward

    assert lease.POLL_RECOVERABILITY_LEASE_SECONDS == 12.0
    assert live_poll.POLL_CURSOR_MAX_PAGES == 3
    assert live_poll.POLL_LIMIT == 1000
    assert live_poll.POLL_INTERVAL_SECONDS == 4.0
    assert forward.LATENCY_BUDGET_SECONDS == 5.0
    assert forward.ENTRY_WINDOW_SECONDS == 20.0
