from __future__ import annotations

import asyncio
from types import SimpleNamespace

from solana_roi import continuity_immediate_recovery_repair as immediate
from solana_roi import continuity_target_frontier_repair as frontier
from solana_roi import live_poll_redundancy as live_poll
from solana_roi import poll_recoverability_lease as lease
from solana_roi.direct_solana import WatchTarget


def _target() -> WatchTarget:
    return WatchTarget(kind="program", address="program-a", source_hint="PUMP_FUN")


class ConfirmingPool:
    def __init__(self, statuses):
        self.statuses = statuses
        self.calls = []

    async def call_with_meta(self, method, params, *, hedge=False):
        self.calls.append((method, params, hedge))
        assert method == "getSignatureStatuses"
        assert hedge is True
        assert params[1] == {"searchTransactionHistory": True}
        return {"value": self.statuses}, "publicnode", 12.0


def test_confirmed_frontier_advances_only_to_slot_minus_one(monkeypatch):
    target = _target()
    key = live_poll._poll_target_key(target)
    plane = SimpleNamespace(
        _roi_real_ws_gap_generations={key: 2},
        _roi_continuity_gap_clocks={key: {"generation": 2, "started_monotonic": 0.0}},
    )
    frontier._observe_target_frontier(plane, "publicnode", target, "sig-500", 500, observed_monotonic=1.0)
    frontier._observe_target_frontier(plane, "solana-mainnet", target, "sig-510", 510, observed_monotonic=2.0)
    pool = ConfirmingPool(
        [
            {"slot": 510, "confirmationStatus": "confirmed"},
            {"slot": 500, "confirmationStatus": "finalized"},
        ]
    )
    monkeypatch.setattr(frontier.isolation, "_recovery_rpc", lambda _plane: pool)

    cursor, anchor = asyncio.run(
        frontier._confirmed_target_frontier_cursor(plane, target, 100, 2)
    )

    assert cursor == 509
    assert anchor is not None
    assert anchor["confirmed_frontier_slot"] == 510
    assert anchor["routine_cursor_slot"] == 100
    assert anchor["same_slot_replay_required"] is True
    assert pool.calls[0][1][0] == ["sig-510", "sig-500"]
    assert lease.POLL_RECOVERABILITY_LEASE_SECONDS == 12.0
    assert live_poll.POLL_CURSOR_MAX_PAGES == 3
    assert live_poll.POLL_LIMIT == 1000


def test_processed_or_slot_mismatched_frontier_is_never_trusted(monkeypatch):
    target = _target()
    key = live_poll._poll_target_key(target)
    plane = SimpleNamespace(
        _roi_real_ws_gap_generations={key: 1},
        _roi_continuity_gap_clocks={key: {"generation": 1, "started_monotonic": 0.0}},
    )
    frontier._observe_target_frontier(plane, "publicnode", target, "sig-new", 700, observed_monotonic=1.0)
    pool = ConfirmingPool([{"slot": 701, "confirmationStatus": "processed"}])
    monkeypatch.setattr(frontier.isolation, "_recovery_rpc", lambda _plane: pool)

    cursor, anchor = asyncio.run(
        frontier._confirmed_target_frontier_cursor(plane, target, 100, 1)
    )

    assert cursor == 100
    assert anchor is None
    assert getattr(plane, "_roi_target_frontier_anchor_fallbacks") == 1


def test_target_frontier_is_exact_target_not_program_source_group():
    left = WatchTarget(kind="program", address="raydium-a", source_hint="RAYDIUM")
    right = WatchTarget(kind="program", address="raydium-b", source_hint="RAYDIUM")
    plane = SimpleNamespace()

    frontier._observe_target_frontier(plane, "publicnode", left, "sig-a", 200)
    frontier._observe_target_frontier(plane, "publicnode", right, "sig-b", 900)

    left_rows = list(frontier._target_history(plane, left))
    right_rows = list(frontier._target_history(plane, right))
    assert [row["signature"] for row in left_rows] == ["sig-a"]
    assert [row["signature"] for row in right_rows] == ["sig-b"]


def test_outer_handler_captures_frontier_before_inner_dispatch():
    target = _target()
    plane = SimpleNamespace()
    observed = []

    async def inner(self, provider, subscription_targets, message):
        rows = list(frontier._target_history(self, target))
        observed.append(rows[-1]["signature"] if rows else None)

    wrapped = frontier._handler_with_target_frontier(inner)
    message = {
        "method": "logsNotification",
        "params": {
            "subscription": 1,
            "result": {
                "context": {"slot": 321},
                "value": {"signature": "sig-321", "err": None, "logs": []},
            },
        },
    }
    asyncio.run(wrapped(plane, "publicnode", {1: target}, message))

    assert observed == ["sig-321"]
    assert getattr(wrapped, "_roi_target_frontier_recovery", False) is True


def test_recovery_uses_frontier_cursor_but_preserves_canonical_task_cursor(monkeypatch):
    target = _target()
    key = live_poll._poll_target_key(target)
    plane = SimpleNamespace(
        _roi_real_ws_gap_generations={key: 1},
        _roi_continuity_gap_clocks={key: {"generation": 1, "started_monotonic": 0.0}},
    )
    live_poll._poll_state(plane)[key] = {"baseline_established": True, "cursor_slot": 100}
    frontier._observe_target_frontier(plane, "publicnode", target, "sig-500", 500)

    async def fake_confirmed(_self, _target, routine_cursor, generation):
        assert routine_cursor == 100
        assert generation == 1
        return 499, {"source": "confirmed-target-websocket-frontier", "confirmed_frontier_slot": 500}

    async def fake_recover(_self, _target, cursor, generation):
        assert cursor == 499
        assert generation == 1
        return ([{"signature": "sig-501", "slot": 501}], True, "publicnode", 5.0)

    monkeypatch.setattr(frontier, "_confirmed_target_frontier_cursor", fake_confirmed)
    monkeypatch.setattr(frontier.isolation, "_recover_with_isolated_rpc", fake_recover)

    async def scenario():
        frontier._kick_with_target_frontier(plane, target, 1)
        task_row = immediate._recovery_tasks(plane)[key]
        assert task_row["cursor_slot"] == 100
        result = await task_row["task"]
        return result

    result = asyncio.run(scenario())
    assert result[1] is True
    assert result[0][0]["slot"] == 501
    assert lease.POLL_RECOVERABILITY_LEASE_SECONDS == 12.0
    assert live_poll.POLL_CURSOR_MAX_PAGES == 3
    assert live_poll.POLL_LIMIT == 1000
