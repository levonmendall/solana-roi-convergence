from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

from solana_roi import certification_runtime_architecture_repair as runtime_arch
from solana_roi import continuity_exact_durable_signature_repair as repair
from solana_roi import continuity_immediate_recovery_repair as immediate
from solana_roi import continuity_recovery_isolation_repair as isolation
from solana_roi import live_poll_redundancy as live_poll
from solana_roi import poll_recoverability_lease as lease
from solana_roi import raw_receipt_dispatch_repair as raw_dispatch
from solana_roi.direct_solana import WatchTarget


def _pump_target() -> WatchTarget:
    return WatchTarget(kind="program", address="pump-program", source_hint="PUMP_FUN")


def test_durable_frontier_is_published_only_after_real_websocket_commit(monkeypatch):
    journal = SimpleNamespace()
    calls = []

    def original(self, **kwargs):
        calls.append(dict(kwargs))
        return True

    monkeypatch.setattr(repair, "_ORIGINAL_RECORD_RECEIPT", original)
    now = datetime.now(timezone.utc)

    assert repair._record_receipt_with_durable_frontier(
        journal,
        signature="poll-row",
        source_key="PUMP_FUN",
        slot=100,
        received_at=now,
        launch_like=False,
    )
    assert repair._journal_frontiers(journal) == {}

    token = raw_dispatch._RECEIPT_WALL_TIME.set(now)
    try:
        assert repair._record_receipt_with_durable_frontier(
            journal,
            signature="ws-durable",
            source_key="PUMP_FUN",
            slot=101,
            received_at=now,
            launch_like=False,
        )
    finally:
        raw_dispatch._RECEIPT_WALL_TIME.reset(token)

    row = repair._journal_frontiers(journal)["PUMP_FUN"]
    assert row["signature"] == "ws-durable"
    assert row["slot"] == 101
    assert row["durable"] is True
    assert row["transport"] == "websocket"
    assert len(calls) == 2


def test_gap_kick_snapshots_only_immediately_preceding_recovered_generation(monkeypatch):
    target = _pump_target()
    key = live_poll._poll_target_key(target)
    journal = SimpleNamespace(
        _roi_exact_durable_ws_frontiers={
            "PUMP_FUN": {
                "signature": "durable-lower",
                "slot": 500,
                "source_key": "PUMP_FUN",
                "committed_monotonic": 5.0,
                "durable": True,
                "transport": "websocket",
            }
        }
    )
    plane = SimpleNamespace(journal=journal)
    delegated = []

    monkeypatch.setattr(lease, "_runtime", lambda _self: {key: {"cursor_ws_generation": 6}})
    monkeypatch.setattr(repair, "_gap_started_monotonic", lambda _self, _target: 10.0)
    monkeypatch.setattr(
        repair,
        "_ORIGINAL_KICK",
        lambda _self, _target, generation: delegated.append(generation),
    )

    repair._kick_with_exact_durable_boundary(plane, target, 7)
    boundary = repair._boundaries(plane)[key]
    assert boundary["generation"] == 7
    assert boundary["previous_generation"] == 6
    assert boundary["signature"] == "durable-lower"
    assert boundary["slot"] == 500
    assert boundary["confirmed"] is False
    assert delegated == [7]

    plane2 = SimpleNamespace(journal=journal)
    monkeypatch.setattr(lease, "_runtime", lambda _self: {key: {"cursor_ws_generation": 5}})
    repair._kick_with_exact_durable_boundary(plane2, target, 7)
    assert key not in repair._boundaries(plane2)

    # Missing generation state must also fail closed rather than defaulting to the
    # expected generation.
    plane3 = SimpleNamespace(journal=journal)
    monkeypatch.setattr(lease, "_runtime", lambda _self: {key: {}})
    repair._kick_with_exact_durable_boundary(plane3, target, 7)
    assert key not in repair._boundaries(plane3)


def test_exact_signature_fetch_excludes_only_proved_durable_same_slot_history(monkeypatch):
    target = _pump_target()
    key = live_poll._poll_target_key(target)
    plane = SimpleNamespace()
    repair._boundaries(plane)[key] = {
        "generation": 7,
        "signature": "lower",
        "slot": 100,
        "confirmed": True,
        "confirmation_failed": False,
        "same_slot_durability_failed": False,
    }

    monkeypatch.setattr(immediate, "_generation", lambda _self, _target: 7)
    monkeypatch.setattr(
        runtime_arch,
        "_recovery_upper_boundaries",
        lambda _self: {
            key: {
                "generation": 7,
                "signature": "upper",
                "slot": 101,
                "source": "first-successfully-recorded-post-gap-websocket-receipt",
            }
        },
    )
    proved = []
    monkeypatch.setattr(
        repair,
        "_durable_signatures_present",
        lambda _self, source, signatures: proved.append((source, list(signatures))) or True,
    )

    class Rpc:
        async def call_with_meta(self, method, params, hedge=False):
            assert method == "getSignaturesForAddress"
            assert params[0] == target.address
            assert params[1]["before"] == "upper"
            assert params[1]["limit"] == live_poll.POLL_LIMIT
            assert hedge is True
            return (
                [
                    {"signature": "new-slot", "slot": 101, "err": None},
                    {"signature": "same-slot-newer", "slot": 100, "err": None},
                    {"signature": "lower", "slot": 100, "err": None},
                    {"signature": "same-slot-older", "slot": 100, "err": None},
                    {"signature": "older-slot", "slot": 99, "err": None},
                ],
                "publicnode",
                12.5,
            )

    monkeypatch.setattr(isolation, "_recovery_rpc", lambda _self: Rpc())

    rows, complete, provider, latency, meta = asyncio.run(
        repair._exact_signature_interval_fetch(plane, target, 90)
    )

    assert complete is True
    assert provider == "publicnode"
    assert latency == 12.5
    assert [row["signature"] for row in rows] == ["same-slot-newer", "new-slot"]
    assert proved == [("PUMP_FUN", ["lower", "same-slot-older"])]
    assert meta["exact_durable_lower_boundary_applied"] is True
    assert meta["exact_durable_lower_reached"] is True
    assert meta["same_slot_tail_proven"] is True
    assert meta["same_slot_older_rows_durable"] is True
    assert meta["hard_page_limit"] == 3
    assert meta["hard_page_size"] == 1000
    assert meta["generation_upper_boundary_applied"] is True


def test_same_slot_not_durable_falls_back_to_slot_minus_one(monkeypatch):
    target = _pump_target()
    key = live_poll._poll_target_key(target)
    plane = SimpleNamespace()
    repair._boundaries(plane)[key] = {
        "generation": 7,
        "signature": "lower",
        "slot": 100,
        "confirmed": True,
        "confirmation_failed": False,
        "same_slot_durability_failed": False,
    }
    monkeypatch.setattr(immediate, "_generation", lambda _self, _target: 7)
    monkeypatch.setattr(runtime_arch, "_recovery_upper_boundaries", lambda _self: {})
    monkeypatch.setattr(repair, "_durable_signatures_present", lambda *_args: False)

    class Rpc:
        async def call_with_meta(self, method, params, hedge=False):
            return (
                [
                    {"signature": "newer", "slot": 101},
                    {"signature": "lower", "slot": 100},
                    {"signature": "same-slot-older", "slot": 100},
                    {"signature": "older-slot", "slot": 99},
                ],
                "publicnode",
                2.0,
            )

    delegated = []

    async def fallback(_self, _target, cursor_slot):
        delegated.append(cursor_slot)
        return ([{"signature": "slot-fallback", "slot": 101}], True, "fallback", 1.0, {"fallback": True})

    monkeypatch.setattr(isolation, "_recovery_rpc", lambda _self: Rpc())
    monkeypatch.setattr(repair, "_ORIGINAL_INTERVAL_FETCH", fallback)
    result = asyncio.run(repair._exact_signature_interval_fetch(plane, target, 90))
    assert result[0][0]["signature"] == "slot-fallback"
    assert delegated == [90]
    assert repair._boundaries(plane)[key]["same_slot_durability_failed"] is True


def test_unconfirmed_exact_boundary_delegates_to_unchanged_slot_fallback(monkeypatch):
    target = _pump_target()
    key = live_poll._poll_target_key(target)
    plane = SimpleNamespace()
    repair._boundaries(plane)[key] = {
        "generation": 4,
        "signature": "lower",
        "slot": 100,
        "confirmed": False,
        "confirmation_failed": False,
        "same_slot_durability_failed": False,
    }
    delegated = []

    monkeypatch.setattr(immediate, "_generation", lambda _self, _target: 4)

    async def no_confirmation(_self, _target, _boundary):
        return False

    async def fallback(_self, _target, cursor_slot):
        delegated.append(cursor_slot)
        return ([{"signature": "slot-fallback", "slot": 101}], True, "fallback", 1.0, {"fallback": True})

    monkeypatch.setattr(repair, "_confirm_boundary", no_confirmation)
    monkeypatch.setattr(repair, "_ORIGINAL_INTERVAL_FETCH", fallback)

    result = asyncio.run(repair._exact_signature_interval_fetch(plane, target, 90))
    assert result[0][0]["signature"] == "slot-fallback"
    assert result[1] is True
    assert delegated == [90]


def test_exact_boundary_incomplete_never_claims_short_page_as_complete(monkeypatch):
    target = _pump_target()
    key = live_poll._poll_target_key(target)
    plane = SimpleNamespace()
    repair._boundaries(plane)[key] = {
        "generation": 8,
        "signature": "confirmed-but-not-returned",
        "slot": 100,
        "confirmed": True,
        "confirmation_failed": False,
        "same_slot_durability_failed": False,
    }
    monkeypatch.setattr(immediate, "_generation", lambda _self, _target: 8)
    monkeypatch.setattr(runtime_arch, "_recovery_upper_boundaries", lambda _self: {})

    class Rpc:
        async def call_with_meta(self, method, params, hedge=False):
            return ([{"signature": "newer", "slot": 101, "err": None}], "publicnode", 4.0)

    monkeypatch.setattr(isolation, "_recovery_rpc", lambda _self: Rpc())
    result = asyncio.run(repair._exact_signature_interval_fetch(plane, target, 90))
    assert result[1] is False
    assert result[4]["exact_durable_lower_reached"] is False
    assert result[4]["hard_page_limit"] == 3
