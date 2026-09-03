from __future__ import annotations

import asyncio
import json
import time
from types import SimpleNamespace

from solana_roi import continuity_early_loss_detection_repair as repair
from solana_roi import continuity_recovery_isolation_repair as isolation
from solana_roi import continuity_target_frontier_repair as frontier
from solana_roi import live_poll_redundancy as live_poll
from solana_roi import poll_recoverability_lease as lease
from solana_roi import rpc_workload_governor as governor
from solana_roi import target_quorum
from solana_roi.direct_solana import WatchTarget
from solana_roi.solana_rpc import RpcEndpoint


def _target() -> WatchTarget:
    return WatchTarget(kind="program", address="pump-amm-program", source_hint="PUMP_AMM")


class ConfirmingPool:
    def __init__(self, statuses):
        self.statuses = statuses
        self.calls = []

    async def call_with_meta(self, method, params, *, hedge=False):
        self.calls.append((method, params, hedge))
        assert method == "getSignatureStatuses"
        assert hedge is True
        return {"value": self.statuses}, "publicnode", 11.0


def test_gap_snapshot_uses_latest_confirmed_target_frontier_and_replays_same_slot(monkeypatch):
    target = _target()
    key = live_poll._poll_target_key(target)
    now = time.monotonic()
    plane = SimpleNamespace(
        _roi_real_ws_gap_generations={key: 3},
        _roi_continuity_gap_clocks={
            key: {"generation": 3, "started_monotonic": now, "started_at": "2026-09-03T00:00:00+00:00"}
        },
    )
    candidates = [
        {"signature": "sig-510", "slot": 510, "provider": "publicnode", "observed_monotonic": now - 0.1},
        {"signature": "sig-525", "slot": 525, "provider": "solana-mainnet", "observed_monotonic": now},
    ]
    pool = ConfirmingPool(
        [
            {"slot": 525, "confirmationStatus": "confirmed"},
            {"slot": 510, "confirmationStatus": "finalized"},
        ]
    )
    monkeypatch.setattr(isolation, "_recovery_rpc", lambda _plane: pool)

    cursor, anchor = asyncio.run(
        repair._confirmed_snapshot_cursor(plane, target, 100, 3, candidates)
    )

    assert cursor == 524
    assert anchor is not None
    assert anchor["confirmed_frontier_slot"] == 525
    assert anchor["captured_at_zero_websocket_coverage"] is True
    assert anchor["same_slot_replay_required"] is True
    assert pool.calls[0][1][0] == ["sig-525", "sig-510"]
    assert lease.POLL_RECOVERABILITY_LEASE_SECONDS == 12.0
    assert live_poll.POLL_CURSOR_MAX_PAGES == 3
    assert live_poll.POLL_LIMIT == 1000


def test_gap_kick_freezes_frontier_before_reconnect_and_restores_critical_priority(monkeypatch):
    target = _target()
    key = live_poll._poll_target_key(target)
    plane = SimpleNamespace(
        _roi_real_ws_gap_generations={key: 1},
        _roi_continuity_gap_clocks={
            key: {"generation": 1, "started_monotonic": time.monotonic(), "started_at": "2026-09-03T00:00:00+00:00"}
        },
    )
    live_poll._poll_state(plane)[key] = {"baseline_established": True, "cursor_slot": 400}
    frontier._observe_target_frontier(plane, "publicnode", target, "pre-gap", 450)

    observed = {}

    async def fake_recover(_self, _target, cursor, generation, candidates):
        observed["cursor"] = cursor
        observed["generation"] = generation
        observed["signatures"] = [row["signature"] for row in candidates]
        observed["task_name"] = asyncio.current_task().get_name()
        observed["workload"] = governor._effective_workload()
        return [], True, "publicnode", 1.0

    monkeypatch.setattr(repair, "_recover_from_gap_snapshot", fake_recover)

    async def scenario():
        repair._kick_recovery_with_gap_snapshot(plane, target, 1)
        task_row = __import__(
            "solana_roi.continuity_immediate_recovery_repair",
            fromlist=["_recovery_tasks"],
        )._recovery_tasks(plane)[key]
        # Reconnect traffic can rotate the live history after the gap transition;
        # the task must still receive only the synchronously captured snapshot.
        frontier._observe_target_frontier(plane, "solana-mainnet", target, "post-gap", 900)
        return await task_row["task"]

    asyncio.run(scenario())

    assert observed["cursor"] == 400
    assert observed["generation"] == 1
    assert observed["signatures"] == ["pre-gap"]
    assert observed["task_name"].startswith("isolated-immediate-gap-recovery:")
    assert observed["workload"] == governor.WORKLOAD_CRITICAL
    assert lease.POLL_RECOVERABILITY_LEASE_SECONDS == 12.0
    assert live_poll.POLL_CURSOR_MAX_PAGES * live_poll.POLL_LIMIT == 3000


def test_failure_attribution_updates_failure_not_stale_success():
    plane = SimpleNamespace(
        _roi_gap_recovery_attribution={
            "last_success": {"target": "old-success"},
            "last_failure": {"target": "current-failure"},
            "failure_counts": {},
            "failure_history": [],
        }
    )
    anchor = {
        "source": "confirmed-target-websocket-frontier-at-gap",
        "confirmed_frontier_slot": 777,
        "snapshot_candidate_count": 9,
    }

    repair._annotate_attribution_row(
        plane,
        succeeded=False,
        anchor=anchor,
        routine_cursor_slot=700,
    )

    state = isolation._attribution_state(plane)
    assert state["last_failure"]["routine_cursor_slot"] == 700
    assert state["last_failure"]["confirmed_frontier_slot"] == 777
    assert state["last_failure"]["frontier_snapshot_at_gap_onset"] is True
    assert "routine_cursor_slot" not in state["last_success"]


class FakeWebSocket:
    def __init__(self, stop: asyncio.Event):
        self.stop = stop
        self.recv_count = 0
        self.sent = []

    async def send(self, payload):
        self.sent.append(json.loads(payload))

    async def recv(self):
        self.recv_count += 1
        if self.recv_count == 1:
            return json.dumps({"jsonrpc": "2.0", "id": 1, "result": 42})
        self.stop.set()
        return json.dumps(
            {
                "jsonrpc": "2.0",
                "method": "logsNotification",
                "params": {
                    "subscription": 42,
                    "result": {
                        "context": {"slot": 123},
                        "value": {"signature": "sig-123", "err": None, "logs": []},
                    },
                },
            }
        )

    async def ping(self):
        async def done():
            return None

        return await done()


class FakeConnect:
    def __init__(self, ws: FakeWebSocket):
        self.ws = ws
        self.kwargs = None

    def __call__(self, _url, **kwargs):
        self.kwargs = kwargs
        ws = self.ws

        class Context:
            async def __aenter__(self):
                return ws

            async def __aexit__(self, exc_type, exc, tb):
                return False

        return Context()


def test_target_transport_keepalive_detects_loss_well_inside_fixed_lease(monkeypatch):
    stop = asyncio.Event()
    ws = FakeWebSocket(stop)
    connect = FakeConnect(ws)
    endpoint = RpcEndpoint("publicnode", "https://publicnode.invalid", "wss://publicnode.invalid")
    target = _target()
    states = []
    handled = []

    async def set_state(_self, _endpoint, _target, *, connected, **kwargs):
        states.append(bool(connected))

    async def handle(_provider, _targets, message):
        handled.append(message)

    plane = SimpleNamespace(_handle_notification=handle)
    monkeypatch.setattr(repair.direct_solana_module.websockets, "connect", connect)
    monkeypatch.setattr(target_quorum, "_quorum_set_target_state", set_state)

    asyncio.run(repair._early_loss_quorum_single_target_stream(plane, endpoint, target, stop))

    assert connect.kwargs["ping_interval"] == repair.TARGET_WS_PING_INTERVAL_SECONDS
    assert connect.kwargs["ping_timeout"] == repair.TARGET_WS_PING_TIMEOUT_SECONDS
    assert connect.kwargs["max_queue"] == repair.fanout.TARGET_WS_MAX_QUEUE
    assert connect.kwargs["max_size"] == repair.fanout.TARGET_WS_MAX_SIZE_BYTES
    assert repair.TARGET_WS_PING_INTERVAL_SECONDS + repair.TARGET_WS_PING_TIMEOUT_SECONDS < lease.POLL_RECOVERABILITY_LEASE_SECONDS
    assert states[0] is True
    assert handled
    assert lease.POLL_RECOVERABILITY_LEASE_SECONDS == 12.0
    assert live_poll.POLL_CURSOR_MAX_PAGES == 3
    assert live_poll.POLL_LIMIT == 1000
