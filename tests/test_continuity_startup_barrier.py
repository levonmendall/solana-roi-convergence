from __future__ import annotations

import asyncio

from solana_roi import target_quorum
from solana_roi.continuity_startup_barrier import (
    POLL_PROVIDER_NAME,
    _handshake_status_code,
    _status_with_startup_barrier,
)
from solana_roi.direct_solana import DirectSolanaJournal, WatchTarget
from solana_roi.observation_store import ObservationEventStore
from solana_roi.solana_rpc import RpcEndpoint


def _plane(tmp_path):
    store = ObservationEventStore(tmp_path / "startup-barrier.sqlite3")

    class Plane:
        watch_targets = (
            WatchTarget("scout", "scout-a", None),
            WatchTarget("program", "program-a", "RAYDIUM"),
        )
        _recovering = False

    plane = Plane()
    plane.store = store
    plane.journal = DirectSolanaJournal(store)
    # The production barrier is intentionally scoped to a real exact-release
    # runtime. Low-level quorum utilities without an epoch retain their original
    # semantics and existing regression contract.
    plane._roi_continuity_epoch = {"release_id": "test-release"}
    return plane


def test_polling_startup_cannot_arm_or_create_gap_before_real_websocket_coverage(tmp_path):
    async def scenario() -> None:
        plane = _plane(tmp_path)
        poll = RpcEndpoint(POLL_PROVIDER_NAME, "https://poll.invalid", "wss://poll.invalid")
        websocket = RpcEndpoint("provider-a", "https://a.invalid", "wss://a.invalid")
        scout, program = plane.watch_targets

        # Polling may baseline first. It covers every target as a transport but is
        # not allowed to arm prospective continuity without real WebSocket coverage.
        await target_quorum._quorum_set_target_state(plane, poll, scout, connected=True)
        await target_quorum._quorum_set_target_state(plane, poll, program, connected=True)
        assert getattr(plane, "_roi_continuity_startup_barrier_armed", False) is False
        assert getattr(plane, "_roi_global_coverage_observed", False) is False
        assert plane.journal.outage_started_at() is None
        assert plane.journal.status()["unresolved_gap"] is False

        await target_quorum._quorum_set_target_state(plane, websocket, scout, connected=True)
        assert getattr(plane, "_roi_continuity_startup_barrier_armed", False) is False
        await target_quorum._quorum_set_target_state(plane, websocket, program, connected=True)
        assert getattr(plane, "_roi_continuity_startup_barrier_armed", False) is True
        assert getattr(plane, "_roi_global_coverage_observed", False) is True

        # After arming, polling can bridge a WebSocket loss without creating a
        # target gap. Losing the polling bridge too is a real prospective outage.
        await target_quorum._quorum_set_target_state(
            plane, websocket, scout, connected=False, error_type="ConnectionClosedError"
        )
        assert plane.journal.outage_started_at() is None
        await target_quorum._quorum_set_target_state(
            plane, poll, scout, connected=False, error_type="LivePollUnavailable"
        )
        assert plane.journal.outage_started_at() is not None

    asyncio.run(scenario())


def test_provider_independence_telemetry_excludes_synthetic_poll_identity(tmp_path):
    async def scenario() -> None:
        plane = _plane(tmp_path)
        poll = RpcEndpoint(POLL_PROVIDER_NAME, "https://poll.invalid", "wss://poll.invalid")
        websocket = RpcEndpoint("provider-a", "https://a.invalid", "wss://a.invalid")
        for target in plane.watch_targets:
            await target_quorum._quorum_set_target_state(plane, poll, target, connected=True)
            await target_quorum._quorum_set_target_state(plane, websocket, target, connected=True)

        wrapped = _status_with_startup_barrier(
            lambda _self: {
                "unresolved_gap": False,
                "provider_runtime_policy": {},
                "full_scope_target_quorum": {},
                "continuity_epoch": {},
            }
        )
        status = wrapped(plane)
        quorum = status["full_scope_target_quorum"]
        assert status["continuity_ok"] is True
        assert quorum["minimum_live_transport_count_per_target"] == 2
        assert quorum["minimum_live_provider_count_per_target"] == 1
        assert quorum["minimum_live_websocket_provider_count_per_target"] == 1
        assert quorum["synthetic_poll_counted_as_independent_provider"] is False
        assert status["continuity_startup_barrier"]["armed"] is True

    asyncio.run(scenario())


def test_websocket_handshake_diagnostics_keep_only_sanitized_http_status_code():
    class Response:
        status_code = 429

    class InvalidStatus(Exception):
        def __init__(self):
            super().__init__("wss://provider.invalid/v2/super-secret-key")
            self.response = Response()

    exc = InvalidStatus()
    code = _handshake_status_code(exc)
    assert code == 429
    assert "super-secret-key" not in repr(code)
