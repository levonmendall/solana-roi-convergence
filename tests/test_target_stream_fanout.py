from __future__ import annotations

import asyncio

from solana_roi.direct_solana import DirectSolanaIngestionPlane, WatchTarget
from solana_roi.observation_store import ObservationEventStore
from solana_roi.solana_rpc import RpcEndpoint
from solana_roi.target_stream_fanout import (
    TARGET_START_STAGGER_SECONDS,
    TARGET_WS_MAX_QUEUE,
    TARGET_WS_MAX_SIZE_BYTES,
    _begin_exact_release_continuity_epoch,
    _provider_event,
    _set_target_state,
)


def test_run_uses_per_target_fanout_and_keeps_bounded_memory():
    assert bool(getattr(DirectSolanaIngestionPlane.run, "_roi_target_fanout", False))
    assert bool(getattr(DirectSolanaIngestionPlane.run, "_roi_worker_partitioned", False))
    assert bool(getattr(DirectSolanaIngestionPlane.status, "_roi_target_fanout", False))
    assert bool(getattr(DirectSolanaIngestionPlane.status, "_roi_target_quorum", False))
    assert TARGET_WS_MAX_QUEUE == 8
    assert TARGET_WS_MAX_SIZE_BYTES == 1024 * 1024
    assert TARGET_START_STAGGER_SECONDS == 0.10


def test_provider_telemetry_is_strict_but_global_continuity_uses_target_coverage():
    async def scenario() -> None:
        provider_states: list[bool] = []
        outages: list[bool] = []

        class Journal:
            def set_provider(self, _provider, *, connected, error_type=None):
                provider_states.append(bool(connected))

            def mark_outage(self, _started_at):
                outages.append(True)

            def outage_started_at(self):
                return None

            def close_outage(self, *, complete, error=None):
                return None

        class Plane:
            watch_targets = (
                WatchTarget("scout", "scout-a", None),
                WatchTarget("program", "program-a", "RAYDIUM"),
            )
            journal = Journal()

        plane = Plane()
        endpoint = RpcEndpoint(name="provider-a", http_url="https://example.invalid", ws_url="wss://example.invalid")
        scout, program = plane.watch_targets

        await _set_target_state(plane, endpoint, scout, connected=True)
        assert _provider_event(plane, endpoint.name).is_set() is False
        assert getattr(plane, "_roi_full_scope_target_coverage_ok") is False

        await _set_target_state(plane, endpoint, program, connected=True)
        assert _provider_event(plane, endpoint.name).is_set() is True
        assert getattr(plane, "_roi_full_scope_target_coverage_ok") is True
        assert provider_states[-1] is True

        await _set_target_state(plane, endpoint, scout, connected=False, error_type="ConnectionClosedError")
        assert _provider_event(plane, endpoint.name).is_set() is False
        assert getattr(plane, "_roi_full_scope_target_coverage_ok") is False
        assert provider_states[-1] is False
        assert outages == [True]

    asyncio.run(scenario())


def test_new_exact_release_records_old_gap_but_same_release_restart_cannot_clear_gap(tmp_path, monkeypatch):
    store = ObservationEventStore(tmp_path / "continuity.sqlite3")
    now = "2026-09-02T16:00:00+00:00"
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE direct_solana_global_state ("
            "id INTEGER PRIMARY KEY CHECK(id=1), outage_started_at TEXT, unresolved_gap INTEGER NOT NULL DEFAULT 0, "
            "last_backfill_complete_at TEXT, last_backfill_error TEXT)"
        )
        store.db.execute(
            "INSERT INTO direct_solana_global_state(id, outage_started_at, unresolved_gap, last_backfill_error) "
            "VALUES (1, ?, 1, ?)",
            (now, "gap backfill exceeded bounded pagination before reaching outage boundary"),
        )
        store.db.execute(
            "CREATE TABLE direct_solana_hydration_queue ("
            "signature TEXT PRIMARY KEY, reason TEXT NOT NULL, status TEXT NOT NULL, last_error TEXT, updated_at TEXT NOT NULL)"
        )
        store.db.execute(
            "INSERT INTO direct_solana_hydration_queue(signature, reason, status, updated_at) "
            "VALUES ('old-gap', 'gap_backfill', 'pending', ?)",
            (now,),
        )

    class Plane:
        pass

    plane = Plane()
    plane.store = store
    monkeypatch.setenv("RENDER_GIT_COMMIT", "release-a")
    _begin_exact_release_continuity_epoch(plane)

    with store._lock:
        global_row = store.db.execute(
            "SELECT outage_started_at, unresolved_gap, last_backfill_complete_at, last_backfill_error "
            "FROM direct_solana_global_state WHERE id=1"
        ).fetchone()
        epoch = store.db.execute(
            "SELECT release_id, prior_gap_unrecovered, prior_outage_started_at, prior_gap_error "
            "FROM direct_solana_continuity_epoch WHERE id=1"
        ).fetchone()
        queue = store.db.execute(
            "SELECT status, last_error FROM direct_solana_hydration_queue WHERE signature='old-gap'"
        ).fetchone()

    assert int(global_row["unresolved_gap"]) == 0
    assert global_row["outage_started_at"] is None
    assert global_row["last_backfill_complete_at"] is None
    assert global_row["last_backfill_error"] is None
    assert epoch["release_id"] == "release-a"
    assert int(epoch["prior_gap_unrecovered"]) == 1
    assert epoch["prior_outage_started_at"] == now
    assert "bounded pagination" in str(epoch["prior_gap_error"])
    assert queue["status"] == "failed"
    assert "new exact-release" in str(queue["last_error"])

    # A restart of the same release must preserve any new gap fail-closed.
    with store._lock, store.db:
        store.db.execute(
            "UPDATE direct_solana_global_state SET outage_started_at=?, unresolved_gap=1, last_backfill_error='new gap' WHERE id=1",
            (now,),
        )
    _begin_exact_release_continuity_epoch(plane)
    with store._lock:
        same_release = store.db.execute(
            "SELECT outage_started_at, unresolved_gap, last_backfill_error FROM direct_solana_global_state WHERE id=1"
        ).fetchone()
    assert int(same_release["unresolved_gap"]) == 1
    assert same_release["outage_started_at"] == now
    assert same_release["last_backfill_error"] == "new gap"
