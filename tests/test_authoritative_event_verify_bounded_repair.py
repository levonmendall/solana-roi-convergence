from __future__ import annotations

from datetime import datetime, timezone

import pytest

from solana_roi import authoritative_event_verify_bounded_repair as repair
from solana_roi import incremental_event_integrity_repair as incremental
from solana_roi.durable_engine import DurablePaperTradingEngine
from solana_roi.observation_store import ObservationEventStore


def _store(tmp_path, count: int = 11) -> ObservationEventStore:
    store = ObservationEventStore(tmp_path / "events.sqlite3")
    now = datetime.now(timezone.utc).isoformat()
    for index in range(count):
        store.append("diagnostic", now, {"index": index})
    return store


def _engine(store: ObservationEventStore) -> DurablePaperTradingEngine:
    engine = DurablePaperTradingEngine.__new__(DurablePaperTradingEngine)
    engine.store = store
    return engine


def _disable_memory_pressure(monkeypatch):
    monkeypatch.setattr(repair.memory, "_guard_raw_cgroup", lambda path: {})
    monkeypatch.setattr(repair.memory, "_trim_process_heap", lambda: True)
    monkeypatch.setattr(
        repair.memory,
        "_cgroup_memory",
        lambda: {"current_bytes": 100, "file_bytes": 50},
    )


def test_full_valid_ledger_verifies_across_multiple_reader_chunks(tmp_path, monkeypatch):
    store = _store(tmp_path, 11)
    _disable_memory_pressure(monkeypatch)
    releases: list[str] = []
    monkeypatch.setattr(repair, "VERIFY_CHUNK_ROWS", 3)
    monkeypatch.setattr(
        repair.memory,
        "_release_sqlite_file_cache",
        lambda path: releases.append(str(path)) or True,
    )

    assert repair._bounded_authoritative_verify(_engine(store)) == (True, 11, None)
    assert len(releases) >= 5
    store.close()


@pytest.mark.parametrize("event_id", [1, 6, 11])
def test_full_verify_detects_old_middle_and_newest_row_tamper(tmp_path, monkeypatch, event_id):
    store = _store(tmp_path, 11)
    _disable_memory_pressure(monkeypatch)
    monkeypatch.setattr(repair.memory, "_release_sqlite_file_cache", lambda path: True)
    with store._lock, store.db:
        store.db.execute(
            "UPDATE events SET payload_json=? WHERE id=?",
            (f'{{"tampered":{event_id}}}', event_id),
        )

    assert repair._bounded_authoritative_verify(_engine(store)) == (False, 0, None)
    store.close()


def test_full_verify_detects_deleted_row(tmp_path, monkeypatch):
    store = _store(tmp_path, 7)
    _disable_memory_pressure(monkeypatch)
    monkeypatch.setattr(repair.memory, "_release_sqlite_file_cache", lambda path: True)
    with store._lock, store.db:
        store.db.execute("DELETE FROM events WHERE id=4")

    assert repair._bounded_authoritative_verify(_engine(store)) == (False, 0, None)
    store.close()


def test_full_verify_detects_previous_hash_rewrite(tmp_path, monkeypatch):
    store = _store(tmp_path, 7)
    _disable_memory_pressure(monkeypatch)
    monkeypatch.setattr(repair.memory, "_release_sqlite_file_cache", lambda path: True)
    with store._lock, store.db:
        store.db.execute("UPDATE events SET previous_hash='tampered' WHERE id=4")

    assert repair._bounded_authoritative_verify(_engine(store)) == (False, 0, None)
    store.close()


def test_valid_incremental_checkpoint_cannot_hide_old_row_tamper(tmp_path, monkeypatch):
    store = _store(tmp_path, 9)
    _disable_memory_pressure(monkeypatch)
    monkeypatch.setattr(repair.memory, "_release_sqlite_file_cache", lambda path: True)

    assert repair._bounded_authoritative_verify(_engine(store)) == (True, 9, None)
    checkpoint = incremental._checkpoint_path(store)
    assert checkpoint.exists()

    with store._lock, store.db:
        store.db.execute("UPDATE events SET observed_at='tampered-old-row' WHERE id=1")

    # The sidecar still describes the previously valid terminal frontier.  Authoritative
    # restart must nevertheless re-hash the retained prefix and reject the old mutation.
    assert repair._bounded_authoritative_verify(_engine(store)) == (False, 0, None)
    store.close()


def test_each_chunk_reader_is_closed_before_cache_release(tmp_path, monkeypatch):
    store = _store(tmp_path, 8)
    _disable_memory_pressure(monkeypatch)
    monkeypatch.setattr(repair, "VERIFY_CHUNK_ROWS", 2)
    original_reader = repair._reader
    active = {"count": 0}
    releases = {"count": 0}

    class ReaderProxy:
        def __init__(self, connection):
            self.connection = connection
            active["count"] += 1

        def execute(self, *args, **kwargs):
            return self.connection.execute(*args, **kwargs)

        def close(self):
            self.connection.close()
            active["count"] -= 1

    monkeypatch.setattr(repair, "_reader", lambda path: ReaderProxy(original_reader(path)))

    def release(path):
        assert active["count"] == 0
        releases["count"] += 1
        return True

    monkeypatch.setattr(repair.memory, "_release_sqlite_file_cache", release)

    assert repair._bounded_authoritative_verify(_engine(store)) == (True, 8, None)
    assert releases["count"] >= 5
    store.close()


def test_verification_fails_closed_if_terminal_frontier_moves(tmp_path, monkeypatch):
    store = _store(tmp_path, 4)
    _disable_memory_pressure(monkeypatch)
    monkeypatch.setattr(repair.memory, "_release_sqlite_file_cache", lambda path: True)
    real_frontier = repair._frontier
    calls = {"count": 0}

    def moving_frontier(subject):
        calls["count"] += 1
        event_id, lineage = real_frontier(subject)
        if calls["count"] == 1:
            return event_id, lineage
        return event_id + 1, "changed"

    monkeypatch.setattr(repair, "_frontier", moving_frontier)

    assert repair._bounded_authoritative_verify(_engine(store)) == (False, 0, None)
    assert repair.status()["last"]["failure_reason"] == "frontier_changed_during_verification"
    store.close()


def test_durable_engine_constructor_invokes_snapshot_verifier_once(tmp_path, monkeypatch):
    store = ObservationEventStore(tmp_path / "empty.sqlite3")
    calls = {"count": 0}

    def verify_once(self):
        calls["count"] += 1
        return True, 0, None

    monkeypatch.setattr(DurablePaperTradingEngine, "_verify_engine_snapshot", verify_once)
    DurablePaperTradingEngine(store=store)

    assert calls["count"] == 1
    store.close()


def test_configuration_supersedes_tail_only_startup_authority(monkeypatch):
    monkeypatch.setattr(repair, "_INSTALLED", False)
    previous_memory_verify = repair.memory._bounded_verify_engine_snapshot
    previous_incremental_full = incremental._ORIGINAL_BOUNDED_VERIFY
    try:
        repair.configure_authoritative_event_verify_bounded_repair()
        assert repair.memory._bounded_verify_engine_snapshot is repair._bounded_authoritative_verify
        assert incremental._ORIGINAL_BOUNDED_VERIFY is repair._bounded_authoritative_verify
        assert getattr(repair._bounded_authoritative_verify, "_roi_durable_bootstrap_memory_bounded") is True
        assert repair.status()["authoritative_startup_verification"] == "complete_retained_history"
        assert repair.status()["incremental_checkpoint_authoritative"] is False
    finally:
        repair.memory._bounded_verify_engine_snapshot = previous_memory_verify
        incremental._ORIGINAL_BOUNDED_VERIFY = previous_incremental_full
        repair._INSTALLED = False
