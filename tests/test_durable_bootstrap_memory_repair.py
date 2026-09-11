from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path

import pytest
from fastapi import HTTPException

from solana_roi import durable_bootstrap_memory_repair as repair
from solana_roi.durable_engine import DurablePaperTradingEngine


def _event_hash(previous: str | None, event_type: str, observed_at: str, raw: str) -> str:
    return hashlib.sha256(f"{previous or ''}|{event_type}|{observed_at}|{raw}".encode()).hexdigest()


def _insert_event(db: sqlite3.Connection, event_id: int, event_type: str, previous: str | None) -> str:
    observed_at = f"2026-09-11T00:00:{event_id:02d}+00:00"
    raw = json.dumps({"id": event_id}, separators=(",", ":"), sort_keys=True)
    lineage = _event_hash(previous, event_type, observed_at, raw)
    db.execute(
        "INSERT INTO events(id, event_type, observed_at, payload_json, previous_hash, lineage_hash) VALUES (?, ?, ?, ?, ?, ?)",
        (event_id, event_type, observed_at, raw, previous, lineage),
    )
    return lineage


class _Store:
    def __init__(self, path: Path) -> None:
        import threading

        self.path = path
        self.db = sqlite3.connect(path, check_same_thread=False)
        self._lock = threading.RLock()
        self._verify_lock = threading.RLock()
        self.db.execute(
            "CREATE TABLE events ("
            "id INTEGER PRIMARY KEY, event_type TEXT NOT NULL, observed_at TEXT NOT NULL, "
            "payload_json TEXT NOT NULL, previous_hash TEXT, lineage_hash TEXT NOT NULL)"
        )
        previous = None
        for event_id, event_type in ((1, "first_touch"), (2, "other"), (3, "trade_outcome")):
            previous = _insert_event(self.db, event_id, event_type, previous)
        self.db.commit()

    def close(self) -> None:
        self.db.close()


class _Engine:
    def __init__(self, store: _Store) -> None:
        self.store = store


def _state(*, current: int, maximum: int, dirty: int = 0, writeback: int = 0):
    return {
        "current_bytes": current,
        "max_bytes": maximum,
        "headroom_bytes": max(0, maximum - current),
        "fraction": current / maximum,
        "anon_bytes": 100,
        "file_bytes": max(0, current - 100),
        "file_dirty_bytes": dirty,
        "file_writeback_bytes": writeback,
        "slab_reclaimable_bytes": 0,
        "oom_events": 0,
        "oom_kill_events": 0,
    }


def test_bounded_verify_preserves_full_chain_and_latest_engine_event(tmp_path, monkeypatch):
    store = _Store(tmp_path / "state.sqlite3")
    engine = _Engine(store)
    monkeypatch.setattr(repair, "_guard_raw_cgroup", lambda path: _state(current=1, maximum=1000))
    monkeypatch.setattr(repair, "_release_sqlite_file_cache", lambda path: True)
    monkeypatch.setattr(repair, "_trim_process_heap", lambda: True)

    assert repair._bounded_verify_engine_snapshot(engine) == (True, 3, 3)
    store.close()


def test_bounded_verify_fails_closed_on_hash_chain_break(tmp_path, monkeypatch):
    store = _Store(tmp_path / "state.sqlite3")
    with store.db:
        store.db.execute("UPDATE events SET previous_hash='wrong' WHERE id=2")
    engine = _Engine(store)
    monkeypatch.setattr(repair, "_guard_raw_cgroup", lambda path: _state(current=1, maximum=1000))
    monkeypatch.setattr(repair, "_release_sqlite_file_cache", lambda path: True)
    monkeypatch.setattr(repair, "_trim_process_heap", lambda: True)

    assert repair._bounded_verify_engine_snapshot(engine) == (False, 0, None)
    store.close()


def test_guarded_pinned_reader_fails_closed_before_open_on_critical_raw_memory(tmp_path, monkeypatch):
    path = tmp_path / "state.sqlite3"
    sqlite3.connect(path).close()

    class Store:
        pass

    store = Store()
    store.path = path

    def fail_guard(source_path):
        raise MemoryError("critical")

    monkeypatch.setattr(repair, "_guard_raw_cgroup", fail_guard)
    with pytest.raises(HTTPException) as exc:
        repair._guarded_pinned_reader(store)
    assert exc.value.status_code == 503
    assert "raw cgroup memory pressure" in str(exc.value.detail)


def test_release_sqlite_file_cache_targets_database_and_sidecars(tmp_path, monkeypatch):
    path = tmp_path / "state.sqlite3"
    paths = [path, Path(str(path) + "-wal"), Path(str(path) + "-shm")]
    for candidate in paths:
        candidate.write_bytes(b"x")
    observed: list[Path] = []
    monkeypatch.setattr(repair, "_advise_dontneed", lambda candidate: observed.append(candidate) or True)

    assert repair._release_sqlite_file_cache(path) is True
    assert observed == paths


def test_cgroup_reclaim_writes_swappiness_zero_and_bounded_budget(tmp_path):
    root = tmp_path / "cgroup"
    root.mkdir()
    reclaim = root / "memory.reclaim"
    reclaim.write_text("", encoding="ascii")
    state = _state(current=1_900_000_000, maximum=2_000_000_000)

    assert repair._request_cgroup_file_reclaim(state, root) is True
    raw = reclaim.read_text(encoding="ascii")
    amount, policy = raw.split()
    assert int(amount) <= repair.MAX_CGROUP_RECLAIM_BYTES
    assert int(amount) >= repair.MIN_CGROUP_RECLAIM_BYTES
    assert policy == "swappiness=0"


def test_cgroup_reclaim_unavailable_remains_best_effort(tmp_path):
    root = tmp_path / "missing"
    root.mkdir()
    state = _state(current=1_900_000_000, maximum=2_000_000_000)
    assert repair._request_cgroup_file_reclaim(state, root) is False


def test_guard_releases_cache_before_checkpoint_and_recovers_without_checkpoint(tmp_path, monkeypatch):
    path = tmp_path / "state.sqlite3"
    path.write_bytes(b"x")
    states = iter([
        _state(current=1_900, maximum=2_000),
        _state(current=1_300, maximum=2_000),
    ])
    calls: list[str] = []
    monkeypatch.setattr(repair, "_cgroup_memory", lambda: next(states))
    monkeypatch.setattr(repair, "_release_sqlite_file_cache", lambda p: calls.append("release") or True)
    monkeypatch.setattr(repair, "_trim_process_heap", lambda: calls.append("trim") or True)
    monkeypatch.setattr(repair, "_request_cgroup_file_reclaim", lambda state: calls.append("reclaim") or False)
    monkeypatch.setattr(repair, "_passive_wal_checkpoint", lambda p: calls.append("checkpoint") or {})
    monkeypatch.setattr(repair, "RECLAIM_SETTLE_SECONDS", 0)

    result = repair._guard_raw_cgroup(path)

    assert result["current_bytes"] == 1_300
    assert calls == ["release", "trim", "reclaim"]


def test_guard_critical_pressure_fails_closed(tmp_path, monkeypatch):
    path = tmp_path / "state.sqlite3"
    path.write_bytes(b"x")
    state = _state(current=1_950, maximum=2_000)
    monkeypatch.setattr(repair, "_cgroup_memory", lambda: dict(state))
    monkeypatch.setattr(repair, "_release_sqlite_file_cache", lambda p: True)
    monkeypatch.setattr(repair, "_trim_process_heap", lambda: True)
    monkeypatch.setattr(repair, "_request_cgroup_file_reclaim", lambda state: False)
    monkeypatch.setattr(repair, "_passive_wal_checkpoint", lambda p: {})
    monkeypatch.setattr(repair, "RECLAIM_SETTLE_SECONDS", 0)

    with pytest.raises(MemoryError, match="raw cgroup memory pressure"):
        repair._guard_raw_cgroup(path)


def test_guard_flushes_dirty_sqlite_pages_before_retry(tmp_path, monkeypatch):
    path = tmp_path / "state.sqlite3"
    path.write_bytes(b"x")
    states = iter([
        _state(current=1_900, maximum=2_000, dirty=repair.DIRTY_WRITEBACK_TRIGGER_BYTES),
        _state(current=1_900, maximum=2_000, dirty=repair.DIRTY_WRITEBACK_TRIGGER_BYTES),
        _state(current=1_250, maximum=2_000, dirty=0),
    ])
    calls: list[str] = []
    monkeypatch.setattr(repair, "_cgroup_memory", lambda: next(states))
    monkeypatch.setattr(repair, "_release_sqlite_file_cache", lambda p: calls.append("release") or True)
    monkeypatch.setattr(repair, "_trim_process_heap", lambda: calls.append("trim") or True)
    monkeypatch.setattr(repair, "_request_cgroup_file_reclaim", lambda state: calls.append("reclaim") or False)
    monkeypatch.setattr(repair, "_sync_sqlite_dirty_pages", lambda p: calls.append("sync") or True)
    monkeypatch.setattr(repair, "_passive_wal_checkpoint", lambda p: calls.append("checkpoint") or {})
    monkeypatch.setattr(repair, "RECLAIM_SETTLE_SECONDS", 0)
    monkeypatch.setattr(repair, "WRITEBACK_SETTLE_SECONDS", 0)

    result = repair._guard_raw_cgroup(path)

    assert result["current_bytes"] == 1_250
    assert calls == ["release", "trim", "reclaim", "sync", "release", "reclaim"]


def test_guard_checkpoint_is_gated_by_actual_wal_size(tmp_path, monkeypatch):
    path = tmp_path / "state.sqlite3"
    path.write_bytes(b"x")
    state = _state(current=1_900, maximum=2_000, dirty=0)
    readings = iter([
        dict(state),
        dict(state),
        _state(current=1_200, maximum=2_000),
    ])
    calls: list[str] = []
    monkeypatch.setattr(repair, "_cgroup_memory", lambda: next(readings))
    monkeypatch.setattr(repair, "_release_sqlite_file_cache", lambda p: calls.append("release") or True)
    monkeypatch.setattr(repair, "_trim_process_heap", lambda: calls.append("trim") or True)
    monkeypatch.setattr(repair, "_request_cgroup_file_reclaim", lambda state: calls.append("reclaim") or False)
    monkeypatch.setattr(repair, "_wal_size_bytes", lambda p: repair.WAL_CHECKPOINT_TRIGGER_BYTES)
    monkeypatch.setattr(
        repair,
        "_passive_wal_checkpoint",
        lambda p: calls.append("checkpoint") or {
            "attempted": True,
            "busy": 0,
            "log_frames": 1,
            "checkpointed_frames": 0,
            "error": None,
            "wal_bytes_after": repair.WAL_CHECKPOINT_TRIGGER_BYTES,
        },
    )
    monkeypatch.setattr(repair, "RECLAIM_SETTLE_SECONDS", 0)

    result = repair._guard_raw_cgroup(path)

    assert result["current_bytes"] == 1_200
    assert calls.count("checkpoint") == 1


def test_dirty_writeback_alone_cannot_trigger_checkpoint(tmp_path, monkeypatch):
    path = tmp_path / "state.sqlite3"
    path.write_bytes(b"x")
    pressured = _state(
        current=1_900,
        maximum=2_000,
        dirty=repair.DIRTY_WRITEBACK_TRIGGER_BYTES + 1,
    )
    states = iter([
        dict(pressured),
        dict(pressured),
        _state(current=1_200, maximum=2_000),
    ])
    calls: list[str] = []
    monkeypatch.setattr(repair, "_cgroup_memory", lambda: next(states))
    monkeypatch.setattr(repair, "_release_sqlite_file_cache", lambda p: calls.append("release") or True)
    monkeypatch.setattr(repair, "_trim_process_heap", lambda: calls.append("trim") or True)
    monkeypatch.setattr(repair, "_request_cgroup_file_reclaim", lambda state: calls.append("reclaim") or False)
    monkeypatch.setattr(repair, "_sync_sqlite_dirty_pages", lambda p: calls.append("sync") or True)
    monkeypatch.setattr(repair, "_wal_size_bytes", lambda p: 4 * 1024 * 1024)
    monkeypatch.setattr(repair, "_passive_wal_checkpoint", lambda p: calls.append("checkpoint") or {})
    monkeypatch.setattr(repair, "RECLAIM_SETTLE_SECONDS", 0)
    monkeypatch.setattr(repair, "WRITEBACK_SETTLE_SECONDS", 0)

    result = repair._guard_raw_cgroup(path)

    assert result["current_bytes"] == 1_200
    assert "sync" in calls
    assert "checkpoint" not in calls


def test_page_finalizer_never_checkpoints(tmp_path, monkeypatch):
    database = tmp_path / "state.sqlite3"
    database.write_bytes(b"db")
    calls: list[str] = []
    monkeypatch.setattr(repair, "_release_sqlite_file_cache", lambda p: calls.append("release") or True)
    monkeypatch.setattr(repair, "_sync_sqlite_dirty_pages", lambda p: calls.append("sync") or True)
    monkeypatch.setattr(repair, "_passive_wal_checkpoint", lambda p: calls.append("checkpoint") or {})
    monkeypatch.setattr(
        repair,
        "_cgroup_memory",
        lambda: {
            "fraction": 0.1,
            "headroom_bytes": 10**9,
            "file_dirty_bytes": 0,
            "file_writeback_bytes": 0,
        },
    )

    assert repair._drop_file_cache_with_sidecars(database) is True
    assert calls == ["release"]


def test_page_finalizer_flushes_material_dirty_pages_without_checkpoint(tmp_path, monkeypatch):
    database = tmp_path / "state.sqlite3"
    database.write_bytes(b"db")
    states = iter([
        {
            "fraction": 0.9,
            "headroom_bytes": 1,
            "file_dirty_bytes": repair.DIRTY_WRITEBACK_TRIGGER_BYTES,
            "file_writeback_bytes": 0,
        },
        {
            "fraction": 0.1,
            "headroom_bytes": 10**9,
            "file_dirty_bytes": 0,
            "file_writeback_bytes": 0,
        },
    ])
    calls: list[str] = []
    monkeypatch.setattr(repair, "_release_sqlite_file_cache", lambda p: calls.append("release") or True)
    monkeypatch.setattr(repair, "_sync_sqlite_dirty_pages", lambda p: calls.append("sync") or True)
    monkeypatch.setattr(repair, "_passive_wal_checkpoint", lambda p: calls.append("checkpoint") or {})
    monkeypatch.setattr(repair, "_cgroup_memory", lambda: next(states))
    monkeypatch.setattr(repair, "WRITEBACK_SETTLE_SECONDS", 0)

    assert repair._drop_file_cache_with_sidecars(database) is True
    assert calls == ["release", "sync", "release"]


def test_critical_fail_closed_boundary_remains_94_percent():
    assert repair.RAW_CRITICAL_FRACTION == 0.94
    assert repair._critical(
        {
            "fraction": 0.94,
            "headroom_bytes": repair.RAW_CRITICAL_RESERVE_BYTES + 1,
        }
    ) is True


def test_install_patches_only_read_paths_and_preserves_authority_contract():
    repair.install_durable_bootstrap_memory_repair()

    from solana_roi import certification_logical_bootstrap as logical
    from solana_roi import certification_service_split as split

    assert getattr(DurablePaperTradingEngine._verify_engine_snapshot, "_roi_durable_bootstrap_memory_bounded", False)
    assert getattr(logical._pinned_reader, "_roi_durable_bootstrap_memory_bounded", False)
    assert getattr(split._drop_file_cache, "_roi_sqlite_sidecar_cache_release", False)
    status = repair.status()
    assert status["repair_version"] == "durable-bootstrap-cgroup-memory-v8-bootstrap-lease-wal-ownership"
    assert status["raw_critical_fraction"] == 0.94
    assert status["full_hash_chain_verification_preserved"] is True
    assert status["logical_bootstrap_keyset_semantics_preserved"] is True
    assert status["cgroup_file_reclaim_best_effort"] is True
    assert status["cgroup_reclaim_swappiness_zero"] is True
    assert status["heap_trim_under_pressure"] is True
    assert status["targeted_sqlite_dirty_writeback"] is True
    assert status["clean_cache_eviction_precedes_checkpoint"] is True
    assert status["dirty_writeback_alone_triggers_checkpoint"] is False
    assert status["passive_wal_checkpoint_gated"] is True
    assert status["bootstrap_active_lease_owns_wal_checkpoint"] is True
    assert status["bootstrap_guard_passive_checkpoint_enabled"] is False
    assert status["wal_checkpoint_max_attempts_per_guard"] == 1
    assert status["page_finalizer_checkpoint_enabled"] is False
    assert status["page_finalizer_clean_cache_release"] is True
    assert status["writeback_changes_logical_state"] is False
    assert status["canonical_evidence_reset"] is False
    assert status["strategy_thresholds_changed"] is False
    assert status["certification_thresholds_changed"] is False
    assert status["continuity_semantics_changed"] is False
    assert status["paper_only"] is True
    assert status["live_money_authority"] is False
    assert status["signing_available"] is False
    assert status["transaction_submission_available"] is False
