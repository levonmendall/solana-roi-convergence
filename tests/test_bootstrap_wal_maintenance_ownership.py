from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from solana_roi import durable_bootstrap_memory_repair as memory


class _Store:
    def __init__(self, path: Path, *, lease_active: bool) -> None:
        self.path = path
        self._roi_certification_bootstrap_autocheckpoint_lease = {"active": lease_active}


def _state(fraction: float) -> dict[str, int | float | None]:
    maximum = 2 * 1024 * 1024 * 1024
    current = int(maximum * fraction)
    return {
        "current_bytes": current,
        "max_bytes": maximum,
        "headroom_bytes": maximum - current,
        "fraction": fraction,
        "anon_bytes": 160 * 1024 * 1024,
        "file_bytes": max(0, current - 160 * 1024 * 1024),
        "file_dirty_bytes": 0,
        "file_writeback_bytes": 0,
        "slab_reclaimable_bytes": 0,
        "oom_events": 0,
        "oom_kill_events": 0,
    }


def _make_db(path: Path) -> None:
    db = sqlite3.connect(path)
    db.execute("CREATE TABLE evidence (id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
    db.commit()
    db.close()


def test_guarded_bootstrap_reader_defers_wal_checkpoint_to_active_lease(tmp_path, monkeypatch):
    path = tmp_path / "state.sqlite3"
    _make_db(path)
    store = _Store(path, lease_active=True)
    calls: list[bool] = []

    def guard(target: Path, *, allow_wal_checkpoint: bool = True):
        calls.append(allow_wal_checkpoint)
        return _state(0.50)

    monkeypatch.setattr(memory, "_guard_raw_cgroup", guard)
    reader = memory._guarded_pinned_reader(store)
    reader.close()

    assert calls == [False]


def test_guarded_reader_retains_guard_checkpoint_authority_without_active_lease(tmp_path, monkeypatch):
    path = tmp_path / "state.sqlite3"
    _make_db(path)
    store = _Store(path, lease_active=False)
    calls: list[bool] = []

    def guard(target: Path, *, allow_wal_checkpoint: bool = True):
        calls.append(allow_wal_checkpoint)
        return _state(0.50)

    monkeypatch.setattr(memory, "_guard_raw_cgroup", guard)
    reader = memory._guarded_pinned_reader(store)
    reader.close()

    assert calls == [True]


def test_active_bootstrap_guard_never_passive_checkpoints_and_still_fails_closed(tmp_path, monkeypatch):
    path = tmp_path / "state.sqlite3"
    _make_db(path)
    critical = _state(memory.RAW_CRITICAL_FRACTION + 0.005)

    monkeypatch.setattr(memory, "RECLAIM_SETTLE_SECONDS", 0)
    monkeypatch.setattr(memory, "WRITEBACK_SETTLE_SECONDS", 0)
    monkeypatch.setattr(memory, "_cgroup_memory", lambda *args, **kwargs: dict(critical))
    monkeypatch.setattr(memory, "_release_sqlite_file_cache", lambda target: True)
    monkeypatch.setattr(memory, "_trim_process_heap", lambda: False)
    monkeypatch.setattr(memory, "_request_cgroup_file_reclaim", lambda state, root=None: False)
    monkeypatch.setattr(memory, "_dirty_writeback_needed", lambda state: False)
    monkeypatch.setattr(memory, "_wal_checkpoint_needed", lambda target, state: True)

    def forbidden_checkpoint(target: Path):
        raise AssertionError("PASSIVE checkpoint must not run while bootstrap lease owns WAL maintenance")

    monkeypatch.setattr(memory, "_passive_wal_checkpoint", forbidden_checkpoint)

    with pytest.raises(MemoryError, match="raw cgroup memory pressure"):
        memory._guard_raw_cgroup(path, allow_wal_checkpoint=False)

    assert memory.RAW_CRITICAL_FRACTION == 0.94


def test_nonbootstrap_guard_retains_existing_passive_checkpoint_path(tmp_path, monkeypatch):
    path = tmp_path / "state.sqlite3"
    _make_db(path)
    pressured = _state(0.90)
    calls: list[Path] = []

    monkeypatch.setattr(memory, "RECLAIM_ATTEMPTS", 1)
    monkeypatch.setattr(memory, "RECLAIM_SETTLE_SECONDS", 0)
    monkeypatch.setattr(memory, "WRITEBACK_SETTLE_SECONDS", 0)
    monkeypatch.setattr(memory, "_cgroup_memory", lambda *args, **kwargs: dict(pressured))
    monkeypatch.setattr(memory, "_release_sqlite_file_cache", lambda target: True)
    monkeypatch.setattr(memory, "_trim_process_heap", lambda: False)
    monkeypatch.setattr(memory, "_request_cgroup_file_reclaim", lambda state, root=None: False)
    monkeypatch.setattr(memory, "_dirty_writeback_needed", lambda state: False)
    monkeypatch.setattr(memory, "_wal_checkpoint_needed", lambda target, state: True)

    def checkpoint(target: Path):
        calls.append(target)
        return {
            "attempted": True,
            "busy": 0,
            "log_frames": 1,
            "checkpointed_frames": 1,
            "error": None,
            "wal_bytes_before": 70 * 1024 * 1024,
            "wal_bytes_after": 70 * 1024 * 1024,
        }

    monkeypatch.setattr(memory, "_passive_wal_checkpoint", checkpoint)
    monkeypatch.setattr(memory, "_sync_sqlite_dirty_pages", lambda target: True)

    result = memory._guard_raw_cgroup(path, allow_wal_checkpoint=True)

    assert calls == [path]
    assert result["fraction"] == 0.90


def test_status_preserves_safety_and_declares_bootstrap_ownership():
    state = memory.status()

    assert state["raw_critical_fraction"] == 0.94
    assert state["bootstrap_active_lease_owns_wal_checkpoint"] is True
    assert state["bootstrap_guard_passive_checkpoint_enabled"] is False
    assert state["strategy_thresholds_changed"] is False
    assert state["certification_thresholds_changed"] is False
    assert state["paper_only"] is True
    assert state["live_money_authority"] is False
    assert state["signing_available"] is False
    assert state["transaction_submission_available"] is False
