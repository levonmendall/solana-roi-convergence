from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

import pytest
from fastapi import HTTPException

from solana_roi import certification_bootstrap_autocheckpoint_lease as lease


class _Store:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA busy_timeout=4321")
        self._lock = threading.RLock()
        self.db.execute("CREATE TABLE evidence (id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
        self.db.commit()

    def close(self) -> None:
        self.db.close()


class _FakeTimer:
    def __init__(self, interval, function, args=(), kwargs=None):
        self.interval = float(interval)
        self.function = function
        self.args = tuple(args)
        self.kwargs = dict(kwargs or {})
        self.cancelled = False
        self.started = False
        self.daemon = False

    def start(self) -> None:
        self.started = True

    def cancel(self) -> None:
        self.cancelled = True


def _autocheckpoint(store: _Store) -> int:
    return int(store.db.execute("PRAGMA wal_autocheckpoint").fetchone()[0])


def _busy_timeout(store: _Store) -> int:
    return int(store.db.execute("PRAGMA busy_timeout").fetchone()[0])


def test_ceiling_maintenance_recovery_reacquires_lease(tmp_path, monkeypatch):
    store = _Store(tmp_path / "state.sqlite3")
    original = _autocheckpoint(store)
    monkeypatch.setattr(lease.threading, "Timer", _FakeTimer)

    wal_values = iter([lease.DEFAULT_MAX_WAL_BYTES + 4096, 0, 0])
    monkeypatch.setattr(lease, "_wal_size_bytes", lambda target: next(wal_values, 0))
    checkpoints: list[str] = []
    monkeypatch.setattr(
        lease,
        "_maintenance_checkpoint_locked",
        lambda target: checkpoints.append("truncate") or (0, 0, 0, None),
    )
    releases: list[Path] = []
    monkeypatch.setattr(lease, "_sync_and_release", lambda path: releases.append(Path(path)))

    state = lease.refresh(store)

    assert checkpoints == ["truncate"]
    assert releases == [store.path]
    assert state["active"] is True
    assert state["last_maintenance_wal_before"] == lease.DEFAULT_MAX_WAL_BYTES + 4096
    assert state["last_maintenance_wal_after"] == 0
    assert _autocheckpoint(store) == 0

    assert lease.finish(store, reason="test_complete") is True
    assert _autocheckpoint(store) == original
    store.close()


def test_busy_ceiling_maintenance_remains_fail_closed(tmp_path, monkeypatch):
    store = _Store(tmp_path / "state.sqlite3")
    original = _autocheckpoint(store)
    monkeypatch.setattr(lease, "_wal_size_bytes", lambda target: lease.DEFAULT_MAX_WAL_BYTES + 1)
    monkeypatch.setattr(lease, "_maintenance_checkpoint_locked", lambda target: (1, 12, 0, None))
    monkeypatch.setattr(lease, "_sync_and_release", lambda path: None)

    with pytest.raises(HTTPException) as exc:
        lease.refresh(store)

    assert exc.value.status_code == 503
    assert "bounded WAL checkpoint maintenance" in str(exc.value.detail)
    assert _autocheckpoint(store) == original
    store.close()


def test_zero_wait_truncate_checkpoint_shrinks_wal_and_restores_busy_timeout(tmp_path):
    store = _Store(tmp_path / "state.sqlite3")
    store.db.execute("PRAGMA wal_autocheckpoint=0")
    with store._lock, store.db:
        for index in range(500):
            store.db.execute("INSERT INTO evidence(value) VALUES (?)", ("x" * 2048 + str(index),))

    before = lease._wal_size_bytes(store)
    assert before > 0
    assert _busy_timeout(store) == 4321

    with store._lock:
        busy, log_frames, checkpointed_frames, error = lease._maintenance_checkpoint_locked(store)

    after = lease._wal_size_bytes(store)
    assert error is None
    assert busy == 0
    assert isinstance(log_frames, int)
    assert isinstance(checkpointed_frames, int)
    assert after < before
    assert after < lease.DEFAULT_MAX_WAL_BYTES
    assert _busy_timeout(store) == 4321
    store.close()


def test_status_exposes_ceiling_only_checkpoint_contract():
    state = lease.status()

    assert state["preworker_checkpoint_enabled"] is False
    assert state["preworker_unconditional_checkpoint_enabled"] is False
    assert state["preworker_ceiling_maintenance_enabled"] is True
    assert state["maintenance_checkpoint_mode"] == "truncate_zero_wait_at_ceiling_only"
    assert state["maintenance_success_reacquires_lease"] is True
    assert state["maintenance_busy_or_error_fail_closed"] is True
    assert state["wal_bound_fail_closed"] is True
    assert state["strategy_thresholds_changed"] is False
    assert state["certification_thresholds_changed"] is False
    assert state["paper_only"] is True
    assert state["live_money_authority"] is False
