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
        self._lock = threading.RLock()
        self.db.execute("CREATE TABLE evidence (id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
        self.db.commit()

    def close(self) -> None:
        self.db.close()


class _FakeTimer:
    created: list["_FakeTimer"] = []

    def __init__(self, interval, function, args=(), kwargs=None):
        self.interval = float(interval)
        self.function = function
        self.args = tuple(args)
        self.kwargs = dict(kwargs or {})
        self.cancelled = False
        self.started = False
        self.daemon = False
        self.__class__.created.append(self)

    def start(self):
        self.started = True

    def cancel(self):
        self.cancelled = True

    def fire(self):
        self.function(*self.args, **self.kwargs)


def _autocheckpoint(store: _Store) -> int:
    return int(store.db.execute("PRAGMA wal_autocheckpoint").fetchone()[0])


def test_refresh_disables_autocheckpoint_and_stale_timer_cannot_restore(tmp_path, monkeypatch):
    store = _Store(tmp_path / "state.sqlite3")
    original = _autocheckpoint(store)
    assert original > 0
    _FakeTimer.created = []
    monkeypatch.setattr(lease.threading, "Timer", _FakeTimer)
    monkeypatch.setattr(lease, "_wal_size_bytes", lambda store: 0)

    lease.refresh(store)
    assert _autocheckpoint(store) == 0
    first = _FakeTimer.created[-1]
    assert first.started is True
    assert first.interval == lease.DEFAULT_IDLE_SECONDS

    lease.refresh(store)
    assert first.cancelled is True
    second = _FakeTimer.created[-1]
    assert second is not first
    assert _autocheckpoint(store) == 0

    # A timer already entering its callback after cancel must be harmless.
    first.fire()
    assert _autocheckpoint(store) == 0

    second.fire()
    assert _autocheckpoint(store) == original
    state = getattr(store, lease.STATE_ATTR)
    assert state["active"] is False
    assert state["restore_reason"] == "idle_timeout"
    store.close()


def test_finish_restores_exact_original_setting_and_preserves_rows(tmp_path, monkeypatch):
    store = _Store(tmp_path / "state.sqlite3")
    store.db.execute("PRAGMA wal_autocheckpoint=37")
    assert _autocheckpoint(store) == 37
    _FakeTimer.created = []
    monkeypatch.setattr(lease.threading, "Timer", _FakeTimer)
    monkeypatch.setattr(lease, "_wal_size_bytes", lambda store: 0)

    lease.refresh(store)
    assert _autocheckpoint(store) == 0
    with store._lock, store.db:
        store.db.execute("INSERT INTO evidence(value) VALUES ('durable')")

    assert lease.finish(store, reason="test_complete") is True
    assert _autocheckpoint(store) == 37
    store.close()

    reopened = sqlite3.connect(tmp_path / "state.sqlite3")
    try:
        assert reopened.execute("SELECT value FROM evidence").fetchall() == [("durable",)]
    finally:
        reopened.close()


def test_wal_bound_restores_policy_runs_one_maintenance_checkpoint_and_pauses(tmp_path, monkeypatch):
    store = _Store(tmp_path / "state.sqlite3")
    original = _autocheckpoint(store)
    _FakeTimer.created = []
    monkeypatch.setattr(lease.threading, "Timer", _FakeTimer)
    wal_bytes = {"value": 0}
    monkeypatch.setattr(lease, "_wal_size_bytes", lambda store: wal_bytes["value"])
    monkeypatch.setattr(lease, "_sync_and_release", lambda path: None)
    checkpoints: list[str] = []
    monkeypatch.setattr(
        lease,
        "_maintenance_checkpoint_locked",
        lambda store: checkpoints.append("checkpoint") or (0, 16384, 16384, None),
    )

    lease.refresh(store)
    assert _autocheckpoint(store) == 0
    wal_bytes["value"] = lease.DEFAULT_MAX_WAL_BYTES

    with pytest.raises(HTTPException) as exc:
        lease.refresh(store)

    assert exc.value.status_code == 503
    assert "bounded WAL checkpoint maintenance" in str(exc.value.detail)
    assert checkpoints == ["checkpoint"]
    assert _autocheckpoint(store) == original
    assert getattr(store, lease.STATE_ATTR)["active"] is False
    store.close()


def test_manifest_wrapper_acquires_lease_before_manifest_work(tmp_path, monkeypatch):
    store = _Store(tmp_path / "state.sqlite3")
    order: list[str] = []
    monkeypatch.setattr(lease, "refresh", lambda store: order.append("lease") or {})
    monkeypatch.setattr(
        lease,
        "set_manifest_tables",
        lambda store, payload: order.append("tables"),
    )
    monkeypatch.setattr(
        lease,
        "_ORIGINAL_MANIFEST",
        lambda store: order.append("manifest") or {"tables": [{"name": "evidence"}]},
    )

    payload = lease._manifest_with_lease(store)

    assert payload["tables"] == [{"name": "evidence"}]
    assert order == ["lease", "manifest", "tables"]
    store.close()


def test_page_wrapper_refreshes_before_read_and_restores_on_final_page(tmp_path, monkeypatch):
    store = _Store(tmp_path / "state.sqlite3")
    order: list[str] = []
    monkeypatch.setattr(lease, "refresh", lambda store: order.append("lease") or {})
    monkeypatch.setattr(
        lease,
        "_ORIGINAL_PAGE",
        lambda store, **kwargs: order.append("page") or {"table": "final", "done": True},
    )
    monkeypatch.setattr(
        lease,
        "finish_if_complete",
        lambda store, payload: order.append("finish") or True,
    )

    payload = lease._page_with_lease(store, table_name="final")

    assert payload == {"table": "final", "done": True}
    assert order == ["lease", "page", "finish"]
    store.close()


def test_finish_if_complete_only_restores_for_manifest_final_table(tmp_path, monkeypatch):
    store = _Store(tmp_path / "state.sqlite3")
    _FakeTimer.created = []
    monkeypatch.setattr(lease.threading, "Timer", _FakeTimer)
    monkeypatch.setattr(lease, "_wal_size_bytes", lambda store: 0)

    lease.refresh(store)
    lease.set_manifest_tables(store, {"tables": [{"name": "first"}, {"name": "final"}]})
    assert lease.finish_if_complete(store, {"table": "first", "done": True}) is False
    assert _autocheckpoint(store) == 0
    assert lease.finish_if_complete(store, {"table": "final", "done": True}) is True
    assert _autocheckpoint(store) > 0
    store.close()


def test_lease_safety_contract_and_retry_window():
    state = lease.status()

    assert lease.DEFAULT_IDLE_SECONDS == 45.0
    assert lease.DEFAULT_IDLE_SECONDS > 30.0
    assert lease.DEFAULT_MAX_WAL_BYTES == 64 * 1024 * 1024
    assert state["wal_bound_fail_closed"] is True
    assert state["original_autocheckpoint_restored"] is True
    assert state["strategy_thresholds_changed"] is False
    assert state["certification_thresholds_changed"] is False
    assert state["canonical_evidence_reset"] is False
    assert state["paper_only"] is True
    assert state["live_money_authority"] is False
    assert state["signing_available"] is False
    assert state["transaction_submission_available"] is False
