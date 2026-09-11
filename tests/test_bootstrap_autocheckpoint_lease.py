from __future__ import annotations

import asyncio
import inspect
import sqlite3
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import BackgroundTasks, HTTPException

from solana_roi import certification_bootstrap_autocheckpoint_lease as lease
from solana_roi import certification_logical_bootstrap as logical


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


def _fake_app_with_routes(runtime_provider, order: list[str]):
    def manifest_endpoint(x_certification_token=None):
        runtime_provider()
        order.append("manifest")
        return {"tables": [{"name": "evidence"}]}

    def page_endpoint(
        background_tasks=None,
        table=None,
        epoch=None,
        schema_fingerprint=None,
        cursor=None,
        limit=250,
        x_certification_token=None,
    ):
        assert isinstance(background_tasks, BackgroundTasks)
        runtime_provider()
        order.append("page")
        return {"table": table, "done": True}

    manifest_route = SimpleNamespace(
        path=lease.MANIFEST_PATH,
        endpoint=manifest_endpoint,
        dependant=SimpleNamespace(call=manifest_endpoint),
    )
    page_route = SimpleNamespace(
        path=lease.PAGE_PATH,
        endpoint=page_endpoint,
        dependant=SimpleNamespace(call=page_endpoint),
    )
    return SimpleNamespace(
        routes=[manifest_route, page_route],
        state=SimpleNamespace(),
    ), manifest_route, page_route


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


def test_installer_wraps_only_registered_routes_and_preserves_logical_globals(tmp_path, monkeypatch):
    store = _Store(tmp_path / "state.sqlite3")
    runtime = SimpleNamespace(store=store)
    order: list[str] = []

    def runtime_provider():
        return runtime

    app, manifest_route, page_route = _fake_app_with_routes(runtime_provider, order)
    original_manifest_function = logical._manifest
    original_page_function = logical._page

    monkeypatch.setattr(
        "solana_roi.certification_incremental_replication._require_shared_token",
        lambda token: order.append("auth"),
    )
    monkeypatch.setattr(lease, "refresh", lambda target: order.append("lease") or {})
    monkeypatch.setattr(lease, "set_manifest_tables", lambda target, payload: order.append("tables"))
    monkeypatch.setattr(lease, "finish_if_complete", lambda target, payload: order.append("finish") or True)
    monkeypatch.setattr(lease, "_install_preworker_quiesce", lambda: None)

    lease.install_certification_bootstrap_autocheckpoint_lease(app)

    assert logical._manifest is original_manifest_function
    assert logical._page is original_page_function
    assert getattr(manifest_route.dependant.call, "_roi_bootstrap_autocheckpoint_lease") is True
    assert getattr(page_route.dependant.call, "_roi_bootstrap_autocheckpoint_lease") is True
    assert inspect.iscoroutinefunction(manifest_route.dependant.call)
    assert inspect.iscoroutinefunction(page_route.dependant.call)

    manifest = asyncio.run(
        manifest_route.dependant.call(x_certification_token="token")
    )
    assert manifest == {"tables": [{"name": "evidence"}]}
    assert order == ["auth", "lease", "manifest", "tables"]

    order.clear()
    page = asyncio.run(
        page_route.dependant.call(
            background_tasks=BackgroundTasks(),
            table="evidence",
            epoch="epoch-12345678",
            schema_fingerprint="f" * 64,
            cursor=None,
            limit=1,
            x_certification_token="token",
        )
    )
    assert page == {"table": "evidence", "done": True}
    assert order == ["auth", "lease", "page", "finish"]
    assert app.state.roi_certification_bootstrap_autocheckpoint_lease is True
    assert app.state.roi_certification_bootstrap_autocheckpoint_lease_active is True
    assert app.state.roi_certification_bootstrap_autocheckpoint_lease_version == lease.LEASE_VERSION
    store.close()


def test_installer_noops_when_split_runtime_disabled_and_routes_absent(monkeypatch):
    app = SimpleNamespace(routes=[], state=SimpleNamespace())
    monkeypatch.setattr(
        "solana_roi.certification_service_split.split_runtime_enabled",
        lambda: False,
    )

    lease.install_certification_bootstrap_autocheckpoint_lease(app, lambda: None)

    assert app.state.roi_certification_bootstrap_autocheckpoint_lease is False
    assert app.state.roi_certification_bootstrap_autocheckpoint_lease_active is False
    assert app.state.roi_certification_bootstrap_autocheckpoint_lease_version == lease.LEASE_VERSION


def test_installer_fails_closed_when_split_runtime_enabled_and_routes_absent(monkeypatch):
    app = SimpleNamespace(routes=[], state=SimpleNamespace())
    monkeypatch.setattr(
        "solana_roi.certification_service_split.split_runtime_enabled",
        lambda: True,
    )

    with pytest.raises(RuntimeError, match="bootstrap route not found"):
        lease.install_certification_bootstrap_autocheckpoint_lease(app, lambda: None)


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
    assert state["scope"] == "authoritative_preworker_plus_registered_bootstrap_routes"
    assert state["module_global_logical_functions_mutated"] is False
    assert state["route_wrappers_async"] is True
    assert state["background_tasks_forwarded"] is True
    assert state["anyio_sync_worker_route_wrapper"] is False
    assert state["preworker_lease_priming"] is True
    assert state["preworker_checkpoint_enabled"] is False
    assert state["preworker_sync_and_file_cache_release"] is True
    assert state["strategy_thresholds_changed"] is False
    assert state["certification_thresholds_changed"] is False
    assert state["canonical_evidence_reset"] is False
    assert state["paper_only"] is True
    assert state["live_money_authority"] is False
    assert state["signing_available"] is False
    assert state["transaction_submission_available"] is False