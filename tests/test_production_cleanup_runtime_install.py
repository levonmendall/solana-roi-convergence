from __future__ import annotations

import asyncio
import json
from pathlib import Path

from fastapi import FastAPI

from solana_roi import production_cleanup_runtime_install as runtime_cleanup

RELEASE = "test-cleanup-release-sha"


def _run_bootstrap() -> None:
    stop = asyncio.Event()
    asyncio.run(runtime_cleanup.bootstrap._bootstrap_and_run(stop))


def _configure_database(tmp_path: Path, monkeypatch, *, create: bool = False) -> Path:
    database = tmp_path / "production.sqlite3"
    if create:
        database.touch()
    monkeypatch.setenv("SOLANA_ROI_DB_PATH", str(database))
    monkeypatch.setenv("RENDER_GIT_COMMIT", RELEASE)
    return database


def _write_establishment(database: Path) -> None:
    marker = database.parent / runtime_cleanup.disk_ownership.ESTABLISHED_FILENAME
    marker.write_text(
        json.dumps(
            {
                "protocol_version": runtime_cleanup.disk_ownership.LOCK_PROTOCOL_VERSION,
                "release_commit": RELEASE,
                "database_path": str(database),
                "established_at": "2026-09-11T00:00:00+00:00",
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def test_disabled_cleanup_establishes_same_release_lease_only_after_full_runtime(
    tmp_path: Path, monkeypatch
) -> None:
    app = FastAPI()
    calls: list[str] = []
    database = _configure_database(tmp_path, monkeypatch)

    async def original(_stop: asyncio.Event) -> None:
        calls.append("runtime")
        runtime_cleanup.bootstrap._BOOTSTRAP_STATE["state"] = "full_runtime"

    def forbidden(*_args, **_kwargs):
        raise AssertionError("disabled cleanup must not execute")

    monkeypatch.delenv(runtime_cleanup.cleanup.ENABLED_ENV, raising=False)
    monkeypatch.setattr(runtime_cleanup.bootstrap, "_bootstrap_and_run", original)
    monkeypatch.setattr(runtime_cleanup.cleanup, "execute_cleanup", forbidden)

    state = runtime_cleanup.install_production_cleanup_runtime(app)

    assert state["status"] == "disabled"
    assert calls == []
    assert runtime_cleanup.disk_ownership.read_establishment(database) is None
    _run_bootstrap()

    assert calls == ["runtime"]
    assert app.state.roi_production_data_cleanup["status"] == "disabled"
    assert runtime_cleanup.disk_ownership.same_release_established(database) is True
    assert app.state.roi_production_disk_ownership["owned"] is False


def test_enabled_cleanup_refuses_first_deploy_without_same_release_establishment(
    tmp_path: Path, monkeypatch
) -> None:
    app = FastAPI()
    calls: list[str] = []
    _configure_database(tmp_path, monkeypatch, create=True)

    async def original(_stop: asyncio.Event) -> None:
        calls.append("runtime")

    def forbidden(*_args, **_kwargs):
        calls.append("cleanup")
        raise AssertionError("first deploy must block before destructive cleanup")

    monkeypatch.setenv(runtime_cleanup.cleanup.ENABLED_ENV, "1")
    monkeypatch.setenv(runtime_cleanup.cleanup.ROLE_ENV, "authoritative")
    monkeypatch.setenv(runtime_cleanup.cleanup.RUN_ID_ENV, "first-deploy-block")
    monkeypatch.setattr(runtime_cleanup.bootstrap, "_bootstrap_and_run", original)
    monkeypatch.setattr(runtime_cleanup.cleanup, "execute_cleanup", forbidden)

    runtime_cleanup.install_production_cleanup_runtime(app)
    _run_bootstrap()

    assert calls == []
    state = app.state.roi_production_data_cleanup
    assert state["status"] == "blocked"
    assert state["phase"] == "cleanup_preflight_or_execution"
    assert "exact release SHA" in state["error"]
    assert runtime_cleanup.bootstrap._BOOTSTRAP_STATE["state"] == "failed_closed"


def test_enabled_cleanup_runs_before_runtime_after_same_release_establishment(
    tmp_path: Path, monkeypatch
) -> None:
    app = FastAPI()
    calls: list[str] = []
    database = _configure_database(tmp_path, monkeypatch, create=True)
    _write_establishment(database)

    async def original(_stop: asyncio.Event) -> None:
        calls.append("runtime")

    def execute(path: Path, **kwargs):
        calls.append("cleanup")
        assert path == database
        assert kwargs["role"] == "authoritative"
        assert kwargs["run_id"] == "runtime-cleanup-1"
        assert kwargs["acknowledged_watermark"] == 42
        assert kwargs["telemetry_hours"] == 12.0
        assert app.state.roi_production_disk_ownership["owned"] is True
        return {
            "version": "production-data-cleanup-v2",
            "status": "success",
            "run_id": "runtime-cleanup-1",
            "role": "authoritative",
            "deleted_rows": {
                "synthetic_candidate_closure": {"v51_candidates": 3},
                "acknowledged_replication_changes": 7,
                "stale_risk_refresh_measurements": 11,
            },
            "orphan_files": {"removed": 2},
            "before": {"integrity": {"ok": True}},
            "after": {"integrity": {"ok": True}},
            "compaction": {"mode": "vacuum"},
            "reclaimed": {
                "database_bytes": 1024,
                "wal_bytes": 512,
                "filesystem_free_bytes": 1536,
            },
        }

    monkeypatch.setenv(runtime_cleanup.cleanup.ENABLED_ENV, "1")
    monkeypatch.setenv(runtime_cleanup.cleanup.ROLE_ENV, "authoritative")
    monkeypatch.setenv(runtime_cleanup.cleanup.RUN_ID_ENV, "runtime-cleanup-1")
    monkeypatch.setenv(runtime_cleanup.cleanup.ACK_WATERMARK_ENV, "42")
    monkeypatch.setenv(runtime_cleanup.cleanup.TELEMETRY_HOURS_ENV, "12")
    monkeypatch.setattr(runtime_cleanup.bootstrap, "_bootstrap_and_run", original)
    monkeypatch.setattr(runtime_cleanup.cleanup, "execute_cleanup", execute)

    state = runtime_cleanup.install_production_cleanup_runtime(app)

    assert state["status"] == "pending"
    assert calls == []
    _run_bootstrap()

    assert calls == ["cleanup", "runtime"]
    completed = app.state.roi_production_data_cleanup
    assert completed["status"] == "success"
    assert completed["deleted_rows"] == 21
    assert completed["orphan_files_removed"] == 2
    assert completed["database_bytes_reclaimed"] == 1024
    assert completed["runtime_started_after_cleanup"] is True
    assert completed["paper_only"] is True
    assert completed["live_money_authority"] is False
    assert app.state.roi_production_disk_ownership["owned"] is False
    assert any(route.path == runtime_cleanup.STATUS_PATH for route in app.routes)


def test_cleanup_failure_blocks_runtime_but_keeps_liveness_surface(
    tmp_path: Path, monkeypatch
) -> None:
    app = FastAPI()
    calls: list[str] = []
    database = _configure_database(tmp_path, monkeypatch, create=True)
    _write_establishment(database)

    async def original(_stop: asyncio.Event) -> None:
        calls.append("runtime")

    def blocked(*_args, **_kwargs):
        calls.append("cleanup")
        raise runtime_cleanup.cleanup.CleanupBlocked("schema protection failed")

    monkeypatch.setenv(runtime_cleanup.cleanup.ENABLED_ENV, "1")
    monkeypatch.setenv(runtime_cleanup.cleanup.ROLE_ENV, "authoritative")
    monkeypatch.setenv(runtime_cleanup.cleanup.RUN_ID_ENV, "runtime-cleanup-blocked")
    monkeypatch.setattr(runtime_cleanup.bootstrap, "_bootstrap_and_run", original)
    monkeypatch.setattr(runtime_cleanup.cleanup, "execute_cleanup", blocked)
    runtime_cleanup.bootstrap._BOOTSTRAP_STATE["state"] = "starting"

    runtime_cleanup.install_production_cleanup_runtime(app)
    _run_bootstrap()

    assert calls == ["cleanup"]
    state = app.state.roi_production_data_cleanup
    assert state["status"] == "blocked"
    assert state["runtime_started_after_cleanup"] is False
    assert state["liveness_available_during_cleanup"] is True
    assert runtime_cleanup.bootstrap._BOOTSTRAP_STATE["state"] == "failed_closed"
    assert runtime_cleanup.bootstrap._BOOTSTRAP_STATE["last_error_type"] == "CleanupBlocked"


def test_wrong_role_fails_closed_before_database_mutation(tmp_path: Path, monkeypatch) -> None:
    app = FastAPI()
    calls: list[str] = []
    database = _configure_database(tmp_path, monkeypatch, create=True)
    _write_establishment(database)

    async def original(_stop: asyncio.Event) -> None:
        calls.append("runtime")

    def forbidden(*_args, **_kwargs):
        raise AssertionError("role mismatch must block before cleanup execution")

    monkeypatch.setenv(runtime_cleanup.cleanup.ENABLED_ENV, "1")
    monkeypatch.setenv(runtime_cleanup.cleanup.ROLE_ENV, "certifier")
    monkeypatch.setenv(runtime_cleanup.cleanup.RUN_ID_ENV, "wrong-role")
    monkeypatch.setattr(runtime_cleanup.bootstrap, "_bootstrap_and_run", original)
    monkeypatch.setattr(runtime_cleanup.cleanup, "execute_cleanup", forbidden)

    runtime_cleanup.install_production_cleanup_runtime(app)
    _run_bootstrap()

    assert calls == []
    assert app.state.roi_production_data_cleanup["status"] == "blocked"
    assert runtime_cleanup.bootstrap._BOOTSTRAP_STATE["state"] == "failed_closed"


def test_install_is_idempotent(monkeypatch) -> None:
    app = FastAPI()

    async def original(_stop: asyncio.Event) -> None:
        return None

    monkeypatch.delenv(runtime_cleanup.cleanup.ENABLED_ENV, raising=False)
    monkeypatch.setattr(runtime_cleanup.bootstrap, "_bootstrap_and_run", original)

    runtime_cleanup.install_production_cleanup_runtime(app)
    first = runtime_cleanup.bootstrap._bootstrap_and_run
    runtime_cleanup.install_production_cleanup_runtime(app)
    second = runtime_cleanup.bootstrap._bootstrap_and_run

    assert first is second
    assert bool(getattr(first, "_roi_production_data_cleanup_bootstrap", False))
