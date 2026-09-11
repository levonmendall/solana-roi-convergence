from __future__ import annotations

import asyncio
from pathlib import Path

from fastapi import FastAPI

from solana_roi import production_cleanup_runtime_install as runtime_cleanup


def _run_bootstrap() -> None:
    stop = asyncio.Event()
    asyncio.run(runtime_cleanup.bootstrap._bootstrap_and_run(stop))


def test_disabled_cleanup_preserves_normal_bootstrap_and_never_opens_cleanup(
    monkeypatch,
) -> None:
    app = FastAPI()
    calls: list[str] = []

    async def original(_stop: asyncio.Event) -> None:
        calls.append("runtime")

    def forbidden(*_args, **_kwargs):
        raise AssertionError("disabled cleanup must not execute")

    monkeypatch.delenv(runtime_cleanup.cleanup.ENABLED_ENV, raising=False)
    monkeypatch.setattr(runtime_cleanup.bootstrap, "_bootstrap_and_run", original)
    monkeypatch.setattr(runtime_cleanup.cleanup, "execute_cleanup", forbidden)

    state = runtime_cleanup.install_production_cleanup_runtime(app)

    assert state["status"] == "disabled"
    assert calls == []
    _run_bootstrap()
    assert calls == ["runtime"]
    assert app.state.roi_production_data_cleanup["status"] == "disabled"


def test_enabled_cleanup_runs_before_runtime_and_keeps_registration_non_mutating(
    tmp_path: Path, monkeypatch
) -> None:
    app = FastAPI()
    calls: list[str] = []
    database = tmp_path / "production.sqlite3"

    async def original(_stop: asyncio.Event) -> None:
        calls.append("runtime")

    def execute(path: Path, **kwargs):
        calls.append("cleanup")
        assert path == database
        assert kwargs["role"] == "authoritative"
        assert kwargs["run_id"] == "runtime-cleanup-1"
        assert kwargs["acknowledged_watermark"] == 42
        assert kwargs["telemetry_hours"] == 12.0
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
    monkeypatch.setenv("SOLANA_ROI_DB_PATH", str(database))
    monkeypatch.setattr(runtime_cleanup.bootstrap, "_bootstrap_and_run", original)
    monkeypatch.setattr(runtime_cleanup.cleanup, "execute_cleanup", execute)

    state = runtime_cleanup.install_production_cleanup_runtime(app)

    # Installation/import is storage-non-mutating even when the future run is armed.
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
    assert any(route.path == runtime_cleanup.STATUS_PATH for route in app.routes)


def test_cleanup_failure_blocks_runtime_but_not_liveness_registration(
    tmp_path: Path, monkeypatch
) -> None:
    app = FastAPI()
    calls: list[str] = []

    async def original(_stop: asyncio.Event) -> None:
        calls.append("runtime")

    def blocked(*_args, **_kwargs):
        calls.append("cleanup")
        raise runtime_cleanup.cleanup.CleanupBlocked("schema protection failed")

    monkeypatch.setenv(runtime_cleanup.cleanup.ENABLED_ENV, "1")
    monkeypatch.setenv(runtime_cleanup.cleanup.ROLE_ENV, "authoritative")
    monkeypatch.setenv(runtime_cleanup.cleanup.RUN_ID_ENV, "runtime-cleanup-blocked")
    monkeypatch.setenv("SOLANA_ROI_DB_PATH", str(tmp_path / "production.sqlite3"))
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

    async def original(_stop: asyncio.Event) -> None:
        calls.append("runtime")

    def forbidden(*_args, **_kwargs):
        raise AssertionError("role mismatch must block before cleanup execution")

    monkeypatch.setenv(runtime_cleanup.cleanup.ENABLED_ENV, "1")
    monkeypatch.setenv(runtime_cleanup.cleanup.ROLE_ENV, "certifier")
    monkeypatch.setenv(runtime_cleanup.cleanup.RUN_ID_ENV, "wrong-role")
    monkeypatch.setenv("SOLANA_ROI_DB_PATH", str(tmp_path / "production.sqlite3"))
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
