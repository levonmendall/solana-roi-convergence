from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI

from solana_roi import certifier_cleanup_service as service


class _Lease:
    def __init__(self, database: Path) -> None:
        self.database_path = database
        self.handle = object()
        self.released = False

    def status(self):
        return {
            "owned": not self.released,
            "database_path": str(self.database_path),
            "release_commit": "test-release",
            "cross_process_exclusive": True,
        }

    def release(self) -> None:
        self.released = True
        self.handle = None


def _reset_service_state() -> None:
    service._STATE.clear()
    service._STATE.update(
        {
            "service_version": service.SERVICE_VERSION,
            "cleanup_version": service.cleanup.CLEANUP_VERSION,
            "enabled": False,
            "status": "not_started",
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        }
    )


def test_cleanup_capable_certifier_has_zero_trading_authority() -> None:
    assert service.PAPER_ONLY is True
    assert service.LIVE_MONEY_AUTHORITY is False
    assert service.SIGNING_AVAILABLE is False
    assert service.TRANSACTION_SUBMISSION_AVAILABLE is False
    assert service.app is service.certifier.app


def test_wrong_cleanup_role_fails_before_execution(monkeypatch) -> None:
    monkeypatch.setenv(service.cleanup.ROLE_ENV, "authoritative")
    monkeypatch.setenv(service.cleanup.RUN_ID_ENV, "certifier-cleanup-test")
    try:
        service._cleanup_arguments()
    except service.cleanup.CleanupBlocked as exc:
        assert "role=certifier" in str(exc)
    else:
        raise AssertionError("wrong cleanup role must fail closed")


def test_disabled_release_marks_lease_only_after_successful_cycle(tmp_path: Path, monkeypatch) -> None:
    _reset_service_state()
    database = tmp_path / "certifier.sqlite3"
    lease = _Lease(database)
    established: list[Path] = []

    async def acquire(path: Path, **_kwargs):
        assert path == database
        return lease

    def mark(path: Path, held):
        assert held is lease
        established.append(path)
        return {"release_commit": "test-release", "database_path": str(path)}

    @asynccontextmanager
    async def original(_app):
        with service.certifier._LOCK:
            service.certifier._STATE["successes"] = int(
                service.certifier._STATE.get("successes", 0) or 0
            ) + 1
        yield

    monkeypatch.delenv(service.cleanup.ENABLED_ENV, raising=False)
    monkeypatch.setattr(service, "_replica_path", lambda: database)
    monkeypatch.setattr(service.disk_ownership, "acquire_runtime_disk_lease", acquire)
    monkeypatch.setattr(service.disk_ownership, "mark_same_release_established", mark)
    monkeypatch.setattr(service, "_ORIGINAL_LIFESPAN", original)

    async def scenario() -> None:
        async with service.lifespan(FastAPI()):
            for _ in range(20):
                if established:
                    break
                await asyncio.sleep(0.02)
            assert established == [database]
            assert service._STATE["status"] == "disabled_release_established"

    asyncio.run(scenario())
    assert lease.released is True


def test_enabled_cleanup_without_same_release_marker_keeps_worker_stopped(
    tmp_path: Path, monkeypatch
) -> None:
    _reset_service_state()
    database = tmp_path / "certifier.sqlite3"
    database.touch()
    lease = _Lease(database)
    entered_worker: list[bool] = []

    async def acquire(_path: Path, **_kwargs):
        return lease

    @asynccontextmanager
    async def original(_app):
        entered_worker.append(True)
        yield

    monkeypatch.setenv(service.cleanup.ENABLED_ENV, "1")
    monkeypatch.setenv(service.cleanup.ROLE_ENV, "certifier")
    monkeypatch.setenv(service.cleanup.RUN_ID_ENV, "no-marker")
    monkeypatch.setattr(service, "_replica_path", lambda: database)
    monkeypatch.setattr(service.disk_ownership, "acquire_runtime_disk_lease", acquire)
    monkeypatch.setattr(service.disk_ownership, "same_release_established", lambda _path: False)
    monkeypatch.setattr(service, "_ORIGINAL_LIFESPAN", original)

    async def scenario() -> None:
        async with service.lifespan(FastAPI()):
            assert service._STATE["status"] == "blocked"
            assert service._STATE["worker_started_after_cleanup"] is False
            assert entered_worker == []

    asyncio.run(scenario())
    assert lease.released is True


def test_enabled_cleanup_runs_before_certifier_worker(tmp_path: Path, monkeypatch) -> None:
    _reset_service_state()
    database = tmp_path / "certifier.sqlite3"
    database.touch()
    lease = _Lease(database)
    calls: list[str] = []

    async def acquire(_path: Path, **_kwargs):
        return lease

    def execute(path: Path, **kwargs):
        calls.append("cleanup")
        assert path == database
        assert kwargs["role"] == "certifier"
        assert kwargs["run_id"] == "certifier-cleanup-success"
        assert kwargs["acknowledged_watermark"] is None
        return {
            "version": service.cleanup.CLEANUP_VERSION,
            "status": "success",
            "run_id": "certifier-cleanup-success",
            "role": "certifier",
            "before": {"integrity": {"ok": True}},
            "after": {"integrity": {"ok": True}},
            "compaction": {"mode": "vacuum"},
            "reclaimed": {
                "database_bytes": 1024,
                "wal_bytes": 256,
                "filesystem_free_bytes": 1280,
            },
        }

    @asynccontextmanager
    async def original(_app):
        calls.append("worker")
        yield

    monkeypatch.setenv(service.cleanup.ENABLED_ENV, "1")
    monkeypatch.setenv(service.cleanup.ROLE_ENV, "certifier")
    monkeypatch.setenv(service.cleanup.RUN_ID_ENV, "certifier-cleanup-success")
    monkeypatch.setattr(service, "_replica_path", lambda: database)
    monkeypatch.setattr(service.disk_ownership, "acquire_runtime_disk_lease", acquire)
    monkeypatch.setattr(service.disk_ownership, "same_release_established", lambda _path: True)
    monkeypatch.setattr(service.cleanup, "execute_cleanup", execute)
    monkeypatch.setattr(service, "_ORIGINAL_LIFESPAN", original)

    async def scenario() -> None:
        async with service.lifespan(FastAPI()):
            assert calls == ["cleanup", "worker"]
            assert service._STATE["status"] == "success"
            assert service._STATE["worker_started_after_cleanup"] is True
            assert service._STATE["database_bytes_reclaimed"] == 1024

    asyncio.run(scenario())
    assert lease.released is True
