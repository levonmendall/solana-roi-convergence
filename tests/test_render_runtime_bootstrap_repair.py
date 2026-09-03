from __future__ import annotations

import asyncio
import sqlite3
import threading
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from solana_roi import render_runtime_bootstrap_repair as repair


class _Worker:
    async def run(self, stop: asyncio.Event) -> None:
        await stop.wait()


class _Runtime:
    def __init__(self):
        self.webhook_worker = _Worker()
        self.direct_ingestion = _Worker()
        self.wallet_discovery = _Worker()
        self.price_clock = _Worker()


def _reset_state() -> None:
    repair._RUNTIME = None
    repair._BOOTSTRAP_STATE.update(
        {
            "state": "not_started",
            "attempts": 0,
            "lock_retries": 0,
            "started_at": None,
            "ready_at": None,
            "last_error_type": None,
            "last_error_message": None,
            "lifespan_active": False,
        }
    )


def test_only_sqlite_lock_busy_is_retryable():
    assert repair._is_sqlite_lock_error(sqlite3.OperationalError("database is locked")) is True
    assert repair._is_sqlite_lock_error(sqlite3.OperationalError("database is busy")) is True
    assert repair._is_sqlite_lock_error(sqlite3.OperationalError("malformed database schema")) is False
    assert repair._is_sqlite_lock_error(RuntimeError("database is locked")) is False


def test_background_bootstrap_retries_lock_then_becomes_ready(monkeypatch):
    _reset_state()
    calls = 0
    runtime = _Runtime()

    def build():
        nonlocal calls
        calls += 1
        if calls == 1:
            raise sqlite3.OperationalError("database is locked")
        return runtime

    monkeypatch.setattr(repair, "_ORIGINAL_INGESTION_RUNTIME", build)
    monkeypatch.setattr(repair, "BOOTSTRAP_RETRY_SECONDS", 0.01)
    result = asyncio.run(repair._build_runtime_until_ready(asyncio.Event()))

    assert result is runtime
    assert repair._RUNTIME is runtime
    assert calls == 2
    assert repair._BOOTSTRAP_STATE["state"] == "ready"
    assert repair._BOOTSTRAP_STATE["lock_retries"] == 1
    assert repair._BOOTSTRAP_STATE["last_error_type"] is None


def test_non_lock_bootstrap_failure_stays_fail_closed_without_retry(monkeypatch):
    _reset_state()
    calls = 0

    def build():
        nonlocal calls
        calls += 1
        raise ValueError("invalid production configuration")

    monkeypatch.setattr(repair, "_ORIGINAL_INGESTION_RUNTIME", build)
    result = asyncio.run(repair._build_runtime_until_ready(asyncio.Event()))

    assert result is None
    assert calls == 1
    assert repair._RUNTIME is None
    assert repair._BOOTSTRAP_STATE["state"] == "failed_closed"
    assert repair._BOOTSTRAP_STATE["last_error_type"] == "ValueError"


def test_deep_runtime_access_returns_503_while_handoff_is_not_ready(monkeypatch):
    _reset_state()
    repair._BOOTSTRAP_STATE["lifespan_active"] = True
    monkeypatch.setattr(repair, "_API", SimpleNamespace(HTTPException=HTTPException))

    with pytest.raises(HTTPException) as caught:
        repair._guarded_ingestion_runtime()

    assert caught.value.status_code == 503
    assert caught.value.detail["error"] == "runtime_bootstrap_not_ready"
    status = caught.value.detail["runtime_bootstrap"]
    assert status["deep_runtime_fail_closed_until_ready"] is True
    assert status["certification_thresholds_unchanged"] is True
    assert status["continuity_lease_unchanged"] is True
    assert status["recovery_bound_unchanged"] is True
    assert status["full_raw_receipt_scope_unchanged"] is True
    assert status["paper_only_authority_unchanged"] is True


def test_lifespan_yields_before_persistent_runtime_build_completes(monkeypatch):
    _reset_state()
    started = threading.Event()
    release = threading.Event()
    runtime = _Runtime()

    def build():
        started.set()
        release.wait(timeout=2.0)
        return runtime

    monkeypatch.setattr(repair, "_ORIGINAL_INGESTION_RUNTIME", build)
    monkeypatch.delenv("SOLANA_ROI_SHADOW_CLOCK_ENABLED", raising=False)

    async def exercise() -> None:
        async with repair._render_handoff_lifespan(SimpleNamespace()):
            # Entering the lifespan must not wait for build() to complete. The
            # background thread can be blocked while the ASGI app is already live.
            await asyncio.wait_for(asyncio.to_thread(started.wait, 1.0), timeout=1.5)
            assert repair._BOOTSTRAP_STATE["lifespan_active"] is True
            assert repair._RUNTIME is None
            assert repair._BOOTSTRAP_STATE["state"] in {"starting", "building_runtime"}
            release.set()
            for _ in range(100):
                if repair._RUNTIME is runtime:
                    break
                await asyncio.sleep(0.01)
            assert repair._RUNTIME is runtime
            assert repair._BOOTSTRAP_STATE["state"] == "ready"

    asyncio.run(exercise())
    assert repair._BOOTSTRAP_STATE["lifespan_active"] is False
    assert repair._BOOTSTRAP_STATE["state"] == "stopped"


def test_production_installs_handoff_and_constant_time_status_route():
    from solana_roi import production

    assert production.app.router.lifespan_context is repair._render_handoff_lifespan
    assert bool(getattr(production.app.state, "roi_runtime_bootstrap_handoff", False)) is True
    assert any(
        getattr(route, "path", None) == "/v1/runtime-bootstrap/status"
        for route in production.app.routes
    )

    from solana_roi.config import BASELINE

    assert BASELINE.max_chase_fraction == 0.15
