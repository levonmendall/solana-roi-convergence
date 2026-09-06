from __future__ import annotations

import asyncio
import sqlite3
import threading

from fastapi import FastAPI

from solana_roi import api
from solana_roi.v51_schema_cache import columns, invalidate, stats, table_exists


class MemoryStore:
    def __init__(self) -> None:
        self.db = sqlite3.connect(":memory:", check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self._lock = threading.RLock()


def test_91_schema_cache_reuses_static_metadata_and_invalidates_on_ddl() -> None:
    store = MemoryStore()
    with store._lock, store.db:
        store.db.execute("CREATE TABLE alpha(id INTEGER PRIMARY KEY, value TEXT)")
    invalidate(store)
    before = stats()
    assert table_exists(store, "alpha") is True
    assert columns(store, "alpha") == {"id", "value"}
    first = stats()
    assert columns(store, "alpha") == {"id", "value"}
    assert table_exists(store, "alpha") is True
    second = stats()
    assert second["column_cache_hits"] > first["column_cache_hits"]
    assert second["table_cache_hits"] > first["table_cache_hits"]
    assert first["column_cache_misses"] > before["column_cache_misses"]

    with store._lock, store.db:
        store.db.execute("ALTER TABLE alpha ADD COLUMN added REAL")
    # PRAGMA schema_version changes on DDL, so the cache must invalidate itself.
    assert columns(store, "alpha") == {"id", "value", "added"}
    after = stats()
    assert after["schema_invalidations"] > second["schema_invalidations"]
    store.db.close()


def test_91_production_installs_precompute_callback_and_schema_cache() -> None:
    from solana_roi import production
    from solana_roi import v51_candidate_ledger, v51_evidence_analytics
    from solana_roi.v51_schema_cache import columns as cached_columns, table_exists as cached_table_exists

    assert callable(production.app.state.roi_v51_system_proof_precompute)
    assert production.app.state.roi_v51_system_proof_precompute_seconds >= 1.0
    assert production.app.state.roi_v51_schema_introspection_cache is True
    assert v51_candidate_ledger._columns is cached_columns
    assert v51_candidate_ledger._table_exists is cached_table_exists
    assert v51_evidence_analytics._columns is cached_columns
    assert v51_evidence_analytics._table_exists is cached_table_exists


def test_91_precompute_loop_runs_callback_off_event_loop() -> None:
    app = FastAPI()
    calls: list[int] = []
    stop = asyncio.Event()

    def precompute() -> dict[str, object]:
        calls.append(threading.get_ident())
        return {"state": "INSUFFICIENT_EVIDENCE"}

    app.state.roi_v51_system_proof_precompute = precompute
    app.state.roi_v51_system_proof_precompute_seconds = 60.0

    async def exercise() -> None:
        event_loop_thread = threading.get_ident()
        task = asyncio.create_task(api._proof_precompute_loop(app, stop))
        for _ in range(100):
            if calls:
                break
            await asyncio.sleep(0.01)
        stop.set()
        await task
        assert calls
        assert calls[0] != event_loop_thread
        assert app.state.roi_v51_system_proof_precompute_state == "healthy"
        assert app.state.roi_v51_system_proof_precompute_last_completed_at is not None

    asyncio.run(exercise())


def test_91_http_work_attribution_is_installed_without_request_content_capture() -> None:
    from solana_roi import production

    assert isinstance(production.app.state.roi_v51_system_proof_http_metrics, dict)
    metrics = production.app.state.roi_v51_system_proof_http_metrics
    assert set(metrics) == {"request_count", "total_duration_seconds"}
    assert "body" not in metrics
    assert "headers" not in metrics


def test_91_direct_solana_still_disables_legacy_helius_by_default(monkeypatch) -> None:
    monkeypatch.setenv("SOLANA_ROI_DIRECT_SOLANA_ENABLED", "true")
    monkeypatch.delenv("SOLANA_ROI_LEGACY_WEBHOOK_WORKER_ENABLED", raising=False)
    assert api.legacy_webhook_worker_enabled() is False
