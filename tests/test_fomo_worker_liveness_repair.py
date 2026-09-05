from __future__ import annotations

import asyncio
import sqlite3
import threading
from types import SimpleNamespace

from solana_roi import canonical_worker_isolation_repair as canonical
from solana_roi import continuation_market_recalibration as continuation
from solana_roi import fomo_canonical_worker_binding_repair as binding
from solana_roi import fomo_worker_liveness_repair as repair


def _runtime_db() -> sqlite3.Connection:
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    db.execute(
        "CREATE TABLE independent_fomo_runtime (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL)"
    )
    return db


def test_runtime_reader_exposes_existing_cycle_heartbeat(monkeypatch) -> None:
    db = _runtime_db()
    rows = [
        ("last_scan_at", "2026-09-05T17:51:00+00:00", "2026-09-05T17:51:00+00:00"),
        ("last_error", "RuntimeError: scanner cycle failed", "2026-09-05T17:51:00+00:00"),
        ("candidate_count", "3", "2026-09-05T17:51:00+00:00"),
        ("opened_count", "1", "2026-09-05T17:51:00+00:00"),
    ]
    db.executemany("INSERT INTO independent_fomo_runtime(key,value,updated_at) VALUES (?,?,?)", rows)
    adapter = SimpleNamespace(store=SimpleNamespace(db=db, _lock=threading.RLock()))

    monkeypatch.setattr(repair.continuation, "_continuation_schema", lambda _adapter: None)
    monkeypatch.setattr(repair, "_ORIGINAL_READ_RUNTIME", lambda _adapter: {"rows_scanned": 12})
    repair._STATE.update(
        {
            "state": "running",
            "starts": 2,
            "restarts": 1,
            "unexpected_exits": 1,
            "last_error": None,
        }
    )

    payload = repair._read_runtime_with_liveness(adapter)

    assert payload["rows_scanned"] == 12
    assert payload["scanner_cycle_last_scan_at"] == "2026-09-05T17:51:00+00:00"
    assert payload["scanner_cycle_last_error"] == "RuntimeError: scanner cycle failed"
    assert payload["scanner_cycle_candidate_count"] == 3
    assert payload["scanner_cycle_opened_count"] == 1
    assert payload["worker_state"] == "running"
    assert payload["worker_starts"] == 2
    assert payload["worker_restarts"] == 1
    assert payload["worker_unexpected_exits"] == 1
    assert payload["strategy_thresholds_changed"] is False
    assert payload["paper_only"] is True
    assert payload["live_money_authority"] is False


def test_supervisor_restarts_only_after_terminal_worker_exit(monkeypatch) -> None:
    calls = 0

    async def fake_worker(_runtime, stop: asyncio.Event) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("initialization failed")
        stop.set()

    monkeypatch.setattr(repair, "_ORIGINAL_FOMO_WORKER", fake_worker)
    monkeypatch.setattr(repair, "RESTART_BACKOFF_SECONDS", 0.001)
    repair._STATE.update(
        {
            "state": "not_started",
            "starts": 0,
            "restarts": 0,
            "unexpected_exits": 0,
            "last_error": None,
        }
    )

    stop = asyncio.Event()
    asyncio.run(repair._supervised_independent_fomo_worker(object(), stop))

    assert calls == 2
    assert repair._STATE["starts"] == 2
    assert repair._STATE["restarts"] == 1
    assert repair._STATE["unexpected_exits"] == 1
    assert repair._STATE["state"] == "stopped"


def test_final_binding_uses_post_isolation_canonical_predecessor(monkeypatch) -> None:
    async def base_workers(_runtime, stop: asyncio.Event) -> None:
        await stop.wait()

    monkeypatch.setattr(canonical, "_INSTALLED", True)
    monkeypatch.setattr(canonical, "_ORIGINAL_RUNTIME_WORKERS", base_workers)
    monkeypatch.setattr(continuation, "_INSTALLED", True)
    monkeypatch.setattr(continuation, "_ORIGINAL_CANONICAL_WORKERS", None)
    monkeypatch.setattr(binding, "_INSTALLED", False)
    monkeypatch.setattr(
        binding,
        "_STATE",
        {"installed": False, "bound": False, "state": "not_started", "predecessor_name": None},
    )

    binding.install_fomo_canonical_worker_binding()

    assert continuation._ORIGINAL_CANONICAL_WORKERS is base_workers
    assert canonical._ORIGINAL_RUNTIME_WORKERS is continuation._canonical_workers_with_independent_fomo
    status = binding.binding_status()
    assert status["bound"] is True
    assert status["canonical_worker_points_to_fomo_wrapper"] is True
    assert status["strategy_thresholds_changed"] is False
    assert status["provider_scope_changed"] is False
    assert status["paper_only"] is True


def test_bound_canonical_graph_starts_fomo_and_persists_scan_heartbeat(monkeypatch) -> None:
    db = _runtime_db()
    adapter = SimpleNamespace(store=SimpleNamespace(db=db, _lock=threading.RLock()))

    async def base_workers(_runtime, stop: asyncio.Event) -> None:
        await stop.wait()

    async def heartbeat_worker(runtime, stop: asyncio.Event) -> None:
        with runtime.store._lock, runtime.store.db:
            runtime.store.db.execute(
                "INSERT OR REPLACE INTO independent_fomo_runtime(key,value,updated_at) VALUES (?,?,?)",
                ("last_scan_at", "2026-09-05T18:30:00+00:00", "2026-09-05T18:30:00+00:00"),
            )
            runtime.store.db.execute(
                "INSERT OR REPLACE INTO independent_fomo_runtime(key,value,updated_at) VALUES (?,?,?)",
                ("candidate_count", "0", "2026-09-05T18:30:00+00:00"),
            )
            runtime.store.db.execute(
                "INSERT OR REPLACE INTO independent_fomo_runtime(key,value,updated_at) VALUES (?,?,?)",
                ("opened_count", "0", "2026-09-05T18:30:00+00:00"),
            )
        await stop.wait()

    monkeypatch.setattr(canonical, "_INSTALLED", True)
    monkeypatch.setattr(canonical, "_ORIGINAL_RUNTIME_WORKERS", base_workers)
    monkeypatch.setattr(continuation, "_INSTALLED", True)
    monkeypatch.setattr(continuation, "_ORIGINAL_CANONICAL_WORKERS", None)
    monkeypatch.setattr(continuation, "_independent_fomo_worker", repair._supervised_independent_fomo_worker)
    monkeypatch.setattr(repair, "_ORIGINAL_FOMO_WORKER", heartbeat_worker)
    monkeypatch.setattr(repair, "_ORIGINAL_READ_RUNTIME", lambda _adapter: {})
    monkeypatch.setattr(repair.continuation, "_continuation_schema", lambda _adapter: None)
    monkeypatch.setattr(binding, "_INSTALLED", False)
    monkeypatch.setattr(
        binding,
        "_STATE",
        {"installed": False, "bound": False, "state": "not_started", "predecessor_name": None},
    )
    repair._STATE.update(
        {
            "state": "installed_waiting_for_runtime",
            "starts": 0,
            "restarts": 0,
            "unexpected_exits": 0,
            "last_error": None,
        }
    )

    binding.install_fomo_canonical_worker_binding()

    async def scenario() -> dict[str, object]:
        stop = asyncio.Event()
        task = asyncio.create_task(canonical._ORIGINAL_RUNTIME_WORKERS(adapter, stop))
        try:
            for _ in range(200):
                with adapter.store._lock:
                    row = adapter.store.db.execute(
                        "SELECT value FROM independent_fomo_runtime WHERE key='last_scan_at'"
                    ).fetchone()
                if repair._STATE["state"] == "running" and row is not None:
                    break
                await asyncio.sleep(0.001)
            return repair._read_runtime_with_liveness(adapter)
        finally:
            stop.set()
            await task

    payload = asyncio.run(scenario())

    assert payload["worker_state"] == "running"
    assert payload["worker_starts"] == 1
    assert payload["scanner_cycle_last_scan_at"] == "2026-09-05T18:30:00+00:00"
    assert payload["scanner_cycle_candidate_count"] == 0
    assert payload["scanner_cycle_opened_count"] == 0
    assert payload["canonical_worker_binding"]["bound"] is True
    assert payload["canonical_worker_binding"]["canonical_worker_points_to_fomo_wrapper"] is True
