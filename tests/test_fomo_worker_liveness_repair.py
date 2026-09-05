from __future__ import annotations

import asyncio
import sqlite3
import threading
from types import SimpleNamespace

from solana_roi import fomo_worker_liveness_repair as repair


def test_runtime_reader_exposes_existing_cycle_heartbeat(monkeypatch) -> None:
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    db.execute(
        "CREATE TABLE independent_fomo_runtime (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL)"
    )
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
