from __future__ import annotations

import asyncio
import sqlite3
import threading
import time
from datetime import datetime, timezone
from types import SimpleNamespace

from solana_roi import wallet_evidence_rpc_repair as repair
from solana_roi import wallet_live_priority_repair as priority


def test_risk_worker_claim_runs_off_event_loop_and_without_duplicate_sync(monkeypatch) -> None:
    main_thread = threading.get_ident()
    claim_threads: list[int] = []
    sync_calls = 0

    def fake_sync(_self) -> None:
        nonlocal sync_calls
        sync_calls += 1
        raise AssertionError("worker must not call _sync_risk_work separately before claim")

    def fake_claim(_self):
        claim_threads.append(threading.get_ident())
        time.sleep(0.04)
        return None

    monkeypatch.setattr(priority, "_sync_risk_work", fake_sync)
    monkeypatch.setattr(priority, "_claim_risk_work", fake_claim)

    tracker = SimpleNamespace(_roi_risk_queue_offloop_calls=0)
    stop = asyncio.Event()

    async def scenario() -> int:
        task = asyncio.create_task(repair._risk_worker_no_lookahead(tracker, stop))
        ticks = 0
        while not claim_threads:
            ticks += 1
            await asyncio.sleep(0.005)
        stop.set()
        await task
        return ticks

    ticks = asyncio.run(scenario())
    assert ticks >= 1
    assert claim_threads
    assert all(thread_id != main_thread for thread_id in claim_threads)
    assert sync_calls == 0
    assert tracker._roi_risk_queue_offloop_calls >= 1


def test_tracker_init_installs_forward_risk_pending_index(monkeypatch) -> None:
    connection = sqlite3.connect(":memory:", check_same_thread=False)
    connection.row_factory = sqlite3.Row
    connection.execute(
        "CREATE TABLE wallet_discovery_forward_observations ("
        "signature TEXT PRIMARY KEY, tracking_transport TEXT, side TEXT, risk_complete INTEGER, received_at TEXT)"
    )

    class Store:
        def __init__(self):
            self.db = connection
            self._lock = threading.RLock()

    tracker = SimpleNamespace(store=Store())
    discovery = SimpleNamespace()

    def fake_original_init(self, _discovery) -> None:
        self.store = tracker.store

    monkeypatch.setattr(repair, "_ORIGINAL_TRACKER_INIT", fake_original_init)
    repair._tracker_init(tracker, discovery)

    indexes = {
        str(row["name"])
        for row in connection.execute("PRAGMA index_list(wallet_discovery_forward_observations)").fetchall()
    }
    assert "ix_wallet_forward_risk_pending" in indexes
    assert tracker._roi_risk_queue_offloop_calls == 0
    assert discovery._roi_risk_prewarm_attempts == 0


def test_finish_risk_work_runs_off_event_loop(monkeypatch) -> None:
    main_thread = threading.get_ident()
    called: list[tuple[int, str, str]] = []

    def fake_finish(_self, signature, *, status, error=None, retry_after_seconds=0.0) -> None:
        called.append((threading.get_ident(), signature, status))
        time.sleep(0.02)

    monkeypatch.setattr(priority, "_finish_risk_work", fake_finish)
    tracker = SimpleNamespace(_roi_risk_queue_offloop_calls=0)
    asyncio.run(repair._finish_risk_work_offloop(tracker, "sig", status="complete"))

    assert called == [(called[0][0], "sig", "complete")]
    assert called[0][0] != main_thread
    assert tracker._roi_risk_queue_offloop_calls == 1
