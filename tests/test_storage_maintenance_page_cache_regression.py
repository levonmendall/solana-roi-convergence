from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

from solana_roi.continuity_storage_capacity_repair import MAINTENANCE_BATCH_ROWS
from solana_roi.direct_solana import DirectSolanaJournal


def _store(path: Path) -> SimpleNamespace:
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    store = SimpleNamespace(path=path, db=db, _lock=threading.RLock())
    DirectSolanaJournal(store)
    return store


def _seed_old_metrics(store: SimpleNamespace, rows: int) -> str:
    hydrated_at = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
    payload = [
        (
            f"sig-{index:08d}",
            "PUMP_FUN",
            hydrated_at,
            hydrated_at,
            "provider",
            1.0,
            1.0,
            1,
            0,
            0,
        )
        for index in range(rows)
    ]
    with store._lock, store.db:
        store.db.executemany(
            "INSERT INTO direct_solana_hydration_metrics("
            "signature, source, trigger_received_at, hydrated_at, rpc_provider, rpc_latency_ms, "
            "total_hydration_ms, normalized, candidate_context_prefilled, historical_recovery) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            payload,
        )
    return hydrated_at


def _prune_selector_plan(store: SimpleNamespace, cutoff: str) -> str:
    with store._lock:
        rows = store.db.execute(
            "EXPLAIN QUERY PLAN SELECT signature FROM direct_solana_hydration_metrics "
            "WHERE historical_recovery=0 AND hydrated_at<? "
            "ORDER BY hydrated_at, signature LIMIT ?",
            (cutoff, MAINTENANCE_BATCH_ROWS),
        ).fetchall()
    return " | ".join(str(row[3]) for row in rows)


def _selector_vm_steps(path: Path, rows: int) -> int:
    store = _store(path)
    cutoff = _seed_old_metrics(store, rows)
    callbacks = 0

    def progress() -> int:
        nonlocal callbacks
        callbacks += 1
        return 0

    store.db.set_progress_handler(progress, 100)
    with store._lock:
        store.db.execute(
            "SELECT signature FROM direct_solana_hydration_metrics "
            "WHERE historical_recovery=0 AND hydrated_at<? "
            "ORDER BY hydrated_at, signature LIMIT ?",
            (cutoff, MAINTENANCE_BATCH_ROWS),
        ).fetchall()
    store.db.set_progress_handler(None, 0)
    store.db.close()
    return callbacks * 100


def test_hydration_metric_prune_uses_bounded_indexed_tail(tmp_path: Path) -> None:
    """The production maintenance selector must not scan/sort full metric history."""
    store = _store(tmp_path / "plan.sqlite3")
    cutoff = _seed_old_metrics(store, 50_000)

    plan = _prune_selector_plan(store, cutoff)

    assert "SCAN direct_solana_hydration_metrics" not in plan, plan
    assert "USE TEMP B-TREE FOR ORDER BY" not in plan, plan
    store.db.close()


def test_hydration_metric_prune_work_does_not_scale_with_history(tmp_path: Path) -> None:
    """Tenfold historical growth must not cause near-tenfold SQLite VM work."""
    small_steps = _selector_vm_steps(tmp_path / "small.sqlite3", 6_000)
    large_steps = _selector_vm_steps(tmp_path / "large.sqlite3", 60_000)

    assert small_steps > 0
    assert large_steps <= small_steps * 2, (small_steps, large_steps)
