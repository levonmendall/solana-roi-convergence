from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

from solana_roi.direct_solana import DirectSolanaJournal


class _Store:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.RLock()
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row

    def close(self) -> None:
        self.db.close()


def _seed_hydration_metrics(store: _Store, rows: int) -> DirectSolanaJournal:
    journal = DirectSolanaJournal(store)
    payload = []
    for index in range(rows):
        # Keep the newest 500 rows identical in meaning between database sizes.
        # Older rows are historical *volume*, not part of the requested working set.
        second = index % 60
        minute = (index // 60) % 60
        hour = (index // 3600) % 24
        day = 1 + ((index // 86400) % 28)
        hydrated_at = f"2026-09-{day:02d}T{hour:02d}:{minute:02d}:{second:02d}+00:00"
        payload.append(
            (
                f"sig-{index:08d}",
                "PUMP_FUN",
                hydrated_at,
                hydrated_at,
                "rpc",
                1.0,
                float(index % 1000),
                index % 2,
                0,
                0,
            )
        )
    with store._lock, store.db:
        store.db.executemany(
            "INSERT INTO direct_solana_hydration_metrics("
            "signature, source, trigger_received_at, hydrated_at, rpc_provider, rpc_latency_ms, "
            "total_hydration_ms, normalized, candidate_context_prefilled, historical_recovery"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            payload,
        )
    return journal


def _status_vm_steps(path: Path, rows: int) -> int:
    store = _Store(path)
    journal = _seed_hydration_metrics(store, rows)
    callbacks = 0

    def progress() -> int:
        nonlocal callbacks
        callbacks += 1
        return 0

    store.db.set_progress_handler(progress, 100)
    payload = journal.status()
    store.db.set_progress_handler(None, 0)
    store.close()

    assert payload["hydration_sample_count"] == 500
    return callbacks * 100


def test_direct_solana_hydration_status_work_is_history_bounded(tmp_path: Path) -> None:
    """Tenfold old hydration growth must not multiply a 500-row status snapshot."""
    small_steps = _status_vm_steps(tmp_path / "small.sqlite3", 6_000)
    large_steps = _status_vm_steps(tmp_path / "large.sqlite3", 60_000)

    assert small_steps > 0
    assert large_steps <= small_steps * 2, (small_steps, large_steps)


def test_current_hydration_status_query_exposes_history_scaled_plan(tmp_path: Path) -> None:
    """Diagnostic proof: the current source query scans and temp-sorts hydration history."""
    store = _Store(tmp_path / "plan.sqlite3")
    _seed_hydration_metrics(store, 6_000)
    with store._lock:
        plan = store.db.execute(
            "EXPLAIN QUERY PLAN "
            "SELECT total_hydration_ms, normalized FROM direct_solana_hydration_metrics "
            "WHERE historical_recovery=0 ORDER BY hydrated_at DESC LIMIT 500"
        ).fetchall()
    detail = " | ".join(str(row[3]) for row in plan)
    store.close()

    assert "SCAN direct_solana_hydration_metrics" in detail, detail
    assert "USE TEMP B-TREE FOR ORDER BY" in detail, detail
