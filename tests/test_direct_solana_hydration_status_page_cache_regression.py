from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path

from solana_roi.direct_solana import DirectSolanaJournal
from solana_roi.direct_solana_hydration_status_repair import (
    BOOTSTRAP_BATCH_ROWS,
    _advance_bootstrap,
    _bounded_status,
    _ensure_state,
)


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


def _bootstrap_until_ready(journal: DirectSolanaJournal) -> None:
    for _ in range(100_000):
        complete, _batch_rows, _cursor = _advance_bootstrap(journal)
        if complete:
            return
    raise AssertionError("hydration status bootstrap did not complete")


def _steady_status_vm_steps(path: Path, rows: int) -> int:
    store = _Store(path)
    journal = _seed_hydration_metrics(store, rows)
    _bootstrap_until_ready(journal)
    callbacks = 0

    def progress() -> int:
        nonlocal callbacks
        callbacks += 1
        return 0

    store.db.set_progress_handler(progress, 100)
    payload = _bounded_status(journal)
    store.db.set_progress_handler(None, 0)
    store.close()

    assert payload["hydration_sample_count"] == 500
    assert payload["hydration_status_repair"]["bootstrap_complete"] is True
    return callbacks * 100


def test_direct_solana_hydration_status_steady_work_is_history_bounded(tmp_path: Path) -> None:
    """Tenfold source growth must not multiply the steady 500-row status read."""
    small_steps = _steady_status_vm_steps(tmp_path / "small.sqlite3", 6_000)
    large_steps = _steady_status_vm_steps(tmp_path / "large.sqlite3", 60_000)

    assert small_steps > 0
    assert large_steps <= small_steps * 2, (small_steps, large_steps)


def test_hydration_status_bootstrap_advances_one_keyset_batch_per_call(tmp_path: Path) -> None:
    store = _Store(tmp_path / "bounded-bootstrap.sqlite3")
    journal = _seed_hydration_metrics(store, BOOTSTRAP_BATCH_ROWS * 3)
    source_before = store.db.execute(
        "SELECT COUNT(*) FROM direct_solana_hydration_metrics"
    ).fetchone()[0]

    complete, batch_rows, cursor = _advance_bootstrap(journal)

    source_after = store.db.execute(
        "SELECT COUNT(*) FROM direct_solana_hydration_metrics"
    ).fetchone()[0]
    store.close()

    assert complete is False
    assert batch_rows == BOOTSTRAP_BATCH_ROWS
    assert cursor == BOOTSTRAP_BATCH_ROWS
    assert source_after == source_before


def test_hydration_status_steady_query_uses_recent_state_index(tmp_path: Path) -> None:
    store = _Store(tmp_path / "plan.sqlite3")
    journal = _seed_hydration_metrics(store, 20_000)
    _bootstrap_until_ready(journal)

    with store._lock:
        plan = store.db.execute(
            "EXPLAIN QUERY PLAN "
            "SELECT total_hydration_ms, normalized "
            "FROM direct_solana_hydration_status_recent "
            "ORDER BY hydrated_at DESC, signature DESC LIMIT ?",
            (500,),
        ).fetchall()
    detail = " | ".join(str(row[3]) for row in plan)
    store.close()

    assert "direct_solana_hydration_metrics" not in detail, detail
    assert "USE TEMP B-TREE FOR ORDER BY" not in detail, detail
    assert "ix_direct_hydration_status_recent_time" in detail, detail


def test_hydration_status_trigger_captures_new_write_during_bootstrap(tmp_path: Path) -> None:
    store = _Store(tmp_path / "trigger.sqlite3")
    journal = _seed_hydration_metrics(store, BOOTSTRAP_BATCH_ROWS * 2)
    complete, batch_rows, _cursor = _advance_bootstrap(journal)
    assert complete is False
    assert batch_rows == BOOTSTRAP_BATCH_ROWS

    now = datetime.now(timezone.utc)
    journal.record_hydration(
        signature="sig-live-during-bootstrap",
        source="PUMP_FUN",
        trigger_received_at=now,
        hydrated_at=now,
        rpc_provider="rpc",
        rpc_latency_ms=1.0,
        normalized=True,
    )
    _bootstrap_until_ready(journal)

    source_row = store.db.execute(
        "SELECT 1 FROM direct_solana_hydration_metrics "
        "WHERE signature='sig-live-during-bootstrap'"
    ).fetchone()
    state_row = store.db.execute(
        "SELECT normalized FROM direct_solana_hydration_status_recent "
        "WHERE signature='sig-live-during-bootstrap'"
    ).fetchone()
    store.close()

    assert source_row is not None
    assert state_row is not None
    assert int(state_row["normalized"]) == 1


def test_hydration_status_helper_does_not_mutate_source_history(tmp_path: Path) -> None:
    store = _Store(tmp_path / "source-preserved.sqlite3")
    journal = _seed_hydration_metrics(store, BOOTSTRAP_BATCH_ROWS + 17)
    source_before = store.db.execute(
        "SELECT COUNT(*) FROM direct_solana_hydration_metrics"
    ).fetchone()[0]
    _ensure_state(journal)
    _bootstrap_until_ready(journal)
    source_after = store.db.execute(
        "SELECT COUNT(*) FROM direct_solana_hydration_metrics"
    ).fetchone()[0]
    store.close()

    assert source_after == source_before
