from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from solana_roi.observation_store import ObservationEventStore


def _seed_first_touches(store: ObservationEventStore, rows: int) -> None:
    now = datetime.now(timezone.utc)
    recent = (now - timedelta(seconds=30)).isoformat()
    old = (now - timedelta(days=2)).isoformat()
    payload = []
    for index in range(rows):
        observed_at = recent if index >= rows - 100 else old
        payload.append(
            (
                f"mint-{index:08d}",
                f"sig-{index:08d}",
                f"wallet-{index:08d}",
                f"entity-{index:08d}",
                "A",
                observed_at,
                1.0,
            )
        )
    with store._lock, store.db:
        store.db.executemany(
            "INSERT INTO token_first_touches("
            "token_mint, signature, wallet, entity_id, tier, observed_at, reference_price_sol"
            ") VALUES (?, ?, ?, ?, ?, ?, ?)",
            payload,
        )


def _tracked_mints_vm_steps(path: Path, rows: int) -> int:
    store = ObservationEventStore(path)
    _seed_first_touches(store, rows)
    callbacks = 0

    def progress() -> int:
        nonlocal callbacks
        callbacks += 1
        return 0

    store.db.set_progress_handler(progress, 100)
    result = store.tracked_mints(
        as_of=datetime.now(timezone.utc),
        horizon_seconds=300.0,
        limit=100,
    )
    store.db.set_progress_handler(None, 0)
    store.close()
    assert len(result) == 100
    return callbacks * 100


def test_shadow_price_tracked_mints_query_is_history_bounded(tmp_path: Path) -> None:
    """Tenfold historical growth must not multiply per-second price-clock DB work."""
    small_steps = _tracked_mints_vm_steps(tmp_path / "small.sqlite3", 6_000)
    large_steps = _tracked_mints_vm_steps(tmp_path / "large.sqlite3", 60_000)

    assert small_steps > 0
    assert large_steps <= small_steps * 2, (small_steps, large_steps)


def test_shadow_price_tracked_mints_avoids_full_scan_and_temp_sort(tmp_path: Path) -> None:
    store = ObservationEventStore(tmp_path / "plan.sqlite3")
    _seed_first_touches(store, 50_000)
    cutoff = (datetime.now(timezone.utc) - timedelta(seconds=300)).isoformat()

    with store._lock:
        plan = store.db.execute(
            "EXPLAIN QUERY PLAN "
            "SELECT token_mint, MAX(observed_at) AS last_touch FROM token_first_touches "
            "WHERE observed_at>=? GROUP BY token_mint ORDER BY last_touch DESC LIMIT ?",
            (cutoff, 100),
        ).fetchall()
    detail = " | ".join(str(row[3]) for row in plan)
    store.close()

    assert "SCAN token_first_touches" not in detail, detail
    assert "USE TEMP B-TREE FOR ORDER BY" not in detail, detail
