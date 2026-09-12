from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from solana_roi.observation_store import ObservationEventStore
from solana_roi.shadow_price_tracking_state_repair import (
    BOOTSTRAP_BATCH_ROWS,
    _advance_bootstrap,
    _bounded_tracked_mints,
)


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


def _bootstrap_until_ready(store: ObservationEventStore) -> list[str]:
    result: list[str] = []
    for _ in range(10_000):
        result = _bounded_tracked_mints(
            store,
            as_of=datetime.now(timezone.utc),
            horizon_seconds=300.0,
            limit=100,
        )
        if result:
            return result
    raise AssertionError("bounded tracked-mint bootstrap did not complete")


def _steady_state_vm_steps(path: Path, rows: int) -> int:
    store = ObservationEventStore(path)
    _seed_first_touches(store, rows)
    result = _bootstrap_until_ready(store)
    assert len(result) == 100

    callbacks = 0

    def progress() -> int:
        nonlocal callbacks
        callbacks += 1
        return 0

    store.db.set_progress_handler(progress, 100)
    result = _bounded_tracked_mints(
        store,
        as_of=datetime.now(timezone.utc),
        horizon_seconds=300.0,
        limit=100,
    )
    store.db.set_progress_handler(None, 0)
    store.close()
    assert len(result) == 100
    return callbacks * 100


def test_shadow_price_tracked_mints_query_is_history_bounded(tmp_path: Path) -> None:
    """Tenfold historical growth must not multiply steady-state price-clock DB work."""
    small_steps = _steady_state_vm_steps(tmp_path / "small.sqlite3", 6_000)
    large_steps = _steady_state_vm_steps(tmp_path / "large.sqlite3", 60_000)

    assert small_steps > 0
    assert large_steps <= small_steps * 2, (small_steps, large_steps)


def test_shadow_price_tracked_mints_uses_recent_state_index(tmp_path: Path) -> None:
    store = ObservationEventStore(tmp_path / "plan.sqlite3")
    _seed_first_touches(store, 50_000)
    _bootstrap_until_ready(store)
    cutoff = (datetime.now(timezone.utc) - timedelta(seconds=300)).isoformat()

    with store._lock:
        plan = store.db.execute(
            "EXPLAIN QUERY PLAN "
            "SELECT token_mint FROM shadow_price_tracked_mints_state "
            "WHERE observed_at>=? ORDER BY observed_at DESC, token_mint LIMIT ?",
            (cutoff, 100),
        ).fetchall()
    detail = " | ".join(str(row[3]) for row in plan)
    store.close()

    assert "token_first_touches" not in detail, detail
    assert "USE TEMP B-TREE FOR ORDER BY" not in detail, detail
    assert "ix_shadow_price_tracked_mints_state_observed" in detail, detail


def test_shadow_price_bootstrap_advances_one_keyset_batch_per_tick(tmp_path: Path) -> None:
    store = ObservationEventStore(tmp_path / "bounded-bootstrap.sqlite3")
    _seed_first_touches(store, BOOTSTRAP_BATCH_ROWS * 3)
    source_count_before = store.db.execute("SELECT COUNT(*) FROM token_first_touches").fetchone()[0]

    complete, batch_rows, cursor = _advance_bootstrap(
        store,
        as_of=datetime.now(timezone.utc),
        horizon_seconds=300.0,
    )

    source_count_after = store.db.execute("SELECT COUNT(*) FROM token_first_touches").fetchone()[0]
    store.close()

    assert complete is False
    assert batch_rows == BOOTSTRAP_BATCH_ROWS
    assert cursor == BOOTSTRAP_BATCH_ROWS
    assert source_count_after == source_count_before


def test_shadow_price_tracking_trigger_captures_new_first_touch_during_bootstrap(tmp_path: Path) -> None:
    store = ObservationEventStore(tmp_path / "trigger.sqlite3")
    _seed_first_touches(store, BOOTSTRAP_BATCH_ROWS * 2)
    now = datetime.now(timezone.utc)

    complete, batch_rows, _cursor = _advance_bootstrap(
        store,
        as_of=now,
        horizon_seconds=300.0,
    )
    assert complete is False
    assert batch_rows == BOOTSTRAP_BATCH_ROWS

    with store._lock, store.db:
        store.db.execute(
            "INSERT INTO token_first_touches("
            "token_mint, signature, wallet, entity_id, tier, observed_at, reference_price_sol"
            ") VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                "mint-live-during-bootstrap",
                "sig-live-during-bootstrap",
                "wallet-live-during-bootstrap",
                "entity-live-during-bootstrap",
                "A",
                now.isoformat(),
                1.0,
            ),
        )

    result = _bootstrap_until_ready(store)
    source_row = store.db.execute(
        "SELECT 1 FROM token_first_touches WHERE token_mint='mint-live-during-bootstrap'"
    ).fetchone()
    state_row = store.db.execute(
        "SELECT 1 FROM shadow_price_tracked_mints_state "
        "WHERE token_mint='mint-live-during-bootstrap'"
    ).fetchone()
    store.close()

    assert "mint-live-during-bootstrap" in result
    assert source_row is not None
    assert state_row is not None
