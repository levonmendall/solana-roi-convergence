from __future__ import annotations

import sqlite3

from solana_roi.active_storage import ActiveStorage, ActiveStorageBudget
from solana_roi.storage import AppendOnlyEventStore


def _page_limits(connection: sqlite3.Connection) -> tuple[int, int]:
    page_size = int(connection.execute("PRAGMA page_size").fetchone()[0])
    max_page_count = int(connection.execute("PRAGMA max_page_count").fetchone()[0])
    return page_size, max_page_count


def test_active_page_budget_is_reinstalled_on_every_storage_connection(tmp_path):
    database = tmp_path / "active.sqlite3"
    hard_bytes = 8 * 1024 * 1024
    budget = ActiveStorageBudget(
        warning_bytes=6 * 1024 * 1024,
        hard_bytes=hard_bytes,
        max_wal_bytes=2 * 1024 * 1024,
    )
    storage = ActiveStorage(database, budget=budget)
    storage.initialize(epoch_id="test-epoch")

    # SQLite does not persist max_page_count across arbitrary new connections.
    raw = sqlite3.connect(database)
    try:
        raw_page_size, raw_max_pages = _page_limits(raw)
    finally:
        raw.close()
    expected_pages = hard_bytes // raw_page_size
    assert raw_max_pages > expected_pages

    # ActiveStorage must therefore re-install the ceiling every time it connects.
    with storage.connect() as connection:
        page_size, max_pages = _page_limits(connection)
    assert page_size == raw_page_size
    assert max_pages == expected_pages

    with storage.connect() as second_connection:
        _, second_max_pages = _page_limits(second_connection)
    assert second_max_pages == expected_pages


def test_event_store_inherits_active_epoch_hard_budget(tmp_path):
    database = tmp_path / "active.sqlite3"
    hard_bytes = 8 * 1024 * 1024
    budget = ActiveStorageBudget(
        warning_bytes=6 * 1024 * 1024,
        hard_bytes=hard_bytes,
        max_wal_bytes=2 * 1024 * 1024,
    )
    ActiveStorage(database, budget=budget).initialize(epoch_id="test-epoch")

    store = AppendOnlyEventStore(database)
    try:
        page_size, max_pages = _page_limits(store.db)
        assert max_pages == hard_bytes // page_size

        # The independently-owned long-lived writer is physically refused before
        # it can allocate pages beyond the active epoch's declared hard budget.
        store.db.execute("CREATE TABLE page_boundary_probe(payload BLOB NOT NULL)")
        store.db.commit()
        failed_closed = False
        for _ in range(256):
            try:
                store.db.execute(
                    "INSERT INTO page_boundary_probe(payload) VALUES(?)",
                    (b"x" * (64 * 1024),),
                )
                store.db.commit()
            except sqlite3.OperationalError as exc:
                assert "full" in str(exc).lower()
                failed_closed = True
                store.db.rollback()
                break
        assert failed_closed, "event-store writer grew beyond the configured page budget"
        page_count = int(store.db.execute("PRAGMA page_count").fetchone()[0])
        assert page_count <= max_pages
    finally:
        store.close()
