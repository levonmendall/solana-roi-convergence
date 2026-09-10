from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

import pytest

from solana_roi import certification_incremental_replication as replication


class _Store:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.RLock()
        self.db = sqlite3.connect(path, check_same_thread=False)

    def close(self) -> None:
        self.db.close()


class _PruneFailureConnection:
    def __init__(self, inner: sqlite3.Connection, message: str) -> None:
        self.inner = inner
        self.message = message

    def __enter__(self):
        self.inner.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb):
        return self.inner.__exit__(exc_type, exc, tb)

    def execute(self, sql: str, parameters=()):
        if sql.startswith(f'DELETE FROM "{replication.CHANGE_TABLE}" WHERE id<=?'):
            raise sqlite3.OperationalError(self.message)
        return self.inner.execute(sql, parameters)

    def __getattr__(self, name: str):
        return getattr(self.inner, name)


def _store(tmp_path: Path) -> _Store:
    store = _Store(tmp_path / "source.sqlite3")
    with store.db:
        store.db.execute("CREATE TABLE sample(id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
    return store


def _seed_acknowledged_delta(store: _Store) -> tuple[dict[str, object], int]:
    identity = replication.prepare_bootstrap(store)
    with store.db:
        store.db.execute("INSERT INTO sample(id,value) VALUES (1,'a')")
    first = replication._delta_payload(
        store,
        from_watermark=0,
        epoch=str(identity["epoch"]),
        schema_fingerprint=str(identity["schema_fingerprint"]),
    )
    return identity, int(first["to_watermark"])


def test_prune_sqlite_lock_defers_cleanup_and_preserves_valid_delta(tmp_path: Path) -> None:
    store = _store(tmp_path)
    try:
        identity, watermark = _seed_acknowledged_delta(store)
        underlying = store.db
        before = underlying.execute(
            f'SELECT COUNT(*) FROM "{replication.CHANGE_TABLE}" WHERE id<=?', (watermark,)
        ).fetchone()[0]
        assert before > 0

        store.db = _PruneFailureConnection(underlying, "database is locked")
        payload = replication._delta_payload(
            store,
            from_watermark=watermark,
            epoch=str(identity["epoch"]),
            schema_fingerprint=str(identity["schema_fingerprint"]),
        )

        assert payload["from_watermark"] == watermark
        assert payload["source_change_count"] == 0
        assert payload["paper_only"] is True
        assert payload["live_money_authority"] is False
        after = underlying.execute(
            f'SELECT COUNT(*) FROM "{replication.CHANGE_TABLE}" WHERE id<=?', (watermark,)
        ).fetchone()[0]
        assert after == before
    finally:
        store.close()


def test_prune_unrelated_operational_error_still_fails_closed(tmp_path: Path) -> None:
    store = _store(tmp_path)
    try:
        identity, watermark = _seed_acknowledged_delta(store)
        underlying = store.db
        store.db = _PruneFailureConnection(underlying, "disk I/O error")

        with pytest.raises(sqlite3.OperationalError, match="disk I/O error"):
            replication._delta_payload(
                store,
                from_watermark=watermark,
                epoch=str(identity["epoch"]),
                schema_fingerprint=str(identity["schema_fingerprint"]),
            )
    finally:
        store.close()


def test_prune_helper_recognizes_only_busy_or_locked_primary_codes() -> None:
    locked = sqlite3.OperationalError("opaque")
    locked.sqlite_errorcode = sqlite3.SQLITE_LOCKED | (7 << 8)
    assert replication._is_transient_prune_lock(locked) is True

    busy = sqlite3.OperationalError("opaque")
    busy.sqlite_errorcode = sqlite3.SQLITE_BUSY | (5 << 8)
    assert replication._is_transient_prune_lock(busy) is True

    unrelated = sqlite3.OperationalError("database is locked-ish")
    assert replication._is_transient_prune_lock(unrelated) is False
