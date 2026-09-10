from __future__ import annotations

import sqlite3
import threading
from pathlib import Path
from typing import Any

import pytest
from fastapi import HTTPException

from solana_roi import certification_incremental_replication as replication
from solana_roi import certification_logical_bootstrap as bootstrap


class _Store:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.RLock()
        self.db = sqlite3.connect(path, check_same_thread=False)

    def close(self) -> None:
        self.db.close()


def _sample_store(path: Path, rows: int = 8) -> _Store:
    store = _Store(path)
    with store.db:
        store.db.execute("PRAGMA journal_mode=WAL")
        store.db.execute("CREATE TABLE sample(id INTEGER PRIMARY KEY,value TEXT NOT NULL)")
        store.db.executemany(
            "INSERT INTO sample(id,value) VALUES (?,?)",
            [(index, f"value-{index}") for index in range(1, rows + 1)],
        )
    return store


def test_page_releases_sqlite_snapshot_before_json_materialization(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _sample_store(tmp_path / "source.sqlite3")
    try:
        identity = replication.prepare_bootstrap(store)
        real_pinned_reader = bootstrap._pinned_reader
        real_stream = bootstrap._stream_page_records
        state = {"closed": False}

        class _TrackingReader:
            def __init__(self, connection: sqlite3.Connection) -> None:
                self.connection = connection

            def execute(self, *args: Any, **kwargs: Any):
                return self.connection.execute(*args, **kwargs)

            def close(self) -> None:
                self.connection.close()
                state["closed"] = True

        monkeypatch.setattr(
            bootstrap,
            "_pinned_reader",
            lambda current_store: _TrackingReader(real_pinned_reader(current_store)),
        )

        def checked_stream(*args: Any, **kwargs: Any):
            assert state["closed"] is True
            return real_stream(*args, **kwargs)

        monkeypatch.setattr(bootstrap, "_stream_page_records", checked_stream)
        page = bootstrap._page(
            store,
            table_name="sample",
            epoch=str(identity["epoch"]),
            fingerprint=str(identity["schema_fingerprint"]),
            cursor=None,
            limit=2,
        )
        assert page["row_count"] == 2
        assert page["done"] is False
    finally:
        store.close()


def test_page_avoids_full_schema_and_all_table_rescans(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _sample_store(tmp_path / "source.sqlite3")
    try:
        identity = replication.prepare_bootstrap(store)

        def forbidden(*args: Any, **kwargs: Any):
            raise AssertionError("history page must not perform a whole-schema/table walk")

        monkeypatch.setattr(replication, "_schema_fingerprint", forbidden)
        monkeypatch.setattr(replication, "_ordinary_tables", forbidden)
        page = bootstrap._page(
            store,
            table_name="sample",
            epoch=str(identity["epoch"]),
            fingerprint=str(identity["schema_fingerprint"]),
            cursor=None,
            limit=3,
        )
        assert page["row_count"] == 3
        assert page["next_cursor"] is not None
    finally:
        store.close()


def test_light_page_identity_still_fails_closed_on_schema_change(tmp_path: Path) -> None:
    store = _sample_store(tmp_path / "source.sqlite3")
    try:
        identity = replication.prepare_bootstrap(store)
        with store.db:
            store.db.execute("CREATE INDEX sample_value_idx ON sample(value)")
        with pytest.raises(HTTPException) as changed:
            bootstrap._page(
                store,
                table_name="sample",
                epoch=str(identity["epoch"]),
                fingerprint=str(identity["schema_fingerprint"]),
                cursor=None,
                limit=2,
            )
        assert changed.value.status_code == 409
        assert "schema_changed" in str(changed.value.detail)
    finally:
        store.close()


def test_raw_prefetch_stops_after_first_proven_page_byte_overflow() -> None:
    megabyte = "x" * (1024 * 1024)

    class _Cursor:
        def __init__(self) -> None:
            self.rows = iter((index, megabyte) for index in range(20))

        def fetchone(self):
            return next(self.rows, None)

    rows = bootstrap._fetch_bounded_raw_rows(
        _Cursor(),
        bounded_limit=500,
        max_bytes=2 * 1024 * 1024,
    )
    # Retain the one overflow row so the exact JSON byte-bound code can defer it;
    # do not materialize the remaining history into Python while the reader is open.
    assert 1 < len(rows) <= 3


def test_rowid_keyset_query_uses_integer_primary_key_seek(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _sample_store(tmp_path / "source.sqlite3", rows=1_000)
    try:
        identity = replication.prepare_bootstrap(store)
        real_pinned_reader = bootstrap._pinned_reader
        traced: list[str] = []

        def traced_reader(current_store: _Store) -> sqlite3.Connection:
            reader = real_pinned_reader(current_store)
            reader.set_trace_callback(traced.append)
            return reader

        monkeypatch.setattr(bootstrap, "_pinned_reader", traced_reader)
        page = bootstrap._page(
            store,
            table_name="sample",
            epoch=str(identity["epoch"]),
            fingerprint=str(identity["schema_fingerprint"]),
            cursor=bootstrap._encode_cursor([900]),
            limit=2,
        )
        assert [row["rowid"] for row in page["rows"]] == [901, 902]
        page_query = next(
            statement
            for statement in traced
            if statement.startswith("SELECT rowid,") and 'FROM "sample" WHERE rowid>' in statement
        )
        plan = store.db.execute("EXPLAIN QUERY PLAN " + page_query).fetchall()
        details = " ".join(str(row[-1]).upper() for row in plan)
        assert "SEARCH SAMPLE USING INTEGER PRIMARY KEY" in details
        assert "USE TEMP B-TREE" not in details
    finally:
        store.close()


def test_key_changing_update_is_exactly_reconciled_from_bootstrap_watermark(tmp_path: Path) -> None:
    store = _Store(tmp_path / "source.sqlite3")
    try:
        with store.db:
            store.db.execute(
                "CREATE TABLE composite(a TEXT NOT NULL,b TEXT NOT NULL,value TEXT NOT NULL,"
                "PRIMARY KEY(a,b)) WITHOUT ROWID"
            )
            store.db.execute("INSERT INTO composite(a,b,value) VALUES ('a','1','before')")
        identity = replication.prepare_bootstrap(store)
        with store.db:
            store.db.execute(
                "UPDATE composite SET a='z',value='after' WHERE a='a' AND b='1'"
            )

        payload = replication._delta_payload(
            store,
            from_watermark=int(identity["watermark"]),
            epoch=str(identity["epoch"]),
            schema_fingerprint=str(identity["schema_fingerprint"]),
        )
        assert payload["source_change_count"] == 2
        assert payload["coalesced_change_count"] == 2
        sql = [str(change["sql"]) for change in payload["changes"]]
        assert any(statement.startswith('DELETE FROM "composite"') for statement in sql)
        assert any(statement.startswith('INSERT OR REPLACE INTO "composite"') for statement in sql)

        replica = sqlite3.connect(tmp_path / "replica.sqlite3")
        try:
            replica.execute(
                "CREATE TABLE composite(a TEXT NOT NULL,b TEXT NOT NULL,value TEXT NOT NULL,"
                "PRIMARY KEY(a,b)) WITHOUT ROWID"
            )
            replica.execute("INSERT INTO composite(a,b,value) VALUES ('a','1','before')")
            for statement in sql:
                replica.execute(statement)
            assert replica.execute("SELECT a,b,value FROM composite").fetchall() == [("z", "1", "after")]
        finally:
            replica.close()
    finally:
        store.close()
