from __future__ import annotations

import sqlite3
import threading
import urllib.parse
from pathlib import Path

import pytest

from solana_roi import certification_incremental_replication as replication
from solana_roi import certification_logical_bootstrap as bootstrap_server
from solana_roi import certification_logical_bootstrap_client as bootstrap_client
from solana_roi import certification_replica_client as replica_client


class _Store:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.RLock()
        self.db = sqlite3.connect(path, check_same_thread=False)

    def close(self) -> None:
        self.db.close()


def _build_source(path: Path) -> _Store:
    store = _Store(path)
    with store.db:
        store.db.execute("CREATE TABLE sample(id INTEGER PRIMARY KEY AUTOINCREMENT,value TEXT NOT NULL)")
        store.db.execute("CREATE TABLE audit(id INTEGER PRIMARY KEY,value TEXT NOT NULL)")
        store.db.execute(
            "CREATE TABLE composite(a TEXT NOT NULL,b TEXT NOT NULL,value BLOB,PRIMARY KEY(a,b)) WITHOUT ROWID"
        )
        store.db.execute("CREATE INDEX sample_value_idx ON sample(value)")
        store.db.execute("CREATE VIEW sample_values AS SELECT id,value FROM sample")
        store.db.execute(
            "CREATE TRIGGER sample_audit AFTER INSERT ON sample BEGIN "
            "INSERT OR REPLACE INTO audit(id,value) VALUES (NEW.id,'seen:' || NEW.value); END"
        )
        store.db.executemany("INSERT INTO sample(value) VALUES (?)", [("one",), ("two",), ("three",), ("four",)])
        store.db.executemany(
            "INSERT INTO composite(a,b,value) VALUES (?,?,?)",
            [("a", "1", b"blob-a"), ("a", "2", b"blob-b"), ("b", "1", None)],
        )
        store.db.execute("UPDATE sqlite_sequence SET seq=41 WHERE name='sample'")
        store.db.execute("PRAGMA user_version=17")
        store.db.execute("PRAGMA application_id=424242")
    return store


def _fake_open_json(store: _Store, *, mutate_after_first_sample_page: bool = False):
    mutated = False

    def open_json(request, *, timeout=30.0):
        nonlocal mutated
        parsed = urllib.parse.urlparse(request.full_url)
        if parsed.path.endswith("/certification-db-logical-bootstrap"):
            payload = bootstrap_server._manifest(store)
            payload["page_default_rows"] = 2
            return payload
        if parsed.path.endswith("/certification-db-logical-bootstrap-page"):
            query = urllib.parse.parse_qs(parsed.query)
            payload = bootstrap_server._page(
                store,
                table_name=query["table"][0],
                epoch=query["epoch"][0],
                fingerprint=query["schema_fingerprint"][0],
                cursor=query.get("cursor", [None])[0],
                limit=int(query.get("limit", [2])[0]),
            )
            if mutate_after_first_sample_page and not mutated and query["table"][0] == "sample":
                mutated = True
                with store.db:
                    store.db.execute("UPDATE sample SET value='one-updated' WHERE id=1")
                    store.db.execute("DELETE FROM sample WHERE id=2")
                    store.db.execute("INSERT INTO sample(value) VALUES ('five')")
            return payload
        raise AssertionError(parsed.path)

    return open_json


def test_logical_bootstrap_reconstructs_schema_rows_and_sqlite_metadata_without_full_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release = "a" * 40
    monkeypatch.setenv("SOLANA_ROI_RELEASE_COMMIT", release)
    store = _build_source(tmp_path / "source.sqlite3")
    destination = tmp_path / "replica.sqlite3"
    try:
        monkeypatch.setattr(bootstrap_client, "_open_json", _fake_open_json(store))
        monkeypatch.setattr(
            replica_client,
            "download_snapshot_chunked",
            lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("full snapshot must not run")),
        )
        identity = bootstrap_client.logical_bootstrap(
            destination,
            base="https://authoritative.invalid",
            token="secret",
            expected_release=release,
        )
        assert identity["last_transport"] == "bounded_logical_bootstrap"
        assert identity["bootstrap_rows"] >= 11
        assert identity["sqlite_sequence_preserved"] is True
        assert identity["sqlite_pragma_metadata_preserved"] is True
        assert destination.is_file()

        replica = sqlite3.connect(destination)
        try:
            assert replica.execute("SELECT id,value FROM sample ORDER BY id").fetchall() == [
                (1, "one"), (2, "two"), (3, "three"), (4, "four")
            ]
            assert replica.execute("SELECT a,b,value FROM composite ORDER BY a,b").fetchall() == [
                ("a", "1", b"blob-a"), ("a", "2", b"blob-b"), ("b", "1", None)
            ]
            assert replica.execute("SELECT id,value FROM sample_values ORDER BY id").fetchall() == [
                (1, "one"), (2, "two"), (3, "three"), (4, "four")
            ]
            assert replica.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='sample_value_idx'"
            ).fetchone()[0] == 1
            assert replica.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name='sample_audit'"
            ).fetchone()[0] == 1
            assert replica.execute("SELECT seq FROM sqlite_sequence WHERE name='sample'").fetchone()[0] == 41
            assert replica.execute("PRAGMA user_version").fetchone()[0] == 17
            assert replica.execute("PRAGMA application_id").fetchone()[0] == 424242
            inserted = replica.execute("INSERT INTO sample(value) VALUES ('after-bootstrap')")
            assert inserted.lastrowid == 42
            replica.rollback()
        finally:
            replica.close()
    finally:
        store.close()


def test_journal_delta_repairs_mutations_that_race_with_keyset_bootstrap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release = "b" * 40
    monkeypatch.setenv("SOLANA_ROI_RELEASE_COMMIT", release)
    store = _build_source(tmp_path / "source.sqlite3")
    destination = tmp_path / "replica.sqlite3"
    try:
        monkeypatch.setattr(
            bootstrap_client,
            "_open_json",
            _fake_open_json(store, mutate_after_first_sample_page=True),
        )
        identity = bootstrap_client.logical_bootstrap(
            destination,
            base="https://authoritative.invalid",
            token="secret",
            expected_release=release,
        )
        state = {
            "client_version": replica_client.CLIENT_VERSION,
            "release_commit": release,
            "replication_version": replication.REPLICATION_VERSION,
            "epoch": identity["epoch"],
            "schema_fingerprint": identity["schema_fingerprint"],
            "watermark": identity["watermark"],
        }
        replica_client._atomic_state(replica_client._state_path(destination), state)
        payload = replication._delta_payload(
            store,
            from_watermark=int(state["watermark"]),
            epoch=str(state["epoch"]),
            schema_fingerprint=str(state["schema_fingerprint"]),
        )
        applied = replica_client._apply_delta(destination, state, payload)
        assert applied["caught_up"] is True

        source_rows = store.db.execute("SELECT id,value FROM sample ORDER BY id").fetchall()
        source_audit = store.db.execute("SELECT id,value FROM audit ORDER BY id").fetchall()
        replica = sqlite3.connect(destination)
        try:
            assert replica.execute("SELECT id,value FROM sample ORDER BY id").fetchall() == source_rows
            assert replica.execute("SELECT id,value FROM audit ORDER BY id").fetchall() == source_audit
        finally:
            replica.close()
    finally:
        store.close()


def test_delta_transport_pages_large_journal_instead_of_forcing_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release = "c" * 40
    monkeypatch.setenv("SOLANA_ROI_RELEASE_COMMIT", release)
    monkeypatch.setenv("SOLANA_ROI_CERTIFICATION_DELTA_MAX_ROWS", "100")
    store = _Store(tmp_path / "source.sqlite3")
    try:
        with store.db:
            store.db.execute("CREATE TABLE sample(id INTEGER PRIMARY KEY,value TEXT NOT NULL)")
        identity = replication.prepare_bootstrap(store)
        with store.db:
            store.db.executemany(
                "INSERT INTO sample(id,value) VALUES (?,?)",
                [(index, f"value-{index}") for index in range(1, 131)],
            )
        first = replication._delta_payload(
            store,
            from_watermark=int(identity["watermark"]),
            epoch=str(identity["epoch"]),
            schema_fingerprint=str(identity["schema_fingerprint"]),
        )
        assert first["source_change_count"] == 100
        assert first["caught_up"] is False
        assert first["bounded_batch"] is True
        assert first["full_snapshot_required"] is False

        second = replication._delta_payload(
            store,
            from_watermark=int(first["to_watermark"]),
            epoch=str(identity["epoch"]),
            schema_fingerprint=str(identity["schema_fingerprint"]),
        )
        assert second["source_change_count"] == 30
        assert second["caught_up"] is True
        assert second["full_snapshot_required"] is False
    finally:
        store.close()


def test_full_snapshot_bootstrap_is_disabled_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SOLANA_ROI_CERTIFIER_ALLOW_FULL_SNAPSHOT_BOOTSTRAP", raising=False)
    assert replica_client._allow_full_snapshot_recovery() is False
    status = replica_client.status()
    assert status["logical_bootstrap_default"] is True
    assert status["full_snapshot_bootstrap_enabled"] is False
    assert status["full_snapshot_role"] == "explicit_recovery_only"
    assert status["authoritative_full_snapshot_per_cycle"] is False
