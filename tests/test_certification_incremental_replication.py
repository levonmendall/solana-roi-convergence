from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path

import pytest
from fastapi import HTTPException

from solana_roi import certification_incremental_replication as replication
from solana_roi import certification_replica_client as client
from solana_roi import certifier_service


class _Store:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.RLock()
        self.db = sqlite3.connect(path, check_same_thread=False)

    def close(self) -> None:
        self.db.close()


def _store(tmp_path: Path) -> _Store:
    store = _Store(tmp_path / "source.sqlite3")
    with store.db:
        store.db.execute("CREATE TABLE sample(id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
        store.db.execute("CREATE TABLE mutable(k TEXT PRIMARY KEY, n INTEGER NOT NULL) WITHOUT ROWID")
    return store


def _apply(replica: sqlite3.Connection, payload: dict[str, object]) -> None:
    changes = payload["changes"]
    assert isinstance(changes, list)
    with replica:
        for change in changes:
            assert isinstance(change, dict)
            replica.execute(str(change["sql"]))


def test_exact_delta_replays_insert_update_delete_and_without_rowid(tmp_path: Path) -> None:
    store = _store(tmp_path)
    try:
        identity = replication.prepare_bootstrap(store)
        replica = sqlite3.connect(":memory:")
        replica.execute("CREATE TABLE sample(id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
        replica.execute("CREATE TABLE mutable(k TEXT PRIMARY KEY, n INTEGER NOT NULL) WITHOUT ROWID")

        with store.db:
            store.db.execute("INSERT INTO sample(id,value) VALUES (1,'a')")
            store.db.execute("INSERT INTO mutable(k,n) VALUES ('x',1)")
        first = replication._delta_payload(
            store,
            from_watermark=0,
            epoch=str(identity["epoch"]),
            schema_fingerprint=str(identity["schema_fingerprint"]),
        )
        assert first["source_change_count"] == 2
        assert first["coalesced_change_count"] == 2
        _apply(replica, first)
        assert replica.execute("SELECT id,value FROM sample").fetchall() == [(1, "a")]
        assert replica.execute("SELECT k,n FROM mutable").fetchall() == [("x", 1)]

        with store.db:
            store.db.execute("UPDATE sample SET value='b' WHERE id=1")
            store.db.execute("UPDATE mutable SET n=2 WHERE k='x'")
        second = replication._delta_payload(
            store,
            from_watermark=int(first["to_watermark"]),
            epoch=str(identity["epoch"]),
            schema_fingerprint=str(identity["schema_fingerprint"]),
        )
        assert second["source_change_count"] == 4
        assert second["coalesced_change_count"] == 2
        _apply(replica, second)
        assert replica.execute("SELECT value FROM sample WHERE id=1").fetchone()[0] == "b"
        assert replica.execute("SELECT n FROM mutable WHERE k='x'").fetchone()[0] == 2

        with store.db:
            store.db.execute("DELETE FROM sample WHERE id=1")
            store.db.execute("DELETE FROM mutable WHERE k='x'")
        third = replication._delta_payload(
            store,
            from_watermark=int(second["to_watermark"]),
            epoch=str(identity["epoch"]),
            schema_fingerprint=str(identity["schema_fingerprint"]),
        )
        _apply(replica, third)
        assert replica.execute("SELECT COUNT(*) FROM sample").fetchone()[0] == 0
        assert replica.execute("SELECT COUNT(*) FROM mutable").fetchone()[0] == 0
    finally:
        store.close()


def test_quiet_cycle_keeps_monotonic_watermark_after_ack_prune(tmp_path: Path) -> None:
    store = _store(tmp_path)
    try:
        identity = replication.prepare_bootstrap(store)
        with store.db:
            store.db.execute("INSERT INTO sample(id,value) VALUES (1,'a')")
        first = replication._delta_payload(
            store,
            from_watermark=0,
            epoch=str(identity["epoch"]),
            schema_fingerprint=str(identity["schema_fingerprint"]),
        )
        watermark = int(first["to_watermark"])
        acknowledged = replication._delta_payload(
            store,
            from_watermark=watermark,
            epoch=str(identity["epoch"]),
            schema_fingerprint=str(identity["schema_fingerprint"]),
        )
        assert int(acknowledged["to_watermark"]) == watermark
        assert acknowledged["source_change_count"] == 0

        # The previous request pruned acknowledged journal rows. sqlite_sequence,
        # not MAX(id), must still preserve the exact high-watermark on a quiet DB.
        quiet = replication._delta_payload(
            store,
            from_watermark=watermark,
            epoch=str(identity["epoch"]),
            schema_fingerprint=str(identity["schema_fingerprint"]),
        )
        assert int(quiet["to_watermark"]) == watermark
        assert quiet["source_change_count"] == 0
    finally:
        store.close()


def test_schema_change_rotates_identity_and_requires_full_bootstrap(tmp_path: Path) -> None:
    store = _store(tmp_path)
    try:
        identity = replication.prepare_bootstrap(store)
        with store.db:
            store.db.execute("CREATE TABLE later_table(id INTEGER PRIMARY KEY, value TEXT)")
        with pytest.raises(HTTPException) as caught:
            replication._delta_payload(
                store,
                from_watermark=int(identity["watermark"]),
                epoch=str(identity["epoch"]),
                schema_fingerprint=str(identity["schema_fingerprint"]),
            )
        assert caught.value.status_code == 409
        refreshed = replication.prepare_bootstrap(store)
        assert refreshed["epoch"] != identity["epoch"]
        assert refreshed["schema_fingerprint"] != identity["schema_fingerprint"]
    finally:
        store.close()


def test_delta_cap_returns_explicit_bounded_continuation_without_partial_state_ambiguity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store(tmp_path)
    try:
        identity = replication.prepare_bootstrap(store)
        monkeypatch.setenv("SOLANA_ROI_CERTIFICATION_DELTA_MAX_ROWS", "100")
        with store.db:
            store.db.executemany(
                "INSERT INTO sample(id,value) VALUES (?,?)",
                [(index, "x") for index in range(1, 102)],
            )
        first = replication._delta_payload(
            store,
            from_watermark=0,
            epoch=str(identity["epoch"]),
            schema_fingerprint=str(identity["schema_fingerprint"]),
        )
        assert first["source_change_count"] == 100
        assert first["bounded_batch"] is True
        assert first["caught_up"] is False
        assert first["full_snapshot_required"] is False
        assert int(first["to_watermark"]) < int(first["observed_current_watermark"])

        second = replication._delta_payload(
            store,
            from_watermark=int(first["to_watermark"]),
            epoch=str(identity["epoch"]),
            schema_fingerprint=str(identity["schema_fingerprint"]),
        )
        assert second["source_change_count"] == 1
        assert second["caught_up"] is True
        assert second["full_snapshot_required"] is False
        assert int(second["to_watermark"]) == int(second["observed_current_watermark"])
    finally:
        store.close()


def test_replication_journal_records_identity_not_row_payload(tmp_path: Path) -> None:
    store = _store(tmp_path)
    try:
        replication.prepare_bootstrap(store)
        marker = "payload-that-must-not-be-duplicated-" + ("z" * 2048)
        with store.db:
            store.db.execute("INSERT INTO sample(id,value) VALUES (?,?)", (7, marker))
        row = store.db.execute(
            f'SELECT table_name,key_sql,operation FROM "{replication.CHANGE_TABLE}" ORDER BY id DESC LIMIT 1'
        ).fetchone()
        assert row is not None
        assert row[0] == "sample"
        assert row[2] == "upsert"
        assert marker not in str(row[1])
    finally:
        store.close()


def test_replica_delta_apply_is_transactional_and_advances_state_only_on_success(tmp_path: Path) -> None:
    replica = tmp_path / "replica.sqlite3"
    connection = sqlite3.connect(replica)
    connection.execute("CREATE TABLE sample(id INTEGER PRIMARY KEY,value TEXT NOT NULL)")
    connection.commit()
    connection.close()

    state = {
        "release_commit": "a" * 40,
        "replication_version": replication.REPLICATION_VERSION,
        "epoch": "epoch-12345678",
        "schema_fingerprint": "b" * 64,
        "watermark": 4,
    }
    client._atomic_state(client._state_path(replica), state)

    good = {
        "to_watermark": 5,
        "changes": [{"table": "sample", "sql": 'INSERT OR REPLACE INTO "sample"("id","value") VALUES (1,\'ok\')'}],
        "source_change_count": 1,
        "payload_bytes": 10,
    }
    result = client._apply_delta(replica, state, good)
    assert result["watermark"] == 5
    persisted = json.loads(client._state_path(replica).read_text(encoding="utf-8"))
    assert persisted["watermark"] == 5

    bad_state = dict(persisted)
    bad = {
        "to_watermark": 6,
        "changes": [
            {"table": "sample", "sql": 'UPDATE "sample" SET "value"=\'changed\' WHERE "id"=1'},
            {"table": "missing", "sql": 'INSERT INTO "missing" VALUES (1)'},
        ],
        "source_change_count": 2,
        "payload_bytes": 20,
    }
    with pytest.raises(sqlite3.DatabaseError):
        client._apply_delta(replica, bad_state, bad)
    check = sqlite3.connect(replica)
    try:
        assert check.execute("SELECT value FROM sample WHERE id=1").fetchone()[0] == "ok"
    finally:
        check.close()
    persisted_after = json.loads(client._state_path(replica).read_text(encoding="utf-8"))
    assert persisted_after["watermark"] == 5


def test_disposable_cycle_clone_cannot_mutate_pristine_replica(tmp_path: Path) -> None:
    replica = tmp_path / "replica.sqlite3"
    destination = tmp_path / "cycle.sqlite3"
    connection = sqlite3.connect(replica)
    connection.execute("CREATE TABLE sample(id INTEGER PRIMARY KEY,value TEXT)")
    connection.execute("INSERT INTO sample VALUES (1,'base')")
    connection.commit()
    connection.close()

    method = client.clone_replica_for_cycle(replica, destination)
    assert method in {"local_reflink", "local_progressive_copy"}
    child = sqlite3.connect(destination)
    child.execute("UPDATE sample SET value='child' WHERE id=1")
    child.commit()
    child.close()
    base = sqlite3.connect(replica)
    try:
        assert base.execute("SELECT value FROM sample WHERE id=1").fetchone()[0] == "base"
    finally:
        base.close()


def test_certifier_normal_cycle_is_incremental_and_failure_retry_is_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SOLANA_ROI_CERTIFIER_CYCLE_INTERVAL_SECONDS", raising=False)
    assert certifier_service._interval_seconds() == 15.0
    assert certifier_service._retry_delay_seconds(1) == 15.0
    assert certifier_service._retry_delay_seconds(2) == 30.0
    assert certifier_service._retry_delay_seconds(3) == 60.0
    assert certifier_service._retry_delay_seconds(9) == 60.0
    health = certifier_service.health()
    integrity = health["snapshot_transfer_integrity"]
    assert integrity["full_snapshot_normal_cycle"] is False
    assert integrity["full_snapshot_role"] == "bootstrap_recovery_reconciliation_only"
    assert integrity["normal_cycle_transport"] == "bounded_incremental_delta"
    assert integrity["authoritative_full_snapshot_per_cycle"] is False
    assert health["paper_only"] is True
    assert health["live_money_authority"] is False
    assert health["signing_available"] is False
    assert health["transaction_submission_available"] is False
