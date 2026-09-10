from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from solana_roi import certification_incremental_replication as replication
from solana_roi import certification_replica_client as replica_client


class _Store:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.RLock()
        self.db = sqlite3.connect(path, check_same_thread=False)
        with self.db:
            self.db.execute("CREATE TABLE sample(id INTEGER PRIMARY KEY,value TEXT NOT NULL)")

    def close(self) -> None:
        self.db.close()


def test_view_index_and_user_trigger_ddl_rotate_exact_replica_identity(tmp_path: Path) -> None:
    store = _Store(tmp_path / "source.sqlite3")
    try:
        first = replication.prepare_bootstrap(store)
        with store.db:
            store.db.execute("CREATE VIEW sample_values AS SELECT id,value FROM sample")
        with pytest.raises(HTTPException) as view_change:
            replication._delta_payload(
                store,
                from_watermark=int(first["watermark"]),
                epoch=str(first["epoch"]),
                schema_fingerprint=str(first["schema_fingerprint"]),
            )
        assert view_change.value.status_code == 409
        second = replication.prepare_bootstrap(store)
        assert second["epoch"] != first["epoch"]
        assert second["schema_fingerprint"] != first["schema_fingerprint"]

        with store.db:
            store.db.execute("CREATE INDEX sample_value_idx ON sample(value)")
        with pytest.raises(HTTPException) as index_change:
            replication._delta_payload(
                store,
                from_watermark=int(second["watermark"]),
                epoch=str(second["epoch"]),
                schema_fingerprint=str(second["schema_fingerprint"]),
            )
        assert index_change.value.status_code == 409
        third = replication.prepare_bootstrap(store)
        assert third["epoch"] != second["epoch"]
        assert third["schema_fingerprint"] != second["schema_fingerprint"]

        with store.db:
            store.db.execute(
                "CREATE TRIGGER sample_user_guard AFTER INSERT ON sample "
                "BEGIN SELECT NEW.id; END"
            )
        with pytest.raises(HTTPException) as trigger_change:
            replication._delta_payload(
                store,
                from_watermark=int(third["watermark"]),
                epoch=str(third["epoch"]),
                schema_fingerprint=str(third["schema_fingerprint"]),
            )
        assert trigger_change.value.status_code == 409
        fourth = replication.prepare_bootstrap(store)
        assert fourth["epoch"] != third["epoch"]
        assert fourth["schema_fingerprint"] != third["schema_fingerprint"]
    finally:
        store.close()


def test_bootstrap_identity_keeps_sqlite_sequence_watermark_after_ack_pruning(tmp_path: Path) -> None:
    store = _Store(tmp_path / "source.sqlite3")
    snapshot = tmp_path / "bootstrap.sqlite3"
    try:
        identity = replication.prepare_bootstrap(store)
        with store.db:
            store.db.execute("INSERT INTO sample(id,value) VALUES (1,'one')")
        delta = replication._delta_payload(
            store,
            from_watermark=int(identity["watermark"]),
            epoch=str(identity["epoch"]),
            schema_fingerprint=str(identity["schema_fingerprint"]),
        )
        watermark = int(delta["to_watermark"])
        assert watermark > 0

        acknowledged = replication._delta_payload(
            store,
            from_watermark=watermark,
            epoch=str(identity["epoch"]),
            schema_fingerprint=str(identity["schema_fingerprint"]),
        )
        assert int(acknowledged["to_watermark"]) == watermark
        assert store.db.execute(
            f'SELECT COUNT(*) FROM "{replication.CHANGE_TABLE}" WHERE id<=?', (watermark,)
        ).fetchone()[0] == 0

        target = sqlite3.connect(snapshot)
        try:
            store.db.backup(target)
        finally:
            target.close()
        bootstrap_identity = replica_client._read_source_replication_identity(snapshot)
        assert int(bootstrap_identity["watermark"]) == watermark
        assert bootstrap_identity["epoch"] == identity["epoch"]
        assert bootstrap_identity["schema_fingerprint"] == identity["schema_fingerprint"]
    finally:
        store.close()


def test_delta_replay_does_not_refire_canonical_user_trigger_side_effects(tmp_path: Path) -> None:
    replica = tmp_path / "replica.sqlite3"
    connection = sqlite3.connect(replica)
    try:
        connection.execute("CREATE TABLE sample(id INTEGER PRIMARY KEY,value TEXT NOT NULL)")
        connection.execute("CREATE TABLE audit(k TEXT PRIMARY KEY,value TEXT NOT NULL)")
        connection.execute(
            "CREATE TRIGGER sample_audit AFTER INSERT ON sample BEGIN "
            "INSERT INTO audit(k,value) VALUES (lower(hex(randomblob(16))),'local-side-effect'); END"
        )
        connection.commit()
    finally:
        connection.close()

    state = {
        "client_version": replica_client.CLIENT_VERSION,
        "release_commit": "a" * 40,
        "replication_version": replication.REPLICATION_VERSION,
        "epoch": "epoch-12345678",
        "schema_fingerprint": "b" * 64,
        "watermark": 10,
    }
    replica_client._atomic_state(replica_client._state_path(replica), state)
    payload = {
        "to_watermark": 12,
        "source_change_count": 2,
        "payload_bytes": 100,
        "changes": [
            {
                "table": "sample",
                "sql": 'INSERT OR REPLACE INTO "sample"("id","value") VALUES (1,\'canonical\')',
            },
            {
                "table": "audit",
                "sql": 'INSERT OR REPLACE INTO "audit"("k","value") VALUES (\'source-key\',\'source-side-effect\')',
            },
        ],
    }
    result = replica_client._apply_delta(replica, state, payload)
    assert result["watermark"] == 12

    check = sqlite3.connect(replica)
    try:
        assert check.execute("SELECT id,value FROM sample").fetchall() == [(1, "canonical")]
        assert check.execute("SELECT k,value FROM audit").fetchall() == [("source-key", "source-side-effect")]
        trigger = check.execute(
            "SELECT sql FROM sqlite_master WHERE type='trigger' AND name='sample_audit'"
        ).fetchone()
        assert trigger is not None and "randomblob" in str(trigger[0]).lower()
    finally:
        check.close()


def test_storeless_composition_stays_up_but_replication_requests_fail_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOLANA_ROI_CERTIFICATION_SHARED_TOKEN", "test-token")
    app = FastAPI()
    original_snapshot_builder = replication.split._snapshot_store_to_file
    try:
        replication.install_certification_incremental_replication(app, lambda: object())

        with TestClient(app) as client:
            status = client.get("/v1/operations/certification-db-replication")
            assert status.status_code == 200
            assert status.json()["ready"] is False
            assert status.json()["reason"] == "canonical_store_unavailable"

            delta = client.get(
                "/v1/operations/certification-db-delta",
                params={
                    "from_watermark": 0,
                    "epoch": "12345678",
                    "schema_fingerprint": "a" * 64,
                },
                headers={"X-Certification-Token": "test-token"},
            )
            assert delta.status_code == 503
            assert "canonical certification store unavailable" in delta.text

        assert app.state.roi_certification_incremental_replication is True
        assert app.state.roi_certification_incremental_replication_version == replication.REPLICATION_VERSION
    finally:
        replication.split._snapshot_store_to_file = original_snapshot_builder
