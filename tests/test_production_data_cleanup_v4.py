from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from solana_roi import production_data_cleanup_v4 as cleanup


def _db(path: Path) -> sqlite3.Connection:
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA foreign_keys=ON")
    return db


def _replication_schema(db: sqlite3.Connection) -> None:
    db.execute(
        "CREATE TABLE certification_replication_changes ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "table_name TEXT NOT NULL,"
        "key_sql TEXT NOT NULL,"
        "operation TEXT NOT NULL CHECK(operation='UPSERT'),"
        "changed_at TEXT NOT NULL)"
    )
    for index in range(10):
        db.execute(
            "INSERT INTO certification_replication_changes("
            "table_name,key_sql,operation,changed_at) VALUES (?,?, 'UPSERT', ?)",
            ("events", f"id={index + 1}", f"2026-09-11T00:00:{index:02d}+00:00"),
        )
    db.commit()


def _schema(db: sqlite3.Connection):
    return cleanup.base.extract_schema(db)


def test_replication_pruning_uses_autoincrement_sequence_not_remaining_max(tmp_path: Path) -> None:
    path = tmp_path / "source.sqlite3"
    db = _db(path)
    try:
        _replication_schema(db)
        schema = _schema(db)
        assert cleanup._replication_sequence_head(db, schema) == 10

        result = cleanup._delete_acknowledged_replication(
            db, schema, acknowledged_watermark=7, batch_size=2
        )

        assert result["status"] == "pruned_to_externally_proven_certifier_watermark"
        assert result["deleted"] == 7
        assert result["sequence_before"] == 10
        assert result["sequence_after"] == 10
        assert result["remaining_rows"] == 3
        assert result["remaining_min_id"] == 8
        assert result["remaining_max_id"] == 10
        assert db.execute(
            "SELECT seq FROM sqlite_sequence WHERE name='certification_replication_changes'"
        ).fetchone()[0] == 10
    finally:
        db.close()


def test_replication_sequence_remains_after_all_rows_are_pruned(tmp_path: Path) -> None:
    path = tmp_path / "source.sqlite3"
    db = _db(path)
    try:
        _replication_schema(db)
        result = cleanup._delete_acknowledged_replication(
            db, _schema(db), acknowledged_watermark=10, batch_size=3
        )
        assert result["deleted"] == 10
        assert result["remaining_rows"] == 0
        assert result["sequence_before"] == 10
        assert result["sequence_after"] == 10
        db.execute(
            "INSERT INTO certification_replication_changes("
            "table_name,key_sql,operation,changed_at) VALUES ('events','id=11','UPSERT','later')"
        )
        db.commit()
        assert db.execute("SELECT id FROM certification_replication_changes").fetchone()[0] == 11
    finally:
        db.close()


def test_replication_ack_above_sequence_fails_closed(tmp_path: Path) -> None:
    path = tmp_path / "source.sqlite3"
    db = _db(path)
    try:
        _replication_schema(db)
        with pytest.raises(cleanup.CleanupBlocked, match="exceeds source AUTOINCREMENT sequence"):
            cleanup._delete_acknowledged_replication(
                db, _schema(db), acknowledged_watermark=11
            )
        assert db.execute("SELECT COUNT(*) FROM certification_replication_changes").fetchone()[0] == 10
    finally:
        db.close()


def test_missing_ack_preserves_replication_journal(tmp_path: Path) -> None:
    path = tmp_path / "source.sqlite3"
    db = _db(path)
    try:
        _replication_schema(db)
        result = cleanup._delete_acknowledged_replication(
            db, _schema(db), acknowledged_watermark=None
        )
        assert result["status"] == "protected_no_acknowledged_watermark"
        assert result["deleted"] == 0
        assert db.execute("SELECT COUNT(*) FROM certification_replication_changes").fetchone()[0] == 10
    finally:
        db.close()


def test_replication_schema_mismatch_fails_closed(tmp_path: Path) -> None:
    path = tmp_path / "source.sqlite3"
    db = _db(path)
    try:
        db.execute(
            "CREATE TABLE certification_replication_changes ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,table_name TEXT,key_sql TEXT,changed_at TEXT)"
        )
        db.execute(
            "INSERT INTO certification_replication_changes(table_name,key_sql,changed_at) "
            "VALUES ('events','id=1','now')"
        )
        db.commit()
        with pytest.raises(cleanup.CleanupBlocked):
            cleanup._replication_sequence_head(db, _schema(db))
    finally:
        db.close()


def _write_certifier_state(database: Path, *, watermark: int = 17) -> Path:
    path = database.with_suffix(database.suffix + ".state.json")
    path.write_text(
        json.dumps(
            {
                "release_commit": "release-sha",
                "replication_version": "incremental-v1",
                "epoch": "epoch-1",
                "schema_fingerprint": "abc123",
                "watermark": watermark,
                "bootstrap_complete": True,
                "catchup_complete": True,
                "last_transport": "delta",
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return path


def test_certifier_state_sidecar_is_durable_watermark_truth(tmp_path: Path) -> None:
    database = tmp_path / "certifier.sqlite3"
    database.touch()
    state_path = _write_certifier_state(database, watermark=29)

    state = cleanup._read_certifier_state(database)

    assert state["path"] == str(state_path)
    assert state["watermark"] == 29
    assert state["bootstrap_complete"] is True
    assert state["catchup_complete"] is True
    assert len(state["sha256"]) == 64


def test_certifier_state_requires_complete_bootstrap(tmp_path: Path) -> None:
    database = tmp_path / "certifier.sqlite3"
    database.touch()
    path = database.with_suffix(database.suffix + ".state.json")
    path.write_text(
        json.dumps(
            {
                "release_commit": "release-sha",
                "replication_version": "incremental-v1",
                "epoch": "epoch-1",
                "schema_fingerprint": "abc123",
                "watermark": 0,
                "bootstrap_complete": False,
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(cleanup.CleanupBlocked, match="bootstrap is incomplete"):
        cleanup._read_certifier_state(database)


def test_authoritative_plan_protects_unproven_synthetic_telemetry_and_unknown(tmp_path: Path) -> None:
    path = tmp_path / "source.sqlite3"
    db = _db(path)
    try:
        db.execute("CREATE TABLE v51_synthetic_provenance(surface TEXT,candidate_id TEXT,synthetic INTEGER)")
        db.execute("CREATE TABLE risk_refresh_measurements(id INTEGER PRIMARY KEY, completed_at TEXT)")
        db.execute("CREATE TABLE future_unknown_table(id INTEGER PRIMARY KEY)")
        db.commit()
        plan = cleanup._protected_plan(_schema(db), role="authoritative")
        assert plan["v51_synthetic_provenance"].startswith("protected:")
        assert plan["risk_refresh_measurements"].startswith("protected:")
        assert plan["future_unknown_table"].startswith("protected:")
    finally:
        db.close()


def test_certifier_plan_protects_every_replica_table(tmp_path: Path) -> None:
    path = tmp_path / "certifier.sqlite3"
    db = _db(path)
    try:
        _replication_schema(db)
        db.execute("CREATE TABLE events(id INTEGER PRIMARY KEY, payload_json TEXT)")
        db.commit()
        plan = cleanup._protected_plan(_schema(db), role="certifier")
        assert plan
        assert all(value == "protected:certifier-replica-equivalence" for value in plan.values())
    finally:
        db.close()
