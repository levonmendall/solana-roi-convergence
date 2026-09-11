from __future__ import annotations

import hashlib
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
    return cleanup._extract_schema_bounded(db)


def _protected_history_schema(db: sqlite3.Connection) -> None:
    db.executescript(
        """
        CREATE TABLE events(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT NOT NULL,
            observed_at TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            previous_hash TEXT,
            lineage_hash TEXT NOT NULL
        );
        CREATE TABLE paper_engine_checkpoint(
            id INTEGER PRIMARY KEY,
            saved_at TEXT NOT NULL,
            last_engine_event_id INTEGER NOT NULL,
            state_json TEXT NOT NULL,
            state_sha256 TEXT NOT NULL
        );
        CREATE TABLE wallet_profiles(wallet TEXT PRIMARY KEY,tier TEXT NOT NULL);
        CREATE TABLE future_unknown_table(id INTEGER PRIMARY KEY,value TEXT);
        """
    )
    previous = None
    for index in range(1, 101):
        lineage = f"hash-{index}"
        db.execute(
            "INSERT INTO events(id,event_type,observed_at,payload_json,previous_hash,lineage_hash) "
            "VALUES(?,?,?,?,?,?)",
            (index, "price", f"2026-09-11T00:00:{index % 60:02d}+00:00", "{}", previous, lineage),
        )
        previous = lineage
    state = "{}"
    digest = hashlib.sha256(state.encode()).hexdigest()
    db.execute(
        "INSERT INTO paper_engine_checkpoint(id,saved_at,last_engine_event_id,state_json,state_sha256) "
        "VALUES(1,'2026-09-11T00:10:00+00:00',100,?,?)",
        (state, digest),
    )
    db.execute("INSERT INTO wallet_profiles VALUES('wallet-a','S')")
    db.execute("INSERT INTO future_unknown_table VALUES(1,'keep')")
    db.commit()


def test_replication_pruning_uses_autoincrement_sequence_without_remaining_count(tmp_path: Path) -> None:
    path = tmp_path / "source.sqlite3"
    db = _db(path)
    try:
        _replication_schema(db)
        schema = _schema(db)
        assert cleanup._replication_sequence_head(db, schema) == 10

        traced: list[str] = []
        db.set_trace_callback(traced.append)
        result = cleanup._delete_acknowledged_replication(
            db, schema, acknowledged_watermark=7, batch_size=2
        )
        db.set_trace_callback(None)

        assert result["status"] == "pruned_to_externally_proven_certifier_watermark"
        assert result["deleted"] == 7
        assert result["sequence_before"] == 10
        assert result["sequence_after"] == 10
        assert result["remaining_rows"] == "not_scanned"
        assert result["remaining_min_id"] == 8
        assert result["remaining_max_id"] == 10
        assert not any("COUNT(" in sql.upper() for sql in traced)
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
        assert result["remaining_rows"] == "not_scanned"
        assert result["remaining_min_id"] is None
        assert result["remaining_max_id"] is None
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


def test_authoritative_plan_protects_unproven_categories_and_unknown(tmp_path: Path) -> None:
    path = tmp_path / "source.sqlite3"
    db = _db(path)
    try:
        db.execute("CREATE TABLE v51_synthetic_provenance(surface TEXT,candidate_id TEXT,synthetic INTEGER)")
        db.execute("CREATE TABLE risk_refresh_measurements(id INTEGER PRIMARY KEY, completed_at TEXT)")
        db.execute("CREATE TABLE anonymous_candidate_latency_failures(id INTEGER PRIMARY KEY, observed_at TEXT)")
        db.execute("CREATE TABLE future_unknown_table(id INTEGER PRIMARY KEY)")
        db.commit()
        plan = cleanup._protected_plan(_schema(db), role="authoritative")
        assert plan["v51_synthetic_provenance"].startswith("protected:")
        assert plan["risk_refresh_measurements"].startswith("protected:")
        assert plan["anonymous_candidate_latency_failures"].startswith("protected:")
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


def test_preflight_metadata_and_protection_do_not_count_protected_history(tmp_path: Path) -> None:
    path = tmp_path / "source.sqlite3"
    db = _db(path)
    try:
        _protected_history_schema(db)
        traced: list[str] = []
        db.set_trace_callback(traced.append)
        schema = cleanup._extract_schema_bounded(db)
        measured = cleanup._measure_bounded(path, db, schema)
        plan = cleanup._protected_plan(schema, role="authoritative")
        protected = cleanup._protection_snapshot(db, schema, plan)
        db.set_trace_callback(None)

        assert measured["row_counts"] == "not_scanned_history_bounded_preflight"
        assert measured["integrity"]["ok"] is None
        assert protected["protected_row_counts"] == "not_scanned_history_bounded_preflight"
        assert protected["event_ledger_head"]["count"] == "not_scanned"
        assert protected["event_ledger_head"]["min_id"] == 1
        assert protected["event_ledger_head"]["max_id"] == 100
        assert protected["paper_engine_checkpoint"]["current"]["last_engine_event_id"] == 100
        history_sql = [sql.upper() for sql in traced if not sql.upper().startswith("PRAGMA")]
        assert not any("COUNT(" in sql for sql in history_sql)
        assert not any("MIN(" in sql or "MAX(" in sql for sql in history_sql)
    finally:
        db.close()


def test_execute_orders_physical_compaction_before_full_integrity(tmp_path: Path, monkeypatch) -> None:
    path = tmp_path / "source.sqlite3"
    db = _db(path)
    db.execute("CREATE TABLE current_state(id INTEGER PRIMARY KEY,value TEXT)")
    db.execute("INSERT INTO current_state VALUES(1,'keep')")
    db.commit()
    db.close()

    calls: list[str] = []

    def compact(database_path: Path, connection: sqlite3.Connection):
        assert database_path == path.resolve()
        calls.append("compact")
        return {"mode": "test", "wal_checkpoint": [0, 0, 0]}

    def integrity(connection: sqlite3.Connection):
        calls.append("integrity")
        return {"ok": True, "integrity_check": ["ok"], "foreign_key_violations": []}

    monkeypatch.setattr(cleanup.base, "_compact", compact)
    monkeypatch.setattr(cleanup, "_final_integrity", integrity)

    result = cleanup.execute_cleanup(
        path,
        role="authoritative",
        run_id="bounded-ordering-1",
    )

    assert calls == ["compact", "integrity"]
    assert result["status"] == "success"
    assert result["preflight"]["history_scaled_counts"] is False
    assert result["preflight"]["event_ledger_full_scan"] is False
    assert result["after"]["integrity"]["ok"] is True
