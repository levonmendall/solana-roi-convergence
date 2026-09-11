from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from solana_roi import production_data_cleanup as cleanup


def _db(path: Path) -> sqlite3.Connection:
    db = sqlite3.connect(path)
    db.execute("PRAGMA journal_mode=WAL")
    db.executescript(
        """
        CREATE TABLE events(id INTEGER PRIMARY KEY AUTOINCREMENT,event_type TEXT NOT NULL,observed_at TEXT NOT NULL,payload_json TEXT NOT NULL,previous_hash TEXT,lineage_hash TEXT NOT NULL);
        CREATE TABLE paper_engine_checkpoint(id INTEGER PRIMARY KEY CHECK(id=1),saved_at TEXT NOT NULL,last_engine_event_id INTEGER NOT NULL,state_json TEXT NOT NULL,state_sha256 TEXT NOT NULL);
        CREATE TABLE wallet_profiles(wallet TEXT PRIMARY KEY,tier TEXT NOT NULL);
        CREATE TABLE wallet_intelligence_snapshots(id INTEGER PRIMARY KEY AUTOINCREMENT,wallet TEXT NOT NULL,entity_id TEXT NOT NULL,observed_at TEXT NOT NULL);
        CREATE TABLE adaptive_wallet_cohorts(id INTEGER PRIMARY KEY AUTOINCREMENT,strategy_version TEXT NOT NULL UNIQUE,status TEXT NOT NULL);
        CREATE TABLE _certification_replica_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL,updated_at TEXT NOT NULL);
        CREATE TABLE certification_replication_changes(id INTEGER PRIMARY KEY AUTOINCREMENT,table_name TEXT NOT NULL,change_type TEXT NOT NULL,row_json TEXT NOT NULL,primary_key_json TEXT NOT NULL,created_at TEXT NOT NULL);
        CREATE TABLE risk_refresh_measurements(id INTEGER PRIMARY KEY AUTOINCREMENT,token_mint TEXT NOT NULL,completed_at TEXT NOT NULL,complete INTEGER NOT NULL,fresh INTEGER NOT NULL);
        CREATE TABLE v51_synthetic_provenance(surface TEXT NOT NULL,candidate_id TEXT NOT NULL,synthetic INTEGER NOT NULL,PRIMARY KEY(surface,candidate_id));
        CREATE TABLE v51_candidates(surface TEXT NOT NULL,candidate_id TEXT NOT NULL,PRIMARY KEY(surface,candidate_id));
        CREATE TABLE v51_candidate_stage_events(id INTEGER PRIMARY KEY AUTOINCREMENT,surface TEXT NOT NULL,candidate_id TEXT NOT NULL);
        CREATE TABLE v51_candidate_current_state(surface TEXT NOT NULL,candidate_id TEXT NOT NULL,stage TEXT NOT NULL,PRIMARY KEY(surface,candidate_id,stage));
        CREATE TABLE v51_candidate_pipeline_audit(surface TEXT NOT NULL,candidate_id TEXT NOT NULL,stage TEXT NOT NULL,PRIMARY KEY(surface,candidate_id,stage));
        CREATE TABLE unknown_future_state(id INTEGER PRIMARY KEY,value TEXT NOT NULL);
        """
    )
    db.execute("INSERT INTO events(event_type,observed_at,payload_json,previous_hash,lineage_hash) VALUES('price','2026-09-11T00:00:00+00:00','{}',NULL,'abc')")
    db.execute("INSERT INTO paper_engine_checkpoint VALUES(1,'2026-09-11T00:00:00+00:00',1,'{}','digest')")
    db.execute("INSERT INTO wallet_profiles VALUES('wallet-a','S')")
    db.execute("INSERT INTO wallet_intelligence_snapshots(wallet,entity_id,observed_at) VALUES('wallet-a','entity-a','2026-09-11T00:00:00+00:00')")
    db.execute("INSERT INTO adaptive_wallet_cohorts(strategy_version,status) VALUES('v5.2','approved')")
    db.execute("INSERT INTO _certification_replica_meta VALUES('source_watermark','2','2026-09-11T00:00:00+00:00')")
    db.execute("INSERT INTO unknown_future_state VALUES(1,'keep')")
    for idx in range(1, 4):
        db.execute(
            "INSERT INTO certification_replication_changes(id,table_name,change_type,row_json,primary_key_json,created_at) VALUES(?,?,?,?,?,?)",
            (idx, "cert_table", "insert", "{}", "{}", "2026-09-10T00:00:00+00:00"),
        )
    db.execute("INSERT INTO risk_refresh_measurements(token_mint,completed_at,complete,fresh) VALUES('old','2026-09-01T00:00:00+00:00',1,1)")
    db.execute("INSERT INTO risk_refresh_measurements(token_mint,completed_at,complete,fresh) VALUES('new','2999-09-11T00:00:00+00:00',1,1)")
    for candidate, synthetic in (("synthetic", 1), ("real", 0)):
        db.execute("INSERT INTO v51_synthetic_provenance VALUES('SOLANA',?,?)", (candidate, synthetic))
        db.execute("INSERT INTO v51_candidates VALUES('SOLANA',?)", (candidate,))
        db.execute("INSERT INTO v51_candidate_stage_events(surface,candidate_id) VALUES('SOLANA',?)", (candidate,))
        db.execute("INSERT INTO v51_candidate_current_state VALUES('SOLANA',?,'detected')", (candidate,))
        db.execute("INSERT INTO v51_candidate_pipeline_audit VALUES('SOLANA',?,'detected')", (candidate,))
    db.commit()
    return db


def test_disabled_cli_does_not_touch_database(tmp_path: Path, monkeypatch, capsys) -> None:
    path = tmp_path / "production.sqlite3"
    _db(path).close()
    before = path.stat().st_mtime_ns
    monkeypatch.delenv(cleanup.ENABLED_ENV, raising=False)

    assert cleanup.main(["--database", str(path), "--role", "authoritative", "--run-id", "disabled-test"]) == 0

    assert json.loads(capsys.readouterr().out)["status"] == "disabled"
    assert path.stat().st_mtime_ns == before
    assert not (tmp_path / ".production-cleanup").exists()


def test_authoritative_deletes_only_proven_exhaust(tmp_path: Path) -> None:
    path = tmp_path / "production.sqlite3"
    _db(path).close()

    result = cleanup.execute_cleanup(
        path,
        role="authoritative",
        run_id="unit-authoritative-1",
        acknowledged_watermark=2,
        telemetry_hours=24.0,
    )

    assert result["status"] == "success"
    assert result["deleted_rows"]["acknowledged_replication_changes"] == 2
    assert result["deleted_rows"]["stale_risk_refresh_measurements"] == 1
    assert result["deleted_rows"]["synthetic_candidate_closure"]["v51_candidates"] == 1
    assert result["protected_set"]["events"].startswith("protected:")
    assert result["protected_set"]["paper_engine_checkpoint"].startswith("protected:")
    assert result["protected_set"]["wallet_intelligence_snapshots"].startswith("protected:")
    assert result["protected_set"]["unknown_future_state"].startswith("protected:")
    assert result["after"]["integrity"]["ok"] is True

    db = sqlite3.connect(path)
    assert db.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 1
    assert db.execute("SELECT COUNT(*) FROM paper_engine_checkpoint").fetchone()[0] == 1
    assert db.execute("SELECT COUNT(*) FROM wallet_profiles").fetchone()[0] == 1
    assert db.execute("SELECT COUNT(*) FROM wallet_intelligence_snapshots").fetchone()[0] == 1
    assert db.execute("SELECT COUNT(*) FROM adaptive_wallet_cohorts").fetchone()[0] == 1
    assert db.execute("SELECT COUNT(*) FROM _certification_replica_meta").fetchone()[0] == 1
    assert db.execute("SELECT COUNT(*) FROM unknown_future_state").fetchone()[0] == 1
    assert db.execute("SELECT COUNT(*) FROM certification_replication_changes").fetchone()[0] == 1
    assert db.execute("SELECT COUNT(*) FROM risk_refresh_measurements").fetchone()[0] == 1
    assert db.execute("SELECT COUNT(*) FROM v51_candidates WHERE candidate_id='synthetic'").fetchone()[0] == 0
    assert db.execute("SELECT COUNT(*) FROM v51_candidates WHERE candidate_id='real'").fetchone()[0] == 1
    db.close()


def test_certifier_preserves_replica_rows_and_captures_durable_watermark(tmp_path: Path) -> None:
    path = tmp_path / "certifier.sqlite3"
    _db(path).close()

    result = cleanup.execute_cleanup(
        path,
        role="certifier",
        run_id="unit-certifier-1",
        acknowledged_watermark=1,
    )

    assert result["status"] == "success"
    assert result["durable_source_watermark"] == 2
    assert result["deleted_rows"] == {
        "synthetic_candidate_closure": {},
        "acknowledged_replication_changes": 0,
        "stale_risk_refresh_measurements": 0,
    }
    assert all(
        reason == "protected:certifier-replica-equivalence"
        for reason in result["protected_set"].values()
    )
    db = sqlite3.connect(path)
    assert db.execute("SELECT COUNT(*) FROM certification_replication_changes").fetchone()[0] == 3
    assert db.execute("SELECT COUNT(*) FROM risk_refresh_measurements").fetchone()[0] == 2
    assert db.execute("SELECT COUNT(*) FROM v51_candidates").fetchone()[0] == 2
    db.close()


def test_watermark_cannot_exceed_authoritative_journal_head(tmp_path: Path) -> None:
    path = tmp_path / "production.sqlite3"
    _db(path).close()

    with pytest.raises(cleanup.CleanupBlocked, match="exceeds source journal head"):
        cleanup.execute_cleanup(
            path,
            role="authoritative",
            run_id="unit-watermark-1",
            acknowledged_watermark=99,
        )


def test_success_run_id_is_idempotent(tmp_path: Path) -> None:
    path = tmp_path / "production.sqlite3"
    _db(path).close()

    first = cleanup.execute_cleanup(path, role="authoritative", run_id="unit-idempotent-1")
    second = cleanup.execute_cleanup(path, role="authoritative", run_id="unit-idempotent-1")

    assert first["status"] == "success"
    assert second["status"] == "success"
    assert second["idempotent_replay"] is True


def test_schema_drift_fails_closed_before_target_delete(tmp_path: Path) -> None:
    path = tmp_path / "production.sqlite3"
    db = _db(path)
    db.execute("ALTER TABLE certification_replication_changes RENAME TO old_changes")
    db.execute("CREATE TABLE certification_replication_changes(id INTEGER PRIMARY KEY,table_name TEXT NOT NULL)")
    db.commit()
    db.close()

    with pytest.raises(cleanup.CleanupBlocked, match="schema mismatch"):
        cleanup.execute_cleanup(
            path,
            role="authoritative",
            run_id="unit-schema-drift-1",
            acknowledged_watermark=1,
        )

    db = sqlite3.connect(path)
    assert db.execute("SELECT COUNT(*) FROM old_changes").fetchone()[0] == 3
    db.close()
