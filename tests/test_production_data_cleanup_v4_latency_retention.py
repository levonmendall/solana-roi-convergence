from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from solana_roi import production_data_cleanup_v4 as cleanup


RELEASE = "b" * 40
BOUNDARY = "2026-09-11T12:00:00+00:00"


def _database(path: Path, *, epoch: bool = True, replication: bool = False) -> sqlite3.Connection:
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA foreign_keys=ON")
    db.executescript(
        """
        CREATE TABLE anonymous_candidate_latency_failures(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            failed_at TEXT NOT NULL,
            reason TEXT NOT NULL,
            outcome TEXT NOT NULL,
            count INTEGER NOT NULL,
            max_age_ms REAL NOT NULL
        );
        CREATE INDEX ix_anonymous_candidate_latency_failures_failed_at
            ON anonymous_candidate_latency_failures(failed_at);
        CREATE TABLE current_authority_state(id INTEGER PRIMARY KEY,value TEXT NOT NULL);
        INSERT INTO current_authority_state VALUES(1,'keep-current');
        """
    )
    if epoch:
        db.execute(
            "CREATE TABLE certification_release_epochs("
            "release_commit TEXT PRIMARY KEY,started_at TEXT NOT NULL)"
        )
        db.execute(
            "INSERT INTO certification_release_epochs(release_commit,started_at) VALUES(?,?)",
            (RELEASE, BOUNDARY),
        )
    if replication:
        db.execute(
            "CREATE TABLE certification_replication_changes("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "table_name TEXT NOT NULL,key_sql TEXT NOT NULL,"
            "operation TEXT NOT NULL,changed_at TEXT NOT NULL)"
        )
        db.execute(
            "INSERT INTO certification_replication_changes("
            "table_name,key_sql,operation,changed_at) VALUES('events','id=1','UPSERT','old')"
        )
    db.executemany(
        "INSERT INTO anonymous_candidate_latency_failures("
        "failed_at,reason,outcome,count,max_age_ms) VALUES(?,?,?,?,?)",
        (
            ("2026-09-10T23:59:59+00:00", "old", "expired_before_entry", 4, 1000.0),
            (BOUNDARY, "boundary", "expired_before_entry", 2, 900.0),
            ("2026-09-11T12:00:01+00:00", "current", "expired_before_entry", 1, 800.0),
        ),
    )
    db.commit()
    return db


def _release(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("RENDER_GIT_COMMIT", raising=False)
    monkeypatch.delenv("GITHUB_SHA", raising=False)
    monkeypatch.setenv("SOLANA_ROI_RELEASE_COMMIT", RELEASE)


def test_live_v4_deletes_only_preboundary_latency_history(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "source.sqlite3"
    _database(path).close()
    _release(monkeypatch)

    result = cleanup.execute_cleanup(
        path,
        role="authoritative",
        run_id="v4-latency-boundary-1",
        acknowledged_watermark=None,
    )

    assert result["status"] == "success"
    assert result["deleted_rows"]["anonymous_candidate_latency_failures"] == 1
    assert result["latency_certification_boundary"]["release_commit"] == RELEASE
    assert result["latency_certification_boundary"]["prospective_start_at"] == BOUNDARY
    assert result["latency_certification_boundary"]["predicate"] == "failed_at < prospective_start_at"
    assert result["preflight"]["all_enabled_deletion_predicates_proven_before_mutation"] is True
    assert result["before"]["row_counts"] == "not_scanned_history_bounded_preflight"
    assert result["after"]["integrity"]["ok"] is True

    db = sqlite3.connect(path)
    latency = db.execute(
        "SELECT failed_at,reason FROM anonymous_candidate_latency_failures ORDER BY failed_at"
    ).fetchall()
    current = db.execute("SELECT value FROM current_authority_state WHERE id=1").fetchone()[0]
    db.close()
    assert latency == [
        (BOUNDARY, "boundary"),
        ("2026-09-11T12:00:01+00:00", "current"),
    ]
    assert current == "keep-current"


def test_live_v4_missing_epoch_blocks_before_replication_or_latency_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "source.sqlite3"
    _database(path, epoch=False, replication=True).close()
    _release(monkeypatch)

    with pytest.raises(cleanup.CleanupBlocked, match="certification_release_epochs"):
        cleanup.execute_cleanup(
            path,
            role="authoritative",
            run_id="v4-missing-epoch-1",
            acknowledged_watermark=1,
        )

    db = sqlite3.connect(path)
    assert db.execute("SELECT COUNT(*) FROM certification_replication_changes").fetchone()[0] == 1
    assert db.execute("SELECT COUNT(*) FROM anonymous_candidate_latency_failures").fetchone()[0] == 3
    assert db.execute("SELECT value FROM current_authority_state WHERE id=1").fetchone()[0] == "keep-current"
    db.close()


def test_live_v4_rejects_non_replication_latency_trigger(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "source.sqlite3"
    db = _database(path)
    db.executescript(
        """
        CREATE TABLE unexpected_audit(id INTEGER PRIMARY KEY AUTOINCREMENT,note TEXT NOT NULL);
        CREATE TRIGGER unexpected_latency_delete
        AFTER DELETE ON anonymous_candidate_latency_failures
        BEGIN
            INSERT INTO unexpected_audit(note) VALUES('deleted');
        END;
        """
    )
    db.commit()
    db.close()
    _release(monkeypatch)

    with pytest.raises(cleanup.CleanupBlocked, match="non-replication trigger dependency"):
        cleanup.execute_cleanup(
            path,
            role="authoritative",
            run_id="v4-unexpected-trigger-1",
        )

    db = sqlite3.connect(path)
    assert db.execute("SELECT COUNT(*) FROM anonymous_candidate_latency_failures").fetchone()[0] == 3
    assert db.execute("SELECT COUNT(*) FROM unexpected_audit").fetchone()[0] == 0
    db.close()
