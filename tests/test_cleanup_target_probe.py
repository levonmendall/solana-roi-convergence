from __future__ import annotations

import sqlite3
from pathlib import Path

from solana_roi.cleanup_target_probe import TARGET_TABLE, probe_cleanup_target


def _database(path: Path) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.executescript(
            f"""
            PRAGMA foreign_keys=ON;
            CREATE TABLE {TARGET_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                candidate_id TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                reason TEXT NOT NULL
            );
            CREATE INDEX idx_latency_failures_observed_at
                ON {TARGET_TABLE}(observed_at);
            CREATE TABLE latency_failure_notes (
                id INTEGER PRIMARY KEY,
                failure_id INTEGER NOT NULL REFERENCES {TARGET_TABLE}(id),
                note TEXT NOT NULL
            );
            INSERT INTO {TARGET_TABLE}(candidate_id, observed_at, reason)
            VALUES ('candidate-a', '2026-09-11T00:00:00Z', 'timeout'),
                   ('candidate-b', '2026-09-11T00:01:00Z', 'timeout');
            ANALYZE;
            """
        )
        connection.commit()
    finally:
        connection.close()


def test_probe_reports_schema_dependencies_and_bounds_without_mutation(tmp_path: Path) -> None:
    database = tmp_path / "probe.sqlite3"
    _database(database)
    before = database.read_bytes()

    result = probe_cleanup_target(database)

    assert result["status"] == "ok"
    assert result["table"] == TARGET_TABLE
    assert result["read_only"] is True
    assert result["payload_rows_scanned"] is False
    assert [column["name"] for column in result["columns"]] == [
        "id",
        "candidate_id",
        "observed_at",
        "reason",
    ]
    assert any(index["name"] == "idx_latency_failures_observed_at" for index in result["indexes"])
    assert result["foreign_keys"] == []
    assert any(item["table"] == "latency_failure_notes" for item in result["reverse_foreign_keys"])
    assert result["sqlite_sequence"] == 2
    assert result["rowid_bounds"] == {"min": 1, "max": 2}
    assert result["sqlite_stat1"]
    assert database.read_bytes() == before


def test_probe_missing_target_is_non_mutating_and_explicit(tmp_path: Path) -> None:
    database = tmp_path / "probe.sqlite3"
    connection = sqlite3.connect(database)
    connection.execute("CREATE TABLE other(id INTEGER PRIMARY KEY)")
    connection.commit()
    connection.close()
    before = database.read_bytes()

    result = probe_cleanup_target(database)

    assert result["status"] == "table_missing"
    assert result["read_only"] is True
    assert database.read_bytes() == before
