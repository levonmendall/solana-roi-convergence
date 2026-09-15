from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

import solana_roi.active_storage_epoch_rollover as rollover
from solana_roi.certification_epoch import release_commit_from_env


def _source_database(path: Path) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            "CREATE TABLE certification_release_epochs("
            "release_commit TEXT PRIMARY KEY,started_at TEXT NOT NULL)"
        )
        connection.execute(
            "INSERT INTO certification_release_epochs(release_commit,started_at) VALUES(?,?)",
            ("older-release", "2026-09-14T00:00:00+00:00"),
        )
        connection.execute(
            "INSERT INTO certification_release_epochs(release_commit,started_at) VALUES(?,?)",
            ("source-release", "2026-09-15T00:00:00+00:00"),
        )
        connection.commit()
    finally:
        connection.close()


def test_shadow_rollover_extracts_source_release_and_restores_target_environment(
    tmp_path, monkeypatch
):
    source = tmp_path / "active.sqlite3"
    successor = tmp_path / "successor.sqlite3"
    _source_database(source)
    monkeypatch.setenv("SOLANA_ROI_RELEASE_COMMIT", "target-release")

    observed: dict[str, object] = {}
    sentinel = object()

    def fake_build_shadow_database(**kwargs):
        observed["release_env"] = release_commit_from_env()
        observed["release_sha"] = kwargs["release_sha"]
        observed["legacy_path"] = kwargs["legacy_path"]
        observed["active_path"] = kwargs["active_path"]
        observed["replace_existing"] = kwargs["replace_existing"]
        return sentinel

    monkeypatch.setattr(rollover, "build_shadow_database", fake_build_shadow_database)

    result = rollover._build_shadow_database_for_rollover(
        source=source,
        successor=successor,
        target_release_sha="target-release",
    )

    assert result is sentinel
    assert observed == {
        "release_env": "source-release",
        "release_sha": "target-release",
        "legacy_path": source,
        "active_path": successor,
        "replace_existing": True,
    }
    assert release_commit_from_env() == "target-release"

    connection = sqlite3.connect(source)
    try:
        rows = connection.execute(
            "SELECT release_commit FROM certification_release_epochs ORDER BY started_at"
        ).fetchall()
    finally:
        connection.close()
    assert rows == [("older-release",), ("source-release",)]


def test_source_certification_frontier_missing_fails_closed(tmp_path):
    source = tmp_path / "active.sqlite3"
    sqlite3.connect(source).close()

    with pytest.raises(
        RuntimeError,
        match="source certification release epoch table missing",
    ):
        rollover._source_certification_release_commit(source)
