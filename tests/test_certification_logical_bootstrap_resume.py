from __future__ import annotations

import sqlite3
import urllib.parse
from pathlib import Path

import pytest

from solana_roi import certification_logical_bootstrap_client as client
from solana_roi.certification_incremental_replication import REPLICATION_VERSION
from solana_roi.certification_logical_bootstrap import BOOTSTRAP_VERSION


def _manifest(release: str) -> dict[str, object]:
    return {
        "bootstrap_version": BOOTSTRAP_VERSION,
        "replication_version": REPLICATION_VERSION,
        "release_commit": release,
        "epoch": "epoch-resume-1234",
        "schema_fingerprint": "f" * 64,
        "start_watermark": 17,
        "page_default_rows": 250,
        "tables": [
            {
                "name": "sample",
                "create_sql": "CREATE TABLE sample(id INTEGER PRIMARY KEY,value TEXT NOT NULL)",
                "columns": ["id", "value"],
                "without_rowid": False,
            }
        ],
        "post_schema": [],
        "sqlite_sequence": [],
        "pragmas": {"user_version": 0, "application_id": 0},
    }


def _page(
    release: str,
    *,
    rows: list[dict[str, object]],
    done: bool,
    next_cursor: str | None,
) -> dict[str, object]:
    return {
        "release_commit": release,
        "epoch": "epoch-resume-1234",
        "schema_fingerprint": "f" * 64,
        "table": "sample",
        "columns": ["id", "value"],
        "rows": rows,
        "payload_bytes": 64,
        "done": done,
        "next_cursor": next_cursor,
    }


def test_transient_503_preserves_page_checkpoint_and_resumes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    release = "a" * 40
    base = "https://runtime.example"
    page_cursors: list[str | None] = []
    failed_once = False

    def fake_open_json(request, *, timeout=30.0):
        nonlocal failed_once
        _ = timeout
        url = request.full_url
        if url.endswith("/v1/operations/certification-db-logical-bootstrap"):
            return _manifest(release)

        query = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
        cursor = query.get("cursor", [None])[0]
        page_cursors.append(cursor)
        if cursor is None:
            return _page(
                release,
                rows=[{"rowid": 1, "values": [1, "first"]}],
                done=False,
                next_cursor="cursor-1",
            )
        if cursor == "cursor-1" and not failed_once:
            failed_once = True
            raise client.LogicalBootstrapTransportError(
                "certification logical bootstrap HTTP failure:503"
            )
        assert cursor == "cursor-1"
        return _page(
            release,
            rows=[{"rowid": 2, "values": [2, "second"]}],
            done=True,
            next_cursor=None,
        )

    monkeypatch.setattr(client, "_open_json", fake_open_json)

    first_destination = tmp_path / "attempt-one.sqlite3"
    with pytest.raises(client.LogicalBootstrapTransportError):
        client.logical_bootstrap(
            first_destination,
            base=base,
            token="test-token",
            expected_release=release,
        )

    assert not first_destination.exists()
    work, checkpoint = client._work_paths(
        first_destination, base=base, expected_release=release
    )
    assert work.exists()
    assert checkpoint.exists()

    second_destination = tmp_path / "attempt-two.sqlite3"
    result = client.logical_bootstrap(
        second_destination,
        base=base,
        token="test-token",
        expected_release=release,
    )

    assert result["resumable_page_checkpoint"] is True
    assert result["bootstrap_rows"] == 2
    assert page_cursors == [None, "cursor-1", "cursor-1"]
    assert second_destination.exists()
    assert not work.exists()
    assert not checkpoint.exists()

    connection = sqlite3.connect(second_destination)
    try:
        rows = connection.execute(
            "SELECT id,value FROM sample ORDER BY id"
        ).fetchall()
    finally:
        connection.close()
    assert rows == [(1, "first"), (2, "second")]


def test_identity_change_discards_partial_bootstrap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    release = "b" * 40
    base = "https://runtime.example"
    phase = {"value": 0}

    def first_attempt(request, *, timeout=30.0):
        _ = timeout
        url = request.full_url
        if url.endswith("/v1/operations/certification-db-logical-bootstrap"):
            return _manifest(release)
        query = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
        cursor = query.get("cursor", [None])[0]
        if cursor is None:
            phase["value"] = 1
            return _page(
                release,
                rows=[{"rowid": 1, "values": [1, "first"]}],
                done=False,
                next_cursor="cursor-1",
            )
        raise client.LogicalBootstrapTransportError("timeout")

    monkeypatch.setattr(client, "_open_json", first_attempt)
    destination = tmp_path / "attempt.sqlite3"

    with pytest.raises(client.LogicalBootstrapTransportError):
        client.logical_bootstrap(
            destination,
            base=base,
            token="test-token",
            expected_release=release,
        )

    work, checkpoint = client._work_paths(
        destination, base=base, expected_release=release
    )
    assert work.exists()
    assert checkpoint.exists()

    def changed_manifest_then_restart(request, *, timeout=30.0):
        _ = timeout
        url = request.full_url
        if url.endswith("/v1/operations/certification-db-logical-bootstrap"):
            manifest = _manifest(release)
            manifest["epoch"] = "epoch-replaced-5678"
            return manifest
        raise client.LogicalBootstrapRestartRequired("identity changed")

    monkeypatch.setattr(client, "_open_json", changed_manifest_then_restart)

    with pytest.raises(client.LogicalBootstrapRestartRequired):
        client.logical_bootstrap(
            destination,
            base=base,
            token="test-token",
            expected_release=release,
        )

    assert not work.exists()
    assert not checkpoint.exists()
