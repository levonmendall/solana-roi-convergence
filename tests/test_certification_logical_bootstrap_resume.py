from __future__ import annotations

import sqlite3
import urllib.error
import urllib.parse
from pathlib import Path

import pytest

from solana_roi import certification_logical_bootstrap_client as client
from solana_roi.certification_incremental_replication import REPLICATION_VERSION
from solana_roi.certification_logical_bootstrap import BOOTSTRAP_VERSION


RELEASE = "a" * 40
EPOCH = "resume-epoch-1"
FINGERPRINT = "b" * 64
BASE = "https://runtime.example"


def _manifest() -> dict[str, object]:
    return {
        "bootstrap_version": BOOTSTRAP_VERSION,
        "replication_version": REPLICATION_VERSION,
        "release_commit": RELEASE,
        "epoch": EPOCH,
        "schema_fingerprint": FINGERPRINT,
        "start_watermark": 17,
        "page_default_rows": 250,
        "tables": [
            {
                "name": "items",
                "create_sql": "CREATE TABLE items(id INTEGER PRIMARY KEY,value TEXT NOT NULL)",
                "columns": ["id", "value"],
                "without_rowid": False,
            }
        ],
        "post_schema": [],
        "sqlite_sequence": [],
        "pragmas": {"user_version": 0, "application_id": 0},
    }


def _page(*, cursor: str | None, done: bool) -> dict[str, object]:
    row_id = 1 if cursor is None else 2
    return {
        "release_commit": RELEASE,
        "epoch": EPOCH,
        "schema_fingerprint": FINGERPRINT,
        "table": "items",
        "columns": ["id", "value"],
        "rows": [{"rowid": row_id, "values": [row_id, f"v{row_id}"]}],
        "payload_bytes": 20,
        "done": done,
        "next_cursor": None if done else "cursor-1",
    }


def test_logical_bootstrap_resumes_exact_cursor_after_resource_pause(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    seen_page_cursors: list[str | None] = []
    allow_resume = False

    def fake_open(request, *, timeout=30.0):
        nonlocal allow_resume
        url = request.full_url
        if "certification-db-logical-bootstrap-page" not in url:
            return _manifest()
        query = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
        cursor = query.get("cursor", [None])[0]
        seen_page_cursors.append(cursor)
        if cursor is None:
            return _page(cursor=None, done=False)
        if not allow_resume:
            raise client.LogicalBootstrapPause("certification logical bootstrap HTTP pause:503")
        return _page(cursor="cursor-1", done=True)

    monkeypatch.setattr(client, "_open_json_resumable", fake_open)
    first_destination = tmp_path / "first.sqlite3"
    with pytest.raises(client.LogicalBootstrapPause):
        client.logical_bootstrap(
            first_destination,
            base=BASE,
            token="secret",
            expected_release=RELEASE,
        )

    partial, checkpoint = client._resume_paths(first_destination, base=BASE, expected_release=RELEASE)
    assert partial.is_file()
    assert checkpoint.is_file()
    with sqlite3.connect(partial) as connection:
        assert connection.execute("SELECT id,value FROM items ORDER BY id").fetchall() == [(1, "v1")]

    allow_resume = True
    second_destination = tmp_path / "second.sqlite3"
    result = client.logical_bootstrap(
        second_destination,
        base=BASE,
        token="secret",
        expected_release=RELEASE,
    )

    assert result["durable_page_resume"] is True
    assert result["resource_pressure_503_resumable"] is True
    assert seen_page_cursors.count(None) == 1
    assert seen_page_cursors[-1] == "cursor-1"
    with sqlite3.connect(second_destination) as connection:
        assert connection.execute("SELECT id,value FROM items ORDER BY id").fetchall() == [
            (1, "v1"),
            (2, "v2"),
        ]
    assert not partial.exists()
    assert not checkpoint.exists()


def test_http_503_is_classified_as_resumable_pause(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail_503(*args, **kwargs):
        raise urllib.error.HTTPError("https://runtime.example/page", 503, "busy", None, None)

    monkeypatch.setattr(client.urllib.request, "urlopen", fail_503)
    request = client._request("https://runtime.example/page", "secret")
    with pytest.raises(client.LogicalBootstrapPause, match="HTTP pause:503"):
        client._open_json(request, timeout=1.0)
