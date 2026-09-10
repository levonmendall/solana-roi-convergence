from __future__ import annotations

import json
import sqlite3
import urllib.error
import urllib.parse
from pathlib import Path

import pytest

from solana_roi import certification_logical_bootstrap as server
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
        "page_max_rows": 500,
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


def test_logical_bootstrap_preserves_cursor_and_reduced_limit_across_resource_pause(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen_pages: list[tuple[str | None, int]] = []
    allow_resume = False

    monkeypatch.setenv("SOLANA_ROI_CERTIFIER_LOGICAL_BOOTSTRAP_ADAPTIVE_PAGE_ATTEMPTS", "2")
    monkeypatch.setattr(client.time, "sleep", lambda _seconds: None)

    def fake_open(request, *, timeout=30.0):
        nonlocal allow_resume
        url = request.full_url
        if "certification-db-logical-bootstrap-page" not in url:
            return _manifest()
        query = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
        cursor = query.get("cursor", [None])[0]
        limit = int(query["limit"][0])
        seen_pages.append((cursor, limit))
        if cursor is None:
            return _page(cursor=None, done=False)
        if not allow_resume:
            raise client.LogicalBootstrapPause(
                "certification logical bootstrap HTTP pause:503",
                status_code=503,
            )
        return _page(cursor="cursor-1", done=True)

    monkeypatch.setattr(client, "_open_json", fake_open)
    first_destination = tmp_path / "first.sqlite3"
    with pytest.raises(client.LogicalBootstrapPause):
        client.logical_bootstrap(
            first_destination,
            base=BASE,
            token="secret",
            expected_release=RELEASE,
        )

    partial, checkpoint_path = client._resume_paths(first_destination, base=BASE, expected_release=RELEASE)
    assert partial.is_file()
    assert checkpoint_path.is_file()
    checkpoint = json.loads(checkpoint_path.read_text())
    assert checkpoint["cursor"] == "cursor-1"
    assert checkpoint["page_rows"] == 62
    assert checkpoint["resource_pressure_pauses"] == 2
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
    assert result["resource_pressure_503_adaptive"] is True
    assert result["cursor_advances_only_after_commit"] is True
    assert seen_pages.count((None, 250)) == 1
    assert seen_pages[-1] == ("cursor-1", 62)
    with sqlite3.connect(second_destination) as connection:
        assert connection.execute("SELECT id,value FROM items ORDER BY id").fetchall() == [
            (1, "v1"),
            (2, "v2"),
        ]
    assert not partial.exists()
    assert not checkpoint_path.exists()


def test_503_retries_same_cursor_at_smaller_limit_without_gaps_or_duplicates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    total = 600
    failed_once = False
    seen_pages: list[tuple[str | None, int]] = []
    monkeypatch.setattr(client.time, "sleep", lambda _seconds: None)

    def fake_open(request, *, timeout=30.0):
        nonlocal failed_once
        parsed = urllib.parse.urlparse(request.full_url)
        if not parsed.path.endswith("-page"):
            return _manifest()
        query = urllib.parse.parse_qs(parsed.query)
        cursor = query.get("cursor", [None])[0]
        limit = int(query["limit"][0])
        seen_pages.append((cursor, limit))
        start = int(cursor.split("-", 1)[1]) if cursor else 0
        if start == 250 and limit == 250 and not failed_once:
            failed_once = True
            raise client.LogicalBootstrapPause(
                "certification logical bootstrap HTTP pause:503",
                status_code=503,
            )
        stop = min(total, start + limit)
        rows = [
            {"rowid": row_id, "values": [row_id, f"v{row_id}"]}
            for row_id in range(start + 1, stop + 1)
        ]
        done = stop >= total
        return {
            "release_commit": RELEASE,
            "epoch": EPOCH,
            "schema_fingerprint": FINGERPRINT,
            "table": "items",
            "columns": ["id", "value"],
            "rows": rows,
            "payload_bytes": len(rows) * 20,
            "done": done,
            "next_cursor": None if done else f"cursor-{stop}",
        }

    monkeypatch.setattr(client, "_open_json", fake_open)
    destination = tmp_path / "replica.sqlite3"
    result = client.logical_bootstrap(
        destination,
        base=BASE,
        token="secret",
        expected_release=RELEASE,
    )

    failed_index = seen_pages.index(("cursor-250", 250))
    assert seen_pages[failed_index + 1] == ("cursor-250", 125)
    assert result["resource_pressure_pauses"] == 1
    assert result["adaptive_page_rows"] <= 250
    with sqlite3.connect(destination) as connection:
        ids = [row[0] for row in connection.execute("SELECT id FROM items ORDER BY id").fetchall()]
    assert ids == list(range(1, total + 1))
    assert len(ids) == len(set(ids))


def test_http_503_is_classified_as_resumable_pressure_pause(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail_503(*args, **kwargs):
        raise urllib.error.HTTPError("https://runtime.example/page", 503, "busy", None, None)

    monkeypatch.setattr(client.urllib.request, "urlopen", fail_503)
    request = client._request("https://runtime.example/page", "secret")
    with pytest.raises(client.LogicalBootstrapPause, match="HTTP pause:503") as exc_info:
        client._open_json(request, timeout=1.0)
    assert exc_info.value.status_code == 503


def test_pinned_reader_disables_mmap_and_uses_private_one_mib_cache(tmp_path: Path) -> None:
    path = tmp_path / "source.sqlite3"
    connection = sqlite3.connect(path)
    connection.execute("CREATE TABLE sample(id INTEGER PRIMARY KEY,value TEXT)")
    connection.commit()
    connection.close()

    class Store:
        pass

    store = Store()
    store.path = path
    reader = server._pinned_reader(store)
    try:
        assert reader.execute("PRAGMA query_only").fetchone()[0] == 1
        assert reader.execute("PRAGMA mmap_size").fetchone()[0] == 0
        assert reader.execute("PRAGMA cache_size").fetchone()[0] == -server.READER_CACHE_KIB
        assert reader.execute("PRAGMA temp_store").fetchone()[0] == 1
    finally:
        reader.close()
