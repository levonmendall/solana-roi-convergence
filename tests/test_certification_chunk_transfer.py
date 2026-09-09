from __future__ import annotations

import io
import json
import sqlite3
import urllib.parse

from fastapi import FastAPI
from fastapi.testclient import TestClient

from solana_roi import certification_chunk_transfer as chunk
from solana_roi import certification_service_split as split
from solana_roi import certifier_service


def _sqlite_payload(tmp_path) -> bytes:
    path = tmp_path / "payload.sqlite3"
    connection = sqlite3.connect(path)
    try:
        connection.execute("CREATE TABLE evidence(id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
        connection.executemany(
            "INSERT INTO evidence(value) VALUES (?)",
            [("canonical",), ("forward",), ("settled",)],
        )
        connection.commit()
    finally:
        connection.close()
    return path.read_bytes()


class _Response:
    def __init__(self, body: bytes, headers: dict[str, str] | None = None):
        self._body = io.BytesIO(body)
        self.headers = headers or {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self, size: int = -1) -> bytes:
        return self._body.read(size)


def test_authoritative_chunk_routes_hold_one_immutable_snapshot_until_release(tmp_path, monkeypatch) -> None:
    source = tmp_path / "canonical.sqlite3"
    connection = sqlite3.connect(source)
    try:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("CREATE TABLE evidence(id INTEGER PRIMARY KEY, value BLOB NOT NULL)")
        connection.executemany(
            "INSERT INTO evidence(value) VALUES (zeroblob(4096))",
            [() for _ in range(32)],
        )
        connection.commit()
    finally:
        connection.close()

    class Store:
        path = source

    class Runtime:
        store = Store()

    monkeypatch.setenv("SOLANA_ROI_CERTIFICATION_SHARED_TOKEN", "test-token")
    monkeypatch.setenv("RENDER_GIT_COMMIT", "exact-release")
    monkeypatch.setenv("SOLANA_ROI_CERTIFICATION_TRANSFER_CHUNK_BYTES", "1048576")
    with chunk._ACTIVE_LOCK:
        chunk._ACTIVE.clear()

    app = FastAPI()
    chunk.install_authoritative_snapshot_chunk_transfer(app, lambda: Runtime())
    client = TestClient(app)
    headers = {"X-Certification-Token": "test-token"}

    manifest = client.get("/v1/operations/certification-db-snapshot-manifest", headers=headers)
    assert manifest.status_code == 200
    payload = manifest.json()
    snapshot_id = payload["snapshot_id"]
    total = payload["size_bytes"]
    assert payload["release_commit"] == "exact-release"
    assert payload["paper_only"] is True
    assert payload["live_money_authority"] is False

    first = client.get(
        f"/v1/operations/certification-db-snapshot-chunk/{snapshot_id}",
        params={"offset": 0, "length": min(65536, total)},
        headers=headers,
    )
    assert first.status_code == 200
    assert int(first.headers["x-certification-snapshot-total-bytes"]) == total
    assert first.headers["x-certification-snapshot-id"] == snapshot_id
    assert first.headers["x-release-commit"] == "exact-release"
    assert len(first.content) == min(65536, total)

    release = client.delete(
        f"/v1/operations/certification-db-snapshot-chunk/{snapshot_id}",
        headers=headers,
    )
    assert release.status_code == 200
    assert release.json()["released"] is True
    missing = client.get(
        f"/v1/operations/certification-db-snapshot-chunk/{snapshot_id}",
        params={"offset": 0, "length": 4096},
        headers=headers,
    )
    assert missing.status_code == 404


def test_certifier_reassembles_snapshot_from_bounded_chunks(tmp_path, monkeypatch) -> None:
    body = _sqlite_payload(tmp_path)
    snapshot_id = "snapshot-1"
    release = "exact-release"
    chunk_bytes = 4096
    released: list[str] = []

    def urlopen(request, timeout):
        url = request.full_url
        if url.endswith("/v1/operations/certification-db-snapshot-manifest"):
            payload = {
                "transfer_version": chunk.TRANSFER_VERSION,
                "snapshot_id": snapshot_id,
                "release_commit": release,
                "size_bytes": len(body),
                "chunk_bytes": chunk_bytes,
            }
            return _Response(json.dumps(payload).encode("utf-8"), {"Content-Type": "application/json"})
        if request.get_method() == "DELETE":
            released.append(url)
            return _Response(b"{}", {"Content-Type": "application/json"})
        parsed = urllib.parse.urlparse(url)
        query = urllib.parse.parse_qs(parsed.query)
        offset = int(query["offset"][0])
        length = int(query["length"][0])
        piece = body[offset : offset + length]
        return _Response(
            piece,
            {
                "X-Release-Commit": release,
                "X-Certification-Snapshot-Id": snapshot_id,
                "X-Certification-Snapshot-Offset": str(offset),
                "X-Certification-Snapshot-Total-Bytes": str(len(body)),
                "Content-Length": str(len(piece)),
            },
        )

    monkeypatch.setattr(chunk.urllib.request, "urlopen", urlopen)
    destination = tmp_path / "download.sqlite3"
    observed_release, expected_bytes = chunk.download_snapshot_chunked(
        destination,
        base="https://runtime.invalid",
        token="test-token",
        expected_release=release,
    )

    assert observed_release == release
    assert expected_bytes == len(body)
    assert destination.read_bytes() == body
    assert released
    assert certifier_service._validate_sqlite_snapshot(destination, expected_bytes) == len(body)


def test_certifier_fails_closed_on_truncated_chunk_and_releases_snapshot(tmp_path, monkeypatch) -> None:
    body = _sqlite_payload(tmp_path)
    snapshot_id = "snapshot-2"
    release = "exact-release"
    released: list[str] = []

    def urlopen(request, timeout):
        url = request.full_url
        if url.endswith("/v1/operations/certification-db-snapshot-manifest"):
            payload = {
                "snapshot_id": snapshot_id,
                "release_commit": release,
                "size_bytes": len(body),
                "chunk_bytes": 4096,
            }
            return _Response(json.dumps(payload).encode("utf-8"))
        if request.get_method() == "DELETE":
            released.append(url)
            return _Response(b"{}")
        parsed = urllib.parse.urlparse(url)
        query = urllib.parse.parse_qs(parsed.query)
        offset = int(query["offset"][0])
        length = int(query["length"][0])
        piece = body[offset : offset + max(0, length - 1)]
        return _Response(
            piece,
            {
                "X-Release-Commit": release,
                "X-Certification-Snapshot-Id": snapshot_id,
                "X-Certification-Snapshot-Offset": str(offset),
                "X-Certification-Snapshot-Total-Bytes": str(len(body)),
            },
        )

    monkeypatch.setattr(chunk.urllib.request, "urlopen", urlopen)
    destination = tmp_path / "truncated.sqlite3"
    try:
        chunk.download_snapshot_chunked(
            destination,
            base="https://runtime.invalid",
            token="test-token",
            expected_release=release,
        )
    except RuntimeError as exc:
        assert "chunk length mismatch" in str(exc)
    else:
        raise AssertionError("truncated certification chunk must fail closed")
    assert released
