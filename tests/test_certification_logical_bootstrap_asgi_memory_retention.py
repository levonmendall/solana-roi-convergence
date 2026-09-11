from __future__ import annotations

import asyncio
import gc
import json
import sqlite3
import threading
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
from fastapi import FastAPI
from starlette.responses import JSONResponse

from solana_roi import certification_incremental_replication as replication
from solana_roi import certification_logical_bootstrap as bootstrap
from solana_roi import certification_logical_bootstrap_client as bootstrap_client
from solana_roi import durable_bootstrap_memory_repair as durable_memory


TARGET_TABLE = "anonymous_candidate_latency_failures"
PAGE_ROWS = 500
PAGE_COUNT = 4
ROW_PAYLOAD_BYTES = 24 * 1024
ANON_RETENTION_PROOF_KIB = 4 * 1024
ANON_RELEASE_PROOF_KIB = 2 * 1024


class _Store:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.RLock()
        self.db = sqlite3.connect(path, check_same_thread=False)

    def close(self) -> None:
        self.db.close()


def _memory_kib() -> dict[str, int]:
    values = {"rss_kib": 0, "anon_kib": 0, "file_kib": 0}
    try:
        fields = Path("/proc/self/status").read_text(encoding="utf-8").splitlines()
    except OSError:
        return values
    mapping = {"VmRSS:": "rss_kib", "RssAnon:": "anon_kib", "RssFile:": "file_kib"}
    for line in fields:
        parts = line.split()
        if len(parts) >= 2 and parts[0] in mapping:
            try:
                values[mapping[parts[0]]] = int(parts[1])
            except ValueError:
                pass
    return values


def _build_source(path: Path) -> _Store:
    store = _Store(path)
    payload = "x" * ROW_PAYLOAD_BYTES
    with store.db:
        store.db.execute("PRAGMA journal_mode=WAL")
        store.db.execute(
            "CREATE TABLE anonymous_candidate_latency_failures("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "failed_at TEXT NOT NULL,reason TEXT NOT NULL,outcome TEXT NOT NULL,"
            "count INTEGER NOT NULL,max_age_ms REAL NOT NULL)"
        )
        store.db.execute(
            "CREATE INDEX ix_anonymous_candidate_latency_failures_failed_at "
            "ON anonymous_candidate_latency_failures(failed_at)"
        )
        rows = (
            (
                f"2026-09-11T12:{index // 60:02d}:{index % 60:02d}+00:00",
                payload,
                "expired_before_entry",
                1,
                float(index),
            )
            for index in range(PAGE_ROWS * PAGE_COUNT)
        )
        store.db.executemany(
            "INSERT INTO anonymous_candidate_latency_failures("
            "failed_at,reason,outcome,count,max_age_ms) VALUES(?,?,?,?,?)",
            rows,
        )
    replication.prepare_bootstrap(store)
    return store


def _build_replica(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    connection.execute(
        "CREATE TABLE anonymous_candidate_latency_failures("
        "id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "failed_at TEXT NOT NULL,reason TEXT NOT NULL,outcome TEXT NOT NULL,"
        "count INTEGER NOT NULL,max_age_ms REAL NOT NULL)"
    )
    connection.commit()
    return connection


def test_large_logical_bootstrap_page_releases_heap_only_after_asgi_send(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Regression for post-ASGI-response anonymous-memory retention.

    The authoritative side must not run page cleanup before FastAPI/Starlette has
    serialized and sent the page. This test drives the real logical-bootstrap HTTP
    endpoint, records RssAnon and RssFile independently at each lifecycle boundary,
    persists the received rows into a certifier-style SQLite replica, and proves that
    large response allocations remain reclaimable after the final ASGI body send.

    Current main is expected to FAIL the final lifecycle assertion because _page()
    invokes split._drop_file_cache() in its finally block before JSONResponse.render()
    and http.response.body. The repair should move the cleanup lifecycle boundary
    after serialization/send without changing pagination, evidence, or the 94% guard.
    """

    release = "a" * 40
    token = "asgi-memory-regression-token"
    monkeypatch.setenv("SOLANA_ROI_RELEASE_COMMIT", release)
    monkeypatch.setenv("SOLANA_ROI_CERTIFICATION_SHARED_TOKEN", token)
    monkeypatch.setenv("SOLANA_ROI_CERTIFICATION_LOGICAL_BOOTSTRAP_PAGE_BYTES", str(16 * 1024 * 1024))
    monkeypatch.setattr(bootstrap.split, "_release_commit", lambda: release)

    source = _build_source(tmp_path / "authoritative.sqlite3")
    replica = _build_replica(tmp_path / "certifier.sqlite3")
    runtime = SimpleNamespace(store=source)
    identity = replication.prepare_bootstrap(source)

    trace: list[dict[str, Any]] = []
    active_page = {"value": -1}

    def sample(phase: str) -> dict[str, Any]:
        record: dict[str, Any] = {
            "page": int(active_page["value"]),
            "phase": phase,
            **_memory_kib(),
        }
        trace.append(record)
        return record

    original_stream = bootstrap._stream_page_records

    def observed_stream(*args, **kwargs):
        sample("sqlite_fetch_cursor_ready")
        result = original_stream(*args, **kwargs)
        sample("row_materialization_complete")
        return result

    monkeypatch.setattr(bootstrap, "_stream_page_records", observed_stream)

    original_page = bootstrap._page

    def observed_page(*args, **kwargs):
        result = original_page(*args, **kwargs)
        sample("page_returned")
        return result

    monkeypatch.setattr(bootstrap, "_page", observed_page)

    def observed_cleanup(path: Path) -> bool:
        sample("page_cleanup_start")
        released = durable_memory._release_sqlite_file_cache(Path(path))
        # Model the pressure branch of the installed production cleanup hook. Doing
        # this unconditionally makes the ordering proof stronger: even an eager heap
        # trim cannot reclaim response objects that do not exist until ASGI serializes.
        durable_memory._trim_process_heap()
        sample("page_cleanup_complete")
        return released

    monkeypatch.setattr(bootstrap.split, "_drop_file_cache", observed_cleanup)

    original_render = JSONResponse.render

    def observed_render(self, content):
        sample("json_serialization_start")
        body = original_render(self, content)
        sample("json_serialization_complete")
        return body

    monkeypatch.setattr(JSONResponse, "render", observed_render)

    app = FastAPI()
    bootstrap.install_certification_logical_bootstrap(app, lambda: runtime)

    class _ObservedASGI:
        def __init__(self, wrapped) -> None:
            self.wrapped = wrapped

        async def __call__(self, scope, receive, send) -> None:
            async def observed_send(message) -> None:
                if message.get("type") == "http.response.start":
                    sample("response_start_send_before")
                    await send(message)
                    sample("response_start_send_after")
                    return
                if message.get("type") == "http.response.body" and not message.get("more_body", False):
                    sample("response_final_send_before")
                    await send(message)
                    sample("response_final_send_after")
                    return
                await send(message)

            await self.wrapped(scope, receive, observed_send)
            sample("asgi_application_returned")

    observed_app = _ObservedASGI(app)

    async def exercise() -> None:
        transport = httpx.ASGITransport(app=observed_app)
        cursor: str | None = None
        async with httpx.AsyncClient(transport=transport, base_url="http://authoritative.test") as client:
            for page_index in range(PAGE_COUNT):
                active_page["value"] = page_index
                gc.collect()
                durable_memory._trim_process_heap()
                sample("pre_page_baseline")
                params = {
                    "table": TARGET_TABLE,
                    "epoch": str(identity["epoch"]),
                    "schema_fingerprint": str(identity["schema_fingerprint"]),
                    "limit": PAGE_ROWS,
                }
                if cursor:
                    params["cursor"] = cursor
                response = await client.get(
                    "/v1/operations/certification-db-logical-bootstrap-page",
                    params=params,
                    headers={"X-Certification-Token": token},
                )
                sample("certifier_receipt")
                assert response.status_code == 200, response.text[:500]
                payload = response.json()
                sample("certifier_json_decoded")
                assert payload["table"] == TARGET_TABLE
                assert int(payload["row_count"]) == PAGE_ROWS

                decoded_rows: list[tuple[Any, ...]] = []
                for record in payload["rows"]:
                    values = tuple(bootstrap_client._decode_value(value) for value in record["values"])
                    decoded_rows.append((int(record["rowid"]), *values))
                replica.executemany(
                    "INSERT OR REPLACE INTO anonymous_candidate_latency_failures("
                    "rowid,id,failed_at,reason,outcome,count,max_age_ms) VALUES(?,?,?,?,?,?,?)",
                    decoded_rows,
                )
                replica.commit()
                sample("certifier_persisted")

                cursor = payload.get("next_cursor")
                assert bool(payload.get("done")) is (page_index == PAGE_COUNT - 1)
                if page_index < PAGE_COUNT - 1:
                    assert isinstance(cursor, str) and cursor

                # Drop every certifier-side page object, then force allocator release.
                # This distinguishes server/ASGI heap retention from SQLite file cache.
                del decoded_rows
                del payload
                del response
                gc.collect()
                durable_memory._trim_process_heap()
                sample("post_page_cleanup")

    try:
        asyncio.run(exercise())

        summaries: list[dict[str, Any]] = []
        lifecycle_failures: list[int] = []
        retention_proofs = 0
        release_proofs = 0
        for page_index in range(PAGE_COUNT):
            records = [record for record in trace if record["page"] == page_index]
            phases = [str(record["phase"]) for record in records]
            by_phase = {str(record["phase"]): record for record in records}
            required = {
                "pre_page_baseline",
                "sqlite_fetch_cursor_ready",
                "row_materialization_complete",
                "page_cleanup_complete",
                "page_returned",
                "json_serialization_start",
                "json_serialization_complete",
                "response_final_send_after",
                "asgi_application_returned",
                "certifier_receipt",
                "certifier_persisted",
                "post_page_cleanup",
            }
            assert required.issubset(by_phase), {"missing": sorted(required - set(by_phase)), "trace": records}

            baseline = by_phase["pre_page_baseline"]
            sent = by_phase["response_final_send_after"]
            cleaned = by_phase["post_page_cleanup"]
            retained_anon = int(sent["anon_kib"]) - int(baseline["anon_kib"])
            retained_file = int(sent["file_kib"]) - int(baseline["file_kib"])
            released_anon = int(sent["anon_kib"]) - int(cleaned["anon_kib"])
            if retained_anon >= ANON_RETENTION_PROOF_KIB:
                retention_proofs += 1
            if released_anon >= ANON_RELEASE_PROOF_KIB:
                release_proofs += 1

            cleanup_after_send = phases.index("page_cleanup_complete") > phases.index("response_final_send_after")
            if not cleanup_after_send:
                lifecycle_failures.append(page_index)
            summaries.append(
                {
                    "page": page_index,
                    "baseline_anon_kib": baseline["anon_kib"],
                    "post_send_anon_kib": sent["anon_kib"],
                    "post_cleanup_anon_kib": cleaned["anon_kib"],
                    "post_send_file_kib": sent["file_kib"],
                    "anon_retained_after_send_kib": retained_anon,
                    "anon_released_after_client_cleanup_kib": released_anon,
                    "file_delta_at_send_kib": retained_file,
                    "cleanup_after_send": cleanup_after_send,
                }
            )

        print("ASGI_BOOTSTRAP_MEMORY_TRACE " + json.dumps(summaries, sort_keys=True), flush=True)

        # Memory proof: large pages must visibly elevate anonymous memory after the
        # final ASGI body send, and explicit post-page object/heap cleanup must release
        # a material part of it. File-backed RSS is reported separately and never used
        # as a substitute for this anonymous-memory proof.
        assert retention_proofs >= PAGE_COUNT - 1, summaries
        assert release_proofs >= PAGE_COUNT - 1, summaries

        # Deterministic current-main failure: cleanup currently happens in _page()'s
        # finally block, before JSON serialization and the final ASGI body send. The
        # lifecycle repair is complete only when every page cleanup occurs afterwards.
        assert not lifecycle_failures, {
            "reason": "logical-bootstrap cleanup ran before final ASGI response send",
            "failing_pages": lifecycle_failures,
            "memory": summaries,
        }
    finally:
        replica.close()
        source.close()
