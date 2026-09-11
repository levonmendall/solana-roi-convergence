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


def _process_memory_kib() -> dict[str, int]:
    values = {"rss_kib": 0, "rss_anon_kib": 0, "rss_file_kib": 0}
    try:
        fields = Path("/proc/self/status").read_text(encoding="utf-8").splitlines()
    except OSError:
        return values
    mapping = {"VmRSS:": "rss_kib", "RssAnon:": "rss_anon_kib", "RssFile:": "rss_file_kib"}
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
    """Prove the real bootstrap endpoint retains response heap past _page().

    This test deliberately runs through FastAPI/ASGI rather than invoking _page()
    directly. It records process anonymous RSS and cgroup anonymous/file memory
    independently through SQLite fetch, Python row materialization, response creation,
    JSON serialization, final ASGI send, certifier receipt/persistence, and cleanup.

    Current main must fail the final lifecycle assertion: _page() invokes
    split._drop_file_cache() in its finally block, before the response object exists,
    before JSON serialization, and before the final ASGI body send. The production
    repair is allowed to move cleanup only; pagination, historical truth, and the 94%
    fail-closed guard are intentionally outside this regression's authority.
    """

    release = "a" * 40
    token = "asgi-memory-regression-token"
    monkeypatch.setenv("SOLANA_ROI_RELEASE_COMMIT", release)
    monkeypatch.setenv("SOLANA_ROI_CERTIFICATION_SHARED_TOKEN", token)
    monkeypatch.setenv(
        "SOLANA_ROI_CERTIFICATION_LOGICAL_BOOTSTRAP_PAGE_BYTES",
        str(16 * 1024 * 1024),
    )
    monkeypatch.setattr(bootstrap.split, "_release_commit", lambda: release)

    source = _build_source(tmp_path / "authoritative.sqlite3")
    replica = _build_replica(tmp_path / "certifier.sqlite3")
    runtime = SimpleNamespace(store=source)
    identity = replication.prepare_bootstrap(source)

    trace: list[dict[str, Any]] = []
    active_page = {"value": -1}

    def sample(phase: str) -> dict[str, Any]:
        cgroup = durable_memory._cgroup_memory()
        record: dict[str, Any] = {
            "page": int(active_page["value"]),
            "phase": phase,
            **_process_memory_kib(),
            "cgroup_anon_kib": int(cgroup.get("anon_bytes") or 0) // 1024,
            "cgroup_file_kib": int(cgroup.get("file_bytes") or 0) // 1024,
            "cgroup_dirty_kib": int(cgroup.get("file_dirty_bytes") or 0) // 1024,
            "cgroup_writeback_kib": int(cgroup.get("file_writeback_bytes") or 0) // 1024,
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
        # Model the production pressure branch unconditionally. Even an eager trim at
        # this point cannot reclaim JSON/body allocations that have not been created.
        durable_memory._trim_process_heap()
        sample("page_cleanup_complete")
        return released

    monkeypatch.setattr(bootstrap.split, "_drop_file_cache", observed_cleanup)

    class _ObservedJSONResponse(JSONResponse):
        def __init__(self, *args, **kwargs) -> None:
            sample("response_creation_start")
            super().__init__(*args, **kwargs)
            sample("response_creation_complete")

        def render(self, content) -> bytes:
            sample("json_serialization_start")
            body = super().render(content)
            sample("json_serialization_complete")
            return body

    # Register the observed response class before the real bootstrap route is added.
    # This instruments FastAPI's actual serialization path instead of monkeypatching a
    # class reference that the route may already have captured.
    app = FastAPI(default_response_class=_ObservedJSONResponse)
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
                    values = tuple(
                        bootstrap_client._decode_value(value)
                        for value in record["values"]
                    )
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
                "response_creation_start",
                "json_serialization_start",
                "json_serialization_complete",
                "response_creation_complete",
                "response_final_send_after",
                "asgi_application_returned",
                "certifier_receipt",
                "certifier_persisted",
                "post_page_cleanup",
            }
            assert required.issubset(by_phase), {
                "missing": sorted(required - set(by_phase)),
                "trace": records,
            }

            baseline = by_phase["pre_page_baseline"]
            sent = by_phase["response_final_send_after"]
            cleaned = by_phase["post_page_cleanup"]
            retained_anon = int(sent["rss_anon_kib"]) - int(baseline["rss_anon_kib"])
            released_anon = int(sent["rss_anon_kib"]) - int(cleaned["rss_anon_kib"])
            if retained_anon >= ANON_RETENTION_PROOF_KIB:
                retention_proofs += 1
            if released_anon >= ANON_RELEASE_PROOF_KIB:
                release_proofs += 1

            cleanup_after_send = (
                phases.index("page_cleanup_complete")
                > phases.index("response_final_send_after")
            )
            if not cleanup_after_send:
                lifecycle_failures.append(page_index)
            summaries.append(
                {
                    "page": page_index,
                    "baseline_rss_anon_kib": baseline["rss_anon_kib"],
                    "post_send_rss_anon_kib": sent["rss_anon_kib"],
                    "post_cleanup_rss_anon_kib": cleaned["rss_anon_kib"],
                    "anon_retained_after_send_kib": retained_anon,
                    "anon_released_after_client_cleanup_kib": released_anon,
                    "baseline_cgroup_anon_kib": baseline["cgroup_anon_kib"],
                    "post_send_cgroup_anon_kib": sent["cgroup_anon_kib"],
                    "post_send_cgroup_file_kib": sent["cgroup_file_kib"],
                    "post_send_cgroup_dirty_kib": sent["cgroup_dirty_kib"],
                    "post_send_cgroup_writeback_kib": sent["cgroup_writeback_kib"],
                    "cleanup_after_send": cleanup_after_send,
                }
            )

        print(
            "ASGI_BOOTSTRAP_MEMORY_TRACE " + json.dumps(summaries, sort_keys=True),
            flush=True,
        )

        # Anonymous-memory proof is independent of the separately reported cgroup
        # file-cache metrics; file pressure cannot satisfy either assertion below.
        assert retention_proofs >= PAGE_COUNT - 1, summaries
        assert release_proofs >= PAGE_COUNT - 1, summaries

        # Current main is expected to fail only here once the memory proof succeeds.
        assert not lifecycle_failures, {
            "reason": "logical-bootstrap cleanup ran before final ASGI response send",
            "failing_pages": lifecycle_failures,
            "memory": summaries,
        }
    finally:
        replica.close()
        source.close()
