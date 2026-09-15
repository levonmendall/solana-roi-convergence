from __future__ import annotations

import asyncio
import sqlite3
import threading
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI
from starlette.responses import JSONResponse

from solana_roi import certification_incremental_replication as replication
from solana_roi import certification_logical_bootstrap as logical
from solana_roi import durable_bootstrap_memory_repair as durable_memory
from solana_roi import logical_bootstrap_page_cache_repair as lifecycle


PAGE_PATH = "/v1/operations/certification-db-logical-bootstrap-page"


def _store(tmp_path: Path):
    source = tmp_path / "authoritative.sqlite"
    db = sqlite3.connect(source, check_same_thread=False)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("CREATE TABLE anonymous_certification_outcomes(value TEXT NOT NULL)")
    db.executemany(
        "INSERT INTO anonymous_certification_outcomes(value) VALUES (?)",
        [(f"outcome-{index:04d}-" + "x" * 4096,) for index in range(64)],
    )
    db.commit()
    return SimpleNamespace(path=source, db=db, _lock=threading.RLock())


def test_heap_trim_runs_after_json_response_call_returns(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The allocator trim must run after Starlette releases the response call stack.

    Production showed anonymous memory rising across thousands of logical-bootstrap
    pages even though the existing regression reported one heap trim per page.  The
    missing boundary is that FastAPI BackgroundTasks executes after the final body
    send but still inside ``Response.__call__``, while the serialized response body is
    still owned by that response object.  Trimming at that point can therefore occur
    before those bytes are eligible for allocator release.

    This regression exercises the exact production composition: the logical-bootstrap
    route plus the page lifecycle gate.  It fails on the current implementation if the
    heap trim runs while JSONResponse.__call__ is still active and passes only when
    cleanup is deferred until the response call has fully unwound.
    """

    store = _store(tmp_path)
    identity = replication.prepare_bootstrap(store)
    response_call_active = {"value": False}
    trim_observations: list[bool] = []

    monkeypatch.setattr(replication, "_require_shared_token", lambda token: None)
    monkeypatch.setattr(
        durable_memory,
        "_guard_raw_cgroup",
        lambda path, **kwargs: {
            "current_bytes": 128 * 1024 * 1024,
            "max_bytes": 2 * 1024 * 1024 * 1024,
            "fraction": 0.0625,
            "anon_bytes": 64 * 1024 * 1024,
            "file_bytes": 64 * 1024 * 1024,
        },
    )
    monkeypatch.setattr(logical.split, "_drop_file_cache", lambda path: True)

    original_call = JSONResponse.__call__

    async def instrumented_call(self, scope, receive, send):
        if scope.get("path") != PAGE_PATH:
            return await original_call(self, scope, receive, send)
        response_call_active["value"] = True
        try:
            return await original_call(self, scope, receive, send)
        finally:
            response_call_active["value"] = False

    monkeypatch.setattr(JSONResponse, "__call__", instrumented_call)

    def observed_trim() -> bool:
        trim_observations.append(bool(response_call_active["value"]))
        return True

    monkeypatch.setattr(durable_memory, "_trim_process_heap", observed_trim)

    app = FastAPI()
    logical.install_certification_logical_bootstrap(
        app,
        lambda: SimpleNamespace(store=store),
    )
    lifecycle.install_logical_bootstrap_page_cache_repair(app)

    async def scenario() -> None:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            response = await client.get(
                PAGE_PATH,
                params={
                    "table": "anonymous_certification_outcomes",
                    "epoch": str(identity["epoch"]),
                    "schema_fingerprint": str(identity["schema_fingerprint"]),
                    "limit": 16,
                },
                headers={"X-Certification-Token": "token"},
            )
            assert response.status_code == 200
            payload = response.json()
            assert payload["row_count"] == 16
            assert payload["paper_only"] is True
            assert payload["live_money_authority"] is False

    try:
        asyncio.run(scenario())
    finally:
        store.db.close()

    assert trim_observations == [False], (
        "heap trim ran while JSONResponse.__call__ still owned the serialized body; "
        f"observed={trim_observations}"
    )
