from __future__ import annotations

import asyncio
import sqlite3
import threading
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI

from solana_roi import certification_incremental_replication as replication
from solana_roi import certification_logical_bootstrap as logical
from solana_roi import durable_bootstrap_memory_repair as durable_memory
from solana_roi import logical_bootstrap_page_cache_repair as lifecycle


PAGE_PATH = "/v1/operations/certification-db-logical-bootstrap-page"
SIMULATED_SERIALIZED_HEAP_BYTES = 48 * 1024 * 1024
SIMULATED_FILE_CACHE_BYTES = 16 * 1024 * 1024
SIMULATED_CRITICAL_BYTES = 192 * 1024 * 1024


def _pids_current() -> int | None:
    try:
        return int(Path("/sys/fs/cgroup/pids.current").read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        return None


def _store(tmp_path: Path):
    source = tmp_path / "authoritative.sqlite"
    db = sqlite3.connect(source, check_same_thread=False)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("CREATE TABLE anonymous_candidate_latency_failures(value TEXT NOT NULL)")
    db.executemany(
        "INSERT INTO anonymous_candidate_latency_failures(value) VALUES (?)",
        [(f"failure-{index:05d}-" + "x" * 4096,) for index in range(2_400)],
    )
    db.commit()
    return SimpleNamespace(path=source, db=db, _lock=threading.RLock())


def _app(store):
    app = FastAPI()
    logical.install_certification_logical_bootstrap(app, lambda: SimpleNamespace(store=store))
    lifecycle.install_logical_bootstrap_page_cache_repair(app)
    return app


def test_real_asgi_pages_reclaim_serialization_heap_after_final_send(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Production-shaped regression for the exact long-bootstrap failure boundary.

    The real logical-bootstrap endpoint, FastAPI serialization, ASGI final-body send,
    Starlette BackgroundTasks, lifecycle gate, SQLite keyset cursor, and bounded worker
    pool all execute. The test models the allocator-resident anonymous bytes observed
    in production only at the final ASGI body send. File cache and anonymous heap are
    tracked separately.

    Before the repair, post-send cleanup evicts SQLite file cache but never performs a
    heap trim after serialization. Anonymous bytes therefore accumulate across pages
    until the unchanged raw-cgroup guard fails closed with 503 at the same cursor.
    The repair must reclaim that serialization heap after final send and before the
    page lifecycle gate releases, so every page continues to advance without changing
    the 94% guard or pagination semantics.
    """

    store = _store(tmp_path)
    identity = replication.prepare_bootstrap(store)
    pressure = {
        "anon_bytes": 0,
        "file_bytes": 0,
        "peak_anon_bytes": 0,
        "peak_file_bytes": 0,
        "guard_calls": 0,
        "guard_failures": 0,
        "cache_drops": 0,
        "heap_trims": 0,
        "final_sends": 0,
    }
    worker_threads: set[int] = set()

    def simulated_guard(path: Path, *, allow_wal_checkpoint: bool = True):
        assert path == Path(store.path)
        worker_threads.add(threading.get_ident())
        pressure["guard_calls"] += 1
        total = pressure["anon_bytes"] + pressure["file_bytes"]
        if total >= SIMULATED_CRITICAL_BYTES:
            pressure["guard_failures"] += 1
            raise MemoryError("simulated raw cgroup pressure")
        pressure["file_bytes"] += SIMULATED_FILE_CACHE_BYTES
        pressure["peak_file_bytes"] = max(pressure["peak_file_bytes"], pressure["file_bytes"])
        return {
            "current_bytes": total,
            "max_bytes": 2_147_483_648,
            "fraction": total / 2_147_483_648,
            "anon_bytes": pressure["anon_bytes"],
            "file_bytes": pressure["file_bytes"],
        }

    def simulated_drop_file_cache(path: Path) -> bool:
        assert path == Path(store.path)
        worker_threads.add(threading.get_ident())
        pressure["cache_drops"] += 1
        pressure["file_bytes"] = 0
        return True

    def simulated_trim_process_heap() -> bool:
        worker_threads.add(threading.get_ident())
        pressure["heap_trims"] += 1
        pressure["anon_bytes"] = 0
        return True

    monkeypatch.setattr(replication, "_require_shared_token", lambda token: None)
    monkeypatch.setattr(durable_memory, "_guard_raw_cgroup", simulated_guard)
    monkeypatch.setattr(durable_memory, "_trim_process_heap", simulated_trim_process_heap)
    monkeypatch.setattr(logical.split, "_drop_file_cache", simulated_drop_file_cache)

    app = _app(store)

    async def instrumented_asgi(scope, receive, send):
        async def send_with_serialization_accounting(message):
            if (
                scope.get("path") == PAGE_PATH
                and message.get("type") == "http.response.body"
                and not bool(message.get("more_body", False))
            ):
                pressure["final_sends"] += 1
                pressure["anon_bytes"] += SIMULATED_SERIALIZED_HEAP_BYTES
                pressure["peak_anon_bytes"] = max(
                    pressure["peak_anon_bytes"], pressure["anon_bytes"]
                )
            await send(message)

        await app(scope, receive, send_with_serialization_accounting)

    async def scenario() -> None:
        baseline_threads = threading.active_count()
        baseline_pids = _pids_current()
        observed_pids: list[int] = []
        cursor: str | None = None
        last_rowid = 0
        page_count = 0

        transport = httpx.ASGITransport(app=instrumented_asgi)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            while True:
                params = {
                    "table": "anonymous_candidate_latency_failures",
                    "epoch": str(identity["epoch"]),
                    "schema_fingerprint": str(identity["schema_fingerprint"]),
                    "limit": 120,
                }
                if cursor:
                    params["cursor"] = cursor
                response = await client.get(
                    PAGE_PATH,
                    params=params,
                    headers={"X-Certification-Token": "token"},
                )
                assert response.status_code == 200, {
                    "status": response.status_code,
                    "cursor": cursor,
                    "pressure": pressure,
                    "body": response.text[:300],
                }
                payload = response.json()
                assert payload["paper_only"] is True
                assert payload["live_money_authority"] is False
                assert payload["rows"]
                next_last_rowid = int(payload["rows"][-1]["rowid"])
                assert next_last_rowid > last_rowid
                last_rowid = next_last_rowid
                page_count += 1

                # httpx.ASGITransport returns only after Starlette runs BackgroundTasks.
                # At this boundary both file cache and serialization heap must have been
                # reclaimed before another page may enter the lifecycle gate.
                assert pressure["file_bytes"] == 0
                assert pressure["anon_bytes"] == 0

                current_pids = _pids_current()
                if current_pids is not None:
                    observed_pids.append(current_pids)

                cursor = payload["next_cursor"]
                if bool(payload["done"]):
                    break
                assert isinstance(cursor, str) and cursor

        assert page_count == 20
        assert last_rowid == 2_400
        assert pressure["final_sends"] == page_count
        assert pressure["cache_drops"] == page_count
        assert pressure["heap_trims"] == page_count
        assert pressure["guard_failures"] == 0
        assert pressure["peak_anon_bytes"] == SIMULATED_SERIALIZED_HEAP_BYTES
        assert pressure["peak_file_bytes"] == SIMULATED_FILE_CACHE_BYTES
        assert len(worker_threads) <= 4
        assert threading.active_count() <= baseline_threads + 4
        if baseline_pids is not None and observed_pids:
            assert max(observed_pids) <= baseline_pids + 6
            assert observed_pids[-1] <= baseline_pids + 4

        status = lifecycle.status(app)
        assert status["active"] is False
        assert status["acquisitions"] == page_count
        assert status["releases"] == page_count
        assert status["timeouts"] == 0

    try:
        asyncio.run(scenario())
    finally:
        store.db.close()


def test_heap_reclaim_contract_preserves_fail_closed_and_paper_only() -> None:
    assert durable_memory.RAW_CRITICAL_FRACTION == 0.94
    assert lifecycle.RAW_CRITICAL_FRACTION_CHANGED is False
    assert lifecycle.STRATEGY_THRESHOLDS_CHANGED is False
    assert lifecycle.CERTIFICATION_THRESHOLDS_CHANGED is False
    assert lifecycle.CONTINUITY_SEMANTICS_CHANGED is False
    assert lifecycle.PAPER_ONLY is True
    assert lifecycle.LIVE_MONEY_AUTHORITY is False
    assert lifecycle.SIGNING_AVAILABLE is False
    assert lifecycle.TRANSACTION_SUBMISSION_AVAILABLE is False
