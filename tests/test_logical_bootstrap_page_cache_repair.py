from __future__ import annotations

import asyncio
import sqlite3
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import BackgroundTasks, FastAPI, HTTPException

from solana_roi import certification_incremental_replication as replication
from solana_roi import certification_logical_bootstrap as logical
from solana_roi import durable_bootstrap_memory_repair as durable_memory
from solana_roi import logical_bootstrap_page_cache_repair as repair


PAGE_PATH = "/v1/operations/certification-db-logical-bootstrap-page"
SIMULATED_PAGE_CACHE_BYTES = 512 * 1024 * 1024
SIMULATED_CRITICAL_BYTES = 400 * 1024 * 1024


def _pids_current() -> int | None:
    try:
        return int(Path("/sys/fs/cgroup/pids.current").read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        return None


def _store(tmp_path: Path):
    source = tmp_path / "authoritative.sqlite"
    db = sqlite3.connect(source, check_same_thread=False)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("CREATE TABLE evidence(value TEXT NOT NULL)")
    db.executemany(
        "INSERT INTO evidence(value) VALUES (?)",
        [(f"row-{index:05d}-" + "x" * 256,) for index in range(2_000)],
    )
    db.commit()
    return SimpleNamespace(path=source, db=db, _lock=threading.RLock())


def _install_app(store):
    app = FastAPI()
    logical.install_certification_logical_bootstrap(app, lambda: SimpleNamespace(store=store))
    repair.install_logical_bootstrap_page_cache_repair(app)
    route = next(route for route in app.routes if getattr(route, "path", None) == PAGE_PATH)
    return app, route


def test_repeated_pages_wait_for_post_send_cache_cleanup_and_recover_503(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Regress the production 503 -> 200 -> advancing-pages cache-pressure cycle.

    Each real SQLite keyset page simulates the clean-cache refault observed in Render.
    The next request is deliberately started immediately after the prior payload exists,
    while that cache is still resident. It must wait outside the raw-cgroup/SQLite work
    until the prior response's *post-send* cleanup has finished, then advance normally.
    """

    store = _store(tmp_path)
    identity = replication.prepare_bootstrap(store)
    original_stream = logical._stream_page_records

    pressure = {
        "cache_bytes": SIMULATED_CRITICAL_BYTES + 1,
        "peak_cache_bytes": 0,
        "guard_calls": 0,
        "guard_failures": 0,
        "cleanup_calls": 0,
        "successful_pages": 0,
    }
    send_state = {"final_sent": False, "failure_cleanup_allowed": True}
    worker_threads: set[int] = set()

    def simulated_guard(path: Path, *, allow_wal_checkpoint: bool = True):
        assert path == Path(store.path)
        worker_threads.add(threading.get_ident())
        pressure["guard_calls"] += 1
        if pressure["cache_bytes"] >= SIMULATED_CRITICAL_BYTES:
            pressure["guard_failures"] += 1
            raise MemoryError("simulated raw cgroup pressure")
        return {
            "current_bytes": 1_700_000_000,
            "max_bytes": 2_147_483_648,
            "fraction": 0.79,
            "file_bytes": pressure["cache_bytes"],
        }

    def simulated_cleanup(path: Path) -> bool:
        assert path == Path(store.path)
        worker_threads.add(threading.get_ident())
        pressure["cleanup_calls"] += 1
        if not send_state["failure_cleanup_allowed"]:
            assert send_state["final_sent"] is True, "successful page cache cleaned before final ASGI send"
        pressure["cache_bytes"] = 0
        return True

    def stream_with_production_cache_refault(*args, **kwargs):
        result = original_stream(*args, **kwargs)
        pressure["successful_pages"] += 1
        pressure["cache_bytes"] += SIMULATED_PAGE_CACHE_BYTES
        pressure["peak_cache_bytes"] = max(
            pressure["peak_cache_bytes"], pressure["cache_bytes"]
        )
        return result

    monkeypatch.setattr(durable_memory, "_guard_raw_cgroup", simulated_guard)
    monkeypatch.setattr(logical.split, "_drop_file_cache", simulated_cleanup)
    monkeypatch.setattr(logical, "_stream_page_records", stream_with_production_cache_refault)
    monkeypatch.setattr(replication, "_require_shared_token", lambda token: None)

    app, page_route = _install_app(store)

    async def call_page(background: BackgroundTasks, cursor: str | None):
        return await page_route.dependant.call(
            background_tasks=background,
            table="evidence",
            epoch=str(identity["epoch"]),
            schema_fingerprint=str(identity["schema_fingerprint"]),
            cursor=cursor,
            limit=250,
            x_certification_token="token",
        )

    async def scenario() -> None:
        ticks = 0
        stop = asyncio.Event()
        baseline_threads = threading.active_count()
        baseline_pids = _pids_current()
        observed_pids: list[int] = []

        async def heartbeat() -> None:
            nonlocal ticks
            while not stop.is_set():
                ticks += 1
                await asyncio.sleep(0.001)

        heartbeat_task = asyncio.create_task(heartbeat())
        try:
            # Start at the exact production failure shape: the unchanged raw guard
            # rejects one request; the existing failed-page path cleans cache off-loop.
            failed_background = BackgroundTasks()
            with pytest.raises(HTTPException) as exc_info:
                await call_page(failed_background, None)
            assert exc_info.value.status_code == 503
            assert pressure["guard_failures"] == 1
            assert pressure["cache_bytes"] == 0

            send_state["failure_cleanup_allowed"] = False
            cursor: str | None = None
            current_background = BackgroundTasks()
            current_payload = await call_page(current_background, cursor)
            observed_rowids: list[int] = []
            ticks_before_pages = ticks

            for page_number in range(8):
                assert current_payload["paper_only"] is True
                assert current_payload["live_money_authority"] is False
                assert current_payload["row_count"] == 250
                observed_rowids.append(int(current_payload["rows"][-1]["rowid"]))
                cursor = current_payload["next_cursor"]

                # The cache must remain resident until after the successful response is
                # sent. This preserves the existing ASGI response-lifecycle contract.
                assert pressure["cache_bytes"] >= SIMULATED_PAGE_CACHE_BYTES
                guards_before_waiter = pressure["guard_calls"]

                next_background = None
                next_task = None
                if page_number < 7:
                    next_background = BackgroundTasks()
                    next_task = asyncio.create_task(call_page(next_background, cursor))
                    await asyncio.sleep(0.01)
                    assert not next_task.done(), "next page entered before prior post-send cleanup"
                    assert pressure["guard_calls"] == guards_before_waiter, (
                        "waiting page reached raw-cgroup/SQLite work before cleanup"
                    )

                # Model the final ASGI body boundary, then execute Starlette's queued
                # BackgroundTasks. The original cleanup runs first; the lifecycle gate
                # is released only in its finally path afterward.
                send_state["final_sent"] = True
                await current_background()
                assert pressure["cache_bytes"] == 0
                send_state["final_sent"] = False

                current_pids = _pids_current()
                if current_pids is not None:
                    observed_pids.append(current_pids)

                if next_task is not None and next_background is not None:
                    current_payload = await asyncio.wait_for(next_task, timeout=2.0)
                    current_background = next_background

            assert observed_rowids == [250, 500, 750, 1000, 1250, 1500, 1750, 2000]
            assert cursor is None
            assert pressure["successful_pages"] == 8
            assert pressure["guard_failures"] == 1, pressure
            assert pressure["guard_calls"] == 9, pressure
            assert pressure["cleanup_calls"] == 9, pressure
            assert pressure["peak_cache_bytes"] >= SIMULATED_PAGE_CACHE_BYTES
            assert ticks > ticks_before_pages + 8, "ASGI heartbeat stalled during page backpressure"
            assert len(worker_threads) <= 4
            assert threading.active_count() <= baseline_threads + 4
            if baseline_pids is not None and observed_pids:
                assert max(observed_pids) <= baseline_pids + 6
                assert observed_pids[-1] <= baseline_pids + 4

            gate_status = repair.status(app)
            assert gate_status["acquisitions"] == 9
            assert gate_status["releases"] == 9
            assert gate_status["active"] is False
            assert gate_status["timeouts"] == 0
        finally:
            stop.set()
            await heartbeat_task

    try:
        asyncio.run(scenario())
    finally:
        store.db.close()


def test_cancelled_waiter_does_not_release_active_page_or_spawn_workers(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    identity = replication.prepare_bootstrap(store)
    monkeypatch.setattr(replication, "_require_shared_token", lambda token: None)
    monkeypatch.setattr(durable_memory, "_guard_raw_cgroup", lambda path, **kwargs: {})
    monkeypatch.setattr(logical.split, "_drop_file_cache", lambda path: True)
    app, page_route = _install_app(store)

    async def call_page(background: BackgroundTasks, cursor: str | None):
        return await page_route.dependant.call(
            background_tasks=background,
            table="evidence",
            epoch=str(identity["epoch"]),
            schema_fingerprint=str(identity["schema_fingerprint"]),
            cursor=cursor,
            limit=250,
            x_certification_token="token",
        )

    async def scenario() -> None:
        baseline_threads = threading.active_count()
        first_background = BackgroundTasks()
        first = await call_page(first_background, None)

        cancelled_background = BackgroundTasks()
        waiter = asyncio.create_task(call_page(cancelled_background, first["next_cursor"]))
        await asyncio.sleep(0.01)
        assert not waiter.done()
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter

        # Cancelling a waiter must not steal/release the active page's gate.
        status_while_active = repair.status(app)
        assert status_while_active["active"] is True
        assert status_while_active["acquisitions"] == 1
        assert status_while_active["releases"] == 0
        assert status_while_active["cancelled_waiters"] == 1

        await first_background()
        assert repair.status(app)["active"] is False

        recovery_background = BackgroundTasks()
        recovered = await asyncio.wait_for(
            call_page(recovery_background, first["next_cursor"]), timeout=2.0
        )
        assert recovered["row_count"] == 250
        await recovery_background()
        final_status = repair.status(app)
        assert final_status["acquisitions"] == 2
        assert final_status["releases"] == 2
        assert final_status["active"] is False
        assert threading.active_count() <= baseline_threads + 4

    try:
        asyncio.run(scenario())
    finally:
        store.db.close()


def test_repair_preserves_post_send_cleanup_raw_guard_and_authority_thresholds() -> None:
    assert durable_memory.RAW_CRITICAL_FRACTION == 0.94
    assert repair.RAW_CRITICAL_FRACTION_CHANGED is False
    assert repair.STRATEGY_THRESHOLDS_CHANGED is False
    assert repair.CERTIFICATION_THRESHOLDS_CHANGED is False
    assert repair.CONTINUITY_SEMANTICS_CHANGED is False
    assert repair.PAPER_ONLY is True
    assert repair.LIVE_MONEY_AUTHORITY is False
    assert repair.SIGNING_AVAILABLE is False
    assert repair.TRANSACTION_SUBMISSION_AVAILABLE is False
    assert repair.status()["post_response_cleanup_preserved"] is True
    assert repair.status()["pre_response_cleanup_added"] is False
    assert repair.status()["next_page_waits_for_prior_cleanup"] is True
