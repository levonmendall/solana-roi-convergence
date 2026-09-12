from __future__ import annotations

import asyncio
import sqlite3
import threading
import time
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


def test_repeated_pages_quiesce_cache_before_next_guard_and_recover_503(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Regress the production 503 -> 200 -> page progress -> 503 cache cycle.

    Every successful page simulates enough clean SQLite cache to make the unchanged
    next raw-cgroup guard fail.  The production repair must evict that cache before
    the endpoint returns, so after one deliberately pressured 503 the retry succeeds
    and eight consecutive keyset pages advance without another pressure failure.
    """

    store = _store(tmp_path)
    identity = replication.prepare_bootstrap(store)
    original_page = logical._page
    original_reader = logical._pinned_reader
    original_manifest = logical._manifest
    original_installed = repair._INSTALLED
    original_repair_page = repair._ORIGINAL_PAGE
    original_repair_reader = repair._ORIGINAL_PINNED_READER
    original_repair_manifest = repair._ORIGINAL_MANIFEST

    pressure = {
        "cache_bytes": SIMULATED_CRITICAL_BYTES + 1,
        "peak_cache_bytes": 0,
        "guard_failures": 0,
        "cleanup_calls": 0,
    }
    page_threads: list[int] = []
    cleanup_threads: list[int] = []

    def simulated_guard(path: Path, *, allow_wal_checkpoint: bool = True):
        assert path == Path(store.path)
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
        cleanup_threads.append(threading.get_ident())
        pressure["cleanup_calls"] += 1
        pressure["cache_bytes"] = 0
        return True

    try:
        # Keep this regression independent of collection order: reinstall the repair
        # around the exact current logical-bootstrap functions, then restore globals.
        repair._INSTALLED = False
        repair._ORIGINAL_PAGE = None
        repair._ORIGINAL_PINNED_READER = None
        repair._ORIGINAL_MANIFEST = None
        logical._page = original_page
        logical._pinned_reader = original_reader
        logical._manifest = original_manifest
        repair.configure_logical_bootstrap_page_cache_repair()

        base_page = repair._ORIGINAL_PAGE
        assert base_page is not None

        def production_shaped_page(target, **kwargs):
            page_threads.append(threading.get_ident())
            result = base_page(target, **kwargs)
            # Model the clean page-cache refault observed in production while keeping
            # the actual SQLite/keyset/identity path in this regression.
            pressure["cache_bytes"] += SIMULATED_PAGE_CACHE_BYTES
            pressure["peak_cache_bytes"] = max(
                pressure["peak_cache_bytes"], pressure["cache_bytes"]
            )
            time.sleep(0.01)
            return result

        repair._ORIGINAL_PAGE = production_shaped_page
        monkeypatch.setattr(durable_memory, "_guard_raw_cgroup", simulated_guard)
        monkeypatch.setattr(logical.split, "_drop_file_cache", simulated_cleanup)
        monkeypatch.setattr(replication, "_require_shared_token", lambda token: None)

        app = FastAPI()
        logical.install_certification_logical_bootstrap(app, lambda: SimpleNamespace(store=store))
        page_route = next(route for route in app.routes if getattr(route, "path", None) == PAGE_PATH)

        async def scenario() -> None:
            ticks = 0
            stop = asyncio.Event()
            baseline_threads = threading.active_count()
            baseline_pids = _pids_current()
            pids: list[int] = []

            async def heartbeat() -> None:
                nonlocal ticks
                while not stop.is_set():
                    ticks += 1
                    await asyncio.sleep(0.001)

            heartbeat_task = asyncio.create_task(heartbeat())
            try:
                failed_tasks = BackgroundTasks()
                with pytest.raises(HTTPException) as exc_info:
                    await page_route.dependant.call(
                        background_tasks=failed_tasks,
                        table="evidence",
                        epoch=str(identity["epoch"]),
                        schema_fingerprint=str(identity["schema_fingerprint"]),
                        cursor=None,
                        limit=250,
                        x_certification_token="token",
                    )
                assert exc_info.value.status_code == 503
                assert pressure["guard_failures"] == 1
                assert pressure["cache_bytes"] == 0, "failed guard did not quiesce cache"

                cursor = None
                observed_rowids: list[int] = []
                ticks_before_pages = ticks
                for _ in range(8):
                    background = BackgroundTasks()
                    payload = await page_route.dependant.call(
                        background_tasks=background,
                        table="evidence",
                        epoch=str(identity["epoch"]),
                        schema_fingerprint=str(identity["schema_fingerprint"]),
                        cursor=cursor,
                        limit=250,
                        x_certification_token="token",
                    )
                    assert payload["paper_only"] is True
                    assert payload["live_money_authority"] is False
                    assert payload["row_count"] > 0
                    observed_rowids.append(int(payload["rows"][-1]["rowid"]))
                    cursor = payload["next_cursor"]

                    # This is the critical production invariant: page read cache rose
                    # inside the worker, but was evicted before the response payload was
                    # handed back. The next request therefore starts below the guard.
                    assert pressure["peak_cache_bytes"] >= SIMULATED_PAGE_CACHE_BYTES
                    assert pressure["cache_bytes"] == 0

                    # Execute the preserved post-send background cleanup as Starlette
                    # would after transmitting the final response body.
                    await background()
                    assert pressure["cache_bytes"] == 0
                    current_pids = _pids_current()
                    if current_pids is not None:
                        pids.append(current_pids)

                assert observed_rowids == sorted(observed_rowids)
                assert observed_rowids[-1] == 2_000
                assert pressure["guard_failures"] == 1, pressure
                assert ticks > ticks_before_pages + 8, "ASGI heartbeat stalled during bounded pages"
                assert page_threads
                assert len(set(page_threads)) <= 4
                assert threading.active_count() <= baseline_threads + 4
                if baseline_pids is not None and pids:
                    assert max(pids) <= baseline_pids + 6
                    assert pids[-1] <= baseline_pids + 4
            finally:
                stop.set()
                await heartbeat_task

        asyncio.run(scenario())
        assert pressure["cleanup_calls"] >= 17
        assert cleanup_threads
        status = repair.status()
        assert status["pre_response_sqlite_cache_quiesce"] is True
        assert status["failed_open_cache_cleanup"] is True
        assert status["bootstrap_lease_wal_aware"] is True
        assert status["raw_critical_fraction_changed"] is False
        assert status["paper_only"] is True
        assert status["live_money_authority"] is False
        assert status["signing_available"] is False
        assert status["transaction_submission_available"] is False
    finally:
        store.db.close()
        logical._page = original_page
        logical._pinned_reader = original_reader
        logical._manifest = original_manifest
        repair._INSTALLED = original_installed
        repair._ORIGINAL_PAGE = original_repair_page
        repair._ORIGINAL_PINNED_READER = original_repair_reader
        repair._ORIGINAL_MANIFEST = original_repair_manifest


def test_repair_does_not_change_raw_guard_or_authority_thresholds() -> None:
    assert durable_memory.RAW_CRITICAL_FRACTION == 0.94
    assert repair.RAW_CRITICAL_FRACTION_CHANGED is False
    assert repair.STRATEGY_THRESHOLDS_CHANGED is False
    assert repair.CERTIFICATION_THRESHOLDS_CHANGED is False
    assert repair.CONTINUITY_SEMANTICS_CHANGED is False
    assert repair.PAPER_ONLY is True
    assert repair.LIVE_MONEY_AUTHORITY is False
    assert repair.SIGNING_AVAILABLE is False
    assert repair.TRANSACTION_SUBMISSION_AVAILABLE is False
