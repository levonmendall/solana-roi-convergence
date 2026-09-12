from __future__ import annotations

import asyncio
import inspect
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import BackgroundTasks, FastAPI

from solana_roi import certification_logical_bootstrap as bootstrap
from solana_roi import durable_bootstrap_memory_repair as durable_memory


PAGE_PATH = "/v1/operations/certification-db-logical-bootstrap-page"
MANIFEST_PATH = "/v1/operations/certification-db-logical-bootstrap"


def _pids_current() -> int | None:
    try:
        return int(Path("/sys/fs/cgroup/pids.current").read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        return None


def _installed_app(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    source_path = tmp_path / "authoritative.sqlite"
    source_path.write_bytes(b"")
    store = SimpleNamespace(path=source_path)

    def runtime_provider():
        return SimpleNamespace(store=store)

    monkeypatch.setattr(bootstrap.replication, "_require_shared_token", lambda token: None)
    app = FastAPI()
    bootstrap.install_certification_logical_bootstrap(app, runtime_provider)
    page_route = next(route for route in app.routes if getattr(route, "path", None) == PAGE_PATH)
    manifest_route = next(route for route in app.routes if getattr(route, "path", None) == MANIFEST_PATH)
    return app, store, page_route, manifest_route


def test_slow_raw_cgroup_guard_is_off_asgi_loop(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    async def scenario() -> None:
        _app, store, page_route, _manifest_route = _installed_app(monkeypatch, tmp_path)
        started = threading.Event()
        release = threading.Event()
        finished = threading.Event()
        guard_threads: list[int] = []
        page_threads: list[int] = []
        loop_thread = threading.get_ident()
        ticks = 0
        stop_tick = asyncio.Event()

        def blocking_guard(path: Path, *args, **kwargs):
            assert path == Path(store.path)
            guard_threads.append(threading.get_ident())
            started.set()
            assert release.wait(timeout=5.0)
            finished.set()
            return {"fraction": 0.80}

        def guarded_page(target, **kwargs):
            assert target is store
            page_threads.append(threading.get_ident())
            durable_memory._guard_raw_cgroup(Path(target.path))
            return {
                "table": kwargs["table_name"],
                "done": False,
                "paper_only": True,
                "live_money_authority": False,
            }

        async def heartbeat() -> None:
            nonlocal ticks
            while not stop_tick.is_set():
                ticks += 1
                await asyncio.sleep(0.002)

        monkeypatch.setattr(durable_memory, "_guard_raw_cgroup", blocking_guard)
        monkeypatch.setattr(bootstrap, "_page", guarded_page)
        monkeypatch.setattr(bootstrap.split, "_drop_file_cache", lambda path: None)

        heartbeat_task = asyncio.create_task(heartbeat())
        request_task = asyncio.create_task(
            page_route.dependant.call(
                background_tasks=BackgroundTasks(),
                table="anonymous_candidate_latency_failures",
                epoch="epoch-12345678",
                schema_fingerprint="f" * 64,
                cursor=None,
                limit=250,
                x_certification_token="token",
            )
        )
        try:
            for _ in range(500):
                if started.is_set():
                    break
                await asyncio.sleep(0.002)
            assert started.is_set(), "production-shaped cgroup guard never entered bounded worker"

            ticks_at_start = ticks
            await asyncio.sleep(0.05)
            assert ticks > ticks_at_start + 3, "ASGI loop stalled behind _guard_raw_cgroup"
            assert not request_task.done()
            assert guard_threads == [guard_threads[0]]
            assert page_threads == guard_threads
            assert guard_threads[0] != loop_thread

            release.set()
            payload = await asyncio.wait_for(request_task, timeout=2.0)
            assert payload["table"] == "anonymous_candidate_latency_failures"
            assert payload["paper_only"] is True
            assert payload["live_money_authority"] is False
            assert finished.is_set()
        finally:
            release.set()
            stop_tick.set()
            await heartbeat_task

    asyncio.run(scenario())


def test_repeated_logical_pages_reuse_bounded_workers_and_pids(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        _app, store, page_route, _manifest_route = _installed_app(monkeypatch, tmp_path)
        worker_threads: list[int] = []
        baseline_threads = threading.active_count()
        peak_threads = baseline_threads
        baseline_pids = _pids_current()
        pids: list[int] = []

        def bounded_page(target, **kwargs):
            assert target is store
            worker_threads.append(threading.get_ident())
            return {
                "table": kwargs["table_name"],
                "done": False,
                "paper_only": True,
                "live_money_authority": False,
            }

        monkeypatch.setattr(bootstrap, "_page", bounded_page)
        monkeypatch.setattr(bootstrap.split, "_drop_file_cache", lambda path: None)

        for cursor_number in range(64):
            payload = await page_route.dependant.call(
                background_tasks=BackgroundTasks(),
                table="anonymous_candidate_latency_failures",
                epoch="epoch-12345678",
                schema_fingerprint="f" * 64,
                cursor=str(cursor_number),
                limit=250,
                x_certification_token="token",
            )
            assert payload["paper_only"] is True
            peak_threads = max(peak_threads, threading.active_count())
            current_pids = _pids_current()
            if current_pids is not None:
                pids.append(current_pids)

        assert worker_threads
        assert len(set(worker_threads)) <= 4, {
            "worker_threads": sorted(set(worker_threads)),
            "worker_calls": len(worker_threads),
        }
        assert peak_threads - baseline_threads <= 4, {
            "baseline_threads": baseline_threads,
            "peak_threads": peak_threads,
        }
        if baseline_pids is not None and pids:
            assert max(pids) <= baseline_pids + 6, {
                "baseline_pids_current": baseline_pids,
                "peak_pids_current": max(pids),
                "final_pids_current": pids[-1],
            }
            assert pids[-1] <= baseline_pids + 4

    asyncio.run(scenario())


def test_cancelled_logical_page_does_not_spawn_recursive_workers(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        _app, store, page_route, _manifest_route = _installed_app(monkeypatch, tmp_path)
        started = threading.Event()
        release = threading.Event()
        finished = threading.Event()
        worker_threads: list[int] = []
        baseline_threads = threading.active_count()

        def blocking_page(target, **kwargs):
            assert target is store
            worker_threads.append(threading.get_ident())
            started.set()
            try:
                assert release.wait(timeout=5.0)
                return {
                    "table": kwargs["table_name"],
                    "done": False,
                    "paper_only": True,
                    "live_money_authority": False,
                }
            finally:
                finished.set()

        monkeypatch.setattr(bootstrap, "_page", blocking_page)
        monkeypatch.setattr(bootstrap.split, "_drop_file_cache", lambda path: None)

        task = asyncio.create_task(
            page_route.dependant.call(
                background_tasks=BackgroundTasks(),
                table="anonymous_candidate_latency_failures",
                epoch="epoch-12345678",
                schema_fingerprint="f" * 64,
                cursor=None,
                limit=250,
                x_certification_token="token",
            )
        )
        for _ in range(500):
            if started.is_set():
                break
            await asyncio.sleep(0.002)
        assert started.is_set()

        task.cancel()
        loop_ticks = 0
        for _ in range(10):
            loop_ticks += 1
            await asyncio.sleep(0.002)
        assert loop_ticks == 10

        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        for _ in range(500):
            if finished.is_set():
                break
            await asyncio.sleep(0.002)
        assert finished.is_set(), "cancelled logical page left bounded worker stranded"
        assert len(set(worker_threads)) == 1
        assert threading.active_count() <= baseline_threads + 4

    asyncio.run(scenario())


def test_manifest_page_and_post_send_cleanup_use_shared_bounded_pool() -> None:
    install_source = inspect.getsource(bootstrap.install_certification_logical_bootstrap)
    cleanup_source = inspect.getsource(bootstrap._post_response_cleanup)

    assert "await run_in_threadpool(_manifest, store)" in install_source
    assert "await run_in_threadpool(" in install_source
    assert "_page," in install_source
    assert "await run_in_threadpool(split._drop_file_cache, source_path)" in install_source
    assert "await run_in_threadpool(split._drop_file_cache, source_path)" in cleanup_source
    assert "asyncio.run" not in install_source
    assert "asyncio.to_thread" not in install_source
    assert bootstrap.PAPER_ONLY is True
    assert bootstrap.LIVE_MONEY_AUTHORITY is False
    assert bootstrap.SIGNING_AVAILABLE is False
    assert bootstrap.TRANSACTION_SUBMISSION_AVAILABLE is False
