from __future__ import annotations

import asyncio
import inspect
import threading
from types import SimpleNamespace

import pytest

from solana_roi import certification_bootstrap_autocheckpoint_lease as lease


def test_sync_bootstrap_endpoint_does_not_block_asgi_loop() -> None:
    async def scenario() -> None:
        started = threading.Event()
        release = threading.Event()
        finished = threading.Event()
        endpoint_threads: list[int] = []
        ticks = 0
        stop_tick = asyncio.Event()
        loop_thread = threading.get_ident()

        def blocking_page(*, value: int) -> dict[str, int]:
            endpoint_threads.append(threading.get_ident())
            started.set()
            assert release.wait(timeout=5.0)
            finished.set()
            return {"value": value}

        async def heartbeat() -> None:
            nonlocal ticks
            while not stop_tick.is_set():
                ticks += 1
                await asyncio.sleep(0.002)

        heartbeat_task = asyncio.create_task(heartbeat())
        page_task = asyncio.create_task(lease._call_endpoint_inline(blocking_page, value=7))
        try:
            for _ in range(500):
                if started.is_set():
                    break
                await asyncio.sleep(0.002)
            assert started.is_set(), "sync bootstrap endpoint never entered worker"

            ticks_at_start = ticks
            await asyncio.sleep(0.03)
            assert ticks > ticks_at_start + 2, "ASGI event loop stalled behind sync bootstrap page work"
            assert not page_task.done()
            assert endpoint_threads == [endpoint_threads[0]]
            assert endpoint_threads[0] != loop_thread

            release.set()
            assert await asyncio.wait_for(page_task, timeout=2.0) == {"value": 7}
            assert finished.is_set()
        finally:
            release.set()
            stop_tick.set()
            await heartbeat_task

    asyncio.run(scenario())


def test_repeated_sync_bootstrap_calls_reuse_bounded_worker_threads() -> None:
    async def scenario() -> None:
        endpoint_threads: list[int] = []
        baseline = threading.active_count()
        peak = baseline

        def page(*, cursor: int) -> dict[str, int]:
            endpoint_threads.append(threading.get_ident())
            return {"cursor": cursor + 1}

        for cursor in range(64):
            payload = await lease._call_endpoint_inline(page, cursor=cursor)
            assert payload == {"cursor": cursor + 1}
            peak = max(peak, threading.active_count())

        # Sequential bootstrap pages must reuse AnyIO's long-lived bounded worker pool;
        # they must not create a per-page executor/event-loop/thread tree.
        assert len(set(endpoint_threads)) <= 4
        assert peak - baseline <= 4

    asyncio.run(scenario())


def test_cancelled_sync_bootstrap_call_leaves_loop_responsive_and_worker_finishes() -> None:
    async def scenario() -> None:
        started = threading.Event()
        release = threading.Event()
        finished = threading.Event()
        loop_ticks = 0

        def blocking_page() -> dict[str, bool]:
            started.set()
            try:
                assert release.wait(timeout=5.0)
                return {"done": True}
            finally:
                finished.set()

        task = asyncio.create_task(lease._call_endpoint_inline(blocking_page))
        for _ in range(500):
            if started.is_set():
                break
            await asyncio.sleep(0.002)
        assert started.is_set()

        task.cancel()
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
        assert finished.is_set(), "cancelled request left sync bootstrap worker stranded"

    asyncio.run(scenario())


def test_async_bootstrap_endpoint_stays_on_existing_loop() -> None:
    async def scenario() -> None:
        loop_thread = threading.get_ident()

        async def endpoint(*, value: int) -> dict[str, int]:
            assert threading.get_ident() == loop_thread
            await asyncio.sleep(0)
            return {"value": value}

        assert await lease._call_endpoint_inline(endpoint, value=9) == {"value": 9}

    asyncio.run(scenario())


def test_blocking_lease_refresh_does_not_block_manifest_asgi_loop(monkeypatch) -> None:
    async def scenario() -> None:
        started = threading.Event()
        release = threading.Event()
        refresh_threads: list[int] = []
        ticks = 0
        stop_tick = asyncio.Event()
        loop_thread = threading.get_ident()
        store = object()

        def runtime_provider():
            return SimpleNamespace(store=store)

        def manifest_endpoint(x_certification_token=None):
            return {"tables": [{"name": "evidence"}]}

        def page_endpoint(**kwargs):
            return {"table": kwargs.get("table"), "done": False}

        manifest_route = SimpleNamespace(
            path=lease.MANIFEST_PATH,
            endpoint=manifest_endpoint,
            dependant=SimpleNamespace(call=manifest_endpoint),
        )
        page_route = SimpleNamespace(
            path=lease.PAGE_PATH,
            endpoint=page_endpoint,
            dependant=SimpleNamespace(call=page_endpoint),
        )
        app = SimpleNamespace(routes=[manifest_route, page_route], state=SimpleNamespace())

        def blocking_refresh(target):
            assert target is store
            refresh_threads.append(threading.get_ident())
            started.set()
            assert release.wait(timeout=5.0)
            return {}

        async def heartbeat() -> None:
            nonlocal ticks
            while not stop_tick.is_set():
                ticks += 1
                await asyncio.sleep(0.002)

        monkeypatch.setattr(lease, "refresh", blocking_refresh)
        monkeypatch.setattr(lease, "set_manifest_tables", lambda target, payload: None)
        monkeypatch.setattr(lease, "finish_if_complete", lambda target, payload: False)
        monkeypatch.setattr(lease, "_install_preworker_quiesce", lambda: None)
        monkeypatch.setattr(
            "solana_roi.certification_incremental_replication._require_shared_token",
            lambda token: None,
        )
        lease.install_certification_bootstrap_autocheckpoint_lease(app, runtime_provider)

        heartbeat_task = asyncio.create_task(heartbeat())
        manifest_task = asyncio.create_task(
            manifest_route.dependant.call(x_certification_token="token")
        )
        try:
            for _ in range(500):
                if started.is_set():
                    break
                await asyncio.sleep(0.002)
            assert started.is_set(), "bootstrap lease refresh never entered bounded worker"

            ticks_at_start = ticks
            await asyncio.sleep(0.03)
            assert ticks > ticks_at_start + 2, "ASGI loop stalled behind bootstrap lease refresh"
            assert not manifest_task.done()
            assert refresh_threads == [refresh_threads[0]]
            assert refresh_threads[0] != loop_thread

            release.set()
            assert await asyncio.wait_for(manifest_task, timeout=2.0) == {
                "tables": [{"name": "evidence"}]
            }
        finally:
            release.set()
            stop_tick.set()
            await heartbeat_task

    asyncio.run(scenario())


def test_route_lifecycle_state_mutations_are_bounded_offloop() -> None:
    source = inspect.getsource(lease.install_certification_bootstrap_autocheckpoint_lease)
    assert "await run_in_threadpool(refresh, store)" in source
    assert "await run_in_threadpool(set_manifest_tables, store, payload)" in source
    assert "await run_in_threadpool(finish_if_complete, store, payload)" in source


def test_offloop_helper_cannot_reintroduce_nested_event_loop_or_per_call_to_thread() -> None:
    source = inspect.getsource(lease._call_endpoint_inline)
    assert "asyncio.run" not in source
    assert "asyncio.to_thread" not in source
    assert "run_in_threadpool" in source


def test_offloop_repair_preserves_paper_only_authority() -> None:
    state = lease.status()
    assert state["anyio_sync_worker_route_wrapper"] is True
    assert state["lease_route_state_offloop"] is True
    assert state["paper_only"] is True
    assert state["live_money_authority"] is False
    assert state["signing_available"] is False
    assert state["transaction_submission_available"] is False
    assert state["strategy_thresholds_changed"] is False
    assert state["certification_thresholds_changed"] is False
    assert state["canonical_evidence_reset"] is False
