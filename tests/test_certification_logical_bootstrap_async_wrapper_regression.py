from __future__ import annotations

import asyncio
import inspect
import sqlite3
import threading
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import anyio.to_thread
import httpx
from fastapi import FastAPI, HTTPException

from solana_roi import certification_bootstrap_autocheckpoint_lease as lease
from solana_roi import certification_incremental_replication as replication
from solana_roi import certification_logical_bootstrap as bootstrap


class _Store:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.RLock()
        self.db = sqlite3.connect(path, check_same_thread=False)
        with self.db:
            self.db.execute("PRAGMA journal_mode=WAL")
            self.db.execute(
                "CREATE TABLE evidence("
                "id INTEGER PRIMARY KEY,value TEXT NOT NULL)"
            )
            self.db.executemany(
                "INSERT INTO evidence(id,value) VALUES(?,?)",
                ((index, f"value-{index}") for index in range(1, 33)),
            )

    def close(self) -> None:
        self.db.close()


def _pids_current() -> int:
    try:
        return int(Path("/sys/fs/cgroup/pids.current").read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        return len([thread for thread in threading.enumerate() if thread.is_alive()])


def _live_thread_ids() -> set[int]:
    return {
        int(thread.ident)
        for thread in threading.enumerate()
        if thread.is_alive() and thread.ident is not None
    }


def test_wrapped_logical_bootstrap_stays_async_and_recovers_without_thread_growth(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Regress the production background_tasks keyword/thread-pool failure.

    The compiled FastAPI dependency graph includes ``background_tasks`` from the real
    logical-bootstrap page route. Replacing only ``dependant.call`` with a synchronous
    callable both rejects that keyword and makes Starlette enter AnyIO's sync worker
    pool. This test exercises the composed production route repeatedly and fails if
    either behavior returns.
    """

    token = "async-wrapper-regression-token"
    release = "b" * 40
    monkeypatch.setenv("SOLANA_ROI_CERTIFICATION_SHARED_TOKEN", token)
    monkeypatch.setenv("SOLANA_ROI_RELEASE_COMMIT", release)
    monkeypatch.setattr(bootstrap.split, "_release_commit", lambda: release)

    store = _Store(tmp_path / "authoritative.sqlite3")
    runtime = SimpleNamespace(store=store)
    identity = replication.prepare_bootstrap(store)

    app = FastAPI()
    bootstrap.install_certification_logical_bootstrap(app, lambda: runtime)

    refresh_calls = {"count": 0}

    def controlled_refresh(target: Any) -> dict[str, Any]:
        assert target is store
        refresh_calls["count"] += 1
        if refresh_calls["count"] == 1:
            raise HTTPException(
                status_code=503,
                detail="certification logical bootstrap paused: deterministic regression guard",
            )
        return {}

    monkeypatch.setattr(lease, "refresh", controlled_refresh)
    monkeypatch.setattr(lease, "finish_if_complete", lambda target, payload: False)
    monkeypatch.setattr(lease, "_install_preworker_quiesce", lambda: None)

    send_state = {"final_sent": False}
    cleanup_phases: list[str] = []

    def observed_cleanup(path: Path) -> bool:
        assert Path(path) == store.path
        assert send_state["final_sent"] is True, "cleanup ran before final ASGI body send"
        cleanup_phases.append("after_final_send")
        return True

    monkeypatch.setattr(bootstrap.split, "_drop_file_cache", observed_cleanup)

    async def forbidden_anyio_worker(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("logical bootstrap entered AnyIO sync worker-thread pool")

    monkeypatch.setattr(anyio.to_thread, "run_sync", forbidden_anyio_worker)

    lease.install_certification_bootstrap_autocheckpoint_lease(app)
    page_route = next(
        route
        for route in app.routes
        if getattr(route, "path", None) == lease.PAGE_PATH
    )
    manifest_route = next(
        route
        for route in app.routes
        if getattr(route, "path", None) == lease.MANIFEST_PATH
    )
    assert inspect.iscoroutinefunction(page_route.dependant.call)
    assert inspect.iscoroutinefunction(manifest_route.dependant.call)

    class _ObservedASGI:
        def __init__(self, wrapped: Any) -> None:
            self.wrapped = wrapped

        async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
            send_state["final_sent"] = False

            async def observed_send(message: dict[str, Any]) -> None:
                await send(message)
                if (
                    message.get("type") == "http.response.body"
                    and not message.get("more_body", False)
                ):
                    send_state["final_sent"] = True

            await self.wrapped(scope, receive, observed_send)

    observed_app = _ObservedASGI(app)

    async def exercise() -> tuple[list[int], set[int], set[int]]:
        baseline_threads = _live_thread_ids()
        pids = [_pids_current()]
        params = {
            "table": "evidence",
            "epoch": str(identity["epoch"]),
            "schema_fingerprint": str(identity["schema_fingerprint"]),
            "limit": 16,
        }
        transport = httpx.ASGITransport(app=observed_app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://authoritative.test",
        ) as client:
            paused = await client.get(
                lease.PAGE_PATH,
                params=params,
                headers={"X-Certification-Token": token},
            )
            assert paused.status_code == 503, paused.text
            assert "deterministic regression guard" in paused.text
            assert cleanup_phases == []
            pids.append(_pids_current())

            for _ in range(64):
                response = await client.get(
                    lease.PAGE_PATH,
                    params=params,
                    headers={"X-Certification-Token": token},
                )
                assert response.status_code == 200, response.text[:500]
                payload = response.json()
                assert payload["table"] == "evidence"
                assert int(payload["row_count"]) == 16
                assert send_state["final_sent"] is True
                pids.append(_pids_current())

        await asyncio.sleep(0)
        return pids, baseline_threads, _live_thread_ids()

    try:
        pids, baseline_threads, final_threads = asyncio.run(exercise())
        assert refresh_calls["count"] == 65
        assert cleanup_phases == ["after_final_send"] * 64
        assert final_threads <= baseline_threads, {
            "baseline_threads": sorted(baseline_threads),
            "final_threads": sorted(final_threads),
            "new_threads": sorted(final_threads - baseline_threads),
        }
        assert max(pids) <= pids[0] + 2, {
            "baseline_pids_current": pids[0],
            "peak_pids_current": max(pids),
            "samples": pids,
        }
        assert pids[-1] <= pids[0] + 1, {
            "baseline_pids_current": pids[0],
            "final_pids_current": pids[-1],
        }
    finally:
        store.close()
