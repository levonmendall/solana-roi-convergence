from __future__ import annotations

import asyncio
import inspect
import os
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


def _owned_task_ids(
    *, proc_root: Path = Path("/proc"), root_pid: int | None = None
) -> set[int]:
    """Return Linux task IDs owned by this process and its descendants.

    The cgroup-wide PID counter includes unrelated jobs. Build the owned process
    tree from PPid in proc status files instead: some Linux/sandbox proc mounts
    omit task/*/children, which must not silently hide child-process worker growth.
    Task directories include native threads, not only Python threading objects.
    """

    root = os.getpid() if root_pid is None else int(root_pid)
    children_by_parent: dict[int, set[int]] = {}
    try:
        process_paths = list(proc_root.iterdir())
    except OSError:
        return _live_thread_ids()
    for process_path in process_paths:
        if not process_path.name.isdigit():
            continue
        try:
            status = (process_path / "status").read_text(encoding="utf-8")
            parent_line = next(line for line in status.splitlines() if line.startswith("PPid:"))
            parent = int(parent_line.partition(":")[2].strip())
        except (OSError, ValueError, StopIteration):
            # Processes can exit between enumerating proc and reading status.
            continue
        children_by_parent.setdefault(parent, set()).add(int(process_path.name))

    pending = [root]
    processes: set[int] = set()
    tasks: set[int] = set()
    while pending:
        pid = pending.pop()
        if pid in processes:
            continue
        processes.add(pid)
        pending.extend(children_by_parent.get(pid, ()))
        try:
            entries = list((proc_root / str(pid) / "task").iterdir())
        except OSError:
            continue
        tasks.update(int(entry.name) for entry in entries if entry.name.isdigit())
    return tasks or _live_thread_ids()


def _fake_proc_process(proc_root: Path, pid: int, parent: int, task_ids: set[int]) -> None:
    process_path = proc_root / str(pid)
    process_path.mkdir(parents=True, exist_ok=True)
    (process_path / "status").write_text(
        f"Name:\tregression-worker\nPid:\t{pid}\nPPid:\t{parent}\n", encoding="utf-8"
    )
    for tid in task_ids:
        (process_path / "task" / str(tid)).mkdir(parents=True, exist_ok=True)
    # Intentionally no task/*/children files; the real verification mount omits them.


def test_owned_task_measurement_detects_genuine_thread_growth(tmp_path: Path) -> None:
    _fake_proc_process(tmp_path, 100, 1, {100})
    baseline = _owned_task_ids(proc_root=tmp_path, root_pid=100)
    _fake_proc_process(tmp_path, 100, 1, {100, 101, 102, 103})
    grown = _owned_task_ids(proc_root=tmp_path, root_pid=100)
    assert baseline == {100}
    assert grown == {100, 101, 102, 103}
    assert len(grown) > len(baseline) + 2


def test_owned_task_measurement_detects_descendants_without_children_files(tmp_path: Path) -> None:
    _fake_proc_process(tmp_path, 100, 1, {100})
    baseline = _owned_task_ids(proc_root=tmp_path, root_pid=100)
    _fake_proc_process(tmp_path, 200, 100, {200, 201})
    _fake_proc_process(tmp_path, 300, 200, {300, 301})
    grown = _owned_task_ids(proc_root=tmp_path, root_pid=100)
    assert grown == {100, 200, 201, 300, 301}
    assert len(grown) > len(baseline) + 2


def test_owned_task_measurement_excludes_unrelated_shared_cgroup_processes(tmp_path: Path) -> None:
    _fake_proc_process(tmp_path, 100, 1, {100})
    _fake_proc_process(tmp_path, 900, 1, {900, 901, 902, 903})
    _fake_proc_process(tmp_path, 950, 900, {950, 951})
    assert _owned_task_ids(proc_root=tmp_path, root_pid=100) == {100}


def test_wrapped_logical_bootstrap_stays_async_and_recovers_without_thread_growth(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Regress async-route semantics while allowing only bounded lifecycle workers.

    The compiled FastAPI dependency graph includes ``background_tasks`` from the real
    logical-bootstrap page route. Replacing ``dependant.call`` with a synchronous route
    callable would both reject that keyword and make Starlette dispatch the whole route
    through a sync worker. The production repair keeps the route callable async while
    explicitly offloading only the blocking lease/SQLite lifecycle operations through
    AnyIO's shared bounded worker pool. This test exercises that composed route
    repeatedly and fails on per-request worker growth or loss of async route semantics.
    """

    token = "async-wrapper-regression-token"
    release = "b" * 40
    monkeypatch.setenv("SOLANA_ROI_CERTIFICATION_SHARED_TOKEN", token)
    monkeypatch.setenv("SOLANA_ROI_RELEASE_COMMIT", release)
    monkeypatch.setattr(bootstrap.split, "_release_commit", lambda: release)
    from solana_roi import durable_bootstrap_memory_repair as durable_memory

    # This regression owns async dispatch and worker lifecycle, not the shared
    # runner's raw-cgroup pressure. Production memory-guard tests cover that
    # boundary independently.
    monkeypatch.setattr(durable_memory, "_guard_raw_cgroup", lambda *_args, **_kwargs: {})

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

    original_anyio_run_sync = anyio.to_thread.run_sync
    worker_threads: list[int] = []

    async def observed_anyio_worker(func: Any, *args: Any, **kwargs: Any) -> Any:
        def observed_call() -> Any:
            worker_threads.append(threading.get_ident())
            return func(*args)

        return await original_anyio_run_sync(observed_call, **kwargs)

    monkeypatch.setattr(anyio.to_thread, "run_sync", observed_anyio_worker)

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

    async def exercise() -> tuple[list[int], list[int], set[int], set[int]]:
        baseline_threads = _live_thread_ids()
        pids = [_pids_current()]
        owned_tasks = [len(_owned_task_ids())]
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
            owned_tasks.append(len(_owned_task_ids()))

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
                owned_tasks.append(len(_owned_task_ids()))

        await asyncio.sleep(0)
        return pids, owned_tasks, baseline_threads, _live_thread_ids()

    try:
        pids, owned_tasks, baseline_threads, final_threads = asyncio.run(exercise())
        assert refresh_calls["count"] == 65
        assert cleanup_phases == ["after_final_send"] * 64
        assert worker_threads, "lease lifecycle never entered bounded AnyIO worker pool"
        assert len(set(worker_threads)) <= 2, {
            "worker_threads": sorted(set(worker_threads)),
            "worker_calls": len(worker_threads),
        }
        assert len(final_threads - baseline_threads) <= 2, {
            "baseline_threads": sorted(baseline_threads),
            "final_threads": sorted(final_threads),
            "new_threads": sorted(final_threads - baseline_threads),
        }
        assert max(owned_tasks) <= owned_tasks[0] + 2, {
            "baseline_owned_tasks": owned_tasks[0],
            "peak_owned_tasks": max(owned_tasks),
            "owned_task_samples": owned_tasks,
            "baseline_pids_current": pids[0],
            "peak_pids_current": max(pids),
            "shared_cgroup_samples": pids,
        }
        assert owned_tasks[-1] <= owned_tasks[0] + 2, {
            "baseline_owned_tasks": owned_tasks[0],
            "final_owned_tasks": owned_tasks[-1],
            "baseline_pids_current": pids[0],
            "final_pids_current": pids[-1],
        }
    finally:
        store.close()
