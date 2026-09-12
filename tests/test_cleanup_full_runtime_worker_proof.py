from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from solana_roi import production_cleanup_runtime_install as cleanup_runtime
from solana_roi import render_runtime_bootstrap_repair as bootstrap


def _reset_bootstrap_state() -> None:
    bootstrap._BOOTSTRAP_STATE.update(
        {
            "state": "ready",
            "full_runtime_at": None,
            "last_error_type": None,
            "last_error_message": None,
        }
    )


def test_live_composed_worker_chain_reaches_full_runtime_without_test_injection(monkeypatch) -> None:
    """The cleanup lease gate must be reachable through the real worker lifecycle."""

    _reset_bootstrap_state()
    started = asyncio.Event()

    async def final_composed_workers(_runtime, stop: asyncio.Event) -> None:
        started.set()
        await stop.wait()

    monkeypatch.setattr(cleanup_runtime, "_ORIGINAL_RUNTIME_WORKERS", final_composed_workers)
    monkeypatch.setattr(cleanup_runtime, "FULL_RUNTIME_SETTLE_SECONDS", 0.01)

    async def scenario() -> None:
        stop = asyncio.Event()
        task = asyncio.create_task(
            cleanup_runtime._runtime_workers_with_full_runtime_marker(
                SimpleNamespace(),
                stop,
            )
        )
        await asyncio.wait_for(started.wait(), timeout=1.0)
        for _ in range(100):
            if bootstrap._BOOTSTRAP_STATE.get("state") == "full_runtime":
                break
            await asyncio.sleep(0.005)

        assert bootstrap._BOOTSTRAP_STATE["state"] == "full_runtime"
        assert bootstrap._BOOTSTRAP_STATE["full_runtime_at"]
        assert task.done() is False

        stop.set()
        await asyncio.wait_for(task, timeout=1.0)

    asyncio.run(scenario())


def test_worker_chain_that_fails_during_startup_never_grants_cleanup_eligibility(monkeypatch) -> None:
    _reset_bootstrap_state()

    async def failing_workers(_runtime, _stop: asyncio.Event) -> None:
        raise RuntimeError("worker startup failed")

    monkeypatch.setattr(cleanup_runtime, "_ORIGINAL_RUNTIME_WORKERS", failing_workers)
    monkeypatch.setattr(cleanup_runtime, "FULL_RUNTIME_SETTLE_SECONDS", 0.05)

    async def scenario() -> None:
        with pytest.raises(RuntimeError, match="worker startup failed"):
            await cleanup_runtime._runtime_workers_with_full_runtime_marker(
                SimpleNamespace(),
                asyncio.Event(),
            )

    asyncio.run(scenario())
    assert bootstrap._BOOTSTRAP_STATE["state"] == "ready"
    assert bootstrap._BOOTSTRAP_STATE["full_runtime_at"] is None


def test_installer_wraps_final_worker_chain_once(monkeypatch) -> None:
    async def final_composed_workers(_runtime, stop: asyncio.Event) -> None:
        await stop.wait()

    monkeypatch.setattr(bootstrap, "_run_runtime_workers", final_composed_workers)
    monkeypatch.setattr(cleanup_runtime, "_ORIGINAL_RUNTIME_WORKERS", None)

    cleanup_runtime._install_full_runtime_worker_marker()
    wrapped = bootstrap._run_runtime_workers

    assert wrapped is cleanup_runtime._runtime_workers_with_full_runtime_marker
    assert cleanup_runtime._ORIGINAL_RUNTIME_WORKERS is final_composed_workers
    assert bool(getattr(wrapped, "_roi_cleanup_full_runtime_worker_marker", False)) is True

    cleanup_runtime._install_full_runtime_worker_marker()
    assert bootstrap._run_runtime_workers is wrapped
    assert cleanup_runtime._ORIGINAL_RUNTIME_WORKERS is final_composed_workers
