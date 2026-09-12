from __future__ import annotations

import asyncio
from types import SimpleNamespace

from solana_roi import continuity_storage_capacity_repair as storage_capacity
from solana_roi import storage_maintenance_lock_isolation_repair as repair


def _run_one_worker_pass(
    monkeypatch,
    *,
    checkpoint_interval_seconds: float | None = None,
) -> tuple[list[float], list[str]]:
    stop = asyncio.Event()
    plane = SimpleNamespace(store=SimpleNamespace())
    observed_timeouts: list[float] = []
    checkpoint_calls: list[str] = []

    monkeypatch.setattr(
        storage_capacity,
        "_prune_operational_rows_once",
        lambda _self: (1, 1),
    )
    monkeypatch.setattr(
        storage_capacity,
        "_checkpoint_wal",
        lambda _self: checkpoint_calls.append("checkpoint") or (0, 0, 0),
    )
    if checkpoint_interval_seconds is not None:
        monkeypatch.setattr(
            storage_capacity,
            "WAL_CHECKPOINT_INTERVAL_SECONDS",
            float(checkpoint_interval_seconds),
        )

    async def fake_wait_for(awaitable, *, timeout: float):
        observed_timeouts.append(float(timeout))
        if hasattr(awaitable, "close"):
            awaitable.close()
        stop.set()
        raise asyncio.TimeoutError

    monkeypatch.setattr(repair.asyncio, "wait_for", fake_wait_for)
    asyncio.run(repair._bounded_storage_maintenance_worker(plane, stop))
    return observed_timeouts, checkpoint_calls


def test_storage_maintenance_does_not_force_wal_checkpoint_on_startup(monkeypatch) -> None:
    observed_timeouts, checkpoint_calls = _run_one_worker_pass(monkeypatch)

    assert checkpoint_calls == []
    assert observed_timeouts == [storage_capacity.MAINTENANCE_IDLE_SECONDS]
    assert bool(
        getattr(
            repair._bounded_storage_maintenance_worker,
            "_roi_storage_maintenance_startup_checkpoint_deferred",
            False,
        )
    )


def test_storage_maintenance_checkpoint_remains_enabled_when_interval_is_due(monkeypatch) -> None:
    observed_timeouts, checkpoint_calls = _run_one_worker_pass(
        monkeypatch,
        checkpoint_interval_seconds=0.0,
    )

    assert checkpoint_calls == ["checkpoint"]
    assert observed_timeouts == [storage_capacity.MAINTENANCE_IDLE_SECONDS]
