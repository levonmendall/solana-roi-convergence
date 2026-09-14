from __future__ import annotations

import asyncio
import threading
from types import SimpleNamespace

import pytest

from solana_roi import certification_bootstrap_autocheckpoint_lease as lease
from solana_roi import render_runtime_bootstrap_repair as handoff
from solana_roi import runtime_storage_composition as storage


def test_shadow_requires_explicit_certification_bootstrap_completion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = SimpleNamespace()
    monkeypatch.setattr(handoff, "_RUNTIME", SimpleNamespace(store=store))

    assert handoff._certification_bootstrap_complete_for_shadow() is False

    setattr(store, lease.STATE_ATTR, {"active": True, "restore_reason": None})
    assert handoff._certification_bootstrap_complete_for_shadow() is False

    # Temporary inactivity is deliberately insufficient: a certifier retry could
    # still resume the large logical bootstrap and race a shadow database copy.
    setattr(store, lease.STATE_ATTR, {"active": False, "restore_reason": "idle_timeout"})
    assert handoff._certification_bootstrap_complete_for_shadow() is False

    setattr(store, lease.STATE_ATTR, {"active": False, "restore_reason": "bootstrap_complete"})
    assert handoff._certification_bootstrap_complete_for_shadow() is True


def test_deferred_shadow_waits_for_bootstrap_before_even_checking_memory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(storage, "shadow_snapshot_requested", lambda: True)
    monkeypatch.setattr(handoff, "_SHADOW_START_DELAY_SECONDS", 0.01)
    monkeypatch.setattr(handoff, "_SHADOW_RETRY_SECONDS", 0.01)

    readiness = iter((False, False, True))
    monkeypatch.setattr(
        handoff,
        "_certification_bootstrap_complete_for_shadow",
        lambda: next(readiness),
    )

    memory_checks: list[str] = []
    monkeypatch.setattr(
        handoff,
        "_shadow_memory_has_headroom",
        lambda: (
            memory_checks.append("checked") is None,
            {
                "memory_fraction": 0.50,
                "memory_headroom_bytes": 1024**3,
            },
        ),
    )

    snapshots: list[str] = []
    monkeypatch.setattr(
        storage,
        "run_requested_shadow_snapshot",
        lambda: snapshots.append("ran") or {
            "equivalent": True,
            "authoritative_runtime_changed": False,
            "paper_only": True,
            "live_money_authority": False,
        },
    )

    async def scenario() -> None:
        await handoff._run_deferred_storage_shadow(asyncio.Event())

    asyncio.run(scenario())

    assert memory_checks == ["checked"]
    assert snapshots == ["ran"]
    assert handoff._SHADOW_STATE["attempts"] == 1
    assert handoff._SHADOW_STATE["state"] == "completed"
    assert handoff._SHADOW_STATE["equivalent"] is True


def test_deferred_shadow_stays_fail_closed_during_incomplete_bootstrap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(storage, "shadow_snapshot_requested", lambda: True)
    monkeypatch.setattr(handoff, "_SHADOW_START_DELAY_SECONDS", 0.01)
    monkeypatch.setattr(handoff, "_SHADOW_RETRY_SECONDS", 0.01)
    monkeypatch.setattr(handoff, "_certification_bootstrap_complete_for_shadow", lambda: False)

    memory_checks: list[str] = []
    snapshots: list[str] = []
    monkeypatch.setattr(
        handoff,
        "_shadow_memory_has_headroom",
        lambda: memory_checks.append("checked") or (True, {}),
    )
    monkeypatch.setattr(
        storage,
        "run_requested_shadow_snapshot",
        lambda: snapshots.append("ran") or {"equivalent": True},
    )

    async def scenario() -> None:
        stop = asyncio.Event()
        task = asyncio.create_task(handoff._run_deferred_storage_shadow(stop))
        await asyncio.sleep(0.04)
        stop.set()
        await task

    asyncio.run(scenario())

    assert memory_checks == []
    assert snapshots == []
    assert handoff._SHADOW_STATE["attempts"] == 0
    assert handoff._SHADOW_STATE["state"] == "waiting_for_certification_bootstrap"


def _gate_store(*, active: bool, reason: str | None) -> SimpleNamespace:
    store = SimpleNamespace(_lock=threading.RLock())
    setattr(store, lease.STATE_ATTR, {"active": active, "restore_reason": reason})
    return store


def _enable_transition_gate(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOLANA_ROI_ACTIVE_STORAGE_SHADOW", "1")
    monkeypatch.setenv("SOLANA_ROI_ACTIVE_STORAGE_ENABLED", "0")
    monkeypatch.setattr(lease, "WORKER_GATE_POLL_SECONDS", 0.005)


def test_worker_gate_rejects_idle_timeout_until_explicit_completion_and_shadow_settles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _enable_transition_gate(monkeypatch)
    monkeypatch.setattr(lease, "_worker_gate_seconds", lambda: 0.25)
    store = _gate_store(active=False, reason="idle_timeout")
    shadow = SimpleNamespace(_SHADOW_STATE={"state": "waiting_for_certification_bootstrap"})

    async def scenario() -> str:
        stop = asyncio.Event()
        task = asyncio.create_task(lease._wait_for_transition_worker_gate(store, stop, shadow))
        await asyncio.sleep(0.02)
        assert not task.done()
        setattr(store, lease.STATE_ATTR, {"active": False, "restore_reason": "bootstrap_complete"})
        shadow._SHADOW_STATE["state"] = "building"
        await asyncio.sleep(0.02)
        assert not task.done()
        shadow._SHADOW_STATE["state"] = "completed"
        return await task

    assert asyncio.run(scenario()) == "released"


def test_worker_gate_timeout_restores_paper_availability_without_authorizing_storage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _enable_transition_gate(monkeypatch)
    monkeypatch.setattr(lease, "_worker_gate_seconds", lambda: 0.03)
    store = _gate_store(active=False, reason="idle_timeout")
    original_state = getattr(store, lease.STATE_ATTR)
    shadow = SimpleNamespace(_SHADOW_STATE={"state": "waiting_for_certification_bootstrap"})

    result = asyncio.run(
        lease._wait_for_transition_worker_gate(store, asyncio.Event(), shadow)
    )

    assert result == "timeout"
    assert getattr(store, lease.STATE_ATTR) is original_state
    assert original_state == {"active": False, "restore_reason": "idle_timeout"}
    assert shadow._SHADOW_STATE["state"] == "waiting_for_certification_bootstrap"


def test_worker_gate_stops_promptly_without_starting_workers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _enable_transition_gate(monkeypatch)
    monkeypatch.setattr(lease, "_worker_gate_seconds", lambda: 1.0)
    store = _gate_store(active=True, reason=None)
    shadow = SimpleNamespace(_SHADOW_STATE={"state": "waiting_for_certification_bootstrap"})

    async def scenario() -> str:
        stop = asyncio.Event()
        task = asyncio.create_task(lease._wait_for_transition_worker_gate(store, stop, shadow))
        await asyncio.sleep(0.01)
        stop.set()
        return await task

    assert asyncio.run(scenario()) == "stopped"


def test_worker_gate_is_transition_only_and_skips_active_storage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SOLANA_ROI_ACTIVE_STORAGE_SHADOW", "1")
    monkeypatch.setenv("SOLANA_ROI_ACTIVE_STORAGE_ENABLED", "1")
    store = _gate_store(active=True, reason=None)
    shadow = SimpleNamespace(_SHADOW_STATE={"state": "building"})

    result = asyncio.run(
        lease._wait_for_transition_worker_gate(store, asyncio.Event(), shadow)
    )

    assert result == "not_requested"
