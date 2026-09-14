from __future__ import annotations

import asyncio
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
