from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

from solana_roi import render_runtime_bootstrap_repair as handoff
from solana_roi import runtime_memory_capacity_repair as memory_capacity
from solana_roi import runtime_storage_composition as storage


class _DummyStore:
    def __init__(self, path: Path) -> None:
        self.path = path

    def containment_status(self) -> dict[str, object]:
        return {"enabled": True, "paper_only": True}


class _DummyEngine:
    def __init__(self, *, store: object) -> None:
        self.store = store


def _shadow_environment(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv(storage.SHADOW_ENV, "1")
    monkeypatch.delenv(storage.ACTIVATE_ENV, raising=False)
    monkeypatch.setenv("RENDER_GIT_COMMIT", "a" * 40)
    monkeypatch.setenv(storage.LEGACY_PATH_ENV, str(tmp_path / "legacy.sqlite3"))
    monkeypatch.setenv(storage.ACTIVE_PATH_ENV, str(tmp_path / "active.sqlite3"))


def test_shadow_request_never_builds_during_runtime_composition(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _shadow_environment(monkeypatch, tmp_path)
    storage._reset_shadow_snapshot_state_for_tests()
    calls: list[str] = []

    def forbidden_snapshot(*, release: str, mode: str) -> dict[str, object]:
        calls.append(f"{release}:{mode}")
        raise AssertionError("shadow snapshot executed on startup-critical composition path")

    monkeypatch.setattr(storage, "_build_exact_snapshot", forbidden_snapshot)
    monkeypatch.setattr(storage, "LegacyContainedObservationEventStore", _DummyStore)
    monkeypatch.setattr(storage, "DurablePaperTradingEngine", _DummyEngine)
    monkeypatch.setattr(storage, "_materialize_verified_genesis_checkpoint_if_needed", lambda *_: None)

    store, _engine, status = storage.compose_runtime_storage()

    assert isinstance(store, _DummyStore)
    assert calls == []
    assert status["mode"] == "legacy_authoritative"
    assert status["shadow"]["requested"] is True
    assert status["shadow"]["execution"] == "post_startup_memory_gated_single_flight"
    assert status["shadow"]["authoritative_runtime_changed"] is False
    assert status["paper_only"] is True
    assert status["live_money_authority"] is False


def test_requested_shadow_snapshot_is_single_flight_per_release(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _shadow_environment(monkeypatch, tmp_path)
    storage._reset_shadow_snapshot_state_for_tests()
    calls: list[tuple[str, str]] = []

    def snapshot(*, release: str, mode: str) -> dict[str, object]:
        calls.append((release, mode))
        return {
            "equivalent": True,
            "release_sha": release,
            "mode": mode,
            "authoritative_runtime_changed": False,
            "paper_only": True,
            "live_money_authority": False,
        }

    monkeypatch.setattr(storage, "_build_exact_snapshot", snapshot)

    first = storage.run_requested_shadow_snapshot()
    second = storage.run_requested_shadow_snapshot()

    assert first is not None and first["equivalent"] is True
    assert second is not None and second["equivalent"] is True
    assert calls == [("a" * 40, "shadow")]
    status = storage.shadow_snapshot_status()
    assert status["state"] == "completed"
    assert status["attempts"] == 1
    assert status["authoritative_runtime_changed"] is False


def test_shadow_memory_gate_reuses_existing_production_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    limit = 2 * 1024**3

    monkeypatch.setattr(
        memory_capacity,
        "cgroup_memory_status",
        lambda: {
            "memory_current_bytes": int(limit * 0.79),
            "memory_max_bytes": limit,
            "memory_fraction": 0.79,
            "memory_headroom_bytes": int(limit * 0.21),
        },
    )
    safe, _ = handoff._shadow_memory_has_headroom()
    assert safe is True

    monkeypatch.setattr(
        memory_capacity,
        "cgroup_memory_status",
        lambda: {
            "memory_current_bytes": int(limit * 0.90),
            "memory_max_bytes": limit,
            "memory_fraction": 0.90,
            "memory_headroom_bytes": int(limit * 0.10),
        },
    )
    safe, _ = handoff._shadow_memory_has_headroom()
    assert safe is False

    monkeypatch.setattr(memory_capacity, "cgroup_memory_status", lambda: {"available": False})
    safe, _ = handoff._shadow_memory_has_headroom()
    assert safe is False


def test_deferred_shadow_does_not_execute_while_memory_pressure_is_high(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _shadow_environment(monkeypatch, tmp_path)
    storage._reset_shadow_snapshot_state_for_tests()
    calls: list[str] = []
    monkeypatch.setattr(handoff, "_SHADOW_START_DELAY_SECONDS", 0.01)
    monkeypatch.setattr(handoff, "_SHADOW_RETRY_SECONDS", 0.01)
    monkeypatch.setattr(
        handoff,
        "_shadow_memory_has_headroom",
        lambda: (
            False,
            {
                "memory_fraction": 0.97,
                "memory_headroom_bytes": 64 * 1024**2,
            },
        ),
    )
    monkeypatch.setattr(
        storage,
        "run_requested_shadow_snapshot",
        lambda: calls.append("ran") or {"equivalent": True},
    )

    async def scenario() -> None:
        stop = asyncio.Event()
        task = asyncio.create_task(handoff._run_deferred_storage_shadow(stop))
        await asyncio.sleep(0.04)
        stop.set()
        await task

    asyncio.run(scenario())

    assert calls == []
    assert handoff._SHADOW_STATE["attempts"] == 0
    assert handoff._SHADOW_STATE["state"] == "waiting_for_memory_headroom"


def test_deferred_shadow_runs_once_after_runtime_headroom_exists(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _shadow_environment(monkeypatch, tmp_path)
    storage._reset_shadow_snapshot_state_for_tests()
    calls: list[str] = []
    monkeypatch.setattr(handoff, "_SHADOW_START_DELAY_SECONDS", 0.01)
    monkeypatch.setattr(
        handoff,
        "_shadow_memory_has_headroom",
        lambda: (
            True,
            {
                "memory_fraction": 0.50,
                "memory_headroom_bytes": 1024**3,
            },
        ),
    )
    monkeypatch.setattr(
        storage,
        "run_requested_shadow_snapshot",
        lambda: calls.append("ran") or {
            "equivalent": True,
            "authoritative_runtime_changed": False,
            "paper_only": True,
            "live_money_authority": False,
        },
    )

    async def scenario() -> None:
        stop = asyncio.Event()
        await handoff._run_deferred_storage_shadow(stop)

    asyncio.run(scenario())

    assert calls == ["ran"]
    assert handoff._SHADOW_STATE["attempts"] == 1
    assert handoff._SHADOW_STATE["state"] == "completed"
    assert handoff._SHADOW_STATE["equivalent"] is True
    assert handoff._SHADOW_STATE["last_error_type"] is None
