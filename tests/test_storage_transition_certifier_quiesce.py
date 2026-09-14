from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from solana_roi import storage_transition_quiesce as transition


def _set_transition(monkeypatch: pytest.MonkeyPatch, *, quiesced: bool = True, shadow: bool = True, active: bool = False) -> None:
    monkeypatch.setenv(transition.QUIESCE_ENV, "1" if quiesced else "0")
    monkeypatch.setenv(transition.SHADOW_ENV, "1" if shadow else "0")
    monkeypatch.setenv(transition.ACTIVE_ENV, "1" if active else "0")


def test_transition_quiesce_is_exactly_legacy_shadow_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_transition(monkeypatch)
    assert transition.transition_certifier_quiesce_requested() is True

    _set_transition(monkeypatch, active=True)
    assert transition.transition_certifier_quiesce_requested() is False

    _set_transition(monkeypatch, shadow=False)
    assert transition.transition_certifier_quiesce_requested() is False

    _set_transition(monkeypatch, quiesced=False)
    assert transition.transition_certifier_quiesce_requested() is False


def test_shadow_only_gate_releases_only_after_shadow_settles() -> None:
    async def exercise() -> None:
        stop = SimpleNamespace(is_set=lambda: False)

        async def never_stop() -> None:
            return None

        stop.wait = never_stop
        bootstrap = SimpleNamespace(_SHADOW_STATE={"state": "completed"})
        result = await transition._wait_for_shadow_only(stop, bootstrap, 1.0)
        assert result == "released"

    asyncio.run(exercise())


def test_certifier_transition_quiesce_never_enters_worker_lifespan(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    _set_transition(monkeypatch)
    from solana_roi import certifier_cleanup_service as service

    original_entered = False

    @asynccontextmanager
    async def original_lifespan(_app):
        nonlocal original_entered
        original_entered = True
        yield

    class Lease:
        def status(self):
            return {"owned": True}

        def release(self):
            return None

    async def acquire(_database_path, *, stop):
        return Lease()

    monkeypatch.setattr(service, "_ORIGINAL_LIFESPAN", original_lifespan)
    monkeypatch.setattr(service, "_replica_path", lambda: tmp_path / "certifier.sqlite3")
    monkeypatch.setattr(service.disk_ownership, "acquire_runtime_disk_lease", acquire)

    async def exercise() -> None:
        async with service.lifespan(service.app):
            assert service._STATE["status"] == "storage_transition_quiesced"
            assert service._STATE["storage_transition_certifier_quiesced"] is True
            assert service._STATE["certification_available_during_storage_transition"] is False
            assert service._STATE["storage_activation_authorized"] is False
            assert original_entered is False

    asyncio.run(exercise())
    assert original_entered is False


def test_authoritative_transition_patch_bypasses_only_bootstrap_prerequisite(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_transition(monkeypatch)
    transition._reset_for_tests()
    try:
        transition.configure_storage_transition_quiesce()
        from solana_roi import render_runtime_bootstrap_repair as render_bootstrap
        from solana_roi import certification_bootstrap_autocheckpoint_lease as lease

        assert render_bootstrap._certification_bootstrap_complete_for_shadow() is True
        status = lease.status()
        assert status["transition_certifier_quiesced"] is True
        assert status["transition_worker_gate_requires_bootstrap_complete"] is False
        assert status["transition_worker_gate_requires_certifier_quiesced"] is True
        assert status["transition_worker_gate_timeout_authorizes_storage"] is False
    finally:
        transition._reset_for_tests()
