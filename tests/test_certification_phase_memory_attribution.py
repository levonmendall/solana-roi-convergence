from __future__ import annotations

import threading
from contextlib import contextmanager

from solana_roi import e2e_status_read_boundary_repair as e2e
from solana_roi import production_proof_read_boundary_repair as proof


def _phase_recorder(recorded: list[str]):
    @contextmanager
    def record(name: str):
        recorded.append(name)
        yield

    return record


def test_e2e_snapshot_builder_is_attributed_to_memory_phase(monkeypatch) -> None:
    phases: list[str] = []
    stop = threading.Event()

    def build(*_args, **_kwargs):
        stop.set()
        return {}

    monkeypatch.setattr(e2e, "memory_forensics_phase", _phase_recorder(phases))
    monkeypatch.setattr(e2e, "build_bounded_e2e_status", build)
    monkeypatch.setattr(e2e, "_publish_snapshot", lambda _payload: None)

    e2e._snapshot_thread_main(object(), lambda: {}, stop)

    assert phases == ["e2e_status_build"]


def test_production_proof_builder_is_attributed_to_memory_phase(monkeypatch) -> None:
    phases: list[str] = []
    stop = threading.Event()

    def build():
        stop.set()
        return {}

    monkeypatch.setattr(proof, "memory_forensics_phase", _phase_recorder(phases))
    monkeypatch.setattr(proof, "_publish_snapshot", lambda _payload: None)

    proof._snapshot_thread_main(build, stop)

    assert phases == ["production_proof_build"]


def test_phase_wrapper_does_not_swallow_builder_failure(monkeypatch) -> None:
    phases: list[str] = []
    stop = threading.Event()

    def build():
        stop.set()
        raise RuntimeError("synthetic-builder-failure")

    monkeypatch.setattr(proof, "memory_forensics_phase", _phase_recorder(phases))

    proof._snapshot_thread_main(build, stop)

    assert phases == ["production_proof_build"]
    state = proof._cache_state()
    assert state["last_error_type"] == "RuntimeError"
    assert state["consecutive_failures"] >= 1
