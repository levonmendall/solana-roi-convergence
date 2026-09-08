from __future__ import annotations

import threading
import time

import pytest

from solana_roi import certification_generation_coordinator as coordinator


def _disable_resource_guard(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        coordinator,
        "resource_guard",
        lambda surface: {"safe_to_start": True, "surface": surface, "blockers": []},
    )


def test_nested_generation_reuses_outer_generation(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOLANA_ROI_CERTIFICATION_LEASE_PATH", str(tmp_path / "generation.lock"))
    _disable_resource_guard(monkeypatch)

    with coordinator.exclusive_generation("outer") as outer:
        with coordinator.exclusive_generation("inner") as inner:
            assert inner.nested is True
            assert inner.generation_id == outer.generation_id
            assert inner.release_commit == outer.release_commit


def test_threads_cannot_overlap_expensive_generation(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOLANA_ROI_CERTIFICATION_LEASE_PATH", str(tmp_path / "generation.lock"))
    _disable_resource_guard(monkeypatch)

    state_lock = threading.Lock()
    active = 0
    max_active = 0
    ids: list[str] = []
    ready = threading.Barrier(3)

    def run(surface: str) -> None:
        nonlocal active, max_active
        ready.wait()
        with coordinator.exclusive_generation(surface, timeout_seconds=2.0) as lease:
            with state_lock:
                ids.append(lease.generation_id)
                active += 1
                max_active = max(max_active, active)
            time.sleep(0.08)
            with state_lock:
                active -= 1

    first = threading.Thread(target=run, args=("e2e",))
    second = threading.Thread(target=run, args=("production-proof",))
    first.start()
    second.start()
    ready.wait()
    first.join(timeout=3.0)
    second.join(timeout=3.0)

    assert not first.is_alive()
    assert not second.is_alive()
    assert max_active == 1
    assert len(ids) == 2
    assert ids[0] != ids[1]


def test_resource_guard_fails_closed_on_known_cgroup_pressure(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SOLANA_ROI_DB_PATH", str(tmp_path / "evidence.sqlite3"))
    monkeypatch.setenv("SOLANA_ROI_CERTIFICATION_MEMORY_START_FRACTION", "0.90")
    monkeypatch.setenv("SOLANA_ROI_CERTIFICATION_DISK_RESERVE_BYTES", "0")
    monkeypatch.setenv("SOLANA_ROI_CERTIFICATION_WAL_START_MAX_BYTES", str(10**12))
    monkeypatch.setattr(
        coordinator.cgroup,
        "capture_snapshot",
        lambda reason: {
            "memory_fraction": 0.95,
            "memory_current_bytes": 2040,
            "memory_max_bytes": 2147,
            "wal_bytes": 0,
        },
    )
    monkeypatch.setattr(coordinator, "_disk_free", lambda: 10**12)
    monkeypatch.setattr(coordinator, "_wal_size", lambda: 0)

    with pytest.raises(
        coordinator.CertificationResourceGuardError,
        match="cgroup_memory_headroom_below_generation_reserve",
    ):
        coordinator.resource_guard("forward-certification")


def test_coordinator_contract_preserves_authority() -> None:
    payload = coordinator.status()
    assert payload["cross_process_single_flight"] is True
    assert payload["resource_guard_enabled"] is True
    assert payload["certification_thresholds_changed"] is False
    assert payload["economic_thresholds_changed"] is False
    assert payload["canonical_evidence_reset"] is False
    assert payload["paper_only"] is True
    assert payload["live_money_authority"] is False
    assert payload["signing_available"] is False
    assert payload["transaction_submission_available"] is False
