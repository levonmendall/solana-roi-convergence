from __future__ import annotations

from contextlib import nullcontext

from solana_roi import production_proof_read_boundary_repair as repair
from solana_roi.certification_generation_coordinator import CertificationResourceGuardError


class _OneShotStop:
    def __init__(self) -> None:
        self.done = False

    def is_set(self) -> bool:
        return self.done

    def wait(self, _seconds: float) -> bool:
        self.done = True
        return True


def _reset_snapshot_state() -> None:
    with repair._SNAPSHOT_LOCK:
        repair._SNAPSHOT = None
        repair._SNAPSHOT_PUBLISHED_MONOTONIC = None
        repair._SNAPSHOT_STATS.update(
            {
                "attempts": 0,
                "successes": 0,
                "failures": 0,
                "consecutive_failures": 0,
                "last_started_at": None,
                "last_completed_at": None,
                "last_duration_seconds": None,
                "last_error_type": None,
            }
        )


def test_publish_transfers_builder_payload_without_whole_proof_deepcopy(monkeypatch) -> None:
    _reset_snapshot_state()
    payload = {"marker": "owned", "nested": {"value": 1}}

    def forbidden_deepcopy(_value):
        raise AssertionError("publication must not deepcopy the whole production proof")

    monkeypatch.setattr(repair.copy, "deepcopy", forbidden_deepcopy)
    repair._publish_snapshot(payload)

    with repair._SNAPSHOT_LOCK:
        assert repair._SNAPSHOT is payload
        assert repair._SNAPSHOT["marker"] == "owned"
        assert repair._SNAPSHOT["read_boundary"]["publication_uses_owned_builder_payload"] is True
        assert repair._SNAPSHOT["read_boundary"]["post_build_resource_guard"] is True


def test_cached_reader_still_returns_isolated_copy() -> None:
    _reset_snapshot_state()
    payload = {"marker": "cached", "nested": {"value": 1}}
    repair._publish_snapshot(payload)

    first = repair._cached_production_proof()
    first["nested"]["value"] = 99
    first["marker"] = "mutated"

    second = repair._cached_production_proof()
    assert second["marker"] == "cached"
    assert second["nested"]["value"] == 1


def test_postbuild_guard_failure_retains_last_known_good_snapshot(monkeypatch) -> None:
    _reset_snapshot_state()
    old_payload = {"marker": "last-known-good"}
    repair._publish_snapshot(old_payload)

    monkeypatch.setattr(repair, "memory_forensics_phase", lambda _name: nullcontext())

    def reject_after_build(_surface: str):
        raise CertificationResourceGuardError("cgroup_memory_headroom_below_generation_reserve")

    monkeypatch.setattr(repair, "resource_guard", reject_after_build)

    repair._snapshot_thread_main(lambda: {"marker": "unsafe-new-proof"}, _OneShotStop())

    with repair._SNAPSHOT_LOCK:
        assert repair._SNAPSHOT is old_payload
        assert repair._SNAPSHOT["marker"] == "last-known-good"
        assert repair._SNAPSHOT_STATS["failures"] == 1
        assert repair._SNAPSHOT_STATS["last_error_type"] == "CertificationResourceGuardError"


def test_postbuild_guard_runs_between_builder_and_publication(monkeypatch) -> None:
    _reset_snapshot_state()
    events: list[str] = []
    monkeypatch.setattr(repair, "memory_forensics_phase", lambda _name: nullcontext())

    def builder():
        events.append("build")
        return {"marker": "new"}

    def guard(surface: str):
        events.append(f"guard:{surface}")
        return {"safe_to_start": True}

    original_publish = repair._publish_snapshot

    def publish(payload):
        events.append("publish")
        original_publish(payload)

    monkeypatch.setattr(repair, "resource_guard", guard)
    monkeypatch.setattr(repair, "_publish_snapshot", publish)

    repair._snapshot_thread_main(builder, _OneShotStop())

    assert events == [
        "build",
        "guard:production_proof_post_build_pre_publish",
        "publish",
    ]
    with repair._SNAPSHOT_LOCK:
        assert repair._SNAPSHOT_STATS["successes"] == 1
        assert repair._SNAPSHOT_STATS["failures"] == 0
