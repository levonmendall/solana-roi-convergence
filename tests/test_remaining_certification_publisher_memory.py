from __future__ import annotations

import threading
from contextlib import contextmanager

from solana_roi import certification_generation_runtime_repair as generation
from solana_roi import e2e_status_read_boundary_repair as e2e
from solana_roi.certification_generation_coordinator import CertificationResourceGuardError


@contextmanager
def _no_phase(_name: str):
    yield


def _reject_guard(_surface: str):
    raise CertificationResourceGuardError("synthetic_resource_pressure")


def test_e2e_publication_transfers_owned_payload_and_reader_isolation(monkeypatch) -> None:
    monkeypatch.setattr(e2e, "_SNAPSHOT", None)
    monkeypatch.setattr(e2e, "_SNAPSHOT_PUBLISHED_MONOTONIC", None)

    payload = {"read_boundary": {}, "nested": {"value": 1}}
    e2e._publish_snapshot(payload)

    assert e2e._SNAPSHOT is payload
    view = e2e._cached_e2e_status()
    assert view is not payload
    view["nested"]["value"] = 2
    assert e2e._SNAPSHOT["nested"]["value"] == 1
    cache = e2e._cache_state()
    assert cache["publication_uses_owned_builder_payload"] is True
    assert cache["post_build_resource_guard"] is True
    assert cache["post_build_memory_limit_fraction"] == 0.90


def test_e2e_post_build_guard_retains_last_known_good_snapshot(monkeypatch) -> None:
    old = {"read_boundary": {}, "marker": "old"}
    e2e._publish_snapshot(old)
    stop = threading.Event()

    def build(*_args, **_kwargs):
        stop.set()
        return {"read_boundary": {}, "marker": "new"}

    monkeypatch.setattr(e2e, "memory_forensics_phase", _no_phase)
    monkeypatch.setattr(e2e, "build_bounded_e2e_status", build)
    monkeypatch.setattr(e2e, "resource_guard", _reject_guard)

    e2e._snapshot_thread_main(object(), lambda: {}, stop)

    assert e2e._SNAPSHOT is old
    assert e2e._SNAPSHOT["marker"] == "old"
    cache = e2e._cache_state()
    assert cache["last_error_type"] == "CertificationResourceGuardError"
    assert cache["guard_rejections"] >= 1


def test_forward_publication_transfers_owned_payload_and_reader_isolation(monkeypatch) -> None:
    monkeypatch.setattr(generation, "_FORWARD_SNAPSHOT", None)
    monkeypatch.setattr(generation, "_FORWARD_PUBLISHED_MONOTONIC", None)

    payload = {"certification_generation": {}, "nested": {"value": 1}}
    generation._publish_forward(payload)

    assert generation._FORWARD_SNAPSHOT is payload
    view = generation._cached_forward_endpoint()
    assert view is not payload
    view["nested"]["value"] = 2
    assert generation._FORWARD_SNAPSHOT["nested"]["value"] == 1
    cache = generation._forward_cache_state()
    assert cache["publication_uses_owned_builder_payload"] is True
    assert cache["post_build_resource_guard"] is True
    assert cache["post_build_memory_limit_fraction"] == 0.90


def test_forward_post_build_guard_retains_last_known_good_snapshot(monkeypatch) -> None:
    old = {"certification_generation": {}, "marker": "old"}
    generation._publish_forward(old)
    stop = threading.Event()

    def build():
        stop.set()
        return {
            "certification_generation": {"generation_id": "synthetic-generation"},
            "marker": "new",
        }

    monkeypatch.setattr(generation, "_ORIGINAL_FORWARD_ENDPOINT", build)
    monkeypatch.setattr(generation, "resource_guard", _reject_guard)

    generation._forward_thread_main(stop)

    assert generation._FORWARD_SNAPSHOT is old
    assert generation._FORWARD_SNAPSHOT["marker"] == "old"
    cache = generation._forward_cache_state()
    assert cache["last_error_type"] == "CertificationResourceGuardError"
    assert cache["guard_rejections"] >= 1
