from __future__ import annotations

import hashlib
import sqlite3

import pytest

from solana_roi.active_storage import ActiveStorage, canonical_json, payload_hash
from solana_roi import storage_transition as transition


def _write_semantic_truth(storage: ActiveStorage, current_truth: dict[str, object]) -> None:
    storage.replace_current("strategy_current", "state_key", "transition", current_truth["strategy"])
    storage.replace_current("wallet_current", "wallet_id", "__transition_state__", current_truth["wallet"])
    storage.replace_current(
        "wallet_current",
        "wallet_id",
        "__transition_watermarks__",
        current_truth["wallet_evidence_watermarks"],
    )
    storage.replace_current("provider_current", "provider_id", "__transition__", current_truth["provider_source"])
    storage.replace_current("system_current", "state_key", "freshness", current_truth["freshness"])
    storage.replace_current("system_current", "state_key", "latest_event_ids", current_truth["latest_event_ids"])
    storage.replace_current("active_candidates", "candidate_id", "__transition__", current_truth["active_candidates"])
    storage.replace_current("active_lifecycles", "lifecycle_id", "__transition__", current_truth["active_lifecycles"])
    storage.replace_current(
        "portfolio_current",
        "state_key",
        "transition",
        current_truth["portfolio"],
        last_engine_event_id=0,
    )
    storage.replace_current(
        "system_current",
        "state_key",
        "replication_watermarks",
        current_truth["replication_watermarks"],
    )
    storage.replace_current("certification_current", "state_key", "transition", current_truth["certification"])
    storage.replace_current("continuity_current", "state_key", "transition", current_truth["continuity"])


def _verified_checkpoint(tmp_path, *, release_sha: str):
    path = tmp_path / "active.sqlite3"
    storage = ActiveStorage(path)
    storage.initialize(epoch_id="cross-release-test")
    current_truth = {section: {} for section in transition._SEMANTIC_SECTIONS}
    _write_semantic_truth(storage, current_truth)
    payload = transition.build_checkpoint_payload(
        release_sha=release_sha,
        current_truth=current_truth,
        provenance={"source": "test"},
    )
    verification = transition.persist_verified_checkpoint(
        storage,
        checkpoint_payload=payload,
        source_truth=current_truth,
    )
    assert verification.equivalent is True
    return path, payload


def _clear_runtime_release_environment(monkeypatch):
    for name in (
        transition.ACTIVATE_ENV,
        transition.ACTIVE_PATH_ENV,
        transition.FINALIZE_ENV,
        "RENDER_GIT_COMMIT",
        "GIT_COMMIT",
    ):
        monkeypatch.delenv(name, raising=False)


def test_compact_checkpoint_does_not_embed_semantic_sections(tmp_path):
    path = tmp_path / "active.sqlite3"
    storage = ActiveStorage(path)
    storage.initialize(epoch_id="compact-checkpoint-test")
    current_truth = {section: {} for section in transition._SEMANTIC_SECTIONS}
    current_truth["wallet"] = {"rows": ["x" * 1024 for _ in range(1024)]}
    _write_semantic_truth(storage, current_truth)

    payload = transition.build_checkpoint_payload(
        release_sha="release-a",
        current_truth=current_truth,
        provenance={"source": "test"},
    )
    embedded_equivalent = dict(payload)
    embedded_equivalent.update(current_truth)

    assert payload["migration_version"] == transition.TRANSITION_MIGRATION_VERSION == 3
    assert payload["sections_storage"] == "active_current_tables"
    assert not (set(transition._SEMANTIC_SECTIONS) & set(payload))
    assert len(canonical_json(payload)) * 100 < len(canonical_json(embedded_equivalent))

    transition.persist_verified_checkpoint(
        storage,
        checkpoint_payload=payload,
        source_truth=current_truth,
    )
    loaded = transition.load_verified_checkpoint(path)
    assert loaded["wallet"] == current_truth["wallet"]


def test_compact_checkpoint_detects_current_row_tamper_even_with_recomputed_row_hash(tmp_path):
    path, _ = _verified_checkpoint(tmp_path, release_sha="release-a")
    tampered = {"tampered": True}
    body = canonical_json(tampered)
    digest = hashlib.sha256(body.encode("utf-8")).hexdigest()
    with sqlite3.connect(path) as conn:
        conn.execute(
            "UPDATE strategy_current SET payload_json=?,payload_hash=? WHERE state_key='transition'",
            (body, digest),
        )
        conn.commit()

    with pytest.raises(RuntimeError, match="section hash mismatch: strategy"):
        transition.load_verified_checkpoint(path)


def test_legacy_v2_checkpoint_remains_readable(tmp_path):
    path = tmp_path / "legacy-v2.sqlite3"
    storage = ActiveStorage(path)
    storage.initialize(epoch_id="legacy-v2-test")
    current_truth = {section: {} for section in transition._SEMANTIC_SECTIONS}
    payload = {
        "checkpoint_id": "legacy-v2",
        "timestamp": "2026-09-15T00:00:00+00:00",
        "schema_version": 2,
        "migration_version": transition.LEGACY_TRANSITION_MIGRATION_VERSION,
        "release_sha": "release-a",
        **current_truth,
        "provenance": {"source": "legacy-test"},
    }
    payload["section_hashes"] = {
        section: payload_hash(payload[section]) for section in transition._SEMANTIC_SECTIONS
    }
    payload["semantic_hash"] = transition.semantic_hash(payload)

    verification = transition.persist_verified_checkpoint(
        storage,
        checkpoint_payload=payload,
        source_truth=current_truth,
    )
    assert verification.equivalent is True
    loaded = transition.load_verified_checkpoint(path)
    assert loaded["migration_version"] == 2
    assert loaded["checkpoint_id"] == "legacy-v2"


def test_explicit_release_mismatch_remains_fail_closed(tmp_path, monkeypatch):
    path, _ = _verified_checkpoint(tmp_path, release_sha="release-a")
    _clear_runtime_release_environment(monkeypatch)

    with pytest.raises(RuntimeError, match="release SHA does not match running release"):
        transition.load_verified_checkpoint(path, expected_release_sha="release-b")


def test_normal_active_restart_accepts_verified_checkpoint_from_prior_release(tmp_path, monkeypatch):
    path, original = _verified_checkpoint(tmp_path, release_sha="release-a")
    _clear_runtime_release_environment(monkeypatch)
    monkeypatch.setenv(transition.ACTIVATE_ENV, "1")
    monkeypatch.setenv(transition.ACTIVE_PATH_ENV, str(path))
    monkeypatch.setenv(transition.FINALIZE_ENV, "0")
    monkeypatch.setenv("RENDER_GIT_COMMIT", "release-b")

    selected = transition.select_runtime_database_from_environment(expected_release_sha="release-b")

    assert selected == path
    persisted = transition.load_verified_checkpoint(path)
    assert persisted["checkpoint_id"] == original["checkpoint_id"]
    assert persisted["release_sha"] == "release-a"


def test_rollforward_requires_expected_sha_to_equal_current_runtime_sha(tmp_path, monkeypatch):
    path, _ = _verified_checkpoint(tmp_path, release_sha="release-a")
    _clear_runtime_release_environment(monkeypatch)
    monkeypatch.setenv(transition.ACTIVATE_ENV, "1")
    monkeypatch.setenv(transition.ACTIVE_PATH_ENV, str(path))
    monkeypatch.setenv(transition.FINALIZE_ENV, "0")
    monkeypatch.setenv("RENDER_GIT_COMMIT", "release-b")

    with pytest.raises(RuntimeError, match="release SHA does not match running release"):
        transition.select_runtime_database_from_environment(expected_release_sha="release-c")


def test_finalization_keeps_same_release_checkpoint_gate(tmp_path, monkeypatch):
    path, _ = _verified_checkpoint(tmp_path, release_sha="release-a")
    _clear_runtime_release_environment(monkeypatch)
    monkeypatch.setenv(transition.ACTIVATE_ENV, "1")
    monkeypatch.setenv(transition.ACTIVE_PATH_ENV, str(path))
    monkeypatch.setenv(transition.FINALIZE_ENV, "1")
    monkeypatch.setenv("RENDER_GIT_COMMIT", "release-b")

    with pytest.raises(RuntimeError, match="release SHA does not match running release"):
        transition.select_runtime_database_from_environment(expected_release_sha="release-b")
