from __future__ import annotations

import pytest

from solana_roi.active_storage import ActiveStorage
from solana_roi import storage_transition as transition


def _verified_checkpoint(tmp_path, *, release_sha: str):
    path = tmp_path / "active.sqlite3"
    storage = ActiveStorage(path)
    storage.initialize(epoch_id="cross-release-test")
    current_truth = {section: {} for section in transition._SEMANTIC_SECTIONS}
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
