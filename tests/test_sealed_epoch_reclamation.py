from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace

import pytest

import solana_roi.sealed_epoch_reclamation as reclamation
from solana_roi.active_storage import payload_hash


CHECKPOINT_ID = "storage-transition-proof"
SOURCE_RELEASE_SHA = "1" * 40
TARGET_RELEASE_SHA = "2" * 40
SCHEMA_FINGERPRINT = "schema-proof"
TRUTH = {"proof": {"value": 1}}


def _paths(tmp_path: Path) -> tuple[Path, Path]:
    active = tmp_path / "active.sqlite3"
    active.write_bytes(b"active-successor")
    sealed_dir = tmp_path / "active-epochs" / "sealed-proof"
    sealed_dir.mkdir(parents=True)
    sealed = sealed_dir / active.name
    sealed.write_bytes(b"sealed-source")
    return active, sealed


def _checkpoint(active: Path, sealed: Path, *, semantic_hash: str | None = None) -> dict:
    return {
        "checkpoint_id": CHECKPOINT_ID,
        "release_sha": TARGET_RELEASE_SHA,
        "semantic_hash": semantic_hash or payload_hash(TRUTH),
        "provenance": {
            "legacy_path": str(active),
            "legacy_size_bytes": sealed.stat().st_size,
            "legacy_schema_fingerprint": SCHEMA_FINGERPRINT,
        },
    }


def _install_exact_source(monkeypatch, sealed: Path, *, truth=TRUTH) -> None:
    monkeypatch.setattr(
        reclamation,
        "_source_certification_release_commit",
        lambda path: SOURCE_RELEASE_SHA if Path(path) == sealed.resolve() else (_ for _ in ()).throw(AssertionError(path)),
    )

    class FakeExtractor:
        def __init__(self, path):
            assert Path(path) == sealed.resolve()

        def extract(self):
            assert os.environ["SOLANA_ROI_RELEASE_COMMIT"] == SOURCE_RELEASE_SHA
            return SimpleNamespace(
                source_size_bytes=sealed.stat().st_size,
                schema_fingerprint=SCHEMA_FINGERPRINT,
                truth=truth,
            )

    monkeypatch.setattr(reclamation, "LegacyCurrentStateExtractor", FakeExtractor)


def test_exact_sealed_predecessor_is_reclaimable_read_only(tmp_path, monkeypatch):
    active, sealed = _paths(tmp_path)
    checkpoint = _checkpoint(active, sealed)
    monkeypatch.setattr(reclamation, "load_verified_checkpoint", lambda *_args, **_kwargs: checkpoint)
    _install_exact_source(monkeypatch, sealed)
    monkeypatch.setenv("SOLANA_ROI_RELEASE_COMMIT", TARGET_RELEASE_SHA)

    result = reclamation.preflight_sealed_epoch_reclamation(
        active,
        expected_release_sha=TARGET_RELEASE_SHA,
        approved_checkpoint_id=CHECKPOINT_ID,
    )

    assert result["status"] == "reclaimable"
    assert result["reclaimable"] is True
    assert result["operator_approval_bound"] is True
    assert result["candidate_count"] == 1
    assert result["eligible_candidate_count"] == 1
    assert result["eligible_candidate"]["source_release_commit"] == SOURCE_RELEASE_SHA
    assert result["eligible_candidate"]["semantic_hash"] == payload_hash(TRUTH)
    assert result["read_only"] is True
    assert result["sealed_source_deleted"] is False
    assert sealed.exists()
    assert active.exists()
    assert os.environ["SOLANA_ROI_RELEASE_COMMIT"] == TARGET_RELEASE_SHA


def test_operator_approval_must_match_current_verified_checkpoint(tmp_path, monkeypatch):
    active, sealed = _paths(tmp_path)
    checkpoint = _checkpoint(active, sealed)
    monkeypatch.setattr(reclamation, "load_verified_checkpoint", lambda *_args, **_kwargs: checkpoint)

    with pytest.raises(
        reclamation.SealedEpochReclamationBlocked,
        match="operator approval is not bound to the current verified checkpoint",
    ):
        reclamation.preflight_sealed_epoch_reclamation(
            active,
            approved_checkpoint_id="different-checkpoint",
        )

    assert sealed.exists()


def test_semantic_mismatch_blocks_without_deleting_predecessor(tmp_path, monkeypatch):
    active, sealed = _paths(tmp_path)
    checkpoint = _checkpoint(active, sealed)
    monkeypatch.setattr(reclamation, "load_verified_checkpoint", lambda *_args, **_kwargs: checkpoint)
    _install_exact_source(monkeypatch, sealed, truth={"proof": {"value": 999}})

    result = reclamation.preflight_sealed_epoch_reclamation(
        active,
        approved_checkpoint_id=CHECKPOINT_ID,
    )

    assert result["reclaimable"] is False
    assert result["eligible_candidate_count"] == 0
    assert "no_exact_sealed_predecessor_match" in result["blockers"]
    assert result["candidate_proofs"][0]["blocker"] == "sealed_candidate_semantic_hash_mismatch"
    assert sealed.exists()


def test_additional_hardlink_blocks_physical_reclamation(tmp_path, monkeypatch):
    active, sealed = _paths(tmp_path)
    extra = tmp_path / "unexpected-second-link.sqlite3"
    os.link(sealed, extra)
    checkpoint = _checkpoint(active, sealed)
    monkeypatch.setattr(reclamation, "load_verified_checkpoint", lambda *_args, **_kwargs: checkpoint)

    result = reclamation.preflight_sealed_epoch_reclamation(
        active,
        approved_checkpoint_id=CHECKPOINT_ID,
    )

    assert result["reclaimable"] is False
    assert result["candidate_proofs"][0]["blocker"] == "sealed_candidate_has_additional_hardlinks"
    assert result["candidate_proofs"][0]["link_count"] == 2
    assert sealed.exists()
    assert extra.exists()


def test_missing_sealed_predecessor_is_non_destructive_not_applicable(tmp_path, monkeypatch):
    active = tmp_path / "active.sqlite3"
    active.write_bytes(b"active-successor")
    checkpoint = {
        "checkpoint_id": CHECKPOINT_ID,
        "release_sha": TARGET_RELEASE_SHA,
        "semantic_hash": payload_hash(TRUTH),
        "provenance": {
            "legacy_path": str(active),
            "legacy_size_bytes": 123,
            "legacy_schema_fingerprint": SCHEMA_FINGERPRINT,
        },
    }
    monkeypatch.setattr(reclamation, "load_verified_checkpoint", lambda *_args, **_kwargs: checkpoint)

    result = reclamation.preflight_sealed_epoch_reclamation(
        active,
        approved_checkpoint_id=CHECKPOINT_ID,
    )

    assert result["reclaimable"] is False
    assert result["candidate_count"] == 0
    assert result["blockers"] == ["no_sealed_predecessor_present"]
    assert result["read_only"] is True
