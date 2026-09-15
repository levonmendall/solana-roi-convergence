from __future__ import annotations

import json
import os
from collections import namedtuple
from pathlib import Path
from types import SimpleNamespace

import pytest

import solana_roi.sealed_epoch_reclamation as reclamation
import solana_roi.sealed_epoch_reclamation_runtime as runtime


RELEASE_SHA = "2" * 40
CHECKPOINT_ID = "checkpoint-approved"
DiskUsage = namedtuple("DiskUsage", "total used free")


def _checkpoint() -> dict:
    return {
        "checkpoint_id": CHECKPOINT_ID,
        "release_sha": RELEASE_SHA,
        "semantic_hash": "a" * 64,
    }


def _files(tmp_path: Path) -> tuple[Path, Path]:
    active = tmp_path / "active.sqlite3"
    active.write_bytes(b"active-successor")
    sealed_dir = tmp_path / "active-epochs" / "sealed-proof"
    sealed_dir.mkdir(parents=True)
    sealed = sealed_dir / active.name
    sealed.write_bytes(b"sealed-predecessor")
    return active, sealed


def _proof(active: Path, sealed: Path) -> dict:
    stat = sealed.stat()
    candidate = {
        "path": str(sealed.resolve()),
        "eligible": True,
        "source_release_commit": "1" * 40,
        "device": int(stat.st_dev),
        "inode": int(stat.st_ino),
        "link_count": 1,
        "size_bytes": int(stat.st_size),
        "allocated_bytes": int(getattr(stat, "st_blocks", 0)) * 512,
        "schema_fingerprint": "schema-proof",
        "semantic_hash": "a" * 64,
    }
    return {
        "status": "reclaimable",
        "reclaimable": True,
        "active_path": str(active.resolve()),
        "checkpoint_id": CHECKPOINT_ID,
        "checkpoint_release_sha": RELEASE_SHA,
        "semantic_hash": "a" * 64,
        "eligible_candidate": candidate,
        "blockers": [],
    }


def test_executor_unlinks_only_exact_candidate_and_is_idempotent(tmp_path, monkeypatch):
    active, sealed = _files(tmp_path)
    monkeypatch.setattr(reclamation, "load_verified_checkpoint", lambda *_args, **_kwargs: _checkpoint())
    monkeypatch.setattr(
        reclamation,
        "preflight_sealed_epoch_reclamation",
        lambda *_args, **_kwargs: _proof(active, sealed),
    )
    usage = iter(
        [
            DiskUsage(total=10_000, used=8_000, free=2_000),
            DiskUsage(total=10_000, used=7_000, free=3_000),
        ]
    )
    monkeypatch.setattr(reclamation.shutil, "disk_usage", lambda _path: next(usage))

    result = reclamation.execute_sealed_epoch_reclamation(
        active,
        expected_release_sha=RELEASE_SHA,
        approved_checkpoint_id=CHECKPOINT_ID,
    )

    assert result["status"] == "complete"
    assert result["sealed_source_deleted"] is True
    assert result["idempotent_replay"] is False
    assert result["filesystem_free_bytes_delta"] == 1_000
    assert not sealed.exists()
    assert active.exists()

    receipt_path = Path(str(active) + reclamation.RECLAMATION_RECEIPT_SUFFIX)
    assert receipt_path.is_file()
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    assert receipt["checkpoint_id"] == CHECKPOINT_ID
    assert receipt["candidate_inode"] == result["candidate_inode"]
    assert receipt["status"] == "complete"

    replay = reclamation.execute_sealed_epoch_reclamation(
        active,
        expected_release_sha=RELEASE_SHA,
        approved_checkpoint_id=CHECKPOINT_ID,
    )
    assert replay["status"] == "complete"
    assert replay["idempotent_replay"] is True
    assert not sealed.exists()


def test_executor_recovers_intent_when_unlink_completed_before_finalize(tmp_path, monkeypatch):
    active, sealed = _files(tmp_path)
    stat = sealed.stat()
    monkeypatch.setattr(reclamation, "load_verified_checkpoint", lambda *_args, **_kwargs: _checkpoint())
    intent = {
        "status": "intent",
        "created_at": "2026-09-15T00:00:00+00:00",
        "active_path": str(active.resolve()),
        "checkpoint_id": CHECKPOINT_ID,
        "checkpoint_release_sha": RELEASE_SHA,
        "semantic_hash": "a" * 64,
        "candidate_path": str(sealed.resolve()),
        "candidate_device": int(stat.st_dev),
        "candidate_inode": int(stat.st_ino),
        "candidate_size_bytes": int(stat.st_size),
        "candidate_allocated_bytes": int(getattr(stat, "st_blocks", 0)) * 512,
        "source_release_commit": "1" * 40,
    }
    reclamation._write_receipt(active, intent)
    sealed.unlink()

    result = reclamation.execute_sealed_epoch_reclamation(
        active,
        expected_release_sha=RELEASE_SHA,
        approved_checkpoint_id=CHECKPOINT_ID,
    )

    assert result["status"] == "complete"
    assert result["recovered_after_interrupted_finalize"] is True
    assert result["sealed_source_deleted"] is True
    assert not sealed.exists()


def test_executor_blocks_if_candidate_identity_changes_after_preflight(tmp_path, monkeypatch):
    active, sealed = _files(tmp_path)
    proof = _proof(active, sealed)
    monkeypatch.setattr(reclamation, "load_verified_checkpoint", lambda *_args, **_kwargs: _checkpoint())
    monkeypatch.setattr(
        reclamation,
        "preflight_sealed_epoch_reclamation",
        lambda *_args, **_kwargs: proof,
    )
    sealed.unlink()
    sealed.write_bytes(b"replacement-predecessor")

    with pytest.raises(
        reclamation.SealedEpochReclamationBlocked,
        match="identity changed after preflight",
    ):
        reclamation.execute_sealed_epoch_reclamation(
            active,
            expected_release_sha=RELEASE_SHA,
            approved_checkpoint_id=CHECKPOINT_ID,
        )

    assert sealed.exists()
    assert active.exists()


def test_runtime_executor_requires_same_release_establishment(tmp_path, monkeypatch):
    active, _sealed = _files(tmp_path)
    monkeypatch.setenv(runtime.ENABLED_ENV, "true")
    monkeypatch.setenv(runtime.CHECKPOINT_APPROVAL_ENV, CHECKPOINT_ID)
    monkeypatch.setenv("SOLANA_ROI_PRODUCTION_CLEANUP_ROLE", "authoritative")
    monkeypatch.setattr(runtime.disk_ownership, "current_release_commit", lambda: RELEASE_SHA)
    monkeypatch.setattr(runtime.disk_ownership, "same_release_established", lambda _path: False)
    lease = SimpleNamespace(handle=object(), database_path=active, release_commit=RELEASE_SHA)

    with pytest.raises(runtime.cleanup.CleanupBlocked, match="exact release SHA"):
        runtime.execute_enabled(active, lease)

    assert active.exists()


def test_runtime_executor_forwards_exact_checkpoint_only_after_handshake(tmp_path, monkeypatch):
    active, sealed = _files(tmp_path)
    monkeypatch.setenv(runtime.ENABLED_ENV, "true")
    monkeypatch.setenv(runtime.CHECKPOINT_APPROVAL_ENV, CHECKPOINT_ID)
    monkeypatch.setenv("SOLANA_ROI_PRODUCTION_CLEANUP_ROLE", "authoritative")
    monkeypatch.setattr(runtime.disk_ownership, "current_release_commit", lambda: RELEASE_SHA)
    monkeypatch.setattr(runtime.disk_ownership, "same_release_established", lambda _path: True)
    observed: dict[str, object] = {}

    def fake_execute(path, *, expected_release_sha, approved_checkpoint_id):
        observed.update(
            {
                "path": Path(path),
                "release": expected_release_sha,
                "checkpoint": approved_checkpoint_id,
            }
        )
        return {
            "status": "complete",
            "checkpoint_id": CHECKPOINT_ID,
            "candidate_path": str(sealed),
            "sealed_source_deleted": True,
        }

    monkeypatch.setattr(runtime, "execute_sealed_epoch_reclamation", fake_execute)
    lease = SimpleNamespace(handle=object(), database_path=active, release_commit=RELEASE_SHA)

    result = runtime.execute_enabled(active, lease)

    assert observed == {
        "path": active,
        "release": RELEASE_SHA,
        "checkpoint": CHECKPOINT_ID,
    }
    assert result["same_release_established"] is True
    assert result["disk_lease_owned"] is True
    assert result["sealed_source_deleted"] is True


def test_observation_is_checkpoint_and_file_metadata_only(tmp_path, monkeypatch):
    active, sealed = _files(tmp_path)
    monkeypatch.setattr(runtime.disk_ownership, "current_release_commit", lambda: RELEASE_SHA)
    monkeypatch.setattr(runtime, "load_verified_checkpoint", lambda *_args, **_kwargs: _checkpoint())

    result = runtime.observe_boundary(active)

    assert result["status"] == "candidate_present"
    assert result["checkpoint_id"] == CHECKPOINT_ID
    assert result["candidate_count"] == 1
    assert result["candidates"][0]["path"] == str(sealed)
    assert result["semantic_reextraction_performed"] is False
    assert result["read_only"] is True
    assert sealed.exists()
    assert active.exists()
