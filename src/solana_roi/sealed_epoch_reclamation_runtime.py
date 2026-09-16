from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from . import production_data_cleanup_v4 as cleanup
from . import production_disk_ownership as disk_ownership
from .active_storage_epoch_rollover import MAX_UNRECLAIMED_SEALED_EPOCHS, SEALED_EPOCH_DIR
from .sealed_epoch_reclamation import execute_sealed_epoch_reclamation
from .storage_transition import load_verified_checkpoint

ENABLED_ENV = "SOLANA_ROI_SEALED_EPOCH_RECLAMATION_ENABLED"
CHECKPOINT_APPROVAL_ENV = "SOLANA_ROI_SEALED_EPOCH_RECLAMATION_CHECKPOINT_ID"
RUNTIME_VERSION = "sealed-epoch-reclamation-runtime-v1"


def enabled() -> bool:
    return os.getenv(ENABLED_ENV, "").strip().lower() in {"1", "true", "yes", "on"}


def _candidate_metadata(database_path: Path) -> list[dict[str, Any]]:
    root = database_path.parent / SEALED_EPOCH_DIR
    if not root.is_dir():
        return []
    result: list[dict[str, Any]] = []
    for candidate in sorted(root.glob(f"sealed-*/{database_path.name}")):
        item: dict[str, Any] = {"path": str(candidate), "is_symlink": candidate.is_symlink()}
        try:
            stat = candidate.stat()
        except OSError as exc:
            item.update({"stat_ok": False, "error_type": type(exc).__name__, "error": str(exc)[:300]})
        else:
            item.update(
                {
                    "stat_ok": True,
                    "device": int(stat.st_dev),
                    "inode": int(stat.st_ino),
                    "link_count": int(stat.st_nlink),
                    "size_bytes": int(stat.st_size),
                    "allocated_bytes": int(getattr(stat, "st_blocks", 0)) * 512,
                }
            )
        result.append(item)
    return result


def observe_boundary(database_path: Path | str) -> dict[str, Any]:
    """Cheap post-full-runtime observation; does not re-read sealed SQLite contents."""
    database_path = Path(database_path)
    release = disk_ownership.current_release_commit()
    if not release:
        raise RuntimeError("release commit unavailable for sealed epoch observation")
    checkpoint = load_verified_checkpoint(database_path, expected_release_sha=release)
    checkpoint_id = str(checkpoint.get("checkpoint_id") or "")
    if not checkpoint_id:
        raise RuntimeError("verified active checkpoint id missing for sealed epoch observation")
    candidates = _candidate_metadata(database_path)
    return {
        "runtime_version": RUNTIME_VERSION,
        "enabled": False,
        "status": "candidate_present" if candidates else "no_candidate",
        "active_path": str(database_path.resolve()),
        "release_sha": release,
        "checkpoint_id": checkpoint_id,
        "semantic_hash": str(checkpoint.get("semantic_hash") or ""),
        "candidate_count": len(candidates),
        "candidates": candidates,
        "semantic_reextraction_performed": False,
        "approval_environment": CHECKPOINT_APPROVAL_ENV,
        "read_only": True,
        "paper_only": True,
        "live_money_authority": False,
    }


def _startup_recovery_binding(database_path: Path, release: str) -> dict[str, Any]:
    """Validate the narrow pre-start recovery exception to same-release startup.

    This does not create or rewrite release identity. It accepts only a durable
    marker written by a prior full runtime for this exact canonical path, a
    checkpoint that verifies under the current ordinary release-rollforward
    rules, and an already-exceeded predecessor ceiling. Exact checkpoint
    approval and every candidate proof remain enforced by the executor.
    """

    marker = disk_ownership.read_establishment(database_path)
    if not isinstance(marker, dict):
        raise cleanup.CleanupBlocked(
            "sealed epoch startup recovery requires a prior full-runtime establishment marker"
        )
    if marker.get("protocol_version") != disk_ownership.LOCK_PROTOCOL_VERSION:
        raise cleanup.CleanupBlocked("sealed epoch startup recovery establishment protocol mismatch")
    if marker.get("database_path") != str(database_path):
        raise cleanup.CleanupBlocked("sealed epoch startup recovery database identity mismatch")
    prior_release = str(marker.get("release_commit") or "")
    if not prior_release or prior_release == release:
        raise cleanup.CleanupBlocked("sealed epoch startup recovery prior release binding is invalid")

    checkpoint = load_verified_checkpoint(database_path, expected_release_sha=release)
    checkpoint_id = str(checkpoint.get("checkpoint_id") or "")
    checkpoint_release = str(checkpoint.get("release_sha") or "")
    if not checkpoint_id or not checkpoint_release:
        raise cleanup.CleanupBlocked("sealed epoch startup recovery verified checkpoint binding missing")
    candidates = _candidate_metadata(database_path)
    physical_identities = {
        (int(item["device"]), int(item["inode"]))
        for item in candidates
        if bool(item.get("stat_ok"))
        and not bool(item.get("is_symlink"))
        and item.get("device") is not None
        and item.get("inode") is not None
    }
    if len(physical_identities) <= MAX_UNRECLAIMED_SEALED_EPOCHS:
        raise cleanup.CleanupBlocked(
            "sealed epoch startup recovery is allowed only after the predecessor ceiling is exceeded"
        )
    return {
        "prior_established_release_sha": prior_release,
        "checkpoint_id": checkpoint_id,
        "checkpoint_release_sha": checkpoint_release,
        "candidate_path_count_before": len(candidates),
        "candidate_count_before": len(physical_identities),
        "ceiling": MAX_UNRECLAIMED_SEALED_EPOCHS,
    }


def execute_enabled(database_path: Path | str, lease: disk_ownership.RuntimeDiskLease) -> dict[str, Any]:
    """Execute only behind the established same-release disk-ownership handshake."""
    database_path = Path(database_path)
    if not enabled():
        raise RuntimeError("sealed epoch reclamation executor called while disabled")
    role = os.getenv(cleanup.ROLE_ENV, "authoritative").strip().lower()
    if role != "authoritative":
        raise cleanup.CleanupBlocked("sealed epoch reclamation requires role=authoritative")
    if lease.handle is None or lease.database_path != database_path:
        raise cleanup.CleanupBlocked("sealed epoch reclamation requires the live canonical disk lease")
    release = disk_ownership.current_release_commit()
    if not release or lease.release_commit != release:
        raise cleanup.CleanupBlocked("sealed epoch reclamation lease/release identity mismatch")
    approved_checkpoint = os.getenv(CHECKPOINT_APPROVAL_ENV, "").strip()
    if not approved_checkpoint:
        raise cleanup.CleanupBlocked("sealed epoch reclamation enabled without exact checkpoint approval")

    same_release = disk_ownership.same_release_established(database_path)
    recovery_binding: dict[str, Any] | None = None
    if not same_release:
        recovery_binding = _startup_recovery_binding(database_path, release)
        if approved_checkpoint != recovery_binding["checkpoint_id"]:
            raise cleanup.CleanupBlocked(
                "sealed epoch startup recovery approval is not bound to the current verified checkpoint"
            )

    result = execute_sealed_epoch_reclamation(
        database_path,
        expected_release_sha=release,
        approved_checkpoint_id=approved_checkpoint,
    )
    payload = dict(result)
    payload.update(
        {
            "runtime_version": RUNTIME_VERSION,
            "enabled": True,
            "release_sha": release,
            "same_release_established": same_release,
            "startup_recovery": recovery_binding is not None,
            "startup_recovery_binding": recovery_binding,
            "disk_lease_owned": True,
            "approval_environment": CHECKPOINT_APPROVAL_ENV,
            "paper_only": True,
            "live_money_authority": False,
        }
    )
    return payload


__all__ = [
    "CHECKPOINT_APPROVAL_ENV",
    "ENABLED_ENV",
    "RUNTIME_VERSION",
    "enabled",
    "execute_enabled",
    "observe_boundary",
]
