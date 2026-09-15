from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from .active_storage import payload_hash
from .active_storage_epoch_rollover import (
    SEALED_EPOCH_DATASET,
    SEALED_EPOCH_DIR,
    _source_certification_release_commit,
)
from .storage_current_state_extractor import LegacyCurrentStateExtractor
from .storage_file_retention import file_contract_for
from .storage_retention import RetentionClass
from .storage_transition import load_verified_checkpoint


class SealedEpochReclamationBlocked(RuntimeError):
    """Raised only when destructive eligibility cannot be proven exactly."""


@contextmanager
def _source_release_environment(release_commit: str) -> Iterator[None]:
    prior = os.environ.get("SOLANA_ROI_RELEASE_COMMIT")
    os.environ["SOLANA_ROI_RELEASE_COMMIT"] = str(release_commit)
    try:
        yield
    finally:
        if prior is None:
            os.environ.pop("SOLANA_ROI_RELEASE_COMMIT", None)
        else:
            os.environ["SOLANA_ROI_RELEASE_COMMIT"] = prior


def _sealed_candidates(active_path: Path) -> tuple[Path, ...]:
    root = active_path.parent / SEALED_EPOCH_DIR
    if not root.is_dir():
        return ()
    return tuple(sorted(root.glob(f"sealed-*/{active_path.name}")))


def _candidate_proof(
    candidate: Path,
    *,
    active_path: Path,
    checkpoint: dict[str, Any],
    expected_source_size: int,
    expected_schema_fingerprint: str,
) -> dict[str, Any]:
    root = (active_path.parent / SEALED_EPOCH_DIR).resolve()
    if candidate.is_symlink():
        return {"path": str(candidate), "eligible": False, "blocker": "sealed_candidate_is_symlink"}
    try:
        resolved = candidate.resolve(strict=True)
        resolved.relative_to(root)
    except (FileNotFoundError, ValueError):
        return {"path": str(candidate), "eligible": False, "blocker": "sealed_candidate_outside_root_or_missing"}

    active_stat = active_path.stat()
    sealed_stat = resolved.stat()
    if sealed_stat.st_dev != active_stat.st_dev:
        return {"path": str(candidate), "eligible": False, "blocker": "sealed_candidate_not_same_filesystem"}
    if sealed_stat.st_ino == active_stat.st_ino:
        return {"path": str(candidate), "eligible": False, "blocker": "sealed_candidate_is_active_inode"}
    if sealed_stat.st_nlink != 1:
        return {
            "path": str(candidate),
            "eligible": False,
            "blocker": "sealed_candidate_has_additional_hardlinks",
            "link_count": int(sealed_stat.st_nlink),
        }
    if int(sealed_stat.st_size) != int(expected_source_size):
        return {
            "path": str(candidate),
            "eligible": False,
            "blocker": "sealed_candidate_size_mismatch",
            "size_bytes": int(sealed_stat.st_size),
            "expected_size_bytes": int(expected_source_size),
        }

    source_release = _source_certification_release_commit(resolved)
    with _source_release_environment(source_release):
        extraction = LegacyCurrentStateExtractor(resolved).extract()
    observed_semantic_hash = payload_hash(extraction.truth)
    expected_semantic_hash = str(checkpoint.get("semantic_hash") or "")
    if int(extraction.source_size_bytes) != int(expected_source_size):
        return {"path": str(candidate), "eligible": False, "blocker": "extracted_source_size_mismatch"}
    if str(extraction.schema_fingerprint) != expected_schema_fingerprint:
        return {"path": str(candidate), "eligible": False, "blocker": "sealed_candidate_schema_fingerprint_mismatch"}
    if observed_semantic_hash != expected_semantic_hash:
        return {"path": str(candidate), "eligible": False, "blocker": "sealed_candidate_semantic_hash_mismatch"}

    return {
        "path": str(resolved),
        "eligible": True,
        "source_release_commit": source_release,
        "device": int(sealed_stat.st_dev),
        "inode": int(sealed_stat.st_ino),
        "link_count": int(sealed_stat.st_nlink),
        "size_bytes": int(sealed_stat.st_size),
        "allocated_bytes": int(getattr(sealed_stat, "st_blocks", 0)) * 512,
        "schema_fingerprint": str(extraction.schema_fingerprint),
        "semantic_hash": observed_semantic_hash,
    }


def preflight_sealed_epoch_reclamation(
    active_path: Path | str,
    *,
    expected_release_sha: str | None = None,
    approved_checkpoint_id: str | None = None,
) -> dict[str, Any]:
    """Prove one sealed predecessor is reclaimable without mutating storage.

    This function deliberately does not unlink, truncate, vacuum, checkpoint or
    rewrite any database.  It only becomes a future destructive authorization input
    after the production disk-ownership/full-runtime handshake is independently
    satisfied and an operator approval is bound to the exact verified checkpoint.
    """

    active = Path(active_path)
    if not active.is_file():
        raise SealedEpochReclamationBlocked(f"active storage unavailable:{active}")

    contract = file_contract_for(SEALED_EPOCH_DATASET)
    if contract.retention_class is not RetentionClass.LEGACY_UNCLASSIFIED:
        raise SealedEpochReclamationBlocked("sealed epoch retention classification changed unexpectedly")
    if contract.startup_access or contract.certification_access:
        raise SealedEpochReclamationBlocked("sealed epoch remains a runtime or certification dependency")

    checkpoint = load_verified_checkpoint(active, expected_release_sha=expected_release_sha)
    checkpoint_id = str(checkpoint.get("checkpoint_id") or "")
    if not checkpoint_id:
        raise SealedEpochReclamationBlocked("verified active checkpoint id missing")
    if approved_checkpoint_id is not None and str(approved_checkpoint_id) != checkpoint_id:
        raise SealedEpochReclamationBlocked("operator approval is not bound to the current verified checkpoint")

    provenance = checkpoint.get("provenance")
    if not isinstance(provenance, dict):
        raise SealedEpochReclamationBlocked("verified active checkpoint provenance missing")
    raw_legacy_path = str(provenance.get("legacy_path") or "")
    try:
        provenance_path = Path(raw_legacy_path).resolve(strict=False)
    except OSError as exc:
        raise SealedEpochReclamationBlocked("verified predecessor path is invalid") from exc
    if provenance_path != active.resolve():
        raise SealedEpochReclamationBlocked("verified checkpoint predecessor path does not match canonical active path")
    try:
        expected_source_size = int(provenance["legacy_size_bytes"])
    except (KeyError, TypeError, ValueError) as exc:
        raise SealedEpochReclamationBlocked("verified predecessor size missing") from exc
    expected_schema_fingerprint = str(provenance.get("legacy_schema_fingerprint") or "")
    if expected_source_size <= 0 or not expected_schema_fingerprint:
        raise SealedEpochReclamationBlocked("verified predecessor provenance is incomplete")

    candidates = _sealed_candidates(active)
    proofs: list[dict[str, Any]] = []
    for candidate in candidates:
        try:
            proof = _candidate_proof(
                candidate,
                active_path=active,
                checkpoint=checkpoint,
                expected_source_size=expected_source_size,
                expected_schema_fingerprint=expected_schema_fingerprint,
            )
        except Exception as exc:
            proof = {
                "path": str(candidate),
                "eligible": False,
                "blocker": f"candidate_preflight_error:{type(exc).__name__}:{exc}",
            }
        proofs.append(proof)

    eligible = [proof for proof in proofs if bool(proof.get("eligible"))]
    blockers: list[str] = []
    if not candidates:
        blockers.append("no_sealed_predecessor_present")
    if len(eligible) == 0 and candidates:
        blockers.append("no_exact_sealed_predecessor_match")
    if len(eligible) > 1:
        blockers.append("multiple_exact_sealed_predecessor_matches")

    reclaimable = len(eligible) == 1 and not blockers
    return {
        "status": "reclaimable" if reclaimable else "blocked",
        "reclaimable": reclaimable,
        "active_path": str(active.resolve()),
        "checkpoint_id": checkpoint_id,
        "checkpoint_release_sha": str(checkpoint.get("release_sha") or ""),
        "semantic_hash": str(checkpoint.get("semantic_hash") or ""),
        "operator_approval_bound": approved_checkpoint_id is not None,
        "retention_class": contract.retention_class.value,
        "startup_dependency": bool(contract.startup_access),
        "certification_dependency": bool(contract.certification_access),
        "candidate_count": len(candidates),
        "eligible_candidate_count": len(eligible),
        "eligible_candidate": eligible[0] if len(eligible) == 1 else None,
        "candidate_proofs": proofs,
        "blockers": blockers,
        "read_only": True,
        "sealed_source_deleted": False,
        "paper_only": True,
        "live_money_authority": False,
    }


__all__ = [
    "SealedEpochReclamationBlocked",
    "preflight_sealed_epoch_reclamation",
]
