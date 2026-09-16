from __future__ import annotations

import json
import os
import shutil
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from .active_storage import payload_hash
from .active_storage_epoch_rollover import (
    SEALED_EPOCH_DATASET,
    SEALED_EPOCH_DIR,
    _source_certification_release_commit,
)
from .storage_current_state_extractor import LegacyCurrentStateExtractor
from .storage_file_retention import assert_persistent_file_registered, file_contract_for
from .storage_retention import RetentionClass
from .storage_transition import load_verified_checkpoint

RECLAMATION_RECEIPT_DATASET = "sealed_epoch_reclamation_receipt"
RECLAMATION_RECEIPT_SUFFIX = ".sealed-epoch-reclamation.json"
RECLAMATION_PROTOCOL_VERSION = "sealed-epoch-reclamation-v1"
LATE_REGISTERED_EVIDENCE_TABLES = (
    "candidate_execution_plane_snapshots",
    "v51_release_compatibility",
    "v52_tournament_exact_evidence",
)
_SEMANTIC_SECTION_NAMES = {
    "strategy", "wallet", "wallet_evidence_watermarks", "provider_source",
    "freshness", "latest_event_ids", "active_candidates", "active_lifecycles",
    "portfolio", "replication_watermarks", "certification", "continuity",
}


class SealedEpochReclamationBlocked(RuntimeError):
    """Raised only when destructive eligibility cannot be proven exactly."""


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _fsync_dir(path: Path) -> None:
    fd = os.open(str(path), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


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


def _project_to_checkpoint_shape(observed: Any, expected: Any) -> Any:
    """Ignore fields unknown to an older receipt without ignoring known fields."""
    if isinstance(expected, dict) and isinstance(observed, dict):
        return {
            key: _project_to_checkpoint_shape(observed.get(key), value)
            for key, value in expected.items()
        }
    return observed


def _late_evidence_coverage(active: Path, candidate: Path) -> dict[str, Any]:
    """Prove evidence registered after old receipts is already in the survivor."""
    result: dict[str, Any] = {"covered": True, "tables": {}, "blockers": []}
    try:
        active_conn = sqlite3.connect(f"file:{active.resolve()}?mode=ro", uri=True)
        candidate_conn = sqlite3.connect(f"file:{candidate.resolve()}?mode=ro", uri=True)
    except sqlite3.Error as exc:
        return {
            "covered": False,
            "tables": {},
            "blockers": [f"late_evidence_database_unreadable:{type(exc).__name__}"],
        }
    try:
        active_tables = {
            str(row[0]) for row in active_conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        candidate_tables = {
            str(row[0]) for row in candidate_conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        for table in LATE_REGISTERED_EVIDENCE_TABLES:
            if table not in candidate_tables:
                result["tables"][table] = {"candidate_rows": 0, "covered_rows": 0}
                continue
            candidate_count = int(candidate_conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0])
            if not candidate_count:
                result["tables"][table] = {"candidate_rows": 0, "covered_rows": 0}
                continue
            if table not in active_tables:
                result["covered"] = False
                result["blockers"].append(f"late_evidence_missing_table:{table}")
                continue
            source_info = candidate_conn.execute(f'PRAGMA table_info("{table}")').fetchall()
            active_info = active_conn.execute(f'PRAGMA table_info("{table}")').fetchall()
            source_columns = [str(row[1]) for row in source_info]
            if source_columns != [str(row[1]) for row in active_info]:
                result["covered"] = False
                result["blockers"].append(f"late_evidence_schema_mismatch:{table}")
                continue
            primary = [str(row[1]) for row in sorted(source_info, key=lambda row: int(row[5])) if int(row[5])]
            if not primary:
                result["covered"] = False
                result["blockers"].append(f"late_evidence_primary_key_missing:{table}")
                continue
            where = " AND ".join(f'"{column}"=?' for column in primary)
            primary_indexes = [source_columns.index(column) for column in primary]
            covered = 0
            cursor = candidate_conn.execute(f'SELECT * FROM "{table}"')
            for source_row in cursor:
                key = tuple(source_row[index] for index in primary_indexes)
                surviving = active_conn.execute(
                    f'SELECT * FROM "{table}" WHERE {where}', key
                ).fetchone()
                if surviving is None or tuple(surviving) != tuple(source_row):
                    result["covered"] = False
                    result["blockers"].append(f"late_evidence_row_missing_or_changed:{table}")
                    break
                covered += 1
            result["tables"][table] = {
                "candidate_rows": candidate_count,
                "covered_rows": covered,
            }
    except sqlite3.Error as exc:
        result["covered"] = False
        result["blockers"].append(f"late_evidence_check_failed:{type(exc).__name__}")
    finally:
        active_conn.close()
        candidate_conn.close()
    return result


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
    expected_truth = {
        key: checkpoint.get(key)
        for key in _SEMANTIC_SECTION_NAMES
        if key in checkpoint
    }
    observed_truth = (
        _project_to_checkpoint_shape(extraction.truth, expected_truth)
        if expected_truth
        else extraction.truth
    )
    observed_semantic_hash = payload_hash(observed_truth)
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
    """Prove one sealed predecessor is reclaimable without mutating storage."""

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
    eligible_chain: list[dict[str, Any]] = []
    protected_blockers: list[str] = []
    if reclaimable:
        selected = dict(eligible[0])
        if any(section in checkpoint for section in _SEMANTIC_SECTION_NAMES):
            coverage = _late_evidence_coverage(active, Path(str(selected["path"])))
            selected["late_registered_evidence"] = coverage
            if not bool(coverage.get("covered")):
                protected_blockers.extend(str(value) for value in coverage.get("blockers") or ())
            else:
                eligible_chain.append(selected)
        else:
            # Small isolated tests and legacy-v2 embedded checkpoints already
            # bind their complete truth directly; there is no late table surface.
            eligible_chain.append(selected)

    remaining = {
        str(candidate.resolve()): candidate
        for candidate in candidates
        if not eligible_chain or candidate.resolve() != Path(str(eligible_chain[0]["path"])).resolve()
    }
    child_path = Path(str(eligible_chain[0]["path"])) if eligible_chain else None
    seen_identities = {
        (int(item.get("device", -1)), int(item.get("inode", -1))) for item in eligible_chain
    }
    while child_path is not None and remaining:
        try:
            child_checkpoint = load_verified_checkpoint(child_path)
            child_provenance = child_checkpoint.get("provenance")
            if not isinstance(child_provenance, dict):
                raise SealedEpochReclamationBlocked("sealed child checkpoint provenance missing")
            if Path(str(child_provenance.get("legacy_path") or "")).resolve(strict=False) != active.resolve():
                raise SealedEpochReclamationBlocked("sealed child predecessor path is not canonical")
            child_size = int(child_provenance["legacy_size_bytes"])
            child_fingerprint = str(child_provenance.get("legacy_schema_fingerprint") or "")
            if child_size <= 0 or not child_fingerprint:
                raise SealedEpochReclamationBlocked("sealed child predecessor provenance incomplete")
        except Exception as exc:
            protected_blockers.append(f"sealed_chain_checkpoint_unusable:{type(exc).__name__}:{exc}")
            break

        matches: list[dict[str, Any]] = []
        for key, candidate in tuple(remaining.items()):
            try:
                candidate_proof = _candidate_proof(
                    candidate,
                    active_path=active,
                    checkpoint=child_checkpoint,
                    expected_source_size=child_size,
                    expected_schema_fingerprint=child_fingerprint,
                )
            except Exception as exc:
                candidate_proof = {
                    "path": str(candidate),
                    "eligible": False,
                    "blocker": f"candidate_preflight_error:{type(exc).__name__}:{exc}",
                }
            if bool(candidate_proof.get("eligible")):
                matches.append(candidate_proof)
        if len(matches) != 1:
            protected_blockers.append(
                "sealed_chain_ambiguous" if len(matches) > 1 else "sealed_chain_predecessor_unproven"
            )
            break
        selected = dict(matches[0])
        identity = (int(selected.get("device", -1)), int(selected.get("inode", -1)))
        if identity in seen_identities:
            protected_blockers.append("sealed_chain_identity_cycle")
            break
        coverage = _late_evidence_coverage(active, Path(str(selected["path"])))
        selected["late_registered_evidence"] = coverage
        if not bool(coverage.get("covered")):
            protected_blockers.extend(str(value) for value in coverage.get("blockers") or ())
            break
        eligible_chain.append(selected)
        seen_identities.add(identity)
        remaining.pop(str(Path(str(selected["path"])).resolve()), None)
        child_path = Path(str(selected["path"]))

    reclaimable = bool(eligible_chain) and not blockers
    reclaimable_allocated = sum(int(item.get("allocated_bytes") or 0) for item in eligible_chain)
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
        "eligible_candidate": eligible_chain[0] if eligible_chain else None,
        "eligible_candidates": eligible_chain,
        "eligible_physical_bytes": reclaimable_allocated,
        "protected_candidate_count": max(0, len(candidates) - len(eligible_chain)),
        "protected_candidate_blockers": protected_blockers,
        "candidate_proofs": proofs,
        "blockers": blockers,
        "read_only": True,
        "sealed_source_deleted": False,
        "paper_only": True,
        "live_money_authority": False,
    }


def _receipt_path(active_path: Path) -> Path:
    return Path(str(active_path) + RECLAMATION_RECEIPT_SUFFIX)


def _read_receipt(active_path: Path) -> dict[str, Any] | None:
    path = _receipt_path(active_path)
    if not path.exists():
        return None
    if path.is_symlink() or not path.is_file():
        raise SealedEpochReclamationBlocked("reclamation receipt is not a regular file")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SealedEpochReclamationBlocked("reclamation receipt is unreadable") from exc
    if not isinstance(payload, dict) or payload.get("protocol_version") != RECLAMATION_PROTOCOL_VERSION:
        raise SealedEpochReclamationBlocked("reclamation receipt is invalid")
    return payload


def _write_receipt(active_path: Path, payload: dict[str, Any]) -> Path:
    assert_persistent_file_registered(RECLAMATION_RECEIPT_DATASET)
    path = _receipt_path(active_path)
    if path.exists() and path.is_symlink():
        raise SealedEpochReclamationBlocked("reclamation receipt must not be a symlink")
    temporary = path.with_name(path.name + f".tmp-{os.getpid()}")
    raw = dict(payload)
    raw["protocol_version"] = RECLAMATION_PROTOCOL_VERSION
    raw["retention_dataset"] = RECLAMATION_RECEIPT_DATASET
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(raw, handle, sort_keys=True, separators=(",", ":"))
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    _fsync_dir(path.parent)
    return path


def _identity_matches(candidate: Path, proof: dict[str, Any]) -> bool:
    try:
        stat = candidate.stat()
    except FileNotFoundError:
        return False
    return bool(
        int(stat.st_dev) == int(proof.get("device", -1))
        and int(stat.st_ino) == int(proof.get("inode", -1))
        and int(stat.st_nlink) == 1
        and int(stat.st_size) == int(proof.get("size_bytes", -1))
    )


def _receipt_candidates(receipt: dict[str, Any]) -> list[dict[str, Any]]:
    candidates = receipt.get("candidates")
    if isinstance(candidates, list) and all(isinstance(item, dict) for item in candidates):
        return [dict(item) for item in candidates]
    path = str(receipt.get("candidate_path") or "")
    if not path:
        return []
    return [{
        "path": path,
        "device": receipt.get("candidate_device"),
        "inode": receipt.get("candidate_inode"),
        "size_bytes": receipt.get("candidate_size_bytes"),
        "allocated_bytes": receipt.get("candidate_allocated_bytes"),
    }]


def execute_sealed_epoch_reclamation(
    active_path: Path | str,
    *,
    expected_release_sha: str,
    approved_checkpoint_id: str,
) -> dict[str, Any]:
    """Unlink exactly one proven predecessor with crash-safe intent/final receipt.

    Cross-process disk ownership and the same-release full-runtime handshake are
    deliberately enforced by the production cleanup installer before this function
    may be called.  This executor independently rechecks the active checkpoint and
    candidate identity immediately before unlinking.
    """

    assert_persistent_file_registered(RECLAMATION_RECEIPT_DATASET)
    active = Path(active_path)
    checkpoint = load_verified_checkpoint(active, expected_release_sha=expected_release_sha)
    checkpoint_id = str(checkpoint.get("checkpoint_id") or "")
    if not checkpoint_id or checkpoint_id != str(approved_checkpoint_id):
        raise SealedEpochReclamationBlocked("operator approval is not bound to the current verified checkpoint")

    receipt = _read_receipt(active)
    resume_intent: dict[str, Any] | None = None
    if receipt is not None:
        receipt_checkpoint = str(receipt.get("checkpoint_id") or "")
        receipt_status = str(receipt.get("status") or "")
        receipt_candidates = _receipt_candidates(receipt)
        present = [Path(str(item.get("path") or "")).exists() for item in receipt_candidates]
        if receipt_checkpoint == checkpoint_id and receipt_status == "complete":
            if any(present):
                raise SealedEpochReclamationBlocked("completed reclamation receipt points to a present predecessor")
            result = dict(receipt)
            result.update({"idempotent_replay": True, "sealed_source_deleted": True})
            return result
        if receipt_checkpoint == checkpoint_id and receipt_status == "intent":
            for item, exists in zip(receipt_candidates, present):
                candidate = Path(str(item.get("path") or ""))
                if exists and not _identity_matches(candidate, item):
                    raise SealedEpochReclamationBlocked(
                        "sealed predecessor identity changed while recovering reclamation intent"
                    )
            if receipt_candidates and not any(present):
                verified = load_verified_checkpoint(active, expected_release_sha=expected_release_sha)
                if str(verified.get("checkpoint_id") or "") != checkpoint_id:
                    raise SealedEpochReclamationBlocked("active checkpoint changed while recovering reclamation intent")
                completed = dict(receipt)
                free_after = int(shutil.disk_usage(active.parent).free)
                free_before = receipt.get("filesystem_free_bytes_before")
                completed.update(
                    {
                        "status": "complete",
                        "completed_at": _utcnow(),
                        "recovered_after_interrupted_finalize": True,
                        "sealed_source_deleted": True,
                        "deleted_candidate_paths": [
                            str(Path(str(item.get("path") or "")).resolve(strict=False))
                            for item in receipt_candidates
                        ],
                        "deleted_candidate_count": len(receipt_candidates),
                        "filesystem_free_bytes_after": free_after,
                        "filesystem_free_bytes_delta": (
                            free_after - int(free_before)
                            if free_before is not None
                            else None
                        ),
                        "paper_only": True,
                        "live_money_authority": False,
                    }
                )
                _write_receipt(active, completed)
                return completed
            if any(present):
                resume_intent = dict(receipt)
                deleted = {
                    str(Path(str(value)).resolve(strict=False))
                    for value in (resume_intent.get("deleted_candidate_paths") or ())
                }
                recovered_missing: list[str] = []
                for item, exists in zip(receipt_candidates, present):
                    if exists:
                        continue
                    path = str(Path(str(item.get("path") or "")).resolve(strict=False))
                    deleted.add(path)
                    recovered_missing.append(path)
                resume_intent["deleted_candidate_paths"] = sorted(deleted)
                if recovered_missing:
                    resume_intent["recovered_missing_candidate_paths"] = recovered_missing
        if receipt_status == "intent" and receipt_checkpoint != checkpoint_id:
            raise SealedEpochReclamationBlocked("unresolved reclamation intent belongs to a different checkpoint")

    if resume_intent is None:
        proof = preflight_sealed_epoch_reclamation(
            active,
            expected_release_sha=expected_release_sha,
            approved_checkpoint_id=approved_checkpoint_id,
        )
        if not bool(proof.get("reclaimable")):
            raise SealedEpochReclamationBlocked(
                "sealed predecessor is not exactly reclaimable:" + ",".join(proof.get("blockers") or ())
            )
        candidate_proofs = [
            dict(item) for item in (proof.get("eligible_candidates") or ()) if isinstance(item, dict)
        ]
        if not candidate_proofs:
            candidate_proofs = [dict(proof.get("eligible_candidate") or {})]
    else:
        candidate_proofs = [
            item for item in _receipt_candidates(resume_intent)
            if Path(str(item.get("path") or "")).exists()
        ]
    if not candidate_proofs or not candidate_proofs[0].get("path"):
        raise SealedEpochReclamationBlocked("reclamation proof contains no eligible predecessor")
    for candidate_proof in candidate_proofs:
        candidate = Path(str(candidate_proof.get("path") or ""))
        if not candidate.is_file() or candidate.is_symlink():
            raise SealedEpochReclamationBlocked("approved sealed predecessor is no longer a regular file")
        if not _identity_matches(candidate, candidate_proof):
            raise SealedEpochReclamationBlocked("approved sealed predecessor identity changed after preflight")

    first = candidate_proofs[0]
    first_candidate = Path(str(first["path"]))

    initial_free_bytes = int(shutil.disk_usage(active.parent).free)
    intent = dict(resume_intent) if resume_intent is not None else {
        "status": "intent",
        "created_at": _utcnow(),
        "active_path": str(active.resolve()),
        "checkpoint_id": checkpoint_id,
        "checkpoint_release_sha": str(checkpoint.get("release_sha") or ""),
        "semantic_hash": str(checkpoint.get("semantic_hash") or ""),
        "candidate_path": str(first_candidate.resolve()),
        "candidate_device": int(first["device"]),
        "candidate_inode": int(first["inode"]),
        "candidate_size_bytes": int(first["size_bytes"]),
        "candidate_allocated_bytes": int(first.get("allocated_bytes") or 0),
        "source_release_commit": first.get("source_release_commit"),
        "candidates": [
            {
                "path": str(Path(str(item["path"])).resolve()),
                "device": int(item["device"]),
                "inode": int(item["inode"]),
                "size_bytes": int(item["size_bytes"]),
                "allocated_bytes": int(item.get("allocated_bytes") or 0),
                "source_release_commit": item.get("source_release_commit"),
                "semantic_hash": item.get("semantic_hash"),
            }
            for item in candidate_proofs
        ],
        "candidate_count": len(candidate_proofs),
        "estimated_reclaimable_physical_bytes": sum(
            int(item.get("allocated_bytes") or 0) for item in candidate_proofs
        ),
        "deleted_candidate_paths": [],
        "filesystem_free_bytes_before": initial_free_bytes,
        "paper_only": True,
        "live_money_authority": False,
    }
    receipt_path = _write_receipt(active, intent)

    verified = load_verified_checkpoint(active, expected_release_sha=expected_release_sha)
    if str(verified.get("checkpoint_id") or "") != checkpoint_id:
        raise SealedEpochReclamationBlocked("active checkpoint changed after reclamation intent")
    for candidate_proof in candidate_proofs:
        candidate = Path(str(candidate_proof["path"]))
        if not _identity_matches(candidate, candidate_proof):
            raise SealedEpochReclamationBlocked("sealed predecessor identity changed after reclamation intent")

    free_before = int(intent.get("filesystem_free_bytes_before", initial_free_bytes))
    deleted_paths: list[str] = list(intent.get("deleted_candidate_paths") or ())
    for candidate_proof in candidate_proofs:
        candidate = Path(str(candidate_proof["path"]))
        if not _identity_matches(candidate, candidate_proof):
            raise SealedEpochReclamationBlocked("sealed predecessor identity changed before unlink")
        candidate.unlink()
        _fsync_dir(candidate.parent)
        if candidate.exists():
            raise SealedEpochReclamationBlocked("sealed predecessor remained present after unlink")
        deleted_paths.append(str(candidate.resolve(strict=False)))
        intent["deleted_candidate_paths"] = list(deleted_paths)
        _write_receipt(active, intent)
        verified_during = load_verified_checkpoint(active, expected_release_sha=expected_release_sha)
        if str(verified_during.get("checkpoint_id") or "") != checkpoint_id:
            raise SealedEpochReclamationBlocked("active checkpoint changed during sealed predecessor unlink")

    verified_after = load_verified_checkpoint(active, expected_release_sha=expected_release_sha)
    if str(verified_after.get("checkpoint_id") or "") != checkpoint_id:
        raise SealedEpochReclamationBlocked("active checkpoint changed after sealed predecessor unlink")
    free_after = int(shutil.disk_usage(active.parent).free)

    completed = dict(intent)
    completed.update(
        {
            "status": "complete",
            "completed_at": _utcnow(),
            "receipt_path": str(receipt_path),
            "filesystem_free_bytes_before": free_before,
            "filesystem_free_bytes_after": free_after,
            "filesystem_free_bytes_delta": free_after - free_before,
            "deleted_candidate_paths": deleted_paths,
            "deleted_candidate_count": len(deleted_paths),
            "idempotent_replay": False,
            "sealed_source_deleted": True,
        }
    )
    _write_receipt(active, completed)
    return completed


__all__ = [
    "RECLAMATION_PROTOCOL_VERSION",
    "RECLAMATION_RECEIPT_DATASET",
    "RECLAMATION_RECEIPT_SUFFIX",
    "SealedEpochReclamationBlocked",
    "execute_sealed_epoch_reclamation",
    "preflight_sealed_epoch_reclamation",
]
