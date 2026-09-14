from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from .active_storage import ACTIVE_SCHEMA_VERSION, ActiveStorage, canonical_json, payload_hash

TRANSITION_MIGRATION_VERSION = 1
ACTIVE_PATH_ENV = "SOLANA_ROI_ACTIVE_DB_PATH"
LEGACY_PATH_ENV = "SOLANA_ROI_DB_PATH"
LEGACY_RECORD_ENV = "SOLANA_ROI_LEGACY_DB_PATH"
ACTIVATE_ENV = "SOLANA_ROI_ACTIVE_STORAGE_ENABLED"

_REQUIRED_CHECKPOINT_FIELDS = {
    "checkpoint_id",
    "timestamp",
    "schema_version",
    "migration_version",
    "release_sha",
    "strategy",
    "wallet",
    "wallet_evidence_watermarks",
    "provider_source",
    "freshness",
    "latest_event_ids",
    "active_candidates",
    "active_lifecycles",
    "portfolio",
    "replication_watermarks",
    "certification",
    "continuity",
    "provenance",
}

_SEMANTIC_SECTIONS = (
    "strategy",
    "wallet",
    "wallet_evidence_watermarks",
    "provider_source",
    "freshness",
    "latest_event_ids",
    "active_candidates",
    "active_lifecycles",
    "portfolio",
    "replication_watermarks",
    "certification",
    "continuity",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


def semantic_projection(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {section: payload.get(section) for section in _SEMANTIC_SECTIONS}


def semantic_hash(payload: Mapping[str, Any]) -> str:
    return payload_hash(semantic_projection(payload))


@dataclass(frozen=True)
class CheckpointVerification:
    equivalent: bool
    expected_hash: str
    observed_hash: str
    mismatched_sections: tuple[str, ...]


def verify_semantic_equivalence(
    source_truth: Mapping[str, Any], checkpoint_payload: Mapping[str, Any]
) -> CheckpointVerification:
    source_projection = semantic_projection(source_truth)
    checkpoint_projection = semantic_projection(checkpoint_payload)
    mismatched = tuple(
        name
        for name in _SEMANTIC_SECTIONS
        if canonical_json(source_projection.get(name)) != canonical_json(checkpoint_projection.get(name))
    )
    expected = payload_hash(source_projection)
    observed = payload_hash(checkpoint_projection)
    return CheckpointVerification(
        equivalent=not mismatched and expected == observed,
        expected_hash=expected,
        observed_hash=observed,
        mismatched_sections=mismatched,
    )


def build_checkpoint_payload(*, release_sha: str, current_truth: Mapping[str, Any], provenance: Mapping[str, Any]) -> dict[str, Any]:
    missing = [section for section in _SEMANTIC_SECTIONS if section not in current_truth]
    if missing:
        raise ValueError("current truth missing checkpoint sections: " + ", ".join(missing))
    checkpoint_id = f"storage-transition-{uuid.uuid4().hex}"
    payload: dict[str, Any] = {
        "checkpoint_id": checkpoint_id,
        "timestamp": _utc_now(),
        "schema_version": ACTIVE_SCHEMA_VERSION,
        "migration_version": TRANSITION_MIGRATION_VERSION,
        "release_sha": str(release_sha),
        **{section: current_truth[section] for section in _SEMANTIC_SECTIONS},
        "provenance": dict(provenance),
    }
    payload["section_hashes"] = {
        section: payload_hash(payload[section]) for section in _SEMANTIC_SECTIONS
    }
    payload["semantic_hash"] = semantic_hash(payload)
    return payload


def persist_verified_checkpoint(
    storage: ActiveStorage,
    *,
    checkpoint_payload: Mapping[str, Any],
    source_truth: Mapping[str, Any],
) -> CheckpointVerification:
    missing = sorted(_REQUIRED_CHECKPOINT_FIELDS - set(checkpoint_payload))
    if missing:
        raise ValueError("checkpoint missing required fields: " + ", ".join(missing))
    verification = verify_semantic_equivalence(source_truth, checkpoint_payload)
    if not verification.equivalent:
        raise RuntimeError(
            "checkpoint semantic equivalence failed: " + ", ".join(verification.mismatched_sections)
        )
    body = canonical_json(dict(checkpoint_payload))
    digest = hashlib.sha256(body.encode("utf-8")).hexdigest()
    checkpoint_id = str(checkpoint_payload["checkpoint_id"])
    with storage.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("UPDATE checkpoint_current SET verified=0 WHERE verified=1")
        conn.execute(
            """
            INSERT INTO checkpoint_current(
                checkpoint_id, created_at, schema_version, migration_version, release_sha,
                payload_json, payload_hash, semantic_hash, verified
            ) VALUES(?,?,?,?,?,?,?,?,1)
            """,
            (
                checkpoint_id,
                str(checkpoint_payload["timestamp"]),
                int(checkpoint_payload["schema_version"]),
                int(checkpoint_payload["migration_version"]),
                str(checkpoint_payload["release_sha"]),
                body,
                digest,
                verification.observed_hash,
            ),
        )
        conn.execute(
            "UPDATE storage_epoch_state SET last_checkpoint_id=?, updated_at=? WHERE singleton_key=1",
            (checkpoint_id, _utc_now()),
        )
        conn.commit()
    return verification


def load_verified_checkpoint(path: Path | str) -> dict[str, Any]:
    active_path = Path(path)
    if not active_path.is_file():
        raise RuntimeError(f"active storage unavailable: {active_path}")
    uri = f"file:{active_path.resolve()}?mode=ro&cache=private"
    with sqlite3.connect(uri, uri=True, timeout=5.0) as conn:
        row = conn.execute(
            """
            SELECT payload_json,payload_hash,semantic_hash,schema_version,migration_version,release_sha
            FROM checkpoint_current WHERE verified=1 ORDER BY created_at DESC LIMIT 1
            """
        ).fetchone()
    if row is None:
        raise RuntimeError("active storage has no verified continuation checkpoint")
    body = str(row[0])
    if hashlib.sha256(body.encode("utf-8")).hexdigest() != str(row[1]):
        raise RuntimeError("active checkpoint payload hash mismatch")
    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError("active checkpoint is not valid JSON") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("active checkpoint has invalid shape")
    if semantic_hash(payload) != str(row[2]):
        raise RuntimeError("active checkpoint semantic hash mismatch")
    if int(row[3]) != ACTIVE_SCHEMA_VERSION or int(row[4]) != TRANSITION_MIGRATION_VERSION:
        raise RuntimeError("active checkpoint storage version mismatch")
    missing = sorted(_REQUIRED_CHECKPOINT_FIELDS - set(payload))
    if missing:
        raise RuntimeError("active checkpoint missing required fields: " + ", ".join(missing))
    return payload


def select_runtime_database_from_environment() -> Path | None:
    """Return the verified active DB path without ever opening legacy as fallback.

    This helper intentionally does not mutate the runtime environment. Activation is
    a separate explicit composition step so merely creating/shadowing active storage
    cannot redirect production writes.
    """
    if not _truthy(os.getenv(ACTIVATE_ENV)):
        return None
    raw_active = (os.getenv(ACTIVE_PATH_ENV) or "").strip()
    if not raw_active:
        raise RuntimeError(f"{ACTIVATE_ENV} is enabled but {ACTIVE_PATH_ENV} is empty")
    active_path = Path(raw_active)
    load_verified_checkpoint(active_path)
    return active_path


def activate_runtime_database_environment() -> Path | None:
    """Explicit fail-closed environment cutover after a verified checkpoint exists."""
    active_path = select_runtime_database_from_environment()
    if active_path is None:
        return None
    current = (os.getenv(LEGACY_PATH_ENV) or "").strip()
    if current and Path(current) != active_path:
        os.environ.setdefault(LEGACY_RECORD_ENV, current)
    os.environ[LEGACY_PATH_ENV] = str(active_path)
    return active_path
