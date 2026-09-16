from __future__ import annotations

import base64
import hashlib
import json
import os
import sqlite3
import uuid
import zlib
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from .active_storage import ACTIVE_SCHEMA_VERSION, ActiveStorage, canonical_json, payload_hash

TRANSITION_MIGRATION_VERSION = 2
ACTIVE_PATH_ENV = "SOLANA_ROI_ACTIVE_DB_PATH"
LEGACY_PATH_ENV = "SOLANA_ROI_DB_PATH"
LEGACY_RECORD_ENV = "SOLANA_ROI_LEGACY_DB_PATH"
ACTIVATE_ENV = "SOLANA_ROI_ACTIVE_STORAGE_ENABLED"
SHADOW_ENV = "SOLANA_ROI_ACTIVE_STORAGE_SHADOW"
FINALIZE_ENV = "SOLANA_ROI_ACTIVE_STORAGE_FINALIZE_FROM_LEGACY"
CHECKPOINT_ENCODING_PREFIX = "zlib-base64-v1:"
CHECKPOINT_COMPRESSION_LEVEL = 6

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
_REQUIRED_CHECKPOINT_FIELDS = {
    "checkpoint_id","timestamp","schema_version","migration_version","release_sha","provenance",
    *_SEMANTIC_SECTIONS,
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1","true","yes","on"}


def _running_release_sha() -> str | None:
    return (os.getenv("RENDER_GIT_COMMIT") or os.getenv("GIT_COMMIT") or "").strip() or None


def _normal_active_release_rollforward(expected_release_sha: str | None) -> bool:
    """Allow an established verified checkpoint to survive an ordinary deploy.

    The checkpoint release SHA records the release that established the active
    storage boundary; it is provenance, not a permanent lock on all later code
    releases.  Release equality remains mandatory everywhere else, including the
    explicit finalize/cutover startup that creates a new checkpoint.
    """
    if expected_release_sha is None:
        return False
    return (
        _truthy(os.getenv(ACTIVATE_ENV))
        and not _truthy(os.getenv(FINALIZE_ENV))
        and _running_release_sha() == str(expected_release_sha)
    )


def semantic_projection(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {section: payload.get(section) for section in _SEMANTIC_SECTIONS}


def semantic_hash(payload: Mapping[str, Any]) -> str:
    return payload_hash(semantic_projection(payload))


@dataclass(frozen=True)
class CheckpointVerification:
    equivalent: bool
    expected_hash: str
    observed_hash: str
    mismatched_sections: tuple[str,...]


def verify_semantic_equivalence(source_truth: Mapping[str, Any], checkpoint_payload: Mapping[str, Any]) -> CheckpointVerification:
    source = semantic_projection(source_truth)
    observed = semantic_projection(checkpoint_payload)
    mismatched = tuple(name for name in _SEMANTIC_SECTIONS if canonical_json(source.get(name)) != canonical_json(observed.get(name)))
    expected_hash = payload_hash(source)
    observed_hash = payload_hash(observed)
    return CheckpointVerification(not mismatched and expected_hash == observed_hash, expected_hash, observed_hash, mismatched)


def build_checkpoint_payload(*, release_sha: str, current_truth: Mapping[str, Any], provenance: Mapping[str, Any]) -> dict[str, Any]:
    missing = [name for name in _SEMANTIC_SECTIONS if name not in current_truth]
    if missing:
        raise ValueError("current truth missing checkpoint sections: " + ", ".join(missing))
    payload: dict[str,Any] = {
        "checkpoint_id": f"storage-transition-{uuid.uuid4().hex}",
        "timestamp": _utc_now(),
        "schema_version": ACTIVE_SCHEMA_VERSION,
        "migration_version": TRANSITION_MIGRATION_VERSION,
        "release_sha": str(release_sha),
        **{name: current_truth[name] for name in _SEMANTIC_SECTIONS},
        "provenance": dict(provenance),
    }
    payload["section_hashes"] = {name: payload_hash(payload[name]) for name in _SEMANTIC_SECTIONS}
    payload["semantic_hash"] = semantic_hash(payload)
    return payload


def _validate_checkpoint_shape(payload: Mapping[str, Any]) -> None:
    missing = sorted(_REQUIRED_CHECKPOINT_FIELDS - set(payload))
    if missing:
        raise RuntimeError("active checkpoint missing required fields: " + ", ".join(missing))
    if int(payload.get("schema_version", -1)) != ACTIVE_SCHEMA_VERSION:
        raise RuntimeError("active checkpoint storage schema version mismatch")
    if int(payload.get("migration_version", -1)) != TRANSITION_MIGRATION_VERSION:
        raise RuntimeError("active checkpoint migration version mismatch")
    section_hashes = payload.get("section_hashes")
    if not isinstance(section_hashes, Mapping):
        raise RuntimeError("active checkpoint section hashes missing")
    for section in _SEMANTIC_SECTIONS:
        if str(section_hashes.get(section) or "") != payload_hash(payload[section]):
            raise RuntimeError(f"active checkpoint section hash mismatch: {section}")
    if str(payload.get("semantic_hash") or "") != semantic_hash(payload):
        raise RuntimeError("active checkpoint semantic hash mismatch")


def _encode_checkpoint_body(body: str) -> str:
    compressed = zlib.compress(body.encode("utf-8"), level=CHECKPOINT_COMPRESSION_LEVEL)
    return CHECKPOINT_ENCODING_PREFIX + base64.b64encode(compressed).decode("ascii")


def _decode_checkpoint_body(stored: str) -> str:
    if not stored.startswith(CHECKPOINT_ENCODING_PREFIX):
        return stored
    encoded = stored[len(CHECKPOINT_ENCODING_PREFIX):]
    try:
        compressed = base64.b64decode(encoded.encode("ascii"), validate=True)
        return zlib.decompress(compressed).decode("utf-8")
    except (ValueError, UnicodeDecodeError, zlib.error) as exc:
        raise RuntimeError("active checkpoint compressed payload unreadable") from exc


def persist_verified_checkpoint(storage: ActiveStorage, *, checkpoint_payload: Mapping[str, Any], source_truth: Mapping[str, Any]) -> CheckpointVerification:
    _validate_checkpoint_shape(checkpoint_payload)
    verification = verify_semantic_equivalence(source_truth, checkpoint_payload)
    if not verification.equivalent:
        raise RuntimeError("checkpoint semantic equivalence failed: " + ", ".join(verification.mismatched_sections))
    body = canonical_json(dict(checkpoint_payload))
    digest = hashlib.sha256(body.encode("utf-8")).hexdigest()
    stored_body = _encode_checkpoint_body(body)
    checkpoint_id = str(checkpoint_payload["checkpoint_id"])
    with storage.connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("UPDATE checkpoint_current SET verified=0 WHERE verified=1")
        conn.execute(
            "INSERT INTO checkpoint_current(checkpoint_id,created_at,schema_version,migration_version,release_sha,payload_json,payload_hash,semantic_hash,verified) VALUES(?,?,?,?,?,?,?,?,1)",
            (checkpoint_id,str(checkpoint_payload["timestamp"]),int(checkpoint_payload["schema_version"]),int(checkpoint_payload["migration_version"]),str(checkpoint_payload["release_sha"]),stored_body,digest,verification.observed_hash),
        )
        conn.execute("UPDATE storage_epoch_state SET last_checkpoint_id=?,updated_at=? WHERE singleton_key=1", (checkpoint_id,_utc_now()))
        conn.commit()
    return verification


def load_verified_checkpoint(path: Path | str, *, expected_release_sha: str | None = None) -> dict[str,Any]:
    active_path = Path(path)
    if not active_path.is_file():
        raise RuntimeError(f"active storage unavailable: {active_path}")
    uri = f"file:{active_path.resolve()}?mode=ro&cache=private"
    try:
        with sqlite3.connect(uri, uri=True, timeout=5.0) as conn:
            row = conn.execute("SELECT payload_json,payload_hash,semantic_hash,schema_version,migration_version,release_sha FROM checkpoint_current WHERE verified=1 ORDER BY created_at DESC LIMIT 1").fetchone()
    except sqlite3.Error as exc:
        raise RuntimeError("active storage checkpoint unreadable") from exc
    if row is None:
        raise RuntimeError("active storage has no verified continuation checkpoint")
    stored_body = str(row[0])
    body = _decode_checkpoint_body(stored_body)
    if hashlib.sha256(body.encode("utf-8")).hexdigest() != str(row[1]):
        raise RuntimeError("active checkpoint payload hash mismatch")
    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError("active checkpoint is not valid JSON") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("active checkpoint has invalid shape")
    _validate_checkpoint_shape(payload)
    if semantic_hash(payload) != str(row[2]):
        raise RuntimeError("active checkpoint stored semantic hash mismatch")
    if int(row[3]) != ACTIVE_SCHEMA_VERSION or int(row[4]) != TRANSITION_MIGRATION_VERSION:
        raise RuntimeError("active checkpoint persisted version mismatch")
    if str(row[5]) != str(payload["release_sha"]):
        raise RuntimeError("active checkpoint release metadata mismatch")
    if expected_release_sha is not None and str(payload["release_sha"]) != str(expected_release_sha):
        if not _normal_active_release_rollforward(expected_release_sha):
            raise RuntimeError("active checkpoint release SHA does not match running release")
    return payload


def active_storage_enabled() -> bool:
    return _truthy(os.getenv(ACTIVATE_ENV))


def shadow_storage_enabled() -> bool:
    return _truthy(os.getenv(SHADOW_ENV))


def select_runtime_database_from_environment(*, expected_release_sha: str | None = None) -> Path | None:
    """Select only a verified active DB.  Legacy is never opened as fallback."""
    if not active_storage_enabled():
        return None
    raw_active = (os.getenv(ACTIVE_PATH_ENV) or "").strip()
    if not raw_active:
        raise RuntimeError(f"{ACTIVATE_ENV} is enabled but {ACTIVE_PATH_ENV} is empty")
    active_path = Path(raw_active)
    load_verified_checkpoint(active_path, expected_release_sha=expected_release_sha)
    return active_path


def activate_runtime_database_environment(*, expected_release_sha: str | None = None) -> Path | None:
    active_path = select_runtime_database_from_environment(expected_release_sha=expected_release_sha)
    if active_path is None:
        return None
    current = (os.getenv(LEGACY_PATH_ENV) or "").strip()
    if current and Path(current) != active_path:
        os.environ.setdefault(LEGACY_RECORD_ENV, current)
    os.environ[LEGACY_PATH_ENV] = str(active_path)
    return active_path
