from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import uuid
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from .active_storage import ACTIVE_SCHEMA_VERSION, ActiveStorage, canonical_json, payload_hash

LEGACY_TRANSITION_MIGRATION_VERSION = 2
TRANSITION_MIGRATION_VERSION = 3
ACTIVE_PATH_ENV = "SOLANA_ROI_ACTIVE_DB_PATH"
LEGACY_PATH_ENV = "SOLANA_ROI_DB_PATH"
LEGACY_RECORD_ENV = "SOLANA_ROI_LEGACY_DB_PATH"
ACTIVATE_ENV = "SOLANA_ROI_ACTIVE_STORAGE_ENABLED"
SHADOW_ENV = "SOLANA_ROI_ACTIVE_STORAGE_SHADOW"
FINALIZE_ENV = "SOLANA_ROI_ACTIVE_STORAGE_FINALIZE_FROM_LEGACY"
_COMPACT_SECTIONS_STORAGE = "active_current_tables"

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
_LEGACY_REQUIRED_CHECKPOINT_FIELDS = {
    "checkpoint_id",
    "timestamp",
    "schema_version",
    "migration_version",
    "release_sha",
    "provenance",
    *_SEMANTIC_SECTIONS,
}
_COMPACT_REQUIRED_CHECKPOINT_FIELDS = {
    "checkpoint_id",
    "timestamp",
    "schema_version",
    "migration_version",
    "release_sha",
    "provenance",
    "section_hashes",
    "semantic_hash",
    "sections_storage",
}
_ACTIVE_SECTION_ROWS = {
    "strategy": ("strategy_current", "state_key", "transition"),
    "wallet": ("wallet_current", "wallet_id", "__transition_state__"),
    "wallet_evidence_watermarks": ("wallet_current", "wallet_id", "__transition_watermarks__"),
    "provider_source": ("provider_current", "provider_id", "__transition__"),
    "freshness": ("system_current", "state_key", "freshness"),
    "latest_event_ids": ("system_current", "state_key", "latest_event_ids"),
    "active_candidates": ("active_candidates", "candidate_id", "__transition__"),
    "active_lifecycles": ("active_lifecycles", "lifecycle_id", "__transition__"),
    "portfolio": ("portfolio_current", "state_key", "transition"),
    "replication_watermarks": ("system_current", "state_key", "replication_watermarks"),
    "certification": ("certification_current", "state_key", "transition"),
    "continuity": ("continuity_current", "state_key", "transition"),
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


def _running_release_sha() -> str | None:
    return (os.getenv("RENDER_GIT_COMMIT") or os.getenv("GIT_COMMIT") or "").strip() or None


def _normal_active_release_rollforward(expected_release_sha: str | None) -> bool:
    """Allow an established verified checkpoint to survive an ordinary deploy.

    The checkpoint release SHA records the release that established the active
    storage boundary; it is provenance, not a permanent lock on all later code
    releases. Release equality remains mandatory everywhere else, including the
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
    mismatched_sections: tuple[str, ...]


def verify_semantic_equivalence(
    source_truth: Mapping[str, Any], checkpoint_payload: Mapping[str, Any]
) -> CheckpointVerification:
    source = semantic_projection(source_truth)
    observed = semantic_projection(checkpoint_payload)
    mismatched = tuple(
        name
        for name in _SEMANTIC_SECTIONS
        if payload_hash(source.get(name)) != payload_hash(observed.get(name))
    )
    expected_hash = payload_hash(source)
    observed_hash = payload_hash(observed)
    return CheckpointVerification(
        not mismatched and expected_hash == observed_hash,
        expected_hash,
        observed_hash,
        mismatched,
    )


def _read_current_payload(
    conn: sqlite3.Connection, table: str, key_column: str, key: str, section: str
) -> Any:
    try:
        row = conn.execute(
            f'SELECT payload_json,payload_hash FROM "{table}" WHERE "{key_column}"=?',
            (key,),
        ).fetchone()
    except sqlite3.Error as exc:
        raise RuntimeError(f"active checkpoint semantic section unreadable: {section}") from exc
    if row is None:
        raise RuntimeError(f"active checkpoint semantic section missing: {section}")
    body = str(row[0])
    if hashlib.sha256(body.encode("utf-8")).hexdigest() != str(row[1]):
        raise RuntimeError(f"active checkpoint semantic section payload hash mismatch: {section}")
    try:
        return json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"active checkpoint semantic section is not valid JSON: {section}") from exc


def _read_active_semantic_sections(conn: sqlite3.Connection) -> dict[str, Any]:
    return {
        section: _read_current_payload(conn, table, key_column, key, section)
        for section, (table, key_column, key) in _ACTIVE_SECTION_ROWS.items()
    }


def build_checkpoint_payload(
    *, release_sha: str, current_truth: Mapping[str, Any], provenance: Mapping[str, Any]
) -> dict[str, Any]:
    missing = [name for name in _SEMANTIC_SECTIONS if name not in current_truth]
    if missing:
        raise ValueError("current truth missing checkpoint sections: " + ", ".join(missing))
    projection = semantic_projection(current_truth)
    return {
        "checkpoint_id": f"storage-transition-{uuid.uuid4().hex}",
        "timestamp": _utc_now(),
        "schema_version": ACTIVE_SCHEMA_VERSION,
        "migration_version": TRANSITION_MIGRATION_VERSION,
        "release_sha": str(release_sha),
        "provenance": dict(provenance),
        "section_hashes": {name: payload_hash(projection[name]) for name in _SEMANTIC_SECTIONS},
        "semantic_hash": payload_hash(projection),
        "sections_storage": _COMPACT_SECTIONS_STORAGE,
    }


def _validate_legacy_checkpoint_shape(payload: Mapping[str, Any]) -> None:
    missing = sorted(_LEGACY_REQUIRED_CHECKPOINT_FIELDS - set(payload))
    if missing:
        raise RuntimeError("active checkpoint missing required fields: " + ", ".join(missing))
    section_hashes = payload.get("section_hashes")
    if not isinstance(section_hashes, Mapping):
        raise RuntimeError("active checkpoint section hashes missing")
    for section in _SEMANTIC_SECTIONS:
        if str(section_hashes.get(section) or "") != payload_hash(payload[section]):
            raise RuntimeError(f"active checkpoint section hash mismatch: {section}")
    if str(payload.get("semantic_hash") or "") != semantic_hash(payload):
        raise RuntimeError("active checkpoint semantic hash mismatch")


def _validate_compact_checkpoint_shape(
    payload: Mapping[str, Any], *, logical_truth: Mapping[str, Any]
) -> None:
    missing = sorted(_COMPACT_REQUIRED_CHECKPOINT_FIELDS - set(payload))
    if missing:
        raise RuntimeError("active checkpoint missing required fields: " + ", ".join(missing))
    if str(payload.get("sections_storage") or "") != _COMPACT_SECTIONS_STORAGE:
        raise RuntimeError("active checkpoint compact semantic storage mismatch")
    embedded = sorted(section for section in _SEMANTIC_SECTIONS if section in payload)
    if embedded:
        raise RuntimeError("active checkpoint compact payload embeds semantic sections: " + ", ".join(embedded))
    section_hashes = payload.get("section_hashes")
    if not isinstance(section_hashes, Mapping):
        raise RuntimeError("active checkpoint section hashes missing")
    for section in _SEMANTIC_SECTIONS:
        if section not in logical_truth:
            raise RuntimeError(f"active checkpoint semantic section missing: {section}")
        if str(section_hashes.get(section) or "") != payload_hash(logical_truth[section]):
            raise RuntimeError(f"active checkpoint section hash mismatch: {section}")
    if str(payload.get("semantic_hash") or "") != semantic_hash(logical_truth):
        raise RuntimeError("active checkpoint semantic hash mismatch")


def _validate_checkpoint_shape(
    payload: Mapping[str, Any], *, logical_truth: Mapping[str, Any] | None = None
) -> None:
    if int(payload.get("schema_version", -1)) != ACTIVE_SCHEMA_VERSION:
        raise RuntimeError("active checkpoint storage schema version mismatch")
    migration_version = int(payload.get("migration_version", -1))
    if migration_version == LEGACY_TRANSITION_MIGRATION_VERSION:
        _validate_legacy_checkpoint_shape(payload)
        return
    if migration_version == TRANSITION_MIGRATION_VERSION:
        if logical_truth is None:
            raise RuntimeError("active checkpoint compact semantic truth unavailable")
        _validate_compact_checkpoint_shape(payload, logical_truth=logical_truth)
        return
    raise RuntimeError("active checkpoint migration version mismatch")


def persist_verified_checkpoint(
    storage: ActiveStorage,
    *,
    checkpoint_payload: Mapping[str, Any],
    source_truth: Mapping[str, Any],
) -> CheckpointVerification:
    migration_version = int(checkpoint_payload.get("migration_version", -1))
    if migration_version == TRANSITION_MIGRATION_VERSION:
        _validate_checkpoint_shape(checkpoint_payload, logical_truth=source_truth)
        expected_hash = str(checkpoint_payload["semantic_hash"])
        verification = CheckpointVerification(True, expected_hash, expected_hash, ())
    else:
        _validate_checkpoint_shape(checkpoint_payload)
        verification = verify_semantic_equivalence(source_truth, checkpoint_payload)
        if not verification.equivalent:
            raise RuntimeError(
                "checkpoint semantic equivalence failed: "
                + ", ".join(verification.mismatched_sections)
            )
    body = canonical_json(dict(checkpoint_payload))
    digest = hashlib.sha256(body.encode("utf-8")).hexdigest()
    checkpoint_id = str(checkpoint_payload["checkpoint_id"])
    with closing(storage.connect()) as conn, conn:
        conn.execute("BEGIN IMMEDIATE")
        if migration_version == TRANSITION_MIGRATION_VERSION:
            # Read each persisted section under the SAME writer transaction as
            # publication. No second full semantic tree is kept in memory, and
            # a concurrent writer cannot replace a section between proof and seal.
            for section, (table, key_column, key) in _ACTIVE_SECTION_ROWS.items():
                value = _read_current_payload(conn, table, key_column, key, section)
                if payload_hash(value) != checkpoint_payload["section_hashes"][section]:
                    raise RuntimeError("checkpoint semantic equivalence failed: " + section)
                del value
        conn.execute("UPDATE checkpoint_current SET verified=0 WHERE verified=1")
        conn.execute(
            "INSERT INTO checkpoint_current(checkpoint_id,created_at,schema_version,migration_version,release_sha,payload_json,payload_hash,semantic_hash,verified) VALUES(?,?,?,?,?,?,?,?,1)",
            (
                checkpoint_id,
                str(checkpoint_payload["timestamp"]),
                int(checkpoint_payload["schema_version"]),
                migration_version,
                str(checkpoint_payload["release_sha"]),
                body,
                digest,
                verification.observed_hash,
            ),
        )
        conn.execute(
            "UPDATE storage_epoch_state SET last_checkpoint_id=?,updated_at=? WHERE singleton_key=1",
            (checkpoint_id, _utc_now()),
        )
        conn.commit()
    return verification


def load_verified_checkpoint(
    path: Path | str, *, expected_release_sha: str | None = None
) -> dict[str, Any]:
    active_path = Path(path)
    if not active_path.is_file():
        raise RuntimeError(f"active storage unavailable: {active_path}")
    uri = f"file:{active_path.resolve()}?mode=ro&cache=private"
    try:
        with closing(sqlite3.connect(uri, uri=True, timeout=5.0)) as conn:
            conn.execute("PRAGMA query_only=ON")
            conn.execute("BEGIN")
            row = conn.execute(
                "SELECT payload_json,payload_hash,semantic_hash,schema_version,migration_version,release_sha "
                "FROM checkpoint_current WHERE verified=1 ORDER BY created_at DESC LIMIT 1"
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
            migration_version = int(payload.get("migration_version", -1))
            if migration_version == TRANSITION_MIGRATION_VERSION:
                logical_truth = _read_active_semantic_sections(conn)
                _validate_checkpoint_shape(payload, logical_truth=logical_truth)
                observed_semantic_hash = semantic_hash(logical_truth)
                materialized = dict(payload)
                materialized.update(logical_truth)
            else:
                _validate_checkpoint_shape(payload)
                observed_semantic_hash = semantic_hash(payload)
                materialized = dict(payload)
    except sqlite3.Error as exc:
        raise RuntimeError("active storage checkpoint unreadable") from exc
    if observed_semantic_hash != str(row[2]):
        raise RuntimeError("active checkpoint stored semantic hash mismatch")
    if int(row[3]) != ACTIVE_SCHEMA_VERSION or int(row[4]) != migration_version:
        raise RuntimeError("active checkpoint persisted version mismatch")
    if str(row[5]) != str(payload["release_sha"]):
        raise RuntimeError("active checkpoint release metadata mismatch")
    if expected_release_sha is not None and str(payload["release_sha"]) != str(expected_release_sha):
        if not _normal_active_release_rollforward(expected_release_sha):
            raise RuntimeError("active checkpoint release SHA does not match running release")
    return materialized


def active_storage_enabled() -> bool:
    return _truthy(os.getenv(ACTIVATE_ENV))


def shadow_storage_enabled() -> bool:
    return _truthy(os.getenv(SHADOW_ENV))


def select_runtime_database_from_environment(
    *, expected_release_sha: str | None = None
) -> Path | None:
    """Select only a verified active DB. Legacy is never opened as fallback."""
    if not active_storage_enabled():
        return None
    raw_active = (os.getenv(ACTIVE_PATH_ENV) or "").strip()
    if not raw_active:
        raise RuntimeError(f"{ACTIVATE_ENV} is enabled but {ACTIVE_PATH_ENV} is empty")
    active_path = Path(raw_active)
    load_verified_checkpoint(active_path, expected_release_sha=expected_release_sha)
    return active_path


def activate_runtime_database_environment(
    *, expected_release_sha: str | None = None
) -> Path | None:
    active_path = select_runtime_database_from_environment(
        expected_release_sha=expected_release_sha
    )
    if active_path is None:
        return None
    current = (os.getenv(LEGACY_PATH_ENV) or "").strip()
    if current and Path(current) != active_path:
        os.environ.setdefault(LEGACY_RECORD_ENV, current)
    os.environ[LEGACY_PATH_ENV] = str(active_path)
    return active_path
