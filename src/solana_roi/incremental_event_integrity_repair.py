from __future__ import annotations

"""Tamper-evident incremental verification for the append-only event ledger.

The first run against an existing ledger performs the already-governed complete hash
verification and records a durable sidecar anchor beside the SQLite database. Ordinary
starts then validate that anchor and hash only the append-only tail. The sidecar never
creates trading authority and is never trusted without verifying its digest and its
anchoring database row. Missing/corrupt/inconsistent sidecar state falls back to the
complete fail-closed verifier rather than skipping integrity work.
"""

import hashlib
import json
import os
import sqlite3
import tempfile
from pathlib import Path
from types import SimpleNamespace
from typing import Any

REPAIR_VERSION = "event-integrity-checkpoint-v1-tail-verified"
CHECKPOINT_SCHEMA = "solana-roi-event-integrity-checkpoint.v1"
CHECKPOINT_PERSIST_EVERY_EVENTS = 2_048
ENGINE_EVENT_TYPES = frozenset({"first_touch", "confirmation", "price", "trade_intent", "trade_outcome"})

PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
STRATEGY_THRESHOLDS_CHANGED = False
CERTIFICATION_THRESHOLDS_CHANGED = False
CANONICAL_EVIDENCE_RESET = False

_INSTALLED = False
_ORIGINAL_STORE_APPEND: Any = None
_ORIGINAL_STORE_VERIFY: Any = None
_ORIGINAL_BOUNDED_VERIFY: Any = None


def _checkpoint_path(store: Any) -> Path:
    configured = os.getenv("SOLANA_ROI_EVENT_INTEGRITY_CHECKPOINT_PATH", "").strip()
    if configured:
        return Path(configured)
    return Path(str(Path(store.path)) + ".event-integrity-checkpoint.json")


def _checkpoint_digest(payload: dict[str, Any]) -> str:
    body = {key: value for key, value in payload.items() if key != "checkpoint_sha256"}
    raw = json.dumps(body, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(raw.encode()).hexdigest()


def _checkpoint_payload(*, event_id: int, lineage_hash: str | None, latest_engine_event_id: int | None) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema": CHECKPOINT_SCHEMA,
        "verified_event_id": int(event_id),
        "verified_lineage_hash": lineage_hash,
        "latest_engine_event_id": int(latest_engine_event_id) if latest_engine_event_id is not None else None,
    }
    payload["checkpoint_sha256"] = _checkpoint_digest(payload)
    return payload


def _atomic_checkpoint(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, raw = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    tmp = Path(raw)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
        try:
            parent_fd = os.open(path.parent, os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(parent_fd)
        except OSError:
            pass
        finally:
            os.close(parent_fd)
    finally:
        tmp.unlink(missing_ok=True)


def _row_hash(event_type: Any, observed_at: Any, raw: Any, previous_hash: Any) -> str:
    return hashlib.sha256(f"{previous_hash or ''}|{event_type}|{observed_at}|{raw}".encode()).hexdigest()


def _load_checkpoint(store: Any) -> dict[str, Any] | None:
    path = _checkpoint_path(store)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict) or payload.get("schema") != CHECKPOINT_SCHEMA:
        return None
    if str(payload.get("checkpoint_sha256") or "") != _checkpoint_digest(payload):
        return None
    try:
        event_id = int(payload.get("verified_event_id"))
    except (TypeError, ValueError):
        return None
    if event_id < 0:
        return None
    lineage_hash = payload.get("verified_lineage_hash")
    if event_id == 0:
        if lineage_hash not in (None, ""):
            return None
    else:
        if not isinstance(lineage_hash, str) or len(lineage_hash) != 64:
            return None
        with store._lock:
            row = store.db.execute(
                "SELECT event_type,observed_at,payload_json,previous_hash,lineage_hash FROM events WHERE id=?",
                (event_id,),
            ).fetchone()
        if row is None:
            return None
        if str(row["lineage_hash"]) != lineage_hash:
            return None
        if _row_hash(row["event_type"], row["observed_at"], row["payload_json"], row["previous_hash"]) != lineage_hash:
            return None

    latest = payload.get("latest_engine_event_id")
    if latest is not None:
        try:
            latest_id = int(latest)
        except (TypeError, ValueError):
            return None
        if latest_id < 0 or latest_id > event_id:
            return None
        if latest_id > 0:
            with store._lock:
                marker = store.db.execute("SELECT event_type FROM events WHERE id=?", (latest_id,)).fetchone()
            if marker is None or str(marker["event_type"]) not in ENGINE_EVENT_TYPES:
                return None
        payload["latest_engine_event_id"] = latest_id
    payload["verified_event_id"] = event_id
    return payload


def _persist_runtime_checkpoint(store: Any, payload: dict[str, Any]) -> None:
    _atomic_checkpoint(_checkpoint_path(store), payload)
    setattr(store, "_roi_integrity_runtime_checkpoint", dict(payload))
    setattr(store, "_roi_integrity_events_since_persist", 0)


def _seed_from_full_verification(store: Any) -> tuple[bool, int, int | None]:
    if _ORIGINAL_BOUNDED_VERIFY is None:
        raise RuntimeError("incremental event integrity verifier is not configured")
    verified, event_id, latest_engine_event_id = _ORIGINAL_BOUNDED_VERIFY(SimpleNamespace(store=store))
    if not verified:
        return False, 0, None
    lineage: str | None = None
    if int(event_id) > 0:
        with store._lock:
            row = store.db.execute("SELECT lineage_hash FROM events WHERE id=?", (int(event_id),)).fetchone()
        if row is None:
            return False, 0, None
        lineage = str(row["lineage_hash"])
    payload = _checkpoint_payload(
        event_id=int(event_id),
        lineage_hash=lineage,
        latest_engine_event_id=latest_engine_event_id,
    )
    _persist_runtime_checkpoint(store, payload)
    return True, int(event_id), latest_engine_event_id


def _verify_tail_locked(store: Any, checkpoint: dict[str, Any]) -> tuple[bool, int, int | None]:
    previous = checkpoint.get("verified_lineage_hash") or None
    verified_event_id = int(checkpoint["verified_event_id"])
    latest_engine_event_id = checkpoint.get("latest_engine_event_id")
    latest_engine_event_id = int(latest_engine_event_id) if latest_engine_event_id is not None else None
    reader: sqlite3.Connection | None = None
    try:
        uri = f"{Path(store.path).resolve().as_uri()}?mode=ro"
        with store._lock:
            reader = sqlite3.connect(uri, uri=True, check_same_thread=False)
            reader.row_factory = sqlite3.Row
            reader.execute("PRAGMA query_only=ON")
            reader.execute("PRAGMA busy_timeout=5000")
            reader.execute("PRAGMA cache_size=-2048")
            reader.execute("PRAGMA mmap_size=0")
            reader.execute("BEGIN")
            cursor = reader.execute(
                "SELECT id,event_type,observed_at,payload_json,previous_hash,lineage_hash "
                "FROM events WHERE id>? ORDER BY id",
                (verified_event_id,),
            )
            row = cursor.fetchone()
        while row is not None:
            recorded_previous = row["previous_hash"]
            lineage = str(row["lineage_hash"])
            if recorded_previous != previous:
                return False, 0, None
            if _row_hash(row["event_type"], row["observed_at"], row["payload_json"], recorded_previous) != lineage:
                return False, 0, None
            previous = lineage
            verified_event_id = int(row["id"])
            if str(row["event_type"]) in ENGINE_EVENT_TYPES:
                latest_engine_event_id = verified_event_id
            row = cursor.fetchone()
    finally:
        if reader is not None:
            reader.close()
        release = getattr(store, "_release_verification_file_cache", None)
        if callable(release):
            release()

    payload = _checkpoint_payload(
        event_id=verified_event_id,
        lineage_hash=previous,
        latest_engine_event_id=latest_engine_event_id,
    )
    _persist_runtime_checkpoint(store, payload)
    return True, verified_event_id, latest_engine_event_id


def _verify_with_checkpoint(store: Any) -> tuple[bool, int, int | None]:
    checkpoint = _load_checkpoint(store)
    if checkpoint is None:
        return _seed_from_full_verification(store)
    with store._verify_lock:
        checkpoint = _load_checkpoint(store)
        if checkpoint is None:
            # Never recurse into the full verifier while holding _verify_lock.
            pass
        else:
            return _verify_tail_locked(store, checkpoint)
    return _seed_from_full_verification(store)


def _incremental_engine_snapshot(self: Any) -> tuple[bool, int, int | None]:
    return _verify_with_checkpoint(self.store)


def _incremental_store_verify(self: Any) -> bool:
    verified, _event_id, _latest_engine_event_id = _verify_with_checkpoint(self)
    return bool(verified)


def _checkpointing_append(self: Any, event_type: str, observed_at: str, payload: dict[str, Any]) -> str:
    if _ORIGINAL_STORE_APPEND is None:
        raise RuntimeError("incremental event integrity append wrapper is not configured")
    lineage = _ORIGINAL_STORE_APPEND(self, event_type, observed_at, payload)
    try:
        with self._lock:
            row = self.db.execute(
                "SELECT id,event_type,observed_at,payload_json,previous_hash,lineage_hash FROM events WHERE lineage_hash=?",
                (lineage,),
            ).fetchone()
        if row is None:
            return lineage
        with self._verify_lock:
            checkpoint = getattr(self, "_roi_integrity_runtime_checkpoint", None)
            if not isinstance(checkpoint, dict):
                checkpoint = _load_checkpoint(self)
            if not isinstance(checkpoint, dict):
                return lineage
            prior_id = int(checkpoint["verified_event_id"])
            prior_hash = checkpoint.get("verified_lineage_hash") or None
            event_id = int(row["id"])
            if event_id != prior_id + 1 or row["previous_hash"] != prior_hash:
                return lineage
            if _row_hash(row["event_type"], row["observed_at"], row["payload_json"], row["previous_hash"]) != str(row["lineage_hash"]):
                return lineage
            latest = checkpoint.get("latest_engine_event_id")
            latest_id = int(latest) if latest is not None else None
            if str(row["event_type"]) in ENGINE_EVENT_TYPES:
                latest_id = event_id
            next_checkpoint = _checkpoint_payload(
                event_id=event_id,
                lineage_hash=str(row["lineage_hash"]),
                latest_engine_event_id=latest_id,
            )
            setattr(self, "_roi_integrity_runtime_checkpoint", next_checkpoint)
            since = int(getattr(self, "_roi_integrity_events_since_persist", 0) or 0) + 1
            setattr(self, "_roi_integrity_events_since_persist", since)
            if since >= CHECKPOINT_PERSIST_EVERY_EVENTS:
                _persist_runtime_checkpoint(self, next_checkpoint)
    except (OSError, sqlite3.Error, TypeError, ValueError, KeyError):
        # The sidecar is a verified acceleration anchor, never canonical truth. If it
        # cannot advance, the prior durable anchor remains valid and startup safely
        # verifies a longer tail (or falls back to the full verifier).
        pass
    return lineage


def full_integrity_audit(store: Any) -> tuple[bool, int, int | None]:
    """Explicit complete-history reconciliation path retained for independent audits."""
    return _seed_from_full_verification(store)


def configure_incremental_event_integrity_repair() -> None:
    global _INSTALLED, _ORIGINAL_STORE_APPEND, _ORIGINAL_STORE_VERIFY, _ORIGINAL_BOUNDED_VERIFY
    if _INSTALLED:
        return
    from . import durable_bootstrap_memory_repair as memory
    from .storage import AppendOnlyEventStore

    _ORIGINAL_STORE_APPEND = AppendOnlyEventStore.append
    _ORIGINAL_STORE_VERIFY = AppendOnlyEventStore.verify
    _ORIGINAL_BOUNDED_VERIFY = memory._bounded_verify_engine_snapshot

    setattr(_checkpointing_append, "_roi_incremental_integrity_append", True)
    setattr(_incremental_store_verify, "_roi_incremental_integrity_verify", True)
    setattr(_incremental_engine_snapshot, "_roi_durable_bootstrap_memory_bounded", True)
    AppendOnlyEventStore.append = _checkpointing_append  # type: ignore[assignment]
    AppendOnlyEventStore.verify = _incremental_store_verify  # type: ignore[assignment]
    memory._bounded_verify_engine_snapshot = _incremental_engine_snapshot
    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "repair_version": REPAIR_VERSION,
        "installed": _INSTALLED,
        "checkpoint_schema": CHECKPOINT_SCHEMA,
        "checkpoint_persist_every_events": CHECKPOINT_PERSIST_EVERY_EVENTS,
        "ordinary_start_verification": "validated_anchor_plus_append_only_tail",
        "first_run_or_invalid_anchor": "complete_hash_chain_fail_closed",
        "explicit_full_integrity_audit_retained": True,
        "canonical_evidence_reset": CANONICAL_EVIDENCE_RESET,
        "strategy_thresholds_changed": STRATEGY_THRESHOLDS_CHANGED,
        "certification_thresholds_changed": CERTIFICATION_THRESHOLDS_CHANGED,
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
        "signing_available": SIGNING_AVAILABLE,
        "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
    }


__all__ = [
    "REPAIR_VERSION",
    "configure_incremental_event_integrity_repair",
    "full_integrity_audit",
    "status",
]
