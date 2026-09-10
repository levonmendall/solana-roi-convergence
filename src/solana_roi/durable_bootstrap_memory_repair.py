from __future__ import annotations

"""Bound raw-cgroup cache pressure during durable restore and certification bootstrap.

The authoritative runtime keeps the canonical SQLite database and paper-only authority.
This repair changes only how large read-only scans interact with SQLite and Linux file
cache: mmap is disabled, the SQLite page cache is small, clean DB/WAL/SHM cache is
advised away incrementally, and a raw cgroup headroom guard fails closed before the
kernel hard limit can OOM-kill the service.
"""

import hashlib
import os
import sqlite3
import time
from pathlib import Path
from typing import Any

from fastapi import HTTPException

REPAIR_VERSION = "durable-bootstrap-cgroup-memory-v1"
VERIFY_CACHE_RELEASE_ROWS = 4_096
SQLITE_READER_CACHE_KIB = 2_048
RAW_RECLAIM_FRACTION = 0.82
RAW_CRITICAL_FRACTION = 0.94
RAW_RECLAIM_RESERVE_BYTES = 384 * 1024 * 1024
RAW_CRITICAL_RESERVE_BYTES = 128 * 1024 * 1024
RECLAIM_SETTLE_SECONDS = 0.01

PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
STRATEGY_THRESHOLDS_CHANGED = False
CERTIFICATION_THRESHOLDS_CHANGED = False
CONTINUITY_SEMANTICS_CHANGED = False
CANONICAL_EVIDENCE_RESET = False

_INSTALLED = False
_ORIGINAL_VERIFY: Any = None
_ORIGINAL_PINNED_READER: Any = None
_ORIGINAL_DROP_FILE_CACHE: Any = None


def _read_scalar(path: Path) -> int | None:
    try:
        raw = path.read_text(encoding="utf-8").strip()
    except (OSError, UnicodeError):
        return None
    if not raw or raw == "max":
        return None
    try:
        return max(0, int(raw))
    except ValueError:
        return None


def _cgroup_memory(root: Path | None = None) -> dict[str, int | float | None]:
    base = root or Path(os.getenv("SOLANA_ROI_CGROUP_ROOT", "/sys/fs/cgroup"))
    current = _read_scalar(base / "memory.current")
    maximum = _read_scalar(base / "memory.max")
    headroom = max(0, maximum - current) if current is not None and maximum is not None else None
    fraction = (
        float(current) / float(maximum)
        if current is not None and maximum not in (None, 0)
        else None
    )
    return {
        "current_bytes": current,
        "max_bytes": maximum,
        "headroom_bytes": headroom,
        "fraction": fraction,
    }


def _sqlite_cache_paths(path: Path) -> tuple[Path, Path, Path]:
    return path, Path(str(path) + "-wal"), Path(str(path) + "-shm")


def _advise_dontneed(path: Path) -> bool:
    fadvise = getattr(os, "posix_fadvise", None)
    advice = getattr(os, "POSIX_FADV_DONTNEED", None)
    if fadvise is None or advice is None:
        return False
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return False
    try:
        try:
            fadvise(fd, 0, 0, advice)
            return True
        except OSError:
            return False
    finally:
        os.close(fd)


def _release_sqlite_file_cache(path: Path) -> bool:
    released = False
    for candidate in _sqlite_cache_paths(path):
        released = _advise_dontneed(candidate) or released
    return released


def _needs_reclaim(state: dict[str, int | float | None]) -> bool:
    fraction = state.get("fraction")
    headroom = state.get("headroom_bytes")
    return bool(
        (isinstance(fraction, (int, float)) and float(fraction) >= RAW_RECLAIM_FRACTION)
        or (isinstance(headroom, int) and headroom <= RAW_RECLAIM_RESERVE_BYTES)
    )


def _critical(state: dict[str, int | float | None]) -> bool:
    fraction = state.get("fraction")
    headroom = state.get("headroom_bytes")
    return bool(
        (isinstance(fraction, (int, float)) and float(fraction) >= RAW_CRITICAL_FRACTION)
        or (isinstance(headroom, int) and headroom <= RAW_CRITICAL_RESERVE_BYTES)
    )


def _guard_raw_cgroup(path: Path) -> dict[str, int | float | None]:
    """Reclaim read cache and fail closed before the cgroup hard boundary.

    The guard is intentionally based on raw ``memory.current`` rather than a
    working-set estimate because Render's OOM boundary includes reclaimable file
    cache. It never changes canonical SQLite state.
    """

    before = _cgroup_memory()
    if not _needs_reclaim(before):
        return before
    _release_sqlite_file_cache(path)
    if RECLAIM_SETTLE_SECONDS > 0:
        time.sleep(RECLAIM_SETTLE_SECONDS)
    after = _cgroup_memory()
    if _critical(after):
        raise MemoryError("authoritative SQLite read deferred: raw cgroup memory pressure")
    return after


def _bounded_verify_engine_snapshot(self: Any) -> tuple[bool, int, int | None]:
    """Preserve full hash-chain verification while bounding cache residency."""

    store = self.store
    with store._verify_lock:
        previous: str | None = None
        verified_through_event_id = 0
        latest_engine_event_id: int | None = None
        reader: sqlite3.Connection | None = None
        source_path = Path(store.path)
        processed = 0
        try:
            _guard_raw_cgroup(source_path)
            uri = f"{source_path.resolve().as_uri()}?mode=ro"
            with store._lock:
                reader = sqlite3.connect(uri, uri=True, check_same_thread=False)
                reader.execute("PRAGMA query_only=ON")
                reader.execute("PRAGMA busy_timeout=5000")
                reader.execute(f"PRAGMA cache_size=-{SQLITE_READER_CACHE_KIB}")
                reader.execute("PRAGMA mmap_size=0")
                reader.execute("BEGIN")
                cursor = reader.execute(
                    "SELECT id, event_type, observed_at, payload_json, previous_hash, lineage_hash "
                    "FROM events ORDER BY id"
                )
                row = cursor.fetchone()
            while row is not None:
                event_id, event_type, observed_at, raw, recorded_previous, lineage = row
                if recorded_previous != previous:
                    return False, 0, None
                expected = hashlib.sha256(
                    f"{previous or ''}|{event_type}|{observed_at}|{raw}".encode()
                ).hexdigest()
                if expected != lineage:
                    return False, 0, None
                previous = lineage
                verified_through_event_id = int(event_id)
                if str(event_type) in {
                    "first_touch",
                    "confirmation",
                    "price",
                    "trade_intent",
                    "trade_outcome",
                }:
                    latest_engine_event_id = int(event_id)
                processed += 1
                if processed % VERIFY_CACHE_RELEASE_ROWS == 0:
                    _release_sqlite_file_cache(source_path)
                    _guard_raw_cgroup(source_path)
                row = cursor.fetchone()
            return True, verified_through_event_id, latest_engine_event_id
        finally:
            if reader is not None:
                reader.close()
            _release_sqlite_file_cache(source_path)


def _guarded_pinned_reader(store: Any) -> sqlite3.Connection:
    """Open one bounded logical-bootstrap reader or fail closed before OOM."""

    source_path = Path(getattr(store, "path", ""))
    if not source_path.is_file():
        raise HTTPException(status_code=503, detail="canonical certification source unavailable")
    try:
        _guard_raw_cgroup(source_path)
    except MemoryError as exc:
        raise HTTPException(
            status_code=503,
            detail="certification logical bootstrap deferred: raw cgroup memory pressure",
        ) from exc
    reader = sqlite3.connect(f"file:{source_path.resolve()}?mode=ro", uri=True, timeout=5.0)
    reader.execute("PRAGMA query_only=ON")
    reader.execute("PRAGMA busy_timeout=5000")
    reader.execute(f"PRAGMA cache_size=-{SQLITE_READER_CACHE_KIB}")
    reader.execute("PRAGMA mmap_size=0")
    reader.execute("BEGIN")
    return reader


def _drop_file_cache_with_sidecars(path: Path) -> bool:
    return _release_sqlite_file_cache(Path(path))


def install_durable_bootstrap_memory_repair() -> None:
    global _INSTALLED, _ORIGINAL_VERIFY, _ORIGINAL_PINNED_READER, _ORIGINAL_DROP_FILE_CACHE
    if _INSTALLED:
        return

    from . import certification_logical_bootstrap as logical
    from . import certification_service_split as split
    from .durable_engine import DurablePaperTradingEngine

    current_verify = DurablePaperTradingEngine._verify_engine_snapshot
    if not bool(getattr(current_verify, "_roi_durable_bootstrap_memory_bounded", False)):
        _ORIGINAL_VERIFY = current_verify
        setattr(_bounded_verify_engine_snapshot, "_roi_durable_bootstrap_memory_bounded", True)
        DurablePaperTradingEngine._verify_engine_snapshot = _bounded_verify_engine_snapshot  # type: ignore[assignment]

    current_reader = logical._pinned_reader
    if not bool(getattr(current_reader, "_roi_durable_bootstrap_memory_bounded", False)):
        _ORIGINAL_PINNED_READER = current_reader
        setattr(_guarded_pinned_reader, "_roi_durable_bootstrap_memory_bounded", True)
        logical._pinned_reader = _guarded_pinned_reader  # type: ignore[assignment]

    current_drop = split._drop_file_cache
    if not bool(getattr(current_drop, "_roi_sqlite_sidecar_cache_release", False)):
        _ORIGINAL_DROP_FILE_CACHE = current_drop
        setattr(_drop_file_cache_with_sidecars, "_roi_sqlite_sidecar_cache_release", True)
        split._drop_file_cache = _drop_file_cache_with_sidecars  # type: ignore[assignment]

    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "repair_version": REPAIR_VERSION,
        "installed": _INSTALLED,
        "verify_cache_release_rows": VERIFY_CACHE_RELEASE_ROWS,
        "sqlite_reader_cache_kib": SQLITE_READER_CACHE_KIB,
        "raw_reclaim_fraction": RAW_RECLAIM_FRACTION,
        "raw_critical_fraction": RAW_CRITICAL_FRACTION,
        "raw_reclaim_reserve_bytes": RAW_RECLAIM_RESERVE_BYTES,
        "raw_critical_reserve_bytes": RAW_CRITICAL_RESERVE_BYTES,
        "cgroup_memory": _cgroup_memory(),
        "full_hash_chain_verification_preserved": True,
        "logical_bootstrap_keyset_semantics_preserved": True,
        "canonical_evidence_reset": CANONICAL_EVIDENCE_RESET,
        "strategy_thresholds_changed": STRATEGY_THRESHOLDS_CHANGED,
        "certification_thresholds_changed": CERTIFICATION_THRESHOLDS_CHANGED,
        "continuity_semantics_changed": CONTINUITY_SEMANTICS_CHANGED,
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
        "signing_available": SIGNING_AVAILABLE,
        "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
    }


__all__ = [
    "REPAIR_VERSION",
    "install_durable_bootstrap_memory_repair",
    "status",
]
