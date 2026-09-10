from __future__ import annotations

"""Bound raw-cgroup cache pressure during durable restore and certification bootstrap.

The authoritative runtime keeps the canonical SQLite database and paper-only authority.
This repair changes only how large read-only scans interact with SQLite, Python/glibc
heap retention, and Linux cgroup file cache. It never mutates canonical evidence or
weakens the fail-closed memory boundary.
"""

import ctypes
import gc
import hashlib
import os
import sqlite3
import time
from pathlib import Path
from typing import Any

from fastapi import HTTPException

REPAIR_VERSION = "durable-bootstrap-cgroup-memory-v5-statement-bootstrap-readers"
VERIFY_CACHE_RELEASE_ROWS = 4_096
SQLITE_READER_CACHE_KIB = 2_048
RAW_RECLAIM_FRACTION = 0.82
RAW_CRITICAL_FRACTION = 0.94
RAW_RECLAIM_TARGET_FRACTION = 0.72
RAW_RECLAIM_RESERVE_BYTES = 384 * 1024 * 1024
RAW_CRITICAL_RESERVE_BYTES = 128 * 1024 * 1024
MIN_CGROUP_RECLAIM_BYTES = 64 * 1024 * 1024
MAX_CGROUP_RECLAIM_BYTES = 1024 * 1024 * 1024
DIRTY_WRITEBACK_TRIGGER_BYTES = 64 * 1024 * 1024
RECLAIM_ATTEMPTS = 4
RECLAIM_SETTLE_SECONDS = 0.05
WRITEBACK_SETTLE_SECONDS = 0.10

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


def _read_key_values(path: Path) -> dict[str, int]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError):
        return {}
    values: dict[str, int] = {}
    for line in lines:
        parts = line.split()
        if len(parts) != 2:
            continue
        try:
            values[str(parts[0])] = int(parts[1])
        except ValueError:
            continue
    return values


def _cgroup_root() -> Path:
    return Path(os.getenv("SOLANA_ROI_CGROUP_ROOT", "/sys/fs/cgroup"))


def _cgroup_memory(root: Path | None = None) -> dict[str, int | float | None]:
    base = root or _cgroup_root()
    current = _read_scalar(base / "memory.current")
    maximum = _read_scalar(base / "memory.max")
    stat = _read_key_values(base / "memory.stat")
    events = _read_key_values(base / "memory.events")
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
        "anon_bytes": int(stat.get("anon", 0)) if stat else None,
        "file_bytes": int(stat.get("file", 0)) if stat else None,
        "file_dirty_bytes": int(stat.get("file_dirty", 0)) if stat else None,
        "file_writeback_bytes": int(stat.get("file_writeback", 0)) if stat else None,
        "slab_reclaimable_bytes": int(stat.get("slab_reclaimable", 0)) if stat else None,
        "oom_events": int(events.get("oom", 0)) if events else None,
        "oom_kill_events": int(events.get("oom_kill", 0)) if events else None,
    }


def _sqlite_cache_paths(path: Path) -> tuple[Path, Path, Path]:
    return path, Path(str(path) + "-wal"), Path(str(path) + "-shm")


def _wal_size_bytes(path: Path) -> int:
    try:
        return int(Path(str(path) + "-wal").stat().st_size)
    except OSError:
        return 0


def _passive_wal_checkpoint(path: Path) -> dict[str, Any]:
    """Best-effort zero-wait WAL checkpoint used only while raw cgroup pressure is high.

    PASSIVE never waits for readers or writers.  A busy or failed checkpoint is
    telemetry, not authority: the existing fail-closed raw-memory guard still decides
    whether a canonical read may continue.  Checkpointing moves already-committed WAL
    pages into the main database and does not change logical rows or strategy state.
    """

    result: dict[str, Any] = {
        "attempted": False,
        "busy": None,
        "log_frames": None,
        "checkpointed_frames": None,
        "error": None,
        "wal_bytes_before": _wal_size_bytes(path),
        "wal_bytes_after": None,
    }
    if not path.is_file():
        result["wal_bytes_after"] = result["wal_bytes_before"]
        return result

    connection: sqlite3.Connection | None = None
    try:
        result["attempted"] = True
        connection = sqlite3.connect(
            str(path),
            timeout=0.0,
            isolation_level=None,
            check_same_thread=False,
        )
        connection.execute("PRAGMA busy_timeout=0")
        row = connection.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()
        if row is not None and len(row) >= 3:
            result["busy"] = int(row[0])
            result["log_frames"] = int(row[1])
            result["checkpointed_frames"] = int(row[2])
    except (sqlite3.Error, OSError) as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        if connection is not None:
            try:
                connection.close()
            except sqlite3.Error:
                pass
        result["wal_bytes_after"] = _wal_size_bytes(path)
    return result


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


def _dirty_writeback_needed(state: dict[str, int | float | None]) -> bool:
    dirty = state.get("file_dirty_bytes")
    return isinstance(dirty, int) and dirty >= DIRTY_WRITEBACK_TRIGGER_BYTES


def _sync_sqlite_dirty_pages(path: Path) -> bool:
    """Force DB/WAL/SHM dirty pages to storage before asking Linux to evict them.

    ``POSIX_FADV_DONTNEED`` cannot discard dirty cache. Production telemetry proved
    that hundreds of MiB of dirty SQLite-backed pages can therefore keep raw cgroup
    memory pinned near the hard limit even though anonymous process memory is small.
    A read-only fdatasync only strengthens durability and does not alter rows or
    SQLite transaction semantics.
    """

    sync = getattr(os, "fdatasync", None) or getattr(os, "fsync", None)
    if sync is None:
        return False
    flushed = False
    for candidate in _sqlite_cache_paths(path):
        try:
            fd = os.open(candidate, os.O_RDONLY)
        except OSError:
            continue
        try:
            try:
                sync(fd)
                flushed = True
            except OSError:
                continue
        finally:
            os.close(fd)
    return flushed


def _trim_process_heap() -> bool:
    """Return unused Python/glibc heap pages without changing live object state."""

    try:
        gc.collect()
    except Exception:
        pass
    try:
        libc = ctypes.CDLL(None)
        trim = getattr(libc, "malloc_trim", None)
        if trim is None:
            return False
        trim.argtypes = [ctypes.c_size_t]
        trim.restype = ctypes.c_int
        return bool(trim(0))
    except Exception:
        return False


def _reclaim_budget(state: dict[str, int | float | None]) -> int:
    current = state.get("current_bytes")
    maximum = state.get("max_bytes")
    if not isinstance(current, int) or not isinstance(maximum, int) or maximum <= 0:
        return 0
    target = int(float(maximum) * RAW_RECLAIM_TARGET_FRACTION)
    needed = max(0, current - target)
    if needed <= 0:
        return 0
    return min(MAX_CGROUP_RECLAIM_BYTES, max(MIN_CGROUP_RECLAIM_BYTES, needed))


def _request_cgroup_file_reclaim(
    state: dict[str, int | float | None], root: Path | None = None
) -> bool:
    """Best-effort cgroup-v2 proactive reclaim, explicitly forbidding anon swap.

    Some hosts expose ``memory.reclaim`` read-only or do not support the swappiness
    key. In either case this simply returns false; the fail-closed guard remains the
    authority. We deliberately do not fall back to unrestricted reclaim because that
    could swap active anonymous runtime memory.
    """

    budget = _reclaim_budget(state)
    if budget <= 0:
        return False
    path = (root or _cgroup_root()) / "memory.reclaim"
    try:
        path.write_text(f"{budget} swappiness=0", encoding="ascii")
        return True
    except (OSError, UnicodeError):
        return False


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


def _safe_metric(value: object) -> str:
    return str(value) if isinstance(value, (int, float)) else "unknown"


def _emit_reclaim_telemetry(
    before: dict[str, int | float | None],
    after: dict[str, int | float | None],
    *,
    attempts: int,
    cgroup_requested: bool,
    heap_trimmed: bool,
    writeback_flushed: bool,
    checkpoint_attempts: int,
    checkpoint_busy: int | None,
    checkpoint_log_frames: int | None,
    checkpointed_frames: int,
    checkpoint_error: str | None,
    wal_bytes_before: int,
    wal_bytes_after: int,
    deferred: bool,
) -> None:
    # Numeric resource telemetry plus bounded error class/message: no paths, tokens,
    # payloads, market data, or canonical evidence contents.
    safe_checkpoint_error = (
        checkpoint_error.replace("\n", " ")[:160] if checkpoint_error is not None else "none"
    )
    print(
        "ROI_CGROUP_RECLAIM "
        f"before={_safe_metric(before.get('current_bytes'))} "
        f"after={_safe_metric(after.get('current_bytes'))} "
        f"max={_safe_metric(after.get('max_bytes'))} "
        f"anon={_safe_metric(after.get('anon_bytes'))} "
        f"file={_safe_metric(after.get('file_bytes'))} "
        f"dirty={_safe_metric(after.get('file_dirty_bytes'))} "
        f"writeback={_safe_metric(after.get('file_writeback_bytes'))} "
        f"slab_reclaimable={_safe_metric(after.get('slab_reclaimable_bytes'))} "
        f"oom_kill_events={_safe_metric(after.get('oom_kill_events'))} "
        f"attempts={attempts} cgroup_requested={str(cgroup_requested).lower()} "
        f"heap_trimmed={str(heap_trimmed).lower()} "
        f"writeback_flushed={str(writeback_flushed).lower()} "
        f"wal_checkpoint_attempts={checkpoint_attempts} "
        f"wal_checkpoint_busy={_safe_metric(checkpoint_busy)} "
        f"wal_checkpoint_log_frames={_safe_metric(checkpoint_log_frames)} "
        f"wal_checkpointed_frames={checkpointed_frames} "
        f"wal_checkpoint_error={safe_checkpoint_error} "
        f"wal_bytes_before={wal_bytes_before} wal_bytes_after={wal_bytes_after} "
        f"deferred={str(deferred).lower()}",
        flush=True,
    )


def _guard_raw_cgroup(path: Path) -> dict[str, int | float | None]:
    """Checkpoint WAL, flush dirty pages, reclaim clean cache, then fail closed."""

    before = _cgroup_memory()
    if not _needs_reclaim(before):
        return before

    after = before
    any_cgroup_request = False
    any_heap_trim = False
    any_writeback_flush = False
    checkpoint_attempts = 0
    checkpoint_busy: int | None = None
    checkpoint_log_frames: int | None = None
    checkpointed_frames = 0
    checkpoint_error: str | None = None
    wal_bytes_before = _wal_size_bytes(path)
    wal_bytes_after = wal_bytes_before
    attempts = 0
    for attempt in range(RECLAIM_ATTEMPTS):
        attempts = attempt + 1
        checkpoint = _passive_wal_checkpoint(path)
        if checkpoint.get("attempted"):
            checkpoint_attempts += 1
        if isinstance(checkpoint.get("busy"), int):
            checkpoint_busy = int(checkpoint["busy"])
        if isinstance(checkpoint.get("log_frames"), int):
            checkpoint_log_frames = int(checkpoint["log_frames"])
        if isinstance(checkpoint.get("checkpointed_frames"), int):
            checkpointed_frames += max(0, int(checkpoint["checkpointed_frames"]))
        if checkpoint.get("error"):
            checkpoint_error = str(checkpoint["error"])
        if isinstance(checkpoint.get("wal_bytes_after"), int):
            wal_bytes_after = int(checkpoint["wal_bytes_after"])

        checkpoint_wrote_pages = bool(
            isinstance(checkpoint.get("checkpointed_frames"), int)
            and int(checkpoint["checkpointed_frames"]) > 0
        )
        if _dirty_writeback_needed(after) or checkpoint_wrote_pages:
            any_writeback_flush = _sync_sqlite_dirty_pages(path) or any_writeback_flush
            if any_writeback_flush and WRITEBACK_SETTLE_SECONDS > 0:
                time.sleep(WRITEBACK_SETTLE_SECONDS)
        _release_sqlite_file_cache(path)
        any_heap_trim = _trim_process_heap() or any_heap_trim
        any_cgroup_request = _request_cgroup_file_reclaim(after) or any_cgroup_request
        if RECLAIM_SETTLE_SECONDS > 0:
            time.sleep(RECLAIM_SETTLE_SECONDS * attempts)
        after = _cgroup_memory()
        if not _needs_reclaim(after):
            _emit_reclaim_telemetry(
                before,
                after,
                attempts=attempts,
                cgroup_requested=any_cgroup_request,
                heap_trimmed=any_heap_trim,
                writeback_flushed=any_writeback_flush,
                checkpoint_attempts=checkpoint_attempts,
                checkpoint_busy=checkpoint_busy,
                checkpoint_log_frames=checkpoint_log_frames,
                checkpointed_frames=checkpointed_frames,
                checkpoint_error=checkpoint_error,
                wal_bytes_before=wal_bytes_before,
                wal_bytes_after=wal_bytes_after,
                deferred=False,
            )
            return after

    deferred = _critical(after)
    _emit_reclaim_telemetry(
        before,
        after,
        attempts=attempts,
        cgroup_requested=any_cgroup_request,
        heap_trimmed=any_heap_trim,
        writeback_flushed=any_writeback_flush,
        checkpoint_attempts=checkpoint_attempts,
        checkpoint_busy=checkpoint_busy,
        checkpoint_log_frames=checkpoint_log_frames,
        checkpointed_frames=checkpointed_frames,
        checkpoint_error=checkpoint_error,
        wal_bytes_before=wal_bytes_before,
        wal_bytes_after=wal_bytes_after,
        deferred=deferred,
    )
    if deferred:
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
            _trim_process_heap()


def _guarded_pinned_reader(store: Any) -> sqlite3.Connection:
    """Open one bounded logical-bootstrap reader or fail closed before OOM.

    The logical bootstrap transport is keyset-paginated and journal-reconciled, so
    this connection must not pin a page-wide WAL snapshot. Each bounded SELECT owns
    only its statement snapshot; the caller revalidates schema identity afterwards.
    """

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
    return reader


def _drop_file_cache_with_sidecars(path: Path) -> bool:
    path = Path(path)
    checkpoint = _passive_wal_checkpoint(path)
    if bool(
        isinstance(checkpoint.get("checkpointed_frames"), int)
        and int(checkpoint["checkpointed_frames"]) > 0
    ):
        _sync_sqlite_dirty_pages(path)
    released = _release_sqlite_file_cache(path)
    if _needs_reclaim(_cgroup_memory()):
        _trim_process_heap()
    return released


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
        "raw_reclaim_target_fraction": RAW_RECLAIM_TARGET_FRACTION,
        "raw_reclaim_reserve_bytes": RAW_RECLAIM_RESERVE_BYTES,
        "raw_critical_reserve_bytes": RAW_CRITICAL_RESERVE_BYTES,
        "dirty_writeback_trigger_bytes": DIRTY_WRITEBACK_TRIGGER_BYTES,
        "reclaim_attempts": RECLAIM_ATTEMPTS,
        "cgroup_file_reclaim_best_effort": True,
        "cgroup_reclaim_swappiness_zero": True,
        "heap_trim_under_pressure": True,
        "targeted_sqlite_dirty_writeback": True,
        "passive_wal_checkpoint_under_pressure": True,
        "wal_checkpoint_busy_timeout_ms": 0,
        "wal_checkpoint_changes_logical_state": False,
        "writeback_changes_logical_state": False,
        "cgroup_memory": _cgroup_memory(),
        "full_hash_chain_verification_preserved": True,
        "logical_bootstrap_keyset_semantics_preserved": True,
        "logical_bootstrap_page_wide_transaction": False,
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
