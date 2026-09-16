from __future__ import annotations

"""Bound authoritative event-ledger verification without weakening history checks.

The durable paper engine must validate every retained event before restoring authority.
This repair keeps that complete hash-chain verification, but it no longer pins one
SQLite read transaction across the entire ledger. Startup captures a stable terminal
frontier, verifies bounded keyset chunks through that frontier, closes each reader
before file-cache advice/reclaim, and finally proves that the terminal frontier did
not move while verification was in progress.

The incremental checkpoint remains a non-authoritative acceleration/telemetry sidecar.
It is refreshed after a successful full verification but is never trusted to skip old
retained rows on authoritative startup.
"""

import hashlib
import sqlite3
import time
from pathlib import Path
from typing import Any

from . import durable_bootstrap_memory_repair as memory

REPAIR_VERSION = "authoritative-event-full-verify-bounded-v1"
DEFAULT_VERIFY_CHUNK_ROWS = 4_096
VERIFY_CHUNK_ROWS = DEFAULT_VERIFY_CHUNK_ROWS
ENGINE_EVENT_TYPES = frozenset(
    {"first_touch", "confirmation", "price", "trade_intent", "trade_outcome"}
)

PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
STRATEGY_THRESHOLDS_CHANGED = False
CERTIFICATION_THRESHOLDS_CHANGED = False
CANONICAL_EVIDENCE_RESET = False
RETENTION_SEMANTICS_CHANGED = False

_INSTALLED = False
_INVOCATION_COUNT = 0
_LAST_STATUS: dict[str, Any] = {
    "invocation": 0,
    "verified": None,
    "rows": 0,
    "chunks": 0,
    "failure_reason": None,
}


def _effective_chunk_rows() -> int:
    """Resolve both the new knob and the pre-existing memory-repair contract.

    Production defaults remain 4,096 rows. Tests/operators that intentionally tune
    this repair's new knob win when they change it from the default; otherwise the
    long-standing ``VERIFY_CACHE_RELEASE_ROWS`` setting continues to control how
    often a reader is closed and clean SQLite pages are reclaimed.
    """

    try:
        local_rows = max(1, int(VERIFY_CHUNK_ROWS))
    except (TypeError, ValueError):
        local_rows = DEFAULT_VERIFY_CHUNK_ROWS
    if local_rows != DEFAULT_VERIFY_CHUNK_ROWS:
        return local_rows
    try:
        return max(
            1,
            int(
                getattr(
                    memory,
                    "VERIFY_CACHE_RELEASE_ROWS",
                    DEFAULT_VERIFY_CHUNK_ROWS,
                )
            ),
        )
    except (TypeError, ValueError):
        return DEFAULT_VERIFY_CHUNK_ROWS


def _reader(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(
        f"{path.resolve().as_uri()}?mode=ro",
        uri=True,
        timeout=5.0,
        check_same_thread=False,
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    connection.execute("PRAGMA busy_timeout=5000")
    connection.execute(f"PRAGMA cache_size=-{memory.SQLITE_READER_CACHE_KIB}")
    connection.execute("PRAGMA mmap_size=0")
    return connection


def _frontier(store: Any) -> tuple[int, str | None]:
    with store._lock:
        row = store.db.execute(
            "SELECT id,lineage_hash FROM events ORDER BY id DESC LIMIT 1"
        ).fetchone()
    if row is None:
        return 0, None
    return int(row["id"]), str(row["lineage_hash"])


def _row_hash(event_type: Any, observed_at: Any, raw: Any, previous_hash: Any) -> str:
    return hashlib.sha256(
        f"{previous_hash or ''}|{event_type}|{observed_at}|{raw}".encode()
    ).hexdigest()


def _persist_non_authoritative_checkpoint(
    store: Any,
    *,
    event_id: int,
    lineage_hash: str | None,
    latest_engine_event_id: int | None,
) -> None:
    """Refresh the existing sidecar after full verification; never make it authority."""

    try:
        from . import incremental_event_integrity_repair as incremental

        payload = incremental._checkpoint_payload(
            event_id=int(event_id),
            lineage_hash=lineage_hash,
            latest_engine_event_id=latest_engine_event_id,
        )
        incremental._persist_runtime_checkpoint(store, payload)
    except (OSError, sqlite3.Error, TypeError, ValueError, RuntimeError):
        # The sidecar is optional acceleration/telemetry. A successful complete
        # retained-history verification must not be converted into a false failure
        # merely because this non-canonical file could not be refreshed.
        return


def _bounded_authoritative_verify(self: Any) -> tuple[bool, int, int | None]:
    global _INVOCATION_COUNT, _LAST_STATUS

    store = self.store
    source_path = Path(store.path)
    started = time.monotonic()
    verified = False
    failure_reason: str | None = "not_started"
    rows_processed = 0
    chunks = 0
    verified_through_event_id = 0
    latest_engine_event_id: int | None = None
    terminal_id = 0
    terminal_hash: str | None = None
    chunk_rows = _effective_chunk_rows()

    with store._verify_lock:
        _INVOCATION_COUNT += 1
        invocation = _INVOCATION_COUNT
        before = memory._cgroup_memory()
        print(
            "ROI_FULL_LEDGER_VERIFY "
            f"stage=start invocation={invocation} reason=durable_engine_start "
            f"chunk_rows={chunk_rows} "
            f"memory_current={before.get('current_bytes')} file={before.get('file_bytes')}",
            flush=True,
        )
        try:
            memory._guard_raw_cgroup(source_path)
            terminal_id, terminal_hash = _frontier(store)
            previous: str | None = None
            last_id = 0
            failure_reason = None

            while last_id < terminal_id:
                connection: sqlite3.Connection | None = None
                batch: list[sqlite3.Row] = []
                try:
                    connection = _reader(source_path)
                    batch = list(
                        connection.execute(
                            "SELECT id,event_type,observed_at,payload_json,previous_hash,lineage_hash "
                            "FROM events WHERE id>? AND id<=? ORDER BY id LIMIT ?",
                            (last_id, terminal_id, chunk_rows),
                        ).fetchall()
                    )
                finally:
                    if connection is not None:
                        connection.close()

                if not batch:
                    failure_reason = "captured_frontier_unreachable"
                    return False, 0, None

                for row in batch:
                    event_id = int(row["id"])
                    if event_id != last_id + 1:
                        failure_reason = "event_id_continuity_invalid"
                        return False, 0, None
                    recorded_previous = row["previous_hash"]
                    lineage = str(row["lineage_hash"])
                    if recorded_previous != previous:
                        failure_reason = "previous_hash_invalid"
                        return False, 0, None
                    if (
                        _row_hash(
                            row["event_type"],
                            row["observed_at"],
                            row["payload_json"],
                            recorded_previous,
                        )
                        != lineage
                    ):
                        failure_reason = "lineage_hash_invalid"
                        return False, 0, None
                    previous = lineage
                    last_id = event_id
                    verified_through_event_id = event_id
                    rows_processed += 1
                    if str(row["event_type"]) in ENGINE_EVENT_TYPES:
                        latest_engine_event_id = event_id

                chunks += 1
                # The SQLite reader is fully closed before any DONTNEED/reclaim call,
                # so the kernel can actually discard clean historical pages.
                memory._release_sqlite_file_cache(source_path)
                memory._guard_raw_cgroup(source_path)

            if verified_through_event_id != terminal_id or previous != terminal_hash:
                failure_reason = "terminal_frontier_mismatch"
                return False, 0, None

            # Startup is expected to be quiescent. Fail closed rather than silently
            # accepting a ledger that changed between independently bounded snapshots.
            final_id, final_hash = _frontier(store)
            if final_id != terminal_id or final_hash != terminal_hash:
                failure_reason = "frontier_changed_during_verification"
                return False, 0, None

            verified = True
            failure_reason = None
            _persist_non_authoritative_checkpoint(
                store,
                event_id=verified_through_event_id,
                lineage_hash=terminal_hash,
                latest_engine_event_id=latest_engine_event_id,
            )
            return True, verified_through_event_id, latest_engine_event_id
        finally:
            memory._release_sqlite_file_cache(source_path)
            memory._trim_process_heap()
            after = memory._cgroup_memory()
            _LAST_STATUS = {
                "invocation": invocation,
                "verified": verified,
                "rows": rows_processed,
                "chunks": chunks,
                "chunk_rows": chunk_rows,
                "terminal_event_id": terminal_id,
                "verified_through_event_id": verified_through_event_id,
                "latest_engine_event_id": latest_engine_event_id,
                "failure_reason": failure_reason,
                "duration_seconds": max(0.0, time.monotonic() - started),
                "memory_before_bytes": before.get("current_bytes"),
                "memory_after_bytes": after.get("current_bytes"),
                "file_before_bytes": before.get("file_bytes"),
                "file_after_bytes": after.get("file_bytes"),
            }
            print(
                "ROI_FULL_LEDGER_VERIFY "
                f"stage=end invocation={invocation} verified={str(verified).lower()} "
                f"rows={rows_processed} chunks={chunks} terminal_event_id={terminal_id} "
                f"failure={failure_reason or 'none'} "
                f"memory_current={after.get('current_bytes')} file={after.get('file_bytes')}",
                flush=True,
            )


def configure_authoritative_event_verify_bounded_repair() -> None:
    """Install before production composition constructs the durable engine."""

    global _INSTALLED
    if _INSTALLED:
        return

    from . import incremental_event_integrity_repair as incremental

    # production_system.install_durable_bootstrap_memory_repair() assigns this symbol
    # to DurablePaperTradingEngine._verify_engine_snapshot. Marking the replacement
    # with the existing bounded attribute preserves that composition contract while
    # changing only the verification transport, not authority or economic behavior.
    setattr(_bounded_authoritative_verify, "_roi_durable_bootstrap_memory_bounded", True)
    memory._bounded_verify_engine_snapshot = _bounded_authoritative_verify

    # Keep explicit full_integrity_audit() on the same bounded complete-history path.
    # The ordinary startup sidecar remains maintained, but it no longer authorizes a
    # tail-only startup shortcut.
    incremental._ORIGINAL_BOUNDED_VERIFY = _bounded_authoritative_verify
    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "repair_version": REPAIR_VERSION,
        "installed": _INSTALLED,
        "verify_chunk_rows": _effective_chunk_rows(),
        "invocation_count": _INVOCATION_COUNT,
        "last": dict(_LAST_STATUS),
        "authoritative_startup_verification": "complete_retained_history",
        "incremental_checkpoint_authoritative": False,
        "reader_closed_before_cache_release": True,
        "stable_terminal_frontier_required": True,
        "full_hash_chain_verification_preserved": True,
        "retention_semantics_changed": RETENTION_SEMANTICS_CHANGED,
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
    "configure_authoritative_event_verify_bounded_repair",
    "status",
]
