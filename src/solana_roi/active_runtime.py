from __future__ import annotations

import hashlib
import json
import os
import signal
import sqlite3
import threading
import time
from contextlib import closing
from pathlib import Path
from typing import Any

# Importing the manifest extends the base retention registry before active
# runtime schemas are validated.
from . import storage_manifest as _storage_manifest  # noqa: F401
from .active_storage import ActiveStorage
from .active_storage_epoch_rollover import request_rollover, rollover_active_epoch_if_needed
from .durable_engine import DurablePaperTradingEngine, _ENGINE_EVENT_TYPES
from .observation_store import ObservationEventStore
from .storage_active_compat_pruning import prune_active_compatibility_database
from .storage_current_v52_pruning import prune_current_v52_database
from .storage_transition import load_verified_checkpoint


ACTIVE_VERIFY_CHUNK_ROWS = 2048
ACTIVE_VERIFY_CACHE_BYTES = 16 * 1024 * 1024


def _verification_io() -> dict[str, int]:
    try:
        return {key: int(value) for key, value in (line.split(":", 1) for line in Path("/proc/self/io").read_text().splitlines())}
    except (OSError, ValueError):
        return {}


class ActiveObservationEventStore(ObservationEventStore):
    """Bounded compatibility adapter over the compact active database.

    The pre-transition event body is deliberately absent. The verified
    transition checkpoint seals its exact high-water ID and lineage hash. New
    events continue from that hash and from high-water+1, preserving lineage and
    monotonically increasing IDs without opening the legacy database.
    """

    active_storage_mode = True

    def __init__(self, path: str | Path, *, expected_release_sha: str | None = None):
        self.path = Path(path)
        self.rollover_report: dict[str, Any] | None = None
        if expected_release_sha:
            # Runtime composition reaches this constructor before the long-lived
            # active SQLite connection or production workers exist. That is the
            # only safe automatic boundary for an active-to-active epoch swap.
            self.rollover_report = rollover_active_epoch_if_needed(
                self.path,
                release_sha=expected_release_sha,
            )
        self.transition_checkpoint = load_verified_checkpoint(self.path, expected_release_sha=expected_release_sha)
        event_head = dict(self.transition_checkpoint.get("latest_event_ids", {}).get("events") or {})
        if not event_head:
            raise RuntimeError("active runtime blocked: transition event head missing")
        try:
            self.transition_event_head_id = int(event_head["id"])
            self.transition_event_head_hash = str(event_head["lineage_hash"])
            self.transition_engine_event_id = int(self.transition_checkpoint["portfolio"]["last_engine_event_id"])
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeError("active runtime blocked: transition event anchor invalid") from exc
        if self.transition_event_head_id < self.transition_engine_event_id:
            raise RuntimeError("active runtime blocked: event head precedes paper checkpoint")
        if len(self.transition_event_head_hash) != 64:
            raise RuntimeError("active runtime blocked: transition lineage hash invalid")
        self._active_write_count = 0
        self._maintenance_stop = threading.Event()
        self._maintenance_thread: threading.Thread | None = None
        super().__init__(self.path)
        with self._lock, self.db:
            local = self.db.execute("SELECT id FROM events ORDER BY id LIMIT 1").fetchone()
            if local is not None and int(local["id"]) <= self.transition_event_head_id:
                raise RuntimeError("active runtime blocked: historical event body present in active database")
            # sqlite_sequence has no declared UNIQUE constraint on name, so do
            # not use ON CONFLICT(name). Advance the sequence explicitly.
            if local is None:
                seq = self.db.execute("SELECT seq FROM sqlite_sequence WHERE name='events'").fetchone()
                if seq is None:
                    self.db.execute(
                        "INSERT INTO sqlite_sequence(name,seq) VALUES('events',?)",
                        (self.transition_event_head_id,),
                    )
                elif int(seq[0] or 0) < self.transition_event_head_id:
                    self.db.execute(
                        "UPDATE sqlite_sequence SET seq=? WHERE name='events'",
                        (self.transition_event_head_id,),
                    )
        self._start_independent_storage_maintenance()

    def _maintenance_interval_seconds(self) -> float:
        raw = (os.getenv("SOLANA_ROI_ACTIVE_STORAGE_MAINTENANCE_SECONDS") or "60").strip()
        try:
            value = float(raw)
        except ValueError:
            value = 60.0
        return max(0.0, value)

    def _start_independent_storage_maintenance(self) -> None:
        interval = self._maintenance_interval_seconds()
        if interval <= 0:
            return

        def run() -> None:
            while not self._maintenance_stop.wait(interval):
                try:
                    if self._bounded_maintenance():
                        return
                except Exception as exc:
                    # A transient SQLite lock or a single pruning failure must
                    # not create an endless restart loop while the active store
                    # still has storage headroom. The persisted page ceiling is
                    # the final fail-closed boundary for independent writers.
                    try:
                        storage = ActiveStorage(self.path)
                        sizes = storage.storage_bytes()
                        over_warning = storage.warning_boundary_exceeded()
                        status_readable = True
                    except Exception as status_exc:
                        sizes = {}
                        over_warning = True
                        status_readable = False
                        status_error = f"{type(status_exc).__name__}:{status_exc}"
                    payload = {
                        "error_type": type(exc).__name__,
                        "error": str(exc),
                        "path": str(self.path),
                        "status_readable": status_readable,
                        "over_warning": over_warning,
                        "sizes": sizes,
                        "paper_only": True,
                        "live_money_authority": False,
                    }
                    if not status_readable:
                        payload["status_error"] = status_error
                    print(
                        "ROI_ACTIVE_STORAGE_MAINTENANCE_FAILED "
                        + json.dumps(payload, sort_keys=True),
                        flush=True,
                    )
                    if over_warning:
                        self._request_quiescent_rollover(
                            reason=f"maintenance_failure_at_boundary:{type(exc).__name__}"
                        )
                        return
                    # Keep the current process alive and retry at the next
                    # interval. No strategy/certification state is modified.
                    continue

        self._maintenance_thread = threading.Thread(
            target=run,
            name="active-storage-maintenance",
            daemon=True,
        )
        self._maintenance_thread.start()

    def _request_quiescent_rollover(self, *, reason: str) -> None:
        if self._maintenance_stop.is_set():
            return
        storage = ActiveStorage(self.path)
        sizes = storage.storage_bytes()
        marker = request_rollover(self.path, reason=reason, sizes=sizes)
        payload = {
            "reason": reason,
            "path": str(self.path),
            "marker": str(marker),
            "main_bytes": sizes["main"],
            "wal_bytes": sizes["wal"],
            "warning_bytes": storage.budget.warning_bytes,
            "hard_bytes": storage.budget.hard_bytes,
            "paper_only": True,
            "live_money_authority": False,
        }
        print("ROI_ACTIVE_STORAGE_ROLLOVER_REQUESTED " + json.dumps(payload, sort_keys=True), flush=True)
        self._maintenance_stop.set()
        # SIGTERM lets uvicorn execute its normal graceful shutdown. SQLite
        # transactions retain their atomicity; the next startup rolls over before
        # any long-lived DB connection or worker is created.
        os.kill(os.getpid(), signal.SIGTERM)

    def _bounded_maintenance(self) -> bool:
        storage = ActiveStorage(self.path)
        storage.prune_v52_market_validation()
        storage.prune_v52_wallet_forward_alpha()
        prune_current_v52_database(self.path)
        prune_active_compatibility_database(self.path)
        storage.prune_expired_diagnostics()
        storage.prune_acknowledged_transport()
        storage.checkpoint_wal()
        storage.reclaim_free_pages()
        storage.assert_positive_schema()
        if storage.warning_boundary_exceeded():
            self._request_quiescent_rollover(reason="warning_boundary_exceeded_after_prune")
            return True
        storage.enforce_hard_budget()
        return False

    def append(self, event_type: str, observed_at: str, payload: dict[str, Any]) -> str:
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
        with self._lock, self.db:
            row = self.db.execute("SELECT id,lineage_hash FROM events ORDER BY id DESC LIMIT 1").fetchone()
            previous = str(row["lineage_hash"]) if row is not None else self.transition_event_head_hash
            lineage = hashlib.sha256(f"{previous}|{event_type}|{observed_at}|{raw}".encode()).hexdigest()
            self.db.execute(
                "INSERT INTO events(event_type,observed_at,payload_json,previous_hash,lineage_hash) VALUES(?,?,?,?,?)",
                (event_type, observed_at, raw, previous, lineage),
            )
            new_row = self.db.execute("SELECT id FROM events WHERE lineage_hash=?", (lineage,)).fetchone()
            if new_row is None or int(new_row["id"]) <= self.transition_event_head_id:
                raise RuntimeError("active event id did not advance beyond transition frontier")
        self._active_write_count += 1
        # Bound maintenance work without turning every evidence write into a
        # pruning transaction. Independent time-based maintenance above covers
        # compatibility writers that never pass through append().
        if self._active_write_count % 128 == 0:
            self._bounded_maintenance()
        return lineage

    def _release_verification_file_cache(self) -> None:
        from .durable_bootstrap_memory_repair import _release_sqlite_file_cache
        _release_sqlite_file_cache(self.path)

    def _verification_reader(self) -> sqlite3.Connection:
        reader = sqlite3.connect(
            f"{self.path.resolve().as_uri()}?mode=ro&cache=private",
            uri=True, timeout=5.0, check_same_thread=False,
        )
        try:
            reader.execute("PRAGMA query_only=ON")
            reader.execute("PRAGMA cache_size=-2048")
            reader.execute("PRAGMA mmap_size=0")
            return reader
        except Exception:
            reader.close()
            raise

    def _verify_active_snapshot(self, *, reason: str) -> tuple[bool, int, int | None]:
        """Verify every retained row in bounded, independently closed readers.

        The transition anchor seals the removed prefix; no mutable sidecar skips
        retained events. A read-only data_version witness has no transaction and
        pins no WAL snapshot. Any commit during the audit fails closed, including
        changes to rows already scanned. This deliberately never blesses a mixture
        of snapshots from different database versions.
        """
        from .durable_bootstrap_memory_repair import _cgroup_memory

        with self._verify_lock:
            started = time.monotonic()
            before = _cgroup_memory()
            peak = int(before.get("current_bytes") or 0)
            io_before = _verification_io()
            invocation = int(getattr(self, "_verification_invocations", 0)) + 1
            self._verification_invocations = invocation
            rows_verified = chunks = 0
            hashed_bytes = cache_bytes = 0
            last_id = self.transition_event_head_id
            previous = self.transition_event_head_hash
            latest_engine = self.transition_engine_event_id
            verified = False
            failure = "not_started"
            try:
                identity = self.path.stat()
                with closing(self._verification_reader()) as witness:
                    version = witness.execute("PRAGMA data_version").fetchone()[0]
                    with closing(self._verification_reader()) as reader:
                        reader.execute("BEGIN")
                        head = reader.execute("SELECT id,lineage_hash FROM events ORDER BY id DESC LIMIT 1").fetchone()
                        sequence = reader.execute("SELECT seq FROM sqlite_sequence WHERE name='events'").fetchone()
                    terminal_id = int(head[0]) if head else last_id
                    terminal_hash = str(head[1]) if head else previous
                    if sequence is None or int(sequence[0]) != terminal_id or terminal_id < last_id:
                        failure = "retained_tail_sequence_mismatch"
                        return False, 0, None
                    while last_id < terminal_id:
                        with closing(self._verification_reader()) as reader:
                            rows = reader.execute(
                                "SELECT id,event_type,observed_at,payload_json,previous_hash,lineage_hash "
                                "FROM events WHERE id>? AND id<=? ORDER BY id LIMIT ?",
                                (last_id, terminal_id, ACTIVE_VERIFY_CHUNK_ROWS),
                            ).fetchall()
                        chunks += 1
                        if not rows:
                            failure = "retained_frontier_unreachable"
                            return False, 0, None
                        for event_id, event_type, observed_at, raw, recorded_previous, lineage in rows:
                            if int(event_id) != last_id + 1:
                                failure = "event_id_discontinuity"
                                return False, 0, None
                            if str(recorded_previous) != previous:
                                failure = "previous_hash_mismatch"
                                return False, 0, None
                            encoded = f"{previous}|{event_type}|{observed_at}|{raw}".encode()
                            expected = hashlib.sha256(encoded).hexdigest()
                            hashed_bytes += len(encoded)
                            cache_bytes += len(encoded)
                            if expected != str(lineage):
                                failure = "lineage_hash_mismatch"
                                return False, 0, None
                            last_id, previous = int(event_id), str(lineage)
                            rows_verified += 1
                            if str(event_type) in _ENGINE_EVENT_TYPES:
                                latest_engine = last_id
                        del rows
                        # Keep a bounded read-ahead window instead of discarding
                        # prefetched pages after every small keyset query.
                        # No chunk reader or transaction remains during advice.
                        if cache_bytes >= ACTIVE_VERIFY_CACHE_BYTES:
                            self._release_verification_file_cache()
                            cache_bytes = 0
                        peak = max(peak, int(_cgroup_memory().get("current_bytes") or 0))
                        if witness.execute("PRAGMA data_version").fetchone()[0] != version:
                            failure = "database_changed_during_verification"
                            return False, 0, None
                    final_identity = self.path.stat()
                    if (final_identity.st_dev, final_identity.st_ino) != (identity.st_dev, identity.st_ino):
                        failure = "database_replaced_during_verification"
                        return False, 0, None
                    if witness.execute("PRAGMA data_version").fetchone()[0] != version:
                        failure = "database_changed_during_verification"
                        return False, 0, None
                    if last_id != terminal_id or previous != terminal_hash:
                        failure = "retained_frontier_mismatch"
                        return False, 0, None
                    verified, failure = True, None
                    return True, last_id, latest_engine
            except Exception as exc:
                failure = type(exc).__name__
                raise
            finally:
                self._release_verification_file_cache()
                after = _cgroup_memory()
                io_after = _verification_io()
                self.verification_status = {
                    "invocation": invocation, "reason": reason, "verified": verified,
                    "failure_reason": failure, "rows": rows_verified, "chunks": chunks,
                    "chunk_rows": ACTIVE_VERIFY_CHUNK_ROWS,
                    "event_bytes_hashed": hashed_bytes,
                    "cache_release_interval_bytes": ACTIVE_VERIFY_CACHE_BYTES,
                    "verified_through_event_id": last_id if verified else None,
                    "duration_seconds": time.monotonic() - started,
                    "memory_before": before, "memory_after": after,
                    "sampled_peak_memory_current_bytes": max(peak, int(after.get("current_bytes") or 0)),
                    "process_physical_read_bytes": max(0, io_after.get("read_bytes", 0) - io_before.get("read_bytes", 0)),
                    "process_read_chars": max(0, io_after.get("rchar", 0) - io_before.get("rchar", 0)),
                    "io_scope": "process_including_concurrent_workers",
                    "reader_closed_before_cache_advice": True,
                    "paper_only": True, "live_money_authority": False,
                }
                print("ROI_ACTIVE_LEDGER_VERIFY " + json.dumps(self.verification_status, sort_keys=True), flush=True)

    def verify(self) -> bool:
        return self._verify_active_snapshot(reason="explicit_full_audit")[0]

    def close(self) -> None:
        self._maintenance_stop.set()
        thread = self._maintenance_thread
        if thread is not None and thread.is_alive() and thread is not threading.current_thread():
            thread.join(timeout=2.0)
        super().close()


class ActiveDurablePaperTradingEngine(DurablePaperTradingEngine):
    """Paper engine restore that trusts only the sealed transition anchor + active tail."""

    def _verify_engine_snapshot(self) -> tuple[bool, int, int | None]:
        store = self.store
        if not isinstance(store, ActiveObservationEventStore):
            return super()._verify_engine_snapshot()
        return store._verify_active_snapshot(reason="durable_engine_restore")

    def _checkpoint_event_marker_valid(self, event_id: int) -> bool:
        store = self.store
        if isinstance(store, ActiveObservationEventStore) and int(event_id) == store.transition_engine_event_id:
            return True
        return super()._checkpoint_event_marker_valid(event_id)
