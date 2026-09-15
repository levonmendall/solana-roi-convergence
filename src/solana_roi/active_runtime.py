from __future__ import annotations

import hashlib
import json
import os
import signal
import sqlite3
import threading
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
                    # Maintenance failure cannot be allowed to turn into silent
                    # unbounded accumulation. Persist a tiny restart-safe marker
                    # and enter the quiescent rollover path on the next process.
                    self._request_quiescent_rollover(
                        reason=f"maintenance_failure:{type(exc).__name__}"
                    )
                    return

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

    def verify(self) -> bool:
        previous = self.transition_event_head_hash
        expected_id_floor = self.transition_event_head_id
        with self._verify_lock:
            reader: sqlite3.Connection | None = None
            try:
                uri = f"{self.path.resolve().as_uri()}?mode=ro&cache=private"
                reader = sqlite3.connect(uri, uri=True, check_same_thread=False)
                reader.execute("PRAGMA query_only=ON")
                reader.execute("BEGIN")
                rows = reader.execute(
                    "SELECT id,event_type,observed_at,payload_json,previous_hash,lineage_hash FROM events ORDER BY id"
                )
                last_id = expected_id_floor
                for event_id, event_type, observed_at, raw, recorded_previous, lineage in rows:
                    if int(event_id) <= expected_id_floor or int(event_id) <= last_id:
                        return False
                    if str(recorded_previous) != previous:
                        return False
                    expected = hashlib.sha256(
                        f"{previous}|{event_type}|{observed_at}|{raw}".encode()
                    ).hexdigest()
                    if expected != str(lineage):
                        return False
                    previous = str(lineage)
                    last_id = int(event_id)
                return True
            finally:
                if reader is not None:
                    reader.close()
                self._release_verification_file_cache()

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
        if not store.verify():
            return False, 0, None
        with store._lock:
            head = store.db.execute("SELECT id FROM events ORDER BY id DESC LIMIT 1").fetchone()
            placeholders = ",".join("?" for _ in _ENGINE_EVENT_TYPES)
            latest = store.db.execute(
                f"SELECT id FROM events WHERE event_type IN ({placeholders}) ORDER BY id DESC LIMIT 1",
                _ENGINE_EVENT_TYPES,
            ).fetchone()
        verified_through = int(head["id"]) if head is not None else store.transition_event_head_id
        latest_engine = int(latest["id"]) if latest is not None else store.transition_engine_event_id
        return True, verified_through, latest_engine

    def _checkpoint_event_marker_valid(self, event_id: int) -> bool:
        store = self.store
        if isinstance(store, ActiveObservationEventStore) and int(event_id) == store.transition_engine_event_id:
            return True
        return super()._checkpoint_event_marker_valid(event_id)
