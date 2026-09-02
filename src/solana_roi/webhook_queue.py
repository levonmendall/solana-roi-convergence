from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .observation_store import ObservationEventStore


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True, slots=True)
class QueuedWebhook:
    id: int
    received_at: datetime
    payload: Any
    payload_sha256: str
    attempts: int


class DurableHeliusWebhookQueue:
    """Durable Helius intake queue.

    The HTTP request is acknowledged only after its payload is committed to
    SQLite. Rows remain pending until the complete downstream ingestion path
    succeeds. Reprocessing is safe because normalized swaps are idempotent.
    """

    def __init__(self, store: ObservationEventStore):
        self.store = store
        with store._lock, store.db:
            store.db.execute(
                "CREATE TABLE IF NOT EXISTS helius_webhook_inbox ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, received_at TEXT NOT NULL, "
                "payload_json TEXT NOT NULL, payload_sha256 TEXT NOT NULL UNIQUE, "
                "state TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0, "
                "last_error TEXT, completed_at TEXT)"
            )
            store.db.execute(
                "CREATE INDEX IF NOT EXISTS ix_helius_webhook_inbox_state_id "
                "ON helius_webhook_inbox(state, id)"
            )

    @staticmethod
    def _encode(payload: Any) -> tuple[str, str]:
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
        return raw, hashlib.sha256(raw.encode()).hexdigest()

    def enqueue(self, payload: Any, *, received_at: datetime | None = None) -> tuple[int, bool]:
        at = received_at or utcnow()
        raw, digest = self._encode(payload)
        with self.store._lock, self.store.db:
            cursor = self.store.db.execute(
                "INSERT OR IGNORE INTO helius_webhook_inbox(received_at, payload_json, payload_sha256) "
                "VALUES (?, ?, ?)",
                (at.isoformat(), raw, digest),
            )
            if cursor.rowcount == 1:
                row_id = int(cursor.lastrowid)
                inserted = True
            else:
                row = self.store.db.execute(
                    "SELECT id FROM helius_webhook_inbox WHERE payload_sha256=?",
                    (digest,),
                ).fetchone()
                if row is None:
                    raise RuntimeError("webhook queue deduplication lookup failed")
                row_id = int(row[0])
                inserted = False
        if inserted:
            self.store.append(
                "helius_webhook_enqueued",
                at.isoformat(),
                {"inbox_id": row_id, "payload_sha256": digest},
            )
        return row_id, inserted

    def next_pending(self) -> QueuedWebhook | None:
        with self.store._lock:
            row = self.store.db.execute(
                "SELECT id, received_at, payload_json, payload_sha256, attempts "
                "FROM helius_webhook_inbox WHERE state='pending' ORDER BY id LIMIT 1"
            ).fetchone()
        if row is None:
            return None
        return QueuedWebhook(
            id=int(row["id"]),
            received_at=datetime.fromisoformat(str(row["received_at"])),
            payload=json.loads(str(row["payload_json"])),
            payload_sha256=str(row["payload_sha256"]),
            attempts=int(row["attempts"]),
        )

    def mark_complete(self, row: QueuedWebhook, *, completed_at: datetime | None = None) -> None:
        at = completed_at or utcnow()
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "UPDATE helius_webhook_inbox SET state='complete', attempts=attempts+1, "
                "last_error=NULL, completed_at=? WHERE id=? AND state='pending'",
                (at.isoformat(), row.id),
            )
        self.store.append(
            "helius_webhook_completed",
            at.isoformat(),
            {"inbox_id": row.id, "payload_sha256": row.payload_sha256},
        )

    def mark_retry(self, row: QueuedWebhook, error: Exception, *, observed_at: datetime | None = None) -> None:
        at = observed_at or utcnow()
        message = f"{type(error).__name__}:{str(error)}"[:1000]
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "UPDATE helius_webhook_inbox SET attempts=attempts+1, last_error=? "
                "WHERE id=? AND state='pending'",
                (message, row.id),
            )
        self.store.append(
            "helius_webhook_retry",
            at.isoformat(),
            {"inbox_id": row.id, "payload_sha256": row.payload_sha256, "error": message},
        )

    def status(self) -> dict[str, Any]:
        with self.store._lock:
            pending = int(self.store.db.execute(
                "SELECT COUNT(*) FROM helius_webhook_inbox WHERE state='pending'"
            ).fetchone()[0])
            complete = int(self.store.db.execute(
                "SELECT COUNT(*) FROM helius_webhook_inbox WHERE state='complete'"
            ).fetchone()[0])
            row = self.store.db.execute(
                "SELECT attempts, last_error FROM helius_webhook_inbox "
                "WHERE state='pending' ORDER BY id LIMIT 1"
            ).fetchone()
        return {
            "durable": True,
            "pending": pending,
            "complete": complete,
            "oldest_pending_attempts": int(row["attempts"]) if row else 0,
            "oldest_pending_last_error": str(row["last_error"]) if row and row["last_error"] else None,
        }


class HeliusWebhookWorker:
    def __init__(
        self,
        *,
        queue: DurableHeliusWebhookQueue,
        service: Any,
        idle_sleep_seconds: float = 0.05,
        error_sleep_seconds: float = 0.5,
    ):
        self.queue = queue
        self.service = service
        self.idle_sleep_seconds = idle_sleep_seconds
        self.error_sleep_seconds = error_sleep_seconds

    async def run(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            row = self.queue.next_pending()
            if row is None:
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=self.idle_sleep_seconds)
                except asyncio.TimeoutError:
                    pass
                continue
            try:
                await self.service.ingest_webhook(row.payload, received_at=row.received_at)
            except Exception as exc:
                self.queue.mark_retry(row, exc)
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=self.error_sleep_seconds)
                except asyncio.TimeoutError:
                    pass
                continue
            self.queue.mark_complete(row)
