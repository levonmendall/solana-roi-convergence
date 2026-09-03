from __future__ import annotations

import asyncio
import hashlib
from collections import defaultdict
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Any

from . import production_capacity_repair as capacity
from . import raw_receipt_dispatch_repair as raw_dispatch
from .direct_solana import DirectSolanaIngestionPlane


FULL_SCOPE_BATCH_MAX = capacity.RAW_RECEIPT_BATCH_MAX


def _launch_like(self: Any, item: Any) -> bool:
    _priority, _mono, _sequence, _received_at, _provider, _targets, message = capacity._parse_dispatch_item(item)
    params = message.get("params") if isinstance(message, dict) else None
    result = params.get("result") if isinstance(params, dict) else None
    value = result.get("value") if isinstance(result, dict) else None
    logs = value.get("logs") if isinstance(value, dict) else []
    return bool(self._launch_like(logs or []))


def _enqueue_hydration_locked(
    self: Any,
    *,
    signature: str,
    slot: int,
    received_at: datetime,
    source_hint: str | None,
    priority: int,
    reason: str,
) -> None:
    """Apply DirectSolanaJournal.enqueue semantics inside the batch transaction."""

    row = self.store.db.execute(
        "SELECT priority, source_hint, status FROM direct_solana_hydration_queue WHERE signature=?",
        (signature,),
    ).fetchone()
    now = received_at.isoformat()
    if row is None:
        self.store.db.execute(
            "INSERT INTO direct_solana_hydration_queue("
            "signature, slot, trigger_received_at, source_hint, priority, reason, status, attempts, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, 'pending', 0, ?)",
            (
                signature,
                int(slot),
                received_at.isoformat(),
                source_hint,
                int(priority),
                reason,
                now,
            ),
        )
        return

    new_priority = min(int(row["priority"]), int(priority))
    existing_hint = str(row["source_hint"] or "") or None
    status = str(row["status"])
    self.store.db.execute(
        "UPDATE direct_solana_hydration_queue SET source_hint=?, priority=?, reason=?, status=?, updated_at=? "
        "WHERE signature=?",
        (
            existing_hint or source_hint,
            new_priority,
            reason,
            "pending" if status == "failed" and int(priority) <= 2 else status,
            now,
            signature,
        ),
    )


def _persist_full_scope_batch(self: Any, items: list[Any]) -> int:
    """Persist a raw dispatch batch in one SQLite transaction, including critical receipts.

    The previous capacity worker set-batched only ordinary no-hydration program
    receipts. Launch, scout, and bootstrap/sample receipts still entered the
    canonical handler one at a time, producing one durable SQLite transaction per
    receipt under the exact traffic bursts that filled the no-drop queue. This
    implementation keeps every unique raw receipt and reproduces the canonical
    hydration-enqueue decisions, but commits the entire dispatch batch together.
    """

    if not items:
        return 0

    parsed: list[dict[str, Any]] = []
    provider_last: dict[str, datetime] = {}
    sources: set[str] = set()

    for item in items:
        _priority, _mono, _sequence, received_at, provider, _targets, _message = capacity._parse_dispatch_item(item)
        fields = capacity._dispatch_fields(item)
        if fields is None:
            raise RuntimeError("invalid raw receipt dispatch item")
        target, slot, signature, failed, source = fields
        source_key = source or f"SCOUT:{str(getattr(target, 'address', '') or '')}"
        launch_like = _launch_like(self, item)
        row = {
            "target": target,
            "slot": int(slot),
            "signature": signature,
            "failed": bool(failed),
            "source": source,
            "source_key": source_key,
            "launch_like": launch_like,
            "received_at": received_at,
            "provider": provider,
        }
        parsed.append(row)
        if source:
            sources.add(str(source))
        previous = provider_last.get(provider)
        if previous is None or received_at > previous:
            provider_last[provider] = received_at

    # Raw receipt persistence does not change normalized-swap coverage, so the
    # source-bootstrap hydration decision is stable for the duration of one batch.
    coverage_needs_more_by_source: dict[str, bool] = {}
    for source in sources:
        coverage_needs_more_by_source[source] = bool(self._coverage_needs_more(source))

    journal = self.journal
    inserted_keys: set[tuple[str, str]] = set()
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    critical_enqueued = 0
    ordinary_enqueued = 0

    with self.store._lock, self.store.db:
        values_sql = ",".join("(?,?,?,?,?,?)" for _ in parsed)
        params: list[Any] = []
        for row in parsed:
            received_at = row["received_at"]
            params.extend(
                [
                    row["signature"],
                    row["source_key"],
                    row["slot"],
                    received_at.isoformat(),
                    1 if row["launch_like"] else 0,
                    (received_at + timedelta(seconds=float(journal.raw_retention_seconds))).isoformat(),
                ]
            )

        try:
            returned = self.store.db.execute(
                "INSERT OR IGNORE INTO direct_solana_recent_receipts("
                "signature, source_key, slot, received_at, launch_like, expires_at) VALUES "
                + values_sql
                + " RETURNING signature, source_key",
                tuple(params),
            ).fetchall()
            inserted_keys = {
                (str(row["signature"]), str(row["source_key"])) for row in returned
            }
        except Exception:
            # Older SQLite builds may not support multi-row RETURNING. Retain the
            # single transaction and all semantics rather than falling back to
            # per-receipt commits.
            for row in parsed:
                received_at = row["received_at"]
                cursor = self.store.db.execute(
                    "INSERT OR IGNORE INTO direct_solana_recent_receipts("
                    "signature, source_key, slot, received_at, launch_like, expires_at) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        row["signature"],
                        row["source_key"],
                        row["slot"],
                        received_at.isoformat(),
                        1 if row["launch_like"] else 0,
                        (received_at + timedelta(seconds=float(journal.raw_retention_seconds))).isoformat(),
                    ),
                )
                if cursor.rowcount == 1:
                    inserted_keys.add((row["signature"], row["source_key"]))

        inserted_rows = [
            row for row in parsed if (row["signature"], row["source_key"]) in inserted_keys
        ]

        for row in inserted_rows:
            source_key = str(row["source_key"])
            if source_key in {"PUMP_FUN", "PUMP_AMM", "RAYDIUM"}:
                bucket = row["received_at"].replace(second=0, microsecond=0).isoformat()
                groups[(bucket, source_key)].append(row)

        for (bucket, source_key), rows in groups.items():
            current = self.store.db.execute(
                "SELECT rolling_sha256 FROM direct_solana_minute_receipts WHERE bucket=? AND source=?",
                (bucket, source_key),
            ).fetchone()
            rolling = str(current["rolling_sha256"]) if current is not None else ""
            for row in rows:
                rolling = hashlib.sha256(
                    f"{rolling}|{row['signature']}|{row['slot']}|{row['received_at'].isoformat()}".encode("utf-8")
                ).hexdigest()
            first_at = rows[0]["received_at"].isoformat()
            last_at = rows[-1]["received_at"].isoformat()
            self.store.db.execute(
                "INSERT INTO direct_solana_minute_receipts("
                "bucket, source, receipt_count, first_received_at, last_received_at, last_slot, rolling_sha256) "
                "VALUES (?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(bucket, source) DO UPDATE SET "
                "receipt_count=direct_solana_minute_receipts.receipt_count+excluded.receipt_count, "
                "last_received_at=excluded.last_received_at, last_slot=excluded.last_slot, "
                "rolling_sha256=excluded.rolling_sha256",
                (
                    bucket,
                    source_key,
                    len(rows),
                    first_at,
                    last_at,
                    int(rows[-1]["slot"]),
                    rolling,
                ),
            )

        # Reproduce the canonical post-record_receipt decision in the exact batch
        # order. Failed notifications remain durable but never gain hydration work.
        for row in inserted_rows:
            if row["failed"]:
                continue
            target = row["target"]
            signature = str(row["signature"])
            slot = int(row["slot"])
            received_at = row["received_at"]
            if str(getattr(target, "kind", "")) == "scout":
                _enqueue_hydration_locked(
                    self,
                    signature=signature,
                    slot=slot,
                    received_at=received_at,
                    source_hint=None,
                    priority=0,
                    reason="frozen_scout_processed_trigger",
                )
                critical_enqueued += 1
                continue

            source = str(row["source"] or "")
            coverage_needs_more = coverage_needs_more_by_source.get(source, True)
            if bool(row["launch_like"]) or (
                coverage_needs_more and self._sample(signature, self.market_sample_modulus)
            ):
                launch_like = bool(row["launch_like"])
                _enqueue_hydration_locked(
                    self,
                    signature=signature,
                    slot=slot,
                    received_at=received_at,
                    source_hint=source or None,
                    priority=10 if launch_like else 20,
                    reason="prospective_launch" if launch_like else "deterministic_market_sample",
                )
                if launch_like:
                    critical_enqueued += 1
                else:
                    ordinary_enqueued += 1

        for provider, received_at in provider_last.items():
            self.store.db.execute(
                "UPDATE direct_solana_provider_state SET last_message_at=? WHERE provider=?",
                (received_at.isoformat(), provider),
            )

        old_count = int(getattr(journal, "_receipt_inserts", 0) or 0)
        new_count = old_count + len(inserted_rows)
        journal._receipt_inserts = new_count
        if new_count // 500 > old_count // 500:
            newest = max(row["received_at"] for row in parsed)
            self.store.db.execute(
                "DELETE FROM direct_solana_recent_receipts WHERE expires_at<?",
                (newest.isoformat(),),
            )

    setattr(
        self,
        "_roi_full_scope_batch_rows",
        int(getattr(self, "_roi_full_scope_batch_rows", 0) or 0) + len(inserted_rows),
    )
    setattr(
        self,
        "_roi_full_scope_batch_commits",
        int(getattr(self, "_roi_full_scope_batch_commits", 0) or 0) + 1,
    )
    setattr(
        self,
        "_roi_full_scope_batch_critical_enqueues",
        int(getattr(self, "_roi_full_scope_batch_critical_enqueues", 0) or 0) + critical_enqueued,
    )
    setattr(
        self,
        "_roi_full_scope_batch_ordinary_enqueues",
        int(getattr(self, "_roi_full_scope_batch_ordinary_enqueues", 0) or 0) + ordinary_enqueued,
    )
    return len(inserted_rows)


async def _full_scope_dispatch_worker(
    self: Any,
    stop: asyncio.Event,
    _handler: Callable[[Any, str, dict[int, Any], dict[str, Any]], Any],
) -> None:
    queue = raw_dispatch._dispatch_queue(self)
    if queue is None:
        return

    while not stop.is_set():
        try:
            first = await asyncio.wait_for(queue.get(), timeout=0.5)
        except asyncio.TimeoutError:
            continue

        items = [first]
        while len(items) < FULL_SCOPE_BATCH_MAX:
            try:
                items.append(queue.get_nowait())
            except asyncio.QueueEmpty:
                break

        try:
            for item in items:
                capacity._observe_dispatch_delay(self, item)
            _persist_full_scope_batch(self, items)
            raw_dispatch._increment(self, "completed", len(items))
            setattr(
                self,
                "_roi_capacity_dispatch_batch_commits",
                int(getattr(self, "_roi_capacity_dispatch_batch_commits", 0) or 0) + 1,
            )
            setattr(
                self,
                "_roi_capacity_dispatch_batched_receipts",
                int(getattr(self, "_roi_capacity_dispatch_batched_receipts", 0) or 0) + len(items),
            )
            setattr(
                self,
                "_roi_capacity_dispatch_max_batch_size",
                max(
                    int(getattr(self, "_roi_capacity_dispatch_max_batch_size", 0) or 0),
                    len(items),
                ),
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raw_dispatch._increment(self, "failed")
            setattr(self, "_roi_raw_receipt_dispatch_last_error_type", type(exc).__name__)
            setattr(self, "_roi_raw_receipt_dispatch_fatal", exc)
            return
        finally:
            for _item in items:
                queue.task_done()


def _status_with_full_scope_batching(
    original: Callable[[Any], dict[str, Any]],
) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        dispatch = payload.get("raw_receipt_dispatch")
        if isinstance(dispatch, dict):
            dispatch.update(
                {
                    "full_scope_set_based_writer": True,
                    "critical_receipts_batched": True,
                    "critical_per_receipt_commits": False,
                    "full_scope_batch_max": FULL_SCOPE_BATCH_MAX,
                    "full_scope_batch_rows": int(getattr(self, "_roi_full_scope_batch_rows", 0) or 0),
                    "full_scope_batch_commits": int(getattr(self, "_roi_full_scope_batch_commits", 0) or 0),
                    "full_scope_batch_critical_enqueues": int(
                        getattr(self, "_roi_full_scope_batch_critical_enqueues", 0) or 0
                    ),
                    "full_scope_batch_ordinary_enqueues": int(
                        getattr(self, "_roi_full_scope_batch_ordinary_enqueues", 0) or 0
                    ),
                    "unique_receipt_durability_unchanged": True,
                    "launch_scout_hydration_priority_unchanged": True,
                    "queue_bound_unchanged": True,
                    "drops_allowed": False,
                }
            )
        policy = payload.setdefault("provider_runtime_policy", {})
        if isinstance(policy, dict):
            policy.update(
                {
                    "raw_receipt_full_scope_set_based_commit": True,
                    "raw_receipt_critical_per_item_commit_removed": True,
                    "raw_receipt_queue_bound_unchanged": True,
                    "raw_receipt_drops_allowed": False,
                    "certification_thresholds_unchanged": True,
                    "paper_only_authority_unchanged": True,
                }
            )
        return payload

    try:
        status.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(status, "_roi_full_scope_dispatch_capacity", True)
    return status


def install_full_scope_dispatch_capacity_repair() -> None:
    """Remove per-critical-receipt commits without changing evidence semantics."""

    raw_dispatch._dispatch_worker = _full_scope_dispatch_worker  # type: ignore[assignment]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_full_scope_dispatch_capacity", False)):
        DirectSolanaIngestionPlane.status = _status_with_full_scope_batching(current_status)  # type: ignore[method-assign]


__all__ = [
    "FULL_SCOPE_BATCH_MAX",
    "_enqueue_hydration_locked",
    "_full_scope_dispatch_worker",
    "_persist_full_scope_batch",
    "install_full_scope_dispatch_capacity_repair",
]
