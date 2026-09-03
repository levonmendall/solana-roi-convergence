from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from solana_roi import raw_receipt_dispatch_repair as raw_dispatch
from solana_roi.direct_solana import DirectSolanaJournal, WatchTarget
from solana_roi.full_scope_dispatch_capacity_repair import (
    _full_scope_dispatch_worker,
    _persist_full_scope_batch,
)
from solana_roi.observation_store import ObservationEventStore


LAUNCH_SENTINEL = "__roi_launch_like__"


def _item(
    *,
    signature: str,
    slot: int,
    received_at: datetime,
    sequence: int,
    kind: str = "program",
    source: str | None = "PUMP_FUN",
    launch: bool = False,
    failed: bool = False,
    provider: str = "publicnode",
):
    target = WatchTarget(
        kind=kind,
        address="wallet" if kind == "scout" else "program",
        source_hint=None if kind == "scout" else source,
    )
    message = {
        "method": "logsNotification",
        "params": {
            "subscription": 1,
            "result": {
                "context": {"slot": slot},
                "value": {
                    "signature": signature,
                    "err": {"InstructionError": [0, "test"]} if failed else None,
                    "logs": [LAUNCH_SENTINEL] if launch else [],
                },
            },
        },
    }
    priority = 0 if launch else (1 if kind == "scout" else 10)
    return (
        priority,
        time.monotonic(),
        sequence,
        received_at,
        provider,
        {1: target},
        message,
    )


def _plane(tmp_path):
    store = ObservationEventStore(tmp_path / "full-scope.sqlite3")
    journal = DirectSolanaJournal(store)
    journal.set_provider("publicnode", connected=True)
    return SimpleNamespace(
        store=store,
        journal=journal,
        market_sample_modulus=1,
        audit_sample_modulus=1000,
        _coverage_needs_more=lambda _source: True,
        _sample=lambda _signature, _modulus: True,
        _launch_like=lambda logs: LAUNCH_SENTINEL in list(logs or []),
    )


def test_full_scope_batch_preserves_raw_receipts_and_canonical_hydration_priorities(tmp_path):
    plane = _plane(tmp_path)
    at = datetime(2026, 9, 3, 15, 30, tzinfo=timezone.utc)
    items = [
        _item(signature="launch", slot=100, received_at=at, sequence=0, launch=True),
        _item(signature="scout", slot=101, received_at=at + timedelta(milliseconds=1), sequence=1, kind="scout"),
        _item(signature="ordinary", slot=102, received_at=at + timedelta(milliseconds=2), sequence=2),
        _item(signature="failed", slot=103, received_at=at + timedelta(milliseconds=3), sequence=3, failed=True),
    ]

    assert _persist_full_scope_batch(plane, items) == 4

    with plane.store._lock:
        receipts = plane.store.db.execute(
            "SELECT signature, launch_like FROM direct_solana_recent_receipts ORDER BY slot"
        ).fetchall()
        hydration = plane.store.db.execute(
            "SELECT signature, source_hint, priority, reason, status "
            "FROM direct_solana_hydration_queue ORDER BY priority, signature"
        ).fetchall()
        minute = plane.store.db.execute(
            "SELECT receipt_count, last_slot FROM direct_solana_minute_receipts WHERE source='PUMP_FUN'"
        ).fetchone()

    assert [(str(row["signature"]), int(row["launch_like"])) for row in receipts] == [
        ("launch", 1),
        ("scout", 0),
        ("ordinary", 0),
        ("failed", 0),
    ]
    by_signature = {str(row["signature"]): dict(row) for row in hydration}
    assert set(by_signature) == {"launch", "scout", "ordinary"}
    assert int(by_signature["scout"]["priority"]) == 0
    assert by_signature["scout"]["reason"] == "frozen_scout_processed_trigger"
    assert int(by_signature["launch"]["priority"]) == 10
    assert by_signature["launch"]["reason"] == "prospective_launch"
    assert int(by_signature["ordinary"]["priority"]) == 20
    assert by_signature["ordinary"]["reason"] == "deterministic_market_sample"
    assert int(minute["receipt_count"]) == 3
    assert int(minute["last_slot"]) == 103
    assert int(getattr(plane, "_roi_full_scope_batch_critical_enqueues", 0)) == 2


def test_duplicate_receipt_remains_unique_and_does_not_duplicate_hydration(tmp_path):
    plane = _plane(tmp_path)
    at = datetime(2026, 9, 3, 15, 31, tzinfo=timezone.utc)
    item = _item(signature="same", slot=200, received_at=at, sequence=0, launch=True)

    assert _persist_full_scope_batch(plane, [item]) == 1
    assert _persist_full_scope_batch(plane, [item]) == 0

    with plane.store._lock:
        assert int(
            plane.store.db.execute(
                "SELECT COUNT(*) FROM direct_solana_recent_receipts WHERE signature='same'"
            ).fetchone()[0]
        ) == 1
        assert int(
            plane.store.db.execute(
                "SELECT COUNT(*) FROM direct_solana_hydration_queue WHERE signature='same'"
            ).fetchone()[0]
        ) == 1
        assert int(
            plane.store.db.execute(
                "SELECT receipt_count FROM direct_solana_minute_receipts WHERE source='PUMP_FUN'"
            ).fetchone()[0]
        ) == 1


def test_full_scope_worker_never_calls_per_receipt_canonical_handler(tmp_path):
    async def exercise() -> None:
        plane = _plane(tmp_path)
        queue: asyncio.PriorityQueue = asyncio.PriorityQueue(maxsize=4096)
        setattr(plane, "_roi_raw_receipt_dispatch_queue", queue)
        at = datetime(2026, 9, 3, 15, 32, tzinfo=timezone.utc)
        await queue.put(_item(signature="launch-worker", slot=300, received_at=at, sequence=0, launch=True))
        await queue.put(_item(signature="scout-worker", slot=301, received_at=at, sequence=1, kind="scout"))
        stop = asyncio.Event()

        async def forbidden_handler(*_args, **_kwargs):
            raise AssertionError("full-scope batch worker must not use per-receipt canonical handler")

        worker = asyncio.create_task(_full_scope_dispatch_worker(plane, stop, forbidden_handler))
        await asyncio.wait_for(queue.join(), timeout=2.0)
        stop.set()
        await asyncio.wait_for(worker, timeout=1.0)

        assert int(getattr(plane, "_roi_raw_receipt_dispatch_completed", 0)) == 2
        assert int(getattr(plane, "_roi_full_scope_batch_commits", 0)) == 1
        assert getattr(plane, "_roi_raw_receipt_dispatch_fatal", None) is None

    asyncio.run(exercise())


def test_existing_failed_hydration_is_only_rearmed_by_canonical_priority_rule(tmp_path):
    plane = _plane(tmp_path)
    at = datetime(2026, 9, 3, 15, 33, tzinfo=timezone.utc)
    with plane.store._lock, plane.store.db:
        plane.store.db.execute(
            "INSERT INTO direct_solana_hydration_queue("
            "signature, slot, trigger_received_at, source_hint, priority, reason, status, attempts, updated_at) "
            "VALUES ('existing', 1, ?, 'PUMP_FUN', 30, 'old', 'failed', 1, ?)",
            (at.isoformat(), at.isoformat()),
        )

    # A launch has canonical priority 10, so DirectSolanaJournal.enqueue would
    # preserve failed status rather than rearm it (only priority <=2 can rearm).
    assert _persist_full_scope_batch(
        plane,
        [_item(signature="existing", slot=400, received_at=at, sequence=0, launch=True)],
    ) == 1
    with plane.store._lock:
        row = plane.store.db.execute(
            "SELECT priority, reason, status FROM direct_solana_hydration_queue WHERE signature='existing'"
        ).fetchone()
    assert int(row["priority"]) == 10
    assert row["reason"] == "prospective_launch"
    assert row["status"] == "failed"
