from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from solana_roi.direct_solana import DirectSolanaJournal, WatchTarget
from solana_roi.full_scope_dispatch_capacity_repair import _persist_full_scope_batch
from solana_roi.observation_store import ObservationEventStore


LAUNCH_SENTINEL = "__roi_launch_like__"


def _plane(tmp_path, *, coverage_needs_more: bool):
    store = ObservationEventStore(tmp_path / "post-minimum.sqlite3")
    journal = DirectSolanaJournal(store)
    journal.set_provider("publicnode", connected=True)
    return SimpleNamespace(
        store=store,
        journal=journal,
        market_sample_modulus=1,
        audit_sample_modulus=1,
        _coverage_needs_more=lambda _source: coverage_needs_more,
        _sample=lambda _signature, _modulus: True,
        _launch_like=lambda logs: LAUNCH_SENTINEL in list(logs or []),
    )


def _item(
    target: WatchTarget,
    *,
    signature: str,
    slot: int,
    received_at: datetime,
    sequence: int,
    launch: bool = False,
):
    message = {
        "method": "logsNotification",
        "params": {
            "subscription": 1,
            "result": {
                "context": {"slot": slot},
                "value": {
                    "signature": signature,
                    "err": None,
                    "logs": [LAUNCH_SENTINEL] if launch else [],
                },
            },
        },
    }
    priority = 0 if launch else (1 if target.kind == "scout" else 10)
    return (
        priority,
        time.monotonic(),
        sequence,
        received_at,
        "publicnode",
        {1: target},
        message,
    )


def test_full_scope_stops_ordinary_hydration_after_source_minimum(tmp_path):
    plane = _plane(tmp_path, coverage_needs_more=False)
    at = datetime(2026, 9, 3, 16, 0, tzinfo=timezone.utc)
    scout = WatchTarget(kind="scout", address="wallet-a", source_hint=None)
    pump = WatchTarget(kind="program", address="pump", source_hint="PUMP_FUN")

    items = [
        _item(
            pump,
            signature="sample-post-minimum",
            slot=200,
            received_at=at,
            sequence=0,
        ),
        _item(
            pump,
            signature="launch-post-minimum",
            slot=201,
            received_at=at + timedelta(milliseconds=1),
            sequence=1,
            launch=True,
        ),
        _item(
            scout,
            signature="scout-post-minimum",
            slot=202,
            received_at=at + timedelta(milliseconds=2),
            sequence=2,
        ),
    ]

    assert _persist_full_scope_batch(plane, items) == 3
    with plane.store._lock:
        receipts = {
            str(row["signature"])
            for row in plane.store.db.execute(
                "SELECT signature FROM direct_solana_recent_receipts"
            ).fetchall()
        }
        queued = {
            str(row["signature"])
            for row in plane.store.db.execute(
                "SELECT signature FROM direct_solana_hydration_queue"
            ).fetchall()
        }

    assert receipts == {
        "sample-post-minimum",
        "launch-post-minimum",
        "scout-post-minimum",
    }
    assert queued == {"launch-post-minimum", "scout-post-minimum"}


def test_full_scope_keeps_ordinary_bootstrap_hydration_below_source_minimum(tmp_path):
    plane = _plane(tmp_path, coverage_needs_more=True)
    at = datetime(2026, 9, 3, 16, 1, tzinfo=timezone.utc)
    pump = WatchTarget(kind="program", address="pump", source_hint="PUMP_FUN")

    assert _persist_full_scope_batch(
        plane,
        [
            _item(
                pump,
                signature="sample-before-minimum",
                slot=300,
                received_at=at,
                sequence=0,
            )
        ],
    ) == 1

    with plane.store._lock:
        row = plane.store.db.execute(
            "SELECT priority, reason FROM direct_solana_hydration_queue WHERE signature=?",
            ("sample-before-minimum",),
        ).fetchone()

    assert row is not None
    assert int(row["priority"]) == 20
    assert str(row["reason"]) == "deterministic_market_sample"
