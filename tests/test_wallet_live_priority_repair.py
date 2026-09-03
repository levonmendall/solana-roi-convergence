from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from solana_roi.observation_store import ObservationEventStore
from solana_roi.wallet_live_priority_repair import (
    RECOVERY_PROVIDER,
    _claim_priority_receipt,
    _claim_risk_work,
    _ensure_priority_schema,
    _sync_risk_work,
    install_wallet_live_priority_repair,
)
from solana_roi.wallet_realtime_tracking_repair import RealtimeWalletTracker


def _receipt_schema(store: ObservationEventStore) -> None:
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE wallet_realtime_receipts ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, signature TEXT NOT NULL UNIQUE, wallet TEXT NOT NULL, "
            "slot INTEGER NOT NULL, received_at TEXT NOT NULL, provider TEXT NOT NULL, status TEXT NOT NULL, "
            "attempts INTEGER NOT NULL DEFAULT 0, last_error TEXT, updated_at TEXT NOT NULL)"
        )


def _insert_receipt(
    store: ObservationEventStore,
    *,
    signature: str,
    received_at: datetime,
    provider: str,
) -> None:
    with store._lock, store.db:
        store.db.execute(
            "INSERT INTO wallet_realtime_receipts("
            "signature, wallet, slot, received_at, provider, status, attempts, updated_at) "
            "VALUES (?, 'wallet', 1, ?, ?, 'pending', 0, ?)",
            (signature, received_at.isoformat(), provider, received_at.isoformat()),
        )


def test_fresh_live_receipt_preempts_stale_live_and_recovery(tmp_path):
    store = ObservationEventStore(tmp_path / "priority.sqlite3")
    _receipt_schema(store)
    now = datetime.now(timezone.utc)
    _insert_receipt(
        store,
        signature="recovery-old",
        received_at=now - timedelta(minutes=4),
        provider=RECOVERY_PROVIDER,
    )
    _insert_receipt(
        store,
        signature="live-stale",
        received_at=now - timedelta(minutes=2),
        provider="publicnode",
    )
    _insert_receipt(
        store,
        signature="live-fresh",
        received_at=now - timedelta(seconds=1),
        provider="publicnode",
    )
    tracker = SimpleNamespace(store=store)

    fresh = _claim_priority_receipt(tracker, "fresh_live")
    assert fresh is not None
    assert fresh["signature"] == "live-fresh"

    backlog = _claim_priority_receipt(tracker, "backlog")
    assert backlog is not None
    # Stale live continuity work is drained before recovery, but neither can use
    # a fresh-live worker slot.
    assert backlog["signature"] == "live-stale"

    recovery = _claim_priority_receipt(tracker, "backlog")
    assert recovery is not None
    assert recovery["signature"] == "recovery-old"


def _risk_observation_schema(store: ObservationEventStore) -> None:
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE wallet_discovery_forward_observations ("
            "signature TEXT PRIMARY KEY, wallet TEXT NOT NULL, token_mint TEXT NOT NULL, side TEXT NOT NULL, "
            "token_amount REAL NOT NULL, observed_at TEXT NOT NULL, received_at TEXT NOT NULL, "
            "wallet_price_sol REAL NOT NULL, source TEXT NOT NULL, tracking_transport TEXT, "
            "risk_complete INTEGER NOT NULL DEFAULT 0, manipulation_flag INTEGER NOT NULL DEFAULT 1, "
            "side_wallet_flag INTEGER NOT NULL DEFAULT 1)"
        )


def _insert_risk_observation(store: ObservationEventStore, signature: str, at: datetime) -> None:
    with store._lock, store.db:
        store.db.execute(
            "INSERT INTO wallet_discovery_forward_observations("
            "signature, wallet, token_mint, side, token_amount, observed_at, received_at, wallet_price_sol, "
            "source, tracking_transport, risk_complete) "
            "VALUES (?, 'wallet', 'mint', 'buy', 1.0, ?, ?, 1.0, 'test', 'logsSubscribe', 0)",
            (signature, at.isoformat(), at.isoformat()),
        )


def test_risk_work_is_claimed_once_and_completed_rows_leave_pending_queue(tmp_path):
    store = ObservationEventStore(tmp_path / "risk.sqlite3")
    _risk_observation_schema(store)
    at = datetime.now(timezone.utc) - timedelta(seconds=2)
    _insert_risk_observation(store, "sig-a", at)
    _insert_risk_observation(store, "sig-b", at + timedelta(milliseconds=1))
    tracker = SimpleNamespace(store=store)

    _ensure_priority_schema(tracker)
    first = _claim_risk_work(tracker)
    second = _claim_risk_work(tracker)
    assert first is not None and second is not None
    assert first["signature"] != second["signature"]

    with store._lock, store.db:
        store.db.execute(
            "UPDATE wallet_discovery_forward_observations SET risk_complete=1 WHERE signature=?",
            (first["signature"],),
        )
    _sync_risk_work(tracker)
    with store._lock:
        status = store.db.execute(
            "SELECT status FROM wallet_realtime_risk_work WHERE signature=?",
            (first["signature"],),
        ).fetchone()[0]
    assert status == "complete"


def test_live_priority_install_replaces_only_tracker_runtime_methods():
    install_wallet_live_priority_repair()
    assert bool(getattr(RealtimeWalletTracker.__init__, "_roi_live_priority_repair", False))
    assert bool(getattr(RealtimeWalletTracker.run, "_roi_live_priority_repair", False))
    assert bool(getattr(RealtimeWalletTracker.status, "_roi_live_priority_repair", False))
    assert bool(getattr(RealtimeWalletTracker._recover_all, "_roi_live_priority_repair", False))
