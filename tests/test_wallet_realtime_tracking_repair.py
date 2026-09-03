from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from solana_roi.direct_solana import DirectSolanaJournal, WatchTarget
from solana_roi.observation_store import ObservationEventStore
from solana_roi.solana_rpc import RpcEndpoint
from solana_roi.wallet_intelligence import ContinuousWalletIntelligence, WalletPerformanceSnapshot
from solana_roi.wallet_realtime_intelligence_boundary import (
    install_wallet_realtime_intelligence_boundary,
)
from solana_roi.wallet_realtime_tracking_repair import (
    _realtime_poll_guard,
    _select_research_endpoints,
    _set_based_background_batch,
)


def _dispatch_item(*, signature: str, slot: int, received_at: datetime, sequence: int = 0):
    target = WatchTarget(kind="program", address="program", source_hint="PUMP_FUN")
    message = {
        "method": "logsNotification",
        "params": {
            "subscription": 1,
            "result": {
                "context": {"slot": slot},
                "value": {"signature": signature, "err": None, "logs": []},
            },
        },
    }
    return (10, time.monotonic(), sequence, received_at, "publicnode", {1: target}, message)


def test_research_rpc_prefers_configured_non_official_capacity():
    endpoints = (
        RpcEndpoint(
            "publicnode",
            "https://solana-rpc.publicnode.com",
            "wss://solana-rpc.publicnode.com",
        ),
        RpcEndpoint(
            "solana-mainnet",
            "https://api.mainnet.solana.com",
            "wss://api.mainnet.solana.com",
        ),
        RpcEndpoint(
            "alchemy",
            "https://solana-mainnet.g.alchemy.com/v2/test",
            "wss://solana-mainnet.streaming.alchemy.com/v2/test",
        ),
    )
    selected = _select_research_endpoints(endpoints)
    assert [row.name for row in selected] == ["alchemy", "publicnode"]
    assert all("api.mainnet.solana.com" not in row.http_url for row in selected)


def test_realtime_tracking_disables_legacy_forward_polling():
    discovery = SimpleNamespace(_roi_realtime_tracker=object())
    assert asyncio.run(_realtime_poll_guard(discovery, "wallet")) == 0


def test_set_based_writer_preserves_unique_receipts_and_grouped_minute_accounting(tmp_path):
    store = ObservationEventStore(tmp_path / "set-based.sqlite3")
    journal = DirectSolanaJournal(store)
    journal.set_provider("publicnode", connected=True)
    plane = SimpleNamespace(store=store, journal=journal)
    at = datetime(2026, 9, 3, 14, 0, tzinfo=timezone.utc)
    items = [
        _dispatch_item(signature="sig-a", slot=100, received_at=at, sequence=0),
        _dispatch_item(signature="sig-b", slot=101, received_at=at + timedelta(milliseconds=10), sequence=1),
        _dispatch_item(signature="sig-c", slot=102, received_at=at + timedelta(milliseconds=20), sequence=2),
    ]

    assert _set_based_background_batch(plane, items) == 3
    with store._lock:
        receipts = store.db.execute(
            "SELECT COUNT(*) FROM direct_solana_recent_receipts WHERE source_key='PUMP_FUN'"
        ).fetchone()[0]
        minute = store.db.execute(
            "SELECT receipt_count, last_slot, rolling_sha256 FROM direct_solana_minute_receipts "
            "WHERE source='PUMP_FUN'"
        ).fetchone()
    assert int(receipts) == 3
    assert int(minute["receipt_count"]) == 3
    assert int(minute["last_slot"]) == 102
    assert len(str(minute["rolling_sha256"])) == 64
    assert int(getattr(plane, "_roi_set_based_batch_rows", 0)) == 3
    assert int(getattr(plane, "_roi_set_based_batch_groups", 0)) == 1

    assert _set_based_background_batch(plane, [items[0]]) == 0
    with store._lock:
        assert int(
            store.db.execute(
                "SELECT receipt_count FROM direct_solana_minute_receipts WHERE source='PUMP_FUN'"
            ).fetchone()[0]
        ) == 3


def _snapshot(wallet: str, at: datetime, *, episodes: int) -> WalletPerformanceSnapshot:
    return WalletPerformanceSnapshot(
        wallet=wallet,
        entity_id=f"graph:{wallet}",
        observed_at=at,
        closed_episodes=episodes,
        copyable_return_on_capital=0.10 if episodes else 0.0,
        geometric_growth=0.02 if episodes else 0.0,
        profit_factor=2.0 if episodes else 0.0,
        hit_rate=0.60 if episodes else 0.0,
        max_drawdown=0.10 if episodes else 0.0,
        copyability_rate=0.90 if episodes else 0.0,
        manipulation_risk=0.0 if episodes else 1.0,
        side_wallet_risk=0.0 if episodes else 1.0,
        median_entry_lag_ms=1000.0,
        source="test-forward",
    )


def test_realtime_epoch_hides_stale_snapshot_without_deleting_append_only_history(tmp_path):
    store = ObservationEventStore(tmp_path / "epoch.sqlite3")
    intelligence = ContinuousWalletIntelligence(store)
    wallet = "wallet-a"
    stale_at = datetime(2026, 9, 3, 12, 0, tzinfo=timezone.utc)
    boundary = stale_at + timedelta(hours=1)
    fresh_at = boundary + timedelta(minutes=1)
    assert intelligence.record_snapshot(_snapshot(wallet, stale_at, episodes=30))

    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE wallet_realtime_state ("
            "wallet TEXT PRIMARY KEY, epoch_started_at TEXT NOT NULL, active INTEGER NOT NULL)"
        )
        store.db.execute(
            "INSERT INTO wallet_realtime_state(wallet, epoch_started_at, active) VALUES (?, ?, 1)",
            (wallet, boundary.isoformat()),
        )

    install_wallet_realtime_intelligence_boundary()
    assert intelligence.latest_snapshot(wallet) is None
    with store._lock:
        assert int(
            store.db.execute(
                "SELECT COUNT(*) FROM wallet_intelligence_snapshots WHERE wallet=?", (wallet,)
            ).fetchone()[0]
        ) == 1

    assert intelligence.record_snapshot(_snapshot(wallet, fresh_at, episodes=30))
    latest = intelligence.latest_snapshot(wallet)
    assert latest is not None
    assert latest.observed_at == fresh_at
    with store._lock:
        assert int(
            store.db.execute(
                "SELECT COUNT(*) FROM wallet_intelligence_snapshots WHERE wallet=?", (wallet,)
            ).fetchone()[0]
        ) == 2
