from __future__ import annotations

from datetime import datetime, timezone

from solana_roi.direct_solana import DirectSolanaJournal
from solana_roi.observation_store import ObservationEventStore


def test_full_market_receipt_journal_deduplicates_and_keeps_compact_source_count(tmp_path):
    store = ObservationEventStore(tmp_path / "direct.sqlite3")
    journal = DirectSolanaJournal(store)
    now = datetime.now(timezone.utc)
    assert journal.record_receipt(
        signature="sig-1", source_key="PUMP_FUN", slot=1, received_at=now, launch_like=False
    ) is True
    assert journal.record_receipt(
        signature="sig-1", source_key="PUMP_FUN", slot=1, received_at=now, launch_like=False
    ) is False
    status = journal.status()
    assert status["raw_receipts_last_hour_by_source"]["PUMP_FUN"] == 1


def test_scout_priority_upgrades_existing_market_hydration_request(tmp_path):
    store = ObservationEventStore(tmp_path / "priority.sqlite3")
    journal = DirectSolanaJournal(store)
    now = datetime.now(timezone.utc)
    journal.enqueue(
        signature="shared", slot=10, trigger_received_at=now,
        source_hint="RAYDIUM", priority=20, reason="market",
    )
    journal.enqueue(
        signature="shared", slot=10, trigger_received_at=now,
        source_hint=None, priority=0, reason="scout",
    )
    row = journal.claim()
    assert row is not None
    assert row["signature"] == "shared"
    assert row["priority"] == 0
    assert row["source_hint"] == "RAYDIUM"


def test_restart_replays_processing_item_and_clears_stale_provider_liveness(tmp_path):
    store = ObservationEventStore(tmp_path / "restart.sqlite3")
    journal = DirectSolanaJournal(store)
    now = datetime.now(timezone.utc)
    journal.enqueue(
        signature="interrupted", slot=11, trigger_received_at=now,
        source_hint="PUMP_AMM", priority=10, reason="test",
    )
    assert journal.claim() is not None
    journal.set_provider("provider-a", connected=True)
    assert journal.status()["connected_provider_count"] == 1

    restarted = DirectSolanaJournal(store)
    assert restarted.status()["connected_provider_count"] == 0
    replayed = restarted.claim()
    assert replayed is not None
    assert replayed["signature"] == "interrupted"


def test_unresolved_gap_blocks_continuity_even_after_provider_reconnect(tmp_path):
    store = ObservationEventStore(tmp_path / "gap.sqlite3")
    journal = DirectSolanaJournal(store)
    now = datetime.now(timezone.utc)
    journal.mark_outage(now)
    journal.close_outage(complete=False, error="bounded backfill incomplete")
    journal.set_provider("provider-a", connected=True)
    status = journal.status()
    assert status["connected_provider_count"] == 1
    assert status["unresolved_gap"] is True
    assert status["continuity_ok"] is False
    assert status["last_backfill_error"] == "bounded backfill incomplete"
