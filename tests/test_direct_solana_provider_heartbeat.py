from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timedelta, timezone

from solana_roi.direct_solana import DirectSolanaJournal
from solana_roi.direct_solana_hydration_status_repair import _bounded_status


class _Store:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.db = sqlite3.connect(":memory:", check_same_thread=False)
        self.db.row_factory = sqlite3.Row


def _durable_last_message_at(store: _Store, provider: str) -> str | None:
    row = store.db.execute(
        "SELECT last_message_at FROM direct_solana_provider_state WHERE provider=?",
        (provider,),
    ).fetchone()
    assert row is not None
    return row["last_message_at"]


def test_provider_heartbeat_coalesces_durable_writes_but_status_stays_current() -> None:
    store = _Store()
    monotonic_now = [100.0]
    journal = DirectSolanaJournal(
        store,
        provider_heartbeat_persist_interval_seconds=1.0,
        monotonic=lambda: monotonic_now[0],
    )
    journal.set_provider("primary", connected=True)

    first = datetime(2026, 9, 15, 20, 0, 0, tzinfo=timezone.utc)
    journal.touch_provider("primary", first)
    assert _durable_last_message_at(store, "primary") == first.isoformat()

    monotonic_now[0] += 0.25
    second = first + timedelta(milliseconds=250)
    journal.touch_provider("primary", second)

    # The durable heartbeat is intentionally bounded, while live status remains exact.
    assert _durable_last_message_at(store, "primary") == first.isoformat()
    status = journal.status()
    provider_state = next(row for row in status["provider_states"] if row["provider"] == "primary")
    assert provider_state["last_message_at"] == second.isoformat()
    assert status["provider_heartbeat_updates_received"] == 2
    assert status["provider_heartbeat_updates_persisted"] == 1
    assert status["provider_heartbeat_updates_coalesced"] == 1

    monotonic_now[0] += 0.80
    third = first + timedelta(seconds=1, milliseconds=50)
    journal.touch_provider("primary", third)
    assert _durable_last_message_at(store, "primary") == third.isoformat()

    status = journal.status()
    assert status["provider_heartbeat_updates_received"] == 3
    assert status["provider_heartbeat_updates_persisted"] == 2
    assert status["provider_heartbeat_updates_coalesced"] == 1


def test_bounded_hydration_status_preserves_live_heartbeat_overlay_and_counters() -> None:
    store = _Store()
    monotonic_now = [300.0]
    journal = DirectSolanaJournal(
        store,
        provider_heartbeat_persist_interval_seconds=5.0,
        monotonic=lambda: monotonic_now[0],
    )
    journal.set_provider("primary", connected=True)

    first = datetime(2026, 9, 15, 20, 0, 0, tzinfo=timezone.utc)
    journal.touch_provider("primary", first)
    monotonic_now[0] += 0.5
    latest = first + timedelta(milliseconds=500)
    journal.touch_provider("primary", latest)
    assert _durable_last_message_at(store, "primary") == first.isoformat()

    status = _bounded_status(journal)
    provider_state = next(row for row in status["provider_states"] if row["provider"] == "primary")
    assert provider_state["last_message_at"] == latest.isoformat()
    assert status["provider_heartbeat_persist_interval_seconds"] == 5.0
    assert status["provider_heartbeat_updates_received"] == 2
    assert status["provider_heartbeat_updates_persisted"] == 1
    assert status["provider_heartbeat_updates_coalesced"] == 1


def test_provider_disconnect_flushes_latest_coalesced_heartbeat() -> None:
    store = _Store()
    monotonic_now = [200.0]
    journal = DirectSolanaJournal(
        store,
        provider_heartbeat_persist_interval_seconds=5.0,
        monotonic=lambda: monotonic_now[0],
    )
    journal.set_provider("primary", connected=True)

    first = datetime(2026, 9, 15, 20, 0, 0, tzinfo=timezone.utc)
    journal.touch_provider("primary", first)
    monotonic_now[0] += 0.5
    latest = first + timedelta(milliseconds=500)
    journal.touch_provider("primary", latest)
    assert _durable_last_message_at(store, "primary") == first.isoformat()

    journal.set_provider("primary", connected=False)
    assert _durable_last_message_at(store, "primary") == latest.isoformat()
    status = journal.status()
    provider_state = next(row for row in status["provider_states"] if row["provider"] == "primary")
    assert provider_state["connected"] == 0
    assert provider_state["last_message_at"] == latest.isoformat()
