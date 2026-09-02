from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from solana_roi import public_data_economics as economics
from solana_roi.direct_solana import DirectSolanaJournal, WatchTarget
from solana_roi.observation_store import ObservationEventStore


def _message(signature: str, *, subscription: int = 1, logs=None):
    return {
        "jsonrpc": "2.0",
        "method": "logsNotification",
        "params": {
            "subscription": subscription,
            "result": {
                "context": {"slot": 123},
                "value": {
                    "signature": signature,
                    "err": None,
                    "logs": list(logs or []),
                },
            },
        },
    }


def test_under_sampled_source_bootstrap_is_bounded_and_stops_at_empirical_minimum(tmp_path, monkeypatch):
    store = ObservationEventStore(tmp_path / "selective.sqlite3")
    journal = DirectSolanaJournal(store)
    coverage = {
        "program_source_counts": {"PUMP_AMM": 1},
        "requirements": {"min_normalized_swaps_per_source": 10},
    }

    class Plane:
        coverage_status_fn = staticmethod(lambda: coverage)
        _launch_like = staticmethod(lambda _logs: False)

    plane = Plane()
    plane.store = store
    plane.journal = journal
    target = WatchTarget("program", "pump-amm", "PUMP_AMM")

    monkeypatch.setattr(economics, "COVERAGE_BOOTSTRAP_MIN_INTERVAL_SECONDS", 0.0)
    subscriptions = {1: target}
    for index in range(3):
        asyncio.run(
            economics._selective_notification_handler(
                plane,
                "publicnode",
                subscriptions,
                _message(f"bootstrap-{index}"),
            )
        )

    with store._lock:
        rows = store.db.execute(
            "SELECT signature, reason, status FROM direct_solana_hydration_queue ORDER BY signature"
        ).fetchall()
    assert len(rows) == economics.COVERAGE_BOOTSTRAP_MAX_OUTSTANDING_PER_SOURCE
    assert {str(row["reason"]) for row in rows} == {"deterministic_market_sample"}
    assert {str(row["status"]) for row in rows} == {"pending"}

    coverage["program_source_counts"]["PUMP_AMM"] = 10
    asyncio.run(
        economics._selective_notification_handler(
            plane,
            "publicnode",
            subscriptions,
            _message("after-minimum"),
        )
    )
    with store._lock:
        count = store.db.execute(
            "SELECT COUNT(*) AS n FROM direct_solana_hydration_queue"
        ).fetchone()["n"]
    assert int(count) == economics.COVERAGE_BOOTSTRAP_MAX_OUTSTANDING_PER_SOURCE


def test_launches_and_scout_activity_remain_fully_hydrated_after_source_minimum(tmp_path):
    store = ObservationEventStore(tmp_path / "material.sqlite3")
    journal = DirectSolanaJournal(store)
    coverage = {
        "program_source_counts": {"PUMP_AMM": 10},
        "requirements": {"min_normalized_swaps_per_source": 10},
    }

    class Plane:
        coverage_status_fn = staticmethod(lambda: coverage)
        _launch_like = staticmethod(lambda logs: bool(logs))

    plane = Plane()
    plane.store = store
    plane.journal = journal

    asyncio.run(
        economics._selective_notification_handler(
            plane,
            "publicnode",
            {1: WatchTarget("program", "pump-amm", "PUMP_AMM")},
            _message("launch", logs=["precise-launch-marker"]),
        )
    )
    asyncio.run(
        economics._selective_notification_handler(
            plane,
            "publicnode",
            {1: WatchTarget("scout", "scout-wallet", None)},
            _message("scout"),
        )
    )

    with store._lock:
        rows = {
            str(row["signature"]): (int(row["priority"]), str(row["reason"]))
            for row in store.db.execute(
                "SELECT signature, priority, reason FROM direct_solana_hydration_queue ORDER BY signature"
            ).fetchall()
        }
    assert rows == {
        "launch": (10, "prospective_launch"),
        "scout": (0, "frozen_scout_processed_trigger"),
    }


def test_restart_retires_legacy_random_market_backlog(tmp_path):
    store = ObservationEventStore(tmp_path / "legacy.sqlite3")
    journal = DirectSolanaJournal(store)
    now = datetime.now(timezone.utc)
    journal.enqueue(
        signature="legacy-sample",
        slot=1,
        trigger_received_at=now,
        source_hint="RAYDIUM",
        priority=20,
        reason="deterministic_market_sample",
    )

    restarted = DirectSolanaJournal(store)
    with store._lock:
        row = store.db.execute(
            "SELECT status, last_error FROM direct_solana_hydration_queue WHERE signature='legacy-sample'"
        ).fetchone()
    assert row is not None
    assert str(row["status"]) == "failed"
    assert str(row["last_error"]) == economics.LEGACY_SAMPLE_RETIREMENT_REASON
    assert restarted._roi_retired_legacy_market_samples == 1
