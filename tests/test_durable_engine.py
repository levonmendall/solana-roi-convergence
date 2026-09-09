from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from solana_roi.durable_engine import DurablePaperTradingEngine
from solana_roi.models import Confirmation, RiskSnapshot, WalletTier, WalletTouch
from solana_roi.observation_store import ObservationEventStore


def test_durable_engine_restores_open_position_candidate_and_marks(tmp_path):
    path = tmp_path / "durable.sqlite3"
    store = ObservationEventStore(path)
    engine = DurablePaperTradingEngine(store=store)
    t0 = datetime(2026, 9, 1, tzinfo=timezone.utc)
    risk = RiskSnapshot(observed_at=t0)

    engine.on_first_touch(
        WalletTouch("mint", "scout", "entity-s", t0, 1.0, None, WalletTier.S, True),
        risk,
        execution_price=1.0,
    )
    engine.on_confirmation(
        Confirmation("mint", "confirm", "entity-c", t0 + timedelta(seconds=10), 1.1, True),
        risk,
        execution_price=1.1,
    )
    engine.on_price("mint", t0 + timedelta(seconds=20), 1.2)

    expected_cash = engine.portfolio.cash_usd
    expected_units = engine.portfolio.positions["mint"].units
    expected_status = engine.strategy.candidates["mint"].status
    store.close()

    restored_store = ObservationEventStore(path)
    restored = DurablePaperTradingEngine(store=restored_store)
    assert restored.portfolio.cash_usd == pytest.approx(expected_cash)
    assert restored.portfolio.positions["mint"].units == pytest.approx(expected_units)
    assert restored.strategy.candidates["mint"].status is expected_status
    assert restored.marks["mint"] == pytest.approx(1.2)
    assert restored_store.verify()


def test_durable_engine_fails_closed_if_engine_event_escapes_checkpoint(tmp_path):
    path = tmp_path / "gap.sqlite3"
    store = ObservationEventStore(path)
    DurablePaperTradingEngine(store=store)
    store.append(
        "price",
        datetime(2026, 9, 1, tzinfo=timezone.utc).isoformat(),
        {"token_mint": "mint", "reference_price": 1.0},
    )
    store.close()

    reopened = ObservationEventStore(path)
    with pytest.raises(RuntimeError, match="without a durable checkpoint"):
        DurablePaperTradingEngine(store=reopened)


def test_checkpoint_records_exact_appended_engine_event_id(tmp_path):
    path = tmp_path / "exact-id.sqlite3"
    store = ObservationEventStore(path)
    engine = DurablePaperTradingEngine(store=store)
    t0 = datetime(2026, 9, 1, tzinfo=timezone.utc)

    engine.on_price("mint", t0, 1.0)

    with store._lock:
        checkpoint = store.db.execute(
            "SELECT last_engine_event_id FROM paper_engine_checkpoint WHERE id=1"
        ).fetchone()
        event = store.db.execute(
            "SELECT id, event_type FROM events ORDER BY id DESC LIMIT 1"
        ).fetchone()
    assert checkpoint is not None
    assert event is not None
    assert int(checkpoint["last_engine_event_id"]) == int(event["id"])
    assert str(event["event_type"]) == "price"


def test_restore_ignores_unrelated_event_newer_than_checkpoint(tmp_path):
    path = tmp_path / "unrelated-tail.sqlite3"
    store = ObservationEventStore(path)
    engine = DurablePaperTradingEngine(store=store)
    t0 = datetime(2026, 9, 1, tzinfo=timezone.utc)
    engine.on_price("mint", t0, 1.0)
    store.append("risk_refresh_measurement", t0.isoformat(), {"complete": True})
    store.close()

    reopened = ObservationEventStore(path)
    restored = DurablePaperTradingEngine(store=reopened)
    assert restored.marks["mint"] == pytest.approx(1.0)


def test_restore_fails_closed_for_relevant_event_newer_than_checkpoint(tmp_path):
    path = tmp_path / "relevant-tail.sqlite3"
    store = ObservationEventStore(path)
    engine = DurablePaperTradingEngine(store=store)
    t0 = datetime(2026, 9, 1, tzinfo=timezone.utc)
    engine.on_price("mint", t0, 1.0)
    store.append(
        "price",
        (t0 + timedelta(seconds=1)).isoformat(),
        {"token_mint": "mint", "reference_price": 1.1},
    )
    store.close()

    reopened = ObservationEventStore(path)
    with pytest.raises(RuntimeError, match="checkpoint does not cover latest engine event"):
        DurablePaperTradingEngine(store=reopened)


def test_restore_fails_closed_if_checkpoint_marker_is_not_engine_event(tmp_path):
    path = tmp_path / "invalid-marker.sqlite3"
    store = ObservationEventStore(path)
    engine = DurablePaperTradingEngine(store=store)
    t0 = datetime(2026, 9, 1, tzinfo=timezone.utc)
    engine.on_price("mint", t0, 1.0)
    lineage = store.append("risk_refresh_measurement", t0.isoformat(), {"complete": True})
    with store._lock, store.db:
        unrelated = store.db.execute(
            "SELECT id FROM events WHERE lineage_hash=?",
            (lineage,),
        ).fetchone()
        assert unrelated is not None
        store.db.execute(
            "UPDATE paper_engine_checkpoint SET last_engine_event_id=? WHERE id=1",
            (int(unrelated["id"]),),
        )
    store.close()

    reopened = ObservationEventStore(path)
    with pytest.raises(RuntimeError, match="checkpoint engine event marker invalid"):
        DurablePaperTradingEngine(store=reopened)


def test_restore_tail_query_starts_after_verified_snapshot(tmp_path):
    path = tmp_path / "tail-query.sqlite3"
    store = ObservationEventStore(path)
    engine = DurablePaperTradingEngine(store=store)
    t0 = datetime(2026, 9, 1, tzinfo=timezone.utc)
    engine.on_price("mint", t0, 1.0)
    store.append("risk_refresh_measurement", t0.isoformat(), {"complete": True})
    store.close()

    reopened = ObservationEventStore(path)
    statements: list[str] = []
    reopened.db.set_trace_callback(statements.append)
    restored = DurablePaperTradingEngine(store=reopened)
    assert restored.marks["mint"] == pytest.approx(1.0)

    normalized = [" ".join(statement.upper().split()) for statement in statements]
    assert not any("MAX(ID)" in statement for statement in normalized)
    assert any("FROM EVENTS WHERE ID>2 AND EVENT_TYPE IN" in statement for statement in normalized)
    assert not any("FROM EVENTS WHERE ID>1 AND EVENT_TYPE IN" in statement for statement in normalized)
