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
