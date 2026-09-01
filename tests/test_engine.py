from datetime import datetime, timedelta, timezone

from solana_roi.engine import PaperTradingEngine
from solana_roi.models import Confirmation, RiskSnapshot, WalletTier, WalletTouch
from solana_roi.storage import AppendOnlyEventStore

T0 = datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc)


def test_end_to_end_s_tier_harvest_and_runner(tmp_path):
    store = AppendOnlyEventStore(tmp_path / "events.sqlite3"); engine = PaperTradingEngine(store=store)
    engine.on_first_touch(WalletTouch("MINT", "scout", "entity-1", T0, 1.0, 10_000, WalletTier.S), RiskSnapshot(T0))
    assert engine.portfolio.positions["MINT"].entry_capital_usd == 3.75
    t1 = T0 + timedelta(seconds=5)
    engine.on_confirmation(Confirmation("MINT", "confirm", "entity-2", t1, 1.05), RiskSnapshot(t1))
    assert engine.portfolio.positions["MINT"].entry_capital_usd > 11.0
    engine.on_price("MINT", T0 + timedelta(seconds=30), 1.58); assert engine.portfolio.positions["MINT"].harvest_hit is True
    engine.on_price("MINT", T0 + timedelta(seconds=40), 2.0); engine.on_price("MINT", T0 + timedelta(seconds=50), 1.19)
    assert len(engine.portfolio.closed) == 1 and engine.portfolio.closed[0].harvest_hit is True and store.verify() is True


def test_no_confirmation_exits_s_starter(tmp_path):
    engine = PaperTradingEngine(store=AppendOnlyEventStore(tmp_path / "events.sqlite3"))
    engine.on_first_touch(WalletTouch("MINT", "scout", "entity-1", T0, 1.0, 10_000, WalletTier.S), RiskSnapshot(T0))
    engine.on_price("MINT", T0 + timedelta(seconds=21), 1.0)
    assert len(engine.portfolio.closed) == 1 and engine.portfolio.closed[0].harvest_hit is False
