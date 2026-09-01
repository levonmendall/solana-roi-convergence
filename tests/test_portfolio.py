from datetime import datetime, timezone

import pytest

from solana_roi.models import IntentKind, TradeIntent
from solana_roi.portfolio import PaperPortfolio

T0 = datetime(2026, 9, 1, tzinfo=timezone.utc)
def intent(kind, fraction=0.0): return TradeIntent(kind, "MINT", T0, fraction_of_full_position=fraction)


def test_initial_full_position_is_2_5_percent_of_nav(): assert PaperPortfolio().full_position_notional() == pytest.approx(12.5)


def test_s_tier_starter_is_30_percent_of_full_position():
    p = PaperPortfolio(); p.apply(intent(IntentKind.OPEN_STARTER, 0.30), scout_wallet="scout", reference_price=1.0)
    assert p.positions["MINT"].entry_capital_usd == pytest.approx(3.75); assert p.cash_usd == pytest.approx(496.25)


def test_harvest_keeps_30_percent_runner_then_closes():
    p = PaperPortfolio(); p.apply(intent(IntentKind.OPEN_FULL, 1.0), scout_wallet="scout", reference_price=1.0); before = p.positions["MINT"].units
    p.apply(intent(IntentKind.HARVEST, 0.70), scout_wallet="scout", reference_price=1.5)
    assert p.positions["MINT"].harvest_hit is True and p.positions["MINT"].units == pytest.approx(before * 0.30)
    p.apply(intent(IntentKind.EXIT_RUNNER), scout_wallet="scout", reference_price=1.2)
    assert p.positions["MINT"].units == 0 and len(p.closed) == 1 and p.closed[0].harvest_hit is True


def test_compounding_increases_next_position_size_after_profit():
    p = PaperPortfolio(); p.apply(intent(IntentKind.OPEN_FULL, 1.0), scout_wallet="scout", reference_price=1.0); p.apply(intent(IntentKind.EXIT_THESIS), scout_wallet="scout", reference_price=2.0)
    assert p.nav() > 500 and p.full_position_notional() > 12.5
