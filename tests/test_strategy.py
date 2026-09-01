from datetime import datetime, timedelta, timezone

from solana_roi.models import Confirmation, IntentKind, RiskSnapshot, WalletTier, WalletTouch
from solana_roi.strategy import RoiConvergenceStrategy

T0 = datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc)


def clean(at=T0): return RiskSnapshot(observed_at=at)
def touch(tier=WalletTier.S, mint="MINT", price=1.0): return WalletTouch(mint, "scout", "entity-scout", T0, price, 10_000, tier)


def test_s_tier_clean_first_touch_opens_only_starter():
    intents = RoiConvergenceStrategy().first_touch(touch(), clean())
    assert len(intents) == 1 and intents[0].kind is IntentKind.OPEN_STARTER and intents[0].fraction_of_full_position == 0.30


def test_a_tier_waits_for_confirmation():
    strategy = RoiConvergenceStrategy(); assert strategy.first_touch(touch(WalletTier.A), clean()) == []
    intents = strategy.confirm(Confirmation("MINT", "confirm", "entity-confirm", T0 + timedelta(seconds=8), 1.08), clean(T0 + timedelta(seconds=8)))
    assert intents[0].kind is IntentKind.OPEN_FULL and intents[0].fraction_of_full_position == 1.0


def test_risk_veto_blocks_entry():
    assert RoiConvergenceStrategy().first_touch(touch(), RiskSnapshot(T0, early_buyers_exiting=True)) == []


def test_same_entity_does_not_confirm():
    strategy = RoiConvergenceStrategy(); strategy.first_touch(touch(), clean())
    assert strategy.confirm(Confirmation("MINT", "side-wallet", "entity-scout", T0 + timedelta(seconds=4), 1.02), clean(T0 + timedelta(seconds=4))) == []


def test_chasing_over_15_percent_does_not_confirm():
    strategy = RoiConvergenceStrategy(); strategy.first_touch(touch(), clean())
    assert strategy.confirm(Confirmation("MINT", "confirm", "entity-confirm", T0 + timedelta(seconds=4), 1.151), clean(T0 + timedelta(seconds=4))) == []


def test_s_tier_exits_starter_after_confirmation_timeout():
    strategy = RoiConvergenceStrategy(); strategy.first_touch(touch(), clean())
    assert strategy.on_clock("MINT", T0 + timedelta(seconds=21), 1.0)[0].kind is IntentKind.EXIT_STARTER


def test_confirmed_trade_harvests_and_trails_runner():
    strategy = RoiConvergenceStrategy(); strategy.first_touch(touch(), clean())
    strategy.confirm(Confirmation("MINT", "confirm", "entity-confirm", T0 + timedelta(seconds=5), 1.02), clean(T0 + timedelta(seconds=5)))
    intents = strategy.on_clock("MINT", T0 + timedelta(seconds=30), 1.53)
    assert intents[0].kind is IntentKind.HARVEST and intents[0].fraction_of_full_position == 0.70
    assert strategy.on_clock("MINT", T0 + timedelta(seconds=40), 2.00) == []
    assert strategy.on_clock("MINT", T0 + timedelta(seconds=50), 1.19)[0].kind is IntentKind.EXIT_RUNNER
