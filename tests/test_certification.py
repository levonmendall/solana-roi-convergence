from datetime import datetime, timedelta, timezone

from solana_roi.certification import ProfitabilityCertifier
from solana_roi.models import TradeOutcome

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


def outcome(i: int, win: bool) -> TradeOutcome:
    pnl = 1.0 if win else -0.6; start = 500.0 + i * 0.01
    return TradeOutcome(f"mint-{i}", f"scout-{i % 7}", T0 + timedelta(minutes=i), T0 + timedelta(minutes=i, seconds=30), start, start + pnl, pnl, pnl / start, win, "test")


def test_strong_300_trade_forward_cohort_can_certify():
    report = ProfitabilityCertifier().evaluate([outcome(i, i < 210) for i in range(300)])
    assert report.status == "certified" and report.hit_rate == 0.70 and report.hit_rate_wilson_lower > 0.5749 and report.pnl_ex_best_trade_usd > 0 and report.pnl_ex_best_scout_usd > 0


def test_small_sample_never_certifies():
    report = ProfitabilityCertifier().evaluate([outcome(i, True) for i in range(50)])
    assert report.status != "certified" and "minimum_300_closed_trades_not_met" in report.blockers
