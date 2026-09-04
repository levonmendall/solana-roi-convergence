from __future__ import annotations

from solana_roi.fomo_continuation_shadow import build_fomo_features
from solana_roi.risk_conditioned_alpha_v5 import _fomo_classify_v5, _fomo_profile_v5


def _features(*, creator_distributing: bool = False, early_exit: float = 0.0, chase: float = 0.10):
    return build_fomo_features(
        token_mint="mint",
        observed_at="2026-09-04T00:00:00+00:00",
        venue="PUMP_AMM",
        lifecycle="pump_amm_early_post_graduation_30_120s",
        regime="high_speculation",
        independent_buyers_short=8,
        independent_buyers_long=12,
        buys_short=12,
        buys_long=20,
        sells_short=1,
        sells_long=4,
        buy_volume_short=100.0,
        buy_volume_long=130.0,
        sell_volume_short=5.0,
        sell_volume_long=20.0,
        momentum_wallet_participation=3,
        creator_accumulating=False,
        creator_distributing=creator_distributing,
        early_holder_exit_fraction=early_exit,
        chase_fraction=chase,
        signal_to_entry_seconds=5.0,
        quote_deterioration_fraction=0.06 if creator_distributing else 0.0,
        depth_growth_fraction=0.20,
        exit_slippage_deterioration_fraction=0.06 if creator_distributing else 0.0,
        risk_complete=True,
        trigger_is_proven_wallet=True,
    )


def test_creator_distribution_and_early_selling_are_hazard_not_veto() -> None:
    result = _fomo_classify_v5(_features(creator_distributing=True, early_exit=0.30))
    assert result.structurally_accessible is True
    assert "creator_distributing" not in result.blockers
    assert "early_holder_distribution" not in result.blockers
    assert "hazard_fomo" in result.experiment_variants
    assert "creator_distributing" in result.experiment_variants
    assert "early_holder_distribution" in result.experiment_variants


def test_moderate_chase_is_challenger_not_automatic_reject() -> None:
    result = _fomo_classify_v5(_features(chase=0.20))
    assert result.structurally_accessible is True
    assert "challenger_15_25pct" in result.experiment_variants


def test_extreme_chase_above_research_ceiling_still_fails_closed() -> None:
    result = _fomo_classify_v5(_features(chase=0.50))
    assert result.structurally_accessible is False
    assert "chase_above_research_ceiling" in result.blockers


def test_fomo_promotion_does_not_require_majority_wins() -> None:
    profile = _fomo_profile_v5([1.0] * 16 + [-0.20] * 24)
    assert profile["positive_rate_pct"] == 40.0
    assert profile["median_residual_roi_pct"] < 0.0
    assert profile["state"] == "promoted_fomo_wallet"
    assert profile["hit_rate_is_promotion_veto"] is False
    assert set(profile["challenger_expected_log_growth"]) == {0.075, 0.10}
    assert profile["best_paper_position_fraction"] <= 0.05
