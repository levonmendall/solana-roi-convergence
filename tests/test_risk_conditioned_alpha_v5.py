from __future__ import annotations

from solana_roi.risk_conditioned_alpha_v5 import (
    chase_band,
    risk_descriptor,
    robust_return_profile,
)


def test_probabilistic_hazards_do_not_become_mechanical_vetoes() -> None:
    risk = risk_descriptor(
        soft_flags={"bundled_launch", "sniper_heavy"},
        hard_flags=set(),
        creator_flow_state="distributing",
        creator_linked_trigger=True,
        early_exit_fraction=0.30,
    )
    assert risk["structurally_tradeable"] is True
    assert risk["mechanical_hard_stops"] == []
    assert "bundled_launch" in risk["hazards"]
    assert "creator_distributing" in risk["hazards"]
    assert "early_holder_distribution" in risk["hazards"]
    assert risk["risk_signature"] != "clean"


def test_mechanical_exit_failure_still_rejects() -> None:
    risk = risk_descriptor(
        soft_flags={"bundled_launch"},
        hard_flags={"sell_route_unavailable"},
    )
    assert risk["structurally_tradeable"] is False
    assert risk["mechanical_hard_stops"] == ["sell_route_unavailable"]


def test_positive_skew_can_promote_below_fifty_percent_hit_rate_and_negative_median() -> None:
    # 40% winners, negative median, but a broad positive right tail. Promotion is
    # intentionally based on leave-best-out compounded edge rather than hit rate.
    returns = [1.0] * 16 + [-0.20] * 24
    profile = robust_return_profile(returns, grid=(0.005, 0.01, 0.02, 0.05), max_fraction=0.05)
    assert profile.sample_count == 40
    assert profile.hit_rate == 0.40
    assert profile.median_return < 0.0
    assert profile.trimmed_mean_ex_best is not None and profile.trimmed_mean_ex_best > 0.0
    assert profile.best_expected_log_growth is not None and profile.best_expected_log_growth > 0.0
    assert profile.state == "promoted_positive_log_growth"
    assert profile.best_fraction > 0.0


def test_chase_thresholds_are_separate_learning_bands() -> None:
    assert chase_band(0.10) == "baseline_le_15pct"
    assert chase_band(0.20) == "challenger_15_25pct"
    assert chase_band(0.30) == "challenger_25_40pct"
    assert chase_band(0.50) == "challenger_gt_40pct"
