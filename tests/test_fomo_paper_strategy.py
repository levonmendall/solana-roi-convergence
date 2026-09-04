from __future__ import annotations

from solana_roi import fomo_continuation_shadow as shadow
from solana_roi import fomo_paper_strategy as paper


def test_fomo_collector_remains_shadow_but_separate_paper_strategy_is_active() -> None:
    assert shadow.ACTIVE_STRATEGY_MUTATION_ALLOWED is False
    assert paper.ACTIVE_FOMO_PAPER_STRATEGY_AUTHORITY is True
    assert paper.PAPER_ONLY is True
    assert paper.LIVE_MONEY_AUTHORITY is False
    assert paper.SIGNING_AVAILABLE is False
    assert paper.TRANSACTION_SUBMISSION_AVAILABLE is False
    assert paper.HISTORICAL_PROMOTION_AUTHORITY is False


def test_fomo_wallet_promotion_is_forward_sample_and_robustness_gated() -> None:
    bootstrap = paper.classify_fomo_wallet_returns([0.10, 0.08, 0.06, 0.04])
    assert bootstrap["state"] == "bootstrap_forward_evidence"
    assert bootstrap["mature"] is False

    promoted = paper.classify_fomo_wallet_returns([0.10, 0.08, 0.06, 0.04, 0.02])
    assert promoted["state"] == "promoted_fomo_wallet"
    assert promoted["mature"] is True
    assert promoted["trimmed_mean_residual_roi_ex_best_1_pct"] > 0.0
    assert promoted["historical_evidence_used_for_promotion"] is False

    demoted = paper.classify_fomo_wallet_returns([0.10, -0.02, -0.03, -0.04, -0.05])
    assert demoted["state"] == "demoted_fomo_wallet"
    assert demoted["mature"] is True


def test_fomo_position_sizing_is_paper_only_and_capped() -> None:
    fraction, growth = paper.best_fomo_position_fraction([0.25, 0.20, 0.15, 0.10, 0.05])
    assert growth is not None and growth > 0.0
    assert 0.0 < fraction <= paper.MAX_FOMO_POSITION_FRACTION
    assert fraction in paper.FOMO_POSITION_FRACTION_GRID

    zero_fraction, nonpositive_growth = paper.best_fomo_position_fraction([-0.10, -0.05, -0.02])
    assert zero_fraction == 0.0
    assert nonpositive_growth is not None and nonpositive_growth <= 0.0


def test_fomo_paper_lane_keeps_existing_execution_boundaries() -> None:
    assert paper.ACTIONABLE_FOMO_STATES == frozenset({"pre_fomo", "active_fomo"})
    assert paper.BOOTSTRAP_PAPER_FRACTION == 0.01
    assert paper.MAX_FOMO_POSITION_FRACTION == 0.05
    assert paper.MIN_FOMO_WALLET_FORWARD_SAMPLES >= 5
