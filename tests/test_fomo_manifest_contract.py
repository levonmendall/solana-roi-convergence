from __future__ import annotations

from solana_roi import fomo_continuation_shadow as fomo


def test_fomo_lane_has_no_live_or_mutation_authority() -> None:
    assert fomo.PAPER_ONLY is True
    assert fomo.LIVE_MONEY_AUTHORITY is False
    assert fomo.SIGNING_AVAILABLE is False
    assert fomo.TRANSACTION_SUBMISSION_AVAILABLE is False
    assert fomo.ACTIVE_STRATEGY_MUTATION_ALLOWED is False
    assert fomo.HISTORICAL_PROMOTION_AUTHORITY is False
    assert fomo.SIGNAL_DECAY_DELAYS_SECONDS == (1, 2, 5, 10, 20, 30, 60)
