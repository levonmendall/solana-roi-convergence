from solana_roi.fomo_continuation_shadow import ACTIVE_STRATEGY_MUTATION_ALLOWED, HISTORICAL_PROMOTION_AUTHORITY


def test_fomo_shadow_never_mutates_active_strategy() -> None:
    assert ACTIVE_STRATEGY_MUTATION_ALLOWED is False
    assert HISTORICAL_PROMOTION_AUTHORITY is False
