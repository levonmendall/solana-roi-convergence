from solana_roi.fomo_continuation_shadow import build_fomo_features, classify_fomo_state


def _features(*, chase: float = 0.15, latency: float = 20.0, proven: bool = False):
    return build_fomo_features(
        token_mint="mint",
        observed_at="2026-09-04T00:00:00+00:00",
        venue="raydium",
        lifecycle="continuation",
        regime="neutral",
        independent_buyers_short=3,
        independent_buyers_long=4,
        buys_short=6,
        buys_long=8,
        sells_short=1,
        sells_long=2,
        buy_volume_short=6.0,
        buy_volume_long=8.0,
        sell_volume_short=1.0,
        sell_volume_long=2.0,
        momentum_wallet_participation=1,
        creator_accumulating=False,
        creator_distributing=False,
        early_holder_exit_fraction=0.0,
        chase_fraction=chase,
        signal_to_entry_seconds=latency,
        risk_complete=True,
        trigger_is_proven_wallet=proven,
    )


def test_fomo_preserves_existing_chase_and_latency_boundaries() -> None:
    state = classify_fomo_state(_features())
    assert "chase_above_limit" not in state.blockers
    assert "signal_to_entry_above_limit" not in state.blockers

    too_late = classify_fomo_state(_features(chase=0.150001, latency=20.001))
    assert "chase_above_limit" in too_late.blockers
    assert "signal_to_entry_above_limit" in too_late.blockers


def test_fomo_experiments_keep_wallet_and_entity_flow_hypotheses_separate() -> None:
    proven = classify_fomo_state(_features(chase=0.05, latency=5.0, proven=True))
    assert "wallet_signal_only" in proven.experiment_variants
    assert "wallet_plus_entity_confirmation" in proven.experiment_variants
    assert "wallet_plus_fomo_acceleration" in proven.experiment_variants

    entity_only = classify_fomo_state(_features(chase=0.05, latency=5.0, proven=False))
    assert "pure_entity_flow_fomo" in entity_only.experiment_variants
    assert "wallet_signal_only" not in entity_only.experiment_variants
