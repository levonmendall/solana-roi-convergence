from solana_roi.fomo_continuation_shadow import build_fomo_features, classify_fomo_state


def test_fomo_preserves_existing_chase_and_latency_boundaries() -> None:
    features = build_fomo_features(
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
        chase_fraction=0.15,
        signal_to_entry_seconds=20.0,
        risk_complete=True,
    )
    state = classify_fomo_state(features)
    assert "chase_above_limit" not in state.blockers
    assert "signal_to_entry_above_limit" not in state.blockers

    too_late = classify_fomo_state(
        build_fomo_features(**{**features.__dict__, "chase_fraction": 0.150001, "signal_to_entry_seconds": 20.001})
    )
    assert "chase_above_limit" in too_late.blockers
    assert "signal_to_entry_above_limit" in too_late.blockers
