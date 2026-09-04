from solana_roi.fomo_continuation_shadow import build_fomo_features, classify_fomo_state


def test_fomo_state_is_market_state_not_venue() -> None:
    common = dict(
        token_mint="mint",
        observed_at="2026-09-04T00:00:00+00:00",
        lifecycle="continuation",
        regime="neutral",
        independent_buyers_short=4,
        independent_buyers_long=5,
        buys_short=8,
        buys_long=10,
        sells_short=1,
        sells_long=3,
        buy_volume_short=12.0,
        buy_volume_long=15.0,
        sell_volume_short=1.0,
        sell_volume_long=3.0,
        momentum_wallet_participation=2,
        creator_accumulating=False,
        creator_distributing=False,
        early_holder_exit_fraction=0.0,
        chase_fraction=0.05,
        signal_to_entry_seconds=5.0,
        risk_complete=True,
    )
    pump = classify_fomo_state(build_fomo_features(venue="pump_fun", **common))
    raydium = classify_fomo_state(build_fomo_features(venue="raydium", **common))
    assert pump.state == raydium.state == "active_fomo"
