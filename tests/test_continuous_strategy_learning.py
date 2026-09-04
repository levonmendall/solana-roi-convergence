from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from solana_roi.continuous_strategy_learning import (
    ACTIVE_STRATEGY_MUTATION_ALLOWED,
    ADDITIONAL_RPC_FANOUT,
    FOMO_ENTRY_WINDOWS_SECONDS,
    FOMO_POST_ENTRY_FLOW_HORIZONS_SECONDS,
    HISTORICAL_PROMOTION_AUTHORITY,
    LIVE_MONEY_AUTHORITY,
    PATH_HORIZONS_SECONDS,
    SIGNING_AVAILABLE,
    STRATEGY_RULES_CHANGED,
    TRANSACTION_SUBMISSION_AVAILABLE,
    _path_metrics,
    _variant_performance_from_rows,
    _window_stats,
)


def test_learning_plane_preserves_authority_and_hotpath_boundaries() -> None:
    assert ACTIVE_STRATEGY_MUTATION_ALLOWED is False
    assert HISTORICAL_PROMOTION_AUTHORITY is False
    assert LIVE_MONEY_AUTHORITY is False
    assert SIGNING_AVAILABLE is False
    assert TRANSACTION_SUBMISSION_AVAILABLE is False
    assert ADDITIONAL_RPC_FANOUT is False
    assert STRATEGY_RULES_CHANGED is False


def test_learning_windows_cover_entry_recalibration_and_post_entry_exit_research() -> None:
    assert FOMO_ENTRY_WINDOWS_SECONDS == (1, 3, 5, 10, 20, 30)
    assert PATH_HORIZONS_SECONDS == (1, 2, 5, 10, 20, 30, 60, 120, 300)
    assert FOMO_POST_ENTRY_FLOW_HORIZONS_SECONDS == (5, 10, 20, 30, 60)


def test_window_stats_count_independent_entities_not_addresses() -> None:
    rows = [
        {
            "wallet": "a1",
            "side": "buy",
            "token_amount": 10.0,
            "copyable_price_sol": 0.2,
        },
        {
            "wallet": "a2",
            "side": "buy",
            "token_amount": 5.0,
            "copyable_price_sol": 0.2,
        },
        {
            "wallet": "creator",
            "side": "sell",
            "token_amount": 2.0,
            "copyable_price_sol": 0.25,
        },
        {
            "wallet": "mom",
            "side": "buy",
            "token_amount": 1.0,
            "copyable_price_sol": 0.3,
        },
    ]
    mapping = {
        "a1": "entity:buyer-cluster",
        "a2": "entity:buyer-cluster",
        "creator": "entity:creator",
        "mom": "entity:momentum",
    }
    stats = _window_stats(
        rows,
        entity_mapping=mapping,
        excluded_entities={"entity:creator"},
        qualified_momentum_wallets={"mom"},
        creator_entity="entity:creator",
    )

    assert stats["independent_buyers"] == 2
    assert stats["buys"] == 3
    assert stats["sells"] == 1
    assert stats["momentum_wallet_participation"] == 1
    assert stats["buy_volume"] == pytest.approx(3.3)
    assert stats["sell_volume"] == pytest.approx(0.5)
    assert stats["creator_sell_volume"] == pytest.approx(0.5)


def test_path_metrics_capture_mfe_mae_timing_and_exit_giveback() -> None:
    start = datetime(2026, 9, 4, 17, 0, tzinfo=timezone.utc)
    marks = [
        {"received_at": (start + timedelta(seconds=1)).isoformat(), "price_sol": 1.05},
        {"received_at": (start + timedelta(seconds=5)).isoformat(), "price_sol": 0.90},
        {"received_at": (start + timedelta(seconds=10)).isoformat(), "price_sol": 1.40},
        {"received_at": (start + timedelta(seconds=20)).isoformat(), "price_sol": 1.15},
    ]
    result = _path_metrics(
        reference_price_sol=1.0,
        reference_at=start,
        marks=marks,
        realized_exit_return=0.15,
    )

    assert result["mark_count"] == 4
    assert result["mfe_mark_return_pct"] == pytest.approx(40.0)
    assert result["mae_mark_return_pct"] == pytest.approx(-10.0)
    assert result["time_to_mfe_seconds"] == pytest.approx(10.0)
    assert result["time_to_mae_seconds"] == pytest.approx(5.0)
    assert result["mark_mfe_minus_realized_exit_return_pct"] == pytest.approx(25.0)


def test_variant_performance_attributes_same_outcome_to_eligible_experiments() -> None:
    rows = [
        {
            "state_json": '{"experiment_variants":["wallet_signal_only","wallet_plus_fomo_acceleration"]}',
            "net_return": 0.20,
        },
        {
            "state_json": '{"experiment_variants":["wallet_signal_only"]}',
            "net_return": -0.05,
        },
        {
            "state_json": '{"experiment_variants":["pure_entity_flow_fomo"]}',
            "net_return": 0.10,
        },
    ]
    result = _variant_performance_from_rows(rows)

    wallet = result["wallet_signal_only"]
    assert wallet["sample_count"] == 2
    assert wallet["mean_residual_roi_pct"] == pytest.approx(7.5)
    assert wallet["positive_rate_pct"] == pytest.approx(50.0)

    combined = result["wallet_plus_fomo_acceleration"]
    assert combined["sample_count"] == 1
    assert combined["mean_residual_roi_pct"] == pytest.approx(20.0)

    pure = result["pure_entity_flow_fomo"]
    assert pure["sample_count"] == 1
    assert pure["mean_residual_roi_pct"] == pytest.approx(10.0)
