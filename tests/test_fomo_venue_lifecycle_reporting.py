from __future__ import annotations

import sqlite3
import threading

import pytest

from solana_roi.fomo_continuation_shadow import FomoContinuationShadow, FomoOutcome
from solana_roi.fomo_venue_lifecycle_reporting import install_fomo_venue_lifecycle_reporting


class Store:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.db = sqlite3.connect(":memory:")
        self.db.row_factory = sqlite3.Row


def _outcome(signature: str, venue: str, lifecycle: str, value: float) -> FomoOutcome:
    return FomoOutcome(
        source_signature=signature,
        observed_at="2026-09-04T00:00:00+00:00",
        venue=venue,
        lifecycle=lifecycle,
        regime="high_speculation",
        fomo_state="active_fomo",
        signal_to_entry_seconds=5.0,
        net_return=value,
        release_commit="release",
    )


def test_fomo_roi_is_reported_separately_by_venue_and_lifecycle() -> None:
    install_fomo_venue_lifecycle_reporting()
    shadow = FomoContinuationShadow(Store(), release_commit="release")
    shadow.record_outcome(_outcome("pump-1", "PUMP_FUN", "pump_bonding_curve", 0.10))
    shadow.record_outcome(_outcome("pump-2", "PUMP_FUN", "pump_bonding_curve", 0.20))
    shadow.record_outcome(_outcome("amm-1", "PUMP_AMM", "pump_amm_post_bonding_curve", -0.05))
    shadow.record_outcome(_outcome("ray-1", "RAYDIUM", "raydium_native_or_migration_unproven", 0.30))

    report = shadow.status()["roi_by_venue_lifecycle"]
    assert report["cross_venue_pooling_for_roi"] is False
    assert report["segment_count"] == 3
    segments = {(row["venue"], row["lifecycle"]): row for row in report["segments"]}

    pump = segments[("PUMP_FUN", "pump_bonding_curve")]
    amm = segments[("PUMP_AMM", "pump_amm_post_bonding_curve")]
    raydium = segments[("RAYDIUM", "raydium_native_or_migration_unproven")]
    assert pump["sample_count"] == 2
    assert pump["mean_residual_roi_pct"] == pytest.approx(15.0)
    assert amm["sample_count"] == 1
    assert amm["mean_residual_roi_pct"] == pytest.approx(-5.0)
    assert raydium["sample_count"] == 1
    assert raydium["mean_residual_roi_pct"] == pytest.approx(30.0)
    assert pump["by_fomo_state"]["active_fomo"]["sample_count"] == 2


def test_fomo_venue_lifecycle_report_has_zero_strategy_authority() -> None:
    install_fomo_venue_lifecycle_reporting()
    shadow = FomoContinuationShadow(Store(), release_commit="release")
    report = shadow.status()["roi_by_venue_lifecycle"]
    assert report["paper_only"] is True
    assert report["live_money_authority"] is False
    assert report["active_strategy_mutation_allowed"] is False
    assert report["historical_promotion_authority"] is False
