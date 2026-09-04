from __future__ import annotations

import json
import sqlite3
import threading

from solana_roi.cross_regime_paper_allocator import build_cross_regime_allocation


class _Store:
    def __init__(self) -> None:
        self.db = sqlite3.connect(":memory:")
        self.db.row_factory = sqlite3.Row
        self._lock = threading.RLock()


def _solana_schema(store: _Store) -> None:
    store.db.execute(
        "CREATE TABLE risk_conditioned_alpha_v5_outcomes ("
        "id INTEGER PRIMARY KEY, release_commit TEXT, lane TEXT, venue TEXT, lifecycle TEXT, "
        "regime TEXT, risk_signature TEXT, net_return REAL)"
    )


def _fomo_schema(store: _Store) -> None:
    store.db.execute(
        "CREATE TABLE fomo_paper_outcomes ("
        "id INTEGER PRIMARY KEY, release_commit TEXT, source_signature TEXT, venue TEXT, "
        "lifecycle TEXT, regime TEXT, net_return REAL)"
    )
    store.db.execute(
        "CREATE TABLE fomo_shadow_observations ("
        "id INTEGER PRIMARY KEY, release_commit TEXT, source_signature TEXT, state_json TEXT)"
    )


def _positive_skew() -> list[float]:
    return [1.0] * 16 + [-0.20] * 24


def test_allocator_does_not_dilute_profitable_lifecycle_with_weak_lifecycle() -> None:
    store = _Store()
    with store.db:
        _solana_schema(store)
        index = 1
        for value in _positive_skew():
            store.db.execute(
                "INSERT INTO risk_conditioned_alpha_v5_outcomes VALUES (?,?,?,?,?,?,?,?)",
                (
                    index,
                    "release",
                    "graduation_continuation",
                    "PUMP_AMM",
                    "pump_amm_immediate_graduation_0_30s",
                    "high_speculation",
                    "clean",
                    value,
                ),
            )
            index += 1
        for _ in range(40):
            store.db.execute(
                "INSERT INTO risk_conditioned_alpha_v5_outcomes VALUES (?,?,?,?,?,?,?,?)",
                (
                    index,
                    "release",
                    "graduation_continuation",
                    "PUMP_AMM",
                    "pump_amm_mature_intraday_momentum",
                    "high_speculation",
                    "clean",
                    -0.30,
                ),
            )
            index += 1

    result = build_cross_regime_allocation(store, "release")
    assert result["allocator_version"] == "cross-regime-paper-allocator-v2-context-isolated"
    assert result["mature_promoted_segments"] == 1
    assert len(result["segment_profiles"]) == 2

    promoted = [
        key
        for key, score in result["segment_scores"].items()
        if score > 0.0
    ]
    assert len(promoted) == 1
    assert "pump_amm_immediate_graduation_0_30s" in promoted[0]
    assert "pump_amm_mature_intraday_momentum" not in promoted[0]
    assert result["family_weights"]["SOLANA_UNDERLYING:PUMP_AMM"] <= 0.25
    assert result["paper_cash_weight"] >= 0.75


def test_allocator_keeps_full_risk_signatures_separate() -> None:
    store = _Store()
    with store.db:
        _solana_schema(store)
        index = 1
        for risk_signature, values in (
            ("clean", _positive_skew()),
            ("creator_distributing", [-0.25] * 40),
        ):
            for value in values:
                store.db.execute(
                    "INSERT INTO risk_conditioned_alpha_v5_outcomes VALUES (?,?,?,?,?,?,?,?)",
                    (
                        index,
                        "release",
                        "entity_flow_momentum",
                        "RAYDIUM",
                        "raydium_native_or_migration_unproven",
                        "neutral",
                        risk_signature,
                        value,
                    ),
                )
                index += 1

    result = build_cross_regime_allocation(store, "release")
    clean = [
        key for key in result["segment_scores"]
        if key.endswith("|clean")
    ]
    hazard = [
        key for key in result["segment_scores"]
        if key.endswith("|creator_distributing")
    ]
    assert len(clean) == 1
    assert len(hazard) == 1
    assert result["segment_scores"][clean[0]] > 0.0
    assert result["segment_scores"][hazard[0]] == 0.0


def test_fomo_and_solana_same_venue_share_unknown_correlation_family_cap() -> None:
    store = _Store()
    with store.db:
        _solana_schema(store)
        _fomo_schema(store)
        for index, value in enumerate(_positive_skew(), start=1):
            store.db.execute(
                "INSERT INTO risk_conditioned_alpha_v5_outcomes VALUES (?,?,?,?,?,?,?,?)",
                (
                    index,
                    "release",
                    "graduation_continuation",
                    "PUMP_AMM",
                    "pump_amm_early_post_graduation_30_120s",
                    "high_speculation",
                    "clean",
                    value,
                ),
            )
            signature = f"fomo-{index}"
            store.db.execute(
                "INSERT INTO fomo_paper_outcomes VALUES (?,?,?,?,?,?,?)",
                (
                    index,
                    "release",
                    signature,
                    "PUMP_AMM",
                    "pump_amm_early_post_graduation_30_120s",
                    "high_speculation",
                    value * 0.8,
                ),
            )
            store.db.execute(
                "INSERT INTO fomo_shadow_observations VALUES (?,?,?,?)",
                (
                    index,
                    "release",
                    signature,
                    json.dumps(
                        {
                            "state": "active_fomo",
                            "experiment_variants": ["clean_fomo"],
                        }
                    ),
                ),
            )

    result = build_cross_regime_allocation(store, "release")
    family = "SOLANA_UNDERLYING:PUMP_AMM"
    assert result["mature_promoted_segments"] == 2
    assert result["family_weights"][family] <= 0.25
    family_segments = [
        key
        for key, meta in result["segment_metadata"].items()
        if meta["correlation_family"] == family
    ]
    assert sum(result["paper_allocation_weights"].get(key, 0.0) for key in family_segments) <= 0.25
    assert result["paper_cash_weight"] >= 0.75
    assert result["live_money_authority"] is False
