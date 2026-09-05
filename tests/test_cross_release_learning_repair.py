from __future__ import annotations

import json
import sqlite3
import threading

from solana_roi import cross_release_learning_repair as repair
from solana_roi import risk_conditioned_alpha_v51 as v51


class _Store:
    def __init__(self) -> None:
        self.db = sqlite3.connect(":memory:")
        self.db.row_factory = sqlite3.Row
        self._lock = threading.RLock()


class _Adapter:
    def __init__(self, store: _Store, release_commit: str) -> None:
        self.store = store
        self.release_commit = release_commit


def _solana_schema(store: _Store) -> None:
    with store.db:
        store.db.execute(
            "CREATE TABLE risk_conditioned_alpha_v5_outcomes ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, release_commit TEXT NOT NULL, strategy_version TEXT NOT NULL, "
            "source_signature TEXT NOT NULL, lane TEXT NOT NULL, venue TEXT NOT NULL, lifecycle TEXT NOT NULL, "
            "regime TEXT NOT NULL, risk_signature TEXT NOT NULL, context_key TEXT NOT NULL, net_return REAL NOT NULL)"
        )


def test_solana_cross_release_epoch_combines_compatible_releases_and_deduplicates() -> None:
    store = _Store()
    _solana_schema(store)
    context_key = (
        "entity:A|elite_wallet_continuation|PUMP_AMM|pump_amm_early_post_graduation_30_120s|"
        "high_speculation|independent_wallet|clean|neutral|baseline_le_15pct|le_5s|le_3pct"
    )
    with store.db:
        # Preserved history from before the compatibility epoch must never gain
        # promotion authority merely because the version label happens to match.
        for index in range(30):
            store.db.execute(
                "INSERT INTO risk_conditioned_alpha_v5_outcomes("
                "release_commit,strategy_version,source_signature,lane,venue,lifecycle,regime,risk_signature,context_key,net_return) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    "release-old",
                    repair.SOLANA_COMPATIBILITY_VERSION,
                    f"old-{index}",
                    "elite_wallet_continuation",
                    "PUMP_AMM",
                    "pump_amm_early_post_graduation_30_120s",
                    "high_speculation",
                    "clean",
                    context_key,
                    9.0,
                ),
            )

    repair._register_release(store, "release-a")
    for index in range(10):
        with store.db:
            store.db.execute(
                "INSERT INTO risk_conditioned_alpha_v5_outcomes("
                "release_commit,strategy_version,source_signature,lane,venue,lifecycle,regime,risk_signature,context_key,net_return) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    "release-a",
                    repair.SOLANA_COMPATIBILITY_VERSION,
                    f"signal-{index}",
                    "elite_wallet_continuation",
                    "PUMP_AMM",
                    "pump_amm_early_post_graduation_30_120s",
                    "high_speculation",
                    "clean",
                    context_key,
                    0.10,
                ),
            )

    adapter = _Adapter(store, "release-b")
    for index in range(10, 20):
        with store.db:
            store.db.execute(
                "INSERT INTO risk_conditioned_alpha_v5_outcomes("
                "release_commit,strategy_version,source_signature,lane,venue,lifecycle,regime,risk_signature,context_key,net_return) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    "release-b",
                    repair.SOLANA_COMPATIBILITY_VERSION,
                    f"signal-{index}",
                    "elite_wallet_continuation",
                    "PUMP_AMM",
                    "pump_amm_early_post_graduation_30_120s",
                    "high_speculation",
                    "clean",
                    context_key,
                    0.10,
                ),
            )
        # The current adapter registers release-b before the statistical query.

    with store.db:
        store.db.execute(
            "INSERT INTO risk_conditioned_alpha_v5_outcomes("
            "release_commit,strategy_version,source_signature,lane,venue,lifecycle,regime,risk_signature,context_key,net_return) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                "release-b",
                repair.SOLANA_COMPATIBILITY_VERSION,
                "signal-0",
                "elite_wallet_continuation",
                "PUMP_AMM",
                "pump_amm_early_post_graduation_30_120s",
                "high_speculation",
                "clean",
                context_key,
                0.20,
            ),
        )

    values, source = repair._solana_context_returns_cross_release(
        adapter,
        lane="elite_wallet_continuation",
        venue="PUMP_AMM",
        lifecycle="pump_amm_early_post_graduation_30_120s",
        regime="high_speculation",
        context_key=context_key,
    )
    assert source == "exact_entity_context_cross_release_epoch"
    assert len(values) == 20
    assert 9.0 not in values
    assert values.count(0.20) == 1
    assert values.count(0.10) == 19
    assert repair._compatible_release_count(store) == 2


def _fomo_schema(store: _Store) -> None:
    with store.db:
        store.db.execute(
            "CREATE TABLE fomo_shadow_observations ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, release_commit TEXT NOT NULL, source_signature TEXT NOT NULL, "
            "venue TEXT NOT NULL, lifecycle TEXT NOT NULL, regime TEXT NOT NULL, state_json TEXT NOT NULL)"
        )
        store.db.execute(
            "CREATE TABLE profit_first_final_trials ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, release_commit TEXT NOT NULL, source_signature TEXT NOT NULL, "
            "lane TEXT NOT NULL, trigger_wallet TEXT NOT NULL)"
        )
        store.db.execute(
            "CREATE TABLE fomo_shadow_outcomes ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, release_commit TEXT NOT NULL, source_signature TEXT NOT NULL, "
            "net_return REAL NOT NULL)"
        )


def _add_fomo_shadow(
    store: _Store,
    *,
    release: str,
    source: str,
    wallet: str,
    value: float,
    feature_version: str,
) -> None:
    state = json.dumps(
        {
            "state": "active_fomo",
            "experiment_variants": ["wallet_plus_fomo_acceleration", "clean_fomo"],
            "feature_version": feature_version,
        }
    )
    with store.db:
        store.db.execute(
            "INSERT INTO fomo_shadow_observations(release_commit,source_signature,venue,lifecycle,regime,state_json) "
            "VALUES (?,?,?,?,?,?)",
            (release, source, "PUMP_AMM", "pump_amm_early_post_graduation_30_120s", "high_speculation", state),
        )
        store.db.execute(
            "INSERT INTO profit_first_final_trials(release_commit,source_signature,lane,trigger_wallet) VALUES (?,?,?,?)",
            (release, source, "unified_profit_maximizer", wallet),
        )
        store.db.execute(
            "INSERT INTO fomo_shadow_outcomes(release_commit,source_signature,net_return) VALUES (?,?,?)",
            (release, source, value),
        )


def test_fomo_cross_release_requires_epoch_and_feature_compatibility() -> None:
    store = _Store()
    _fomo_schema(store)
    _add_fomo_shadow(
        store,
        release="release-old",
        source="old",
        wallet="wallet-A",
        value=8.0,
        feature_version=repair.FOMO_FEATURE_COMPATIBILITY_VERSION,
    )
    repair._register_release(store, "release-a")
    _add_fomo_shadow(
        store,
        release="release-a",
        source="a",
        wallet="wallet-A",
        value=0.10,
        feature_version=repair.FOMO_FEATURE_COMPATIBILITY_VERSION,
    )
    _add_fomo_shadow(
        store,
        release="release-a",
        source="incompatible",
        wallet="wallet-A",
        value=7.0,
        feature_version="older-fomo-economics",
    )
    adapter = _Adapter(store, "release-b")
    _add_fomo_shadow(
        store,
        release="release-b",
        source="b",
        wallet="wallet-A",
        value=0.20,
        feature_version=repair.FOMO_FEATURE_COMPATIBILITY_VERSION,
    )

    values = repair._fomo_context_returns_cross_release(
        adapter,
        wallet="wallet-A",
        venue="PUMP_AMM",
        lifecycle="pump_amm_early_post_graduation_30_120s",
        regime="high_speculation",
        hazard_signature="clean",
    )
    assert values == [0.10, 0.20]
    assert repair._compatible_release_count(store) == 2


def _allocator_schema(store: _Store) -> None:
    with store.db:
        store.db.execute(
            "CREATE TABLE fomo_paper_outcomes ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, release_commit TEXT NOT NULL, strategy_version TEXT NOT NULL, "
            "source_signature TEXT NOT NULL, venue TEXT NOT NULL, lifecycle TEXT NOT NULL, regime TEXT NOT NULL, "
            "net_return REAL NOT NULL)"
        )
        store.db.execute(
            "CREATE TABLE fomo_shadow_observations ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, release_commit TEXT NOT NULL, source_signature TEXT NOT NULL, "
            "state_json TEXT)"
        )


def test_allocator_keeps_tracked_and_independent_fomo_in_separate_lanes() -> None:
    store = _Store()
    _allocator_schema(store)
    repair._register_release(store, "release-a")
    state = json.dumps(
        {
            "state": "active_fomo",
            "experiment_variants": ["wallet_plus_fomo_acceleration", "clean_fomo"],
            "feature_version": repair.FOMO_FEATURE_COMPATIBILITY_VERSION,
        }
    )
    with store.db:
        store.db.execute(
            "INSERT INTO fomo_paper_outcomes(release_commit,strategy_version,source_signature,venue,lifecycle,regime,net_return) "
            "VALUES (?,?,?,?,?,?,?)",
            (
                "release-a",
                repair.FOMO_TRACKED_STRATEGY_VERSION,
                "tracked",
                "PUMP_AMM",
                "pump_amm_early_post_graduation_30_120s",
                "high_speculation",
                0.10,
            ),
        )
        store.db.execute(
            "INSERT INTO fomo_shadow_observations(release_commit,source_signature,state_json) VALUES (?,?,?)",
            ("release-a", "tracked", state),
        )
        store.db.execute(
            "INSERT INTO fomo_paper_outcomes(release_commit,strategy_version,source_signature,venue,lifecycle,regime,net_return) "
            "VALUES (?,?,?,?,?,?,?)",
            (
                "release-a",
                repair.FOMO_INDEPENDENT_STRATEGY_VERSION,
                "independent",
                "PUMP_AMM",
                "pump_amm_early_post_graduation_30_120s",
                "high_speculation",
                0.20,
            ),
        )

    grouped, metadata = repair._segment_returns_cross_release(store, "release-a")
    lanes = {item["lane"] for item in metadata.values() if item["surface"] == "FOMO"}
    assert lanes == {"fomo_continuation", "independent_fomo_continuation"}
    assert sum(len(values) for values in grouped.values()) == 2


def test_repair_declares_paper_only_and_no_historical_epoch_promotion() -> None:
    assert repair.PAPER_ONLY is True
    assert repair.LIVE_MONEY_AUTHORITY is False
    assert repair.SIGNING_AVAILABLE is False
    assert repair.TRANSACTION_SUBMISSION_AVAILABLE is False
    assert repair.HISTORICAL_PRE_EPOCH_PROMOTION_AUTHORITY is False
    assert v51.V51_VERSION == repair.SOLANA_COMPATIBILITY_VERSION
