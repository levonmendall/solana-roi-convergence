from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

from solana_roi.risk_conditioned_alpha_v51 import (
    _context_key_v51,
    _context_returns_v51,
    _rh_context_returns_v51,
    _risk_descriptor_v51,
    execution_cost_band,
    fomo_hazard_severity,
    fomo_hazard_signature,
    robust_cost_ceiling,
)


class _Store:
    def __init__(self) -> None:
        self.db = sqlite3.connect(":memory:")
        self.db.row_factory = sqlite3.Row
        self._lock = threading.RLock()


class _SolanaAdapter:
    def __init__(self) -> None:
        self.store = _Store()
        self.release_commit = "release"


class _RobinhoodAdapter:
    def __init__(self) -> None:
        self.store = _Store()
        self.release_commit = "release"

    @staticmethod
    def _v5_context_key(
        *,
        entity: str,
        role: str,
        lane: str,
        venue: str,
        lifecycle: str,
        regime: str,
        risk_signature: str,
        flow_state: str,
        latency: str = "chain_poll",
    ) -> str:
        return "|".join(
            (
                entity,
                role,
                lane,
                venue,
                lifecycle,
                regime,
                risk_signature,
                flow_state,
                latency,
            )
        )


def _pre(entity: str, *, cost: float = 0.02) -> dict[str, object]:
    return {
        "trigger_entity": entity,
        "venue": "PUMP_AMM",
        "lifecycle": "pump_amm_early_post_graduation_30_120s",
        "regime": "high_speculation",
        "role": "independent_wallet",
        "risk": {"risk_signature": "clean"},
        "flow_state": "neutral",
        "round_trip_cost_fraction": cost,
    }


def test_context_key_is_entity_and_execution_cost_exact() -> None:
    key_a = _context_key_v51(_pre("entity:A", cost=0.02), "elite_wallet_continuation", chase=0.10, latency=4.0)
    key_b = _context_key_v51(_pre("entity:B", cost=0.02), "elite_wallet_continuation", chase=0.10, latency=4.0)
    key_cost = _context_key_v51(_pre("entity:A", cost=0.09), "elite_wallet_continuation", chase=0.10, latency=4.0)

    assert key_a != key_b
    assert key_a != key_cost
    assert key_a.startswith("entity:A|elite_wallet_continuation|PUMP_AMM|")
    assert key_a.endswith("|le_3pct")
    assert key_cost.endswith("|7_15pct")
    assert execution_cost_band(0.151) == "gt_15pct"


def test_solana_context_backoff_never_borrows_another_entity() -> None:
    adapter = _SolanaAdapter()
    with adapter.store.db:
        adapter.store.db.execute(
            "CREATE TABLE risk_conditioned_alpha_v5_outcomes ("
            "id INTEGER PRIMARY KEY, release_commit TEXT, lane TEXT, venue TEXT, lifecycle TEXT, "
            "regime TEXT, risk_signature TEXT, context_key TEXT, net_return REAL)"
        )
        key_a = _context_key_v51(_pre("entity:A"), "elite_wallet_continuation", chase=0.10, latency=4.0)
        key_b = _context_key_v51(_pre("entity:B"), "elite_wallet_continuation", chase=0.10, latency=4.0)
        index = 1
        for _ in range(5):
            adapter.store.db.execute(
                "INSERT INTO risk_conditioned_alpha_v5_outcomes VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    index,
                    "release",
                    "elite_wallet_continuation",
                    "PUMP_AMM",
                    "pump_amm_early_post_graduation_30_120s",
                    "high_speculation",
                    "clean",
                    key_a,
                    -0.10,
                ),
            )
            index += 1
        for _ in range(40):
            adapter.store.db.execute(
                "INSERT INTO risk_conditioned_alpha_v5_outcomes VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    index,
                    "release",
                    "elite_wallet_continuation",
                    "PUMP_AMM",
                    "pump_amm_early_post_graduation_30_120s",
                    "high_speculation",
                    "clean",
                    key_b,
                    1.00,
                ),
            )
            index += 1

    values, source = _context_returns_v51(
        adapter,
        lane="elite_wallet_continuation",
        venue="PUMP_AMM",
        lifecycle="pump_amm_early_post_graduation_30_120s",
        regime="high_speculation",
        context_key=key_a,
    )
    assert source == "exact_entity_bootstrap"
    assert len(values) == 5
    assert values == [-0.10] * 5


def test_unknown_hard_risk_fails_closed() -> None:
    result = _risk_descriptor_v51(
        soft_flags=(),
        hard_flags=("new_unclassified_hard_condition",),
    )
    assert result["structurally_tradeable"] is False
    assert result["unclassified_hard_stops"] == ["new_unclassified_hard_condition"]


def test_fomo_hazard_signature_preserves_hazard_identity_and_severity() -> None:
    creator = {
        "experiment_variants": [
            "wallet_plus_fomo_acceleration",
            "hazard_fomo",
            "creator_distributing",
        ]
    }
    early = {
        "experiment_variants": [
            "wallet_plus_fomo_acceleration",
            "hazard_fomo",
            "early_holder_distribution",
        ]
    }
    both = {
        "experiment_variants": [
            "hazard_fomo",
            "creator_distributing",
            "early_holder_distribution",
        ]
    }
    assert fomo_hazard_signature(creator) == "creator_distributing"
    assert fomo_hazard_signature(early) == "early_holder_distribution"
    assert fomo_hazard_signature(both) == "creator_distributing+early_holder_distribution"
    assert fomo_hazard_severity(both) > fomo_hazard_severity(creator)


def test_robinhood_context_backoff_never_borrows_another_entity() -> None:
    adapter = _RobinhoodAdapter()
    with adapter.store.db:
        adapter.store.db.execute(
            "CREATE TABLE robinhood_paper_trials ("
            "id INTEGER PRIMARY KEY, trigger_entity TEXT, venue TEXT, lifecycle TEXT)"
        )
        adapter.store.db.execute(
            "CREATE TABLE robinhood_v5_trial_context ("
            "trial_id INTEGER PRIMARY KEY, trigger_role TEXT, lane TEXT, regime TEXT, "
            "risk_signature TEXT, context_key TEXT)"
        )
        adapter.store.db.execute(
            "CREATE TABLE robinhood_paper_outcomes ("
            "id INTEGER PRIMARY KEY, release_commit TEXT, trial_id INTEGER, net_return REAL)"
        )
        index = 1
        for entity, count, value in (("entity:A", 5, -0.10), ("entity:B", 40, 1.00)):
            for _ in range(count):
                context_key = adapter._v5_context_key(
                    entity=entity,
                    role="independent_entity",
                    lane="elite_entity_continuation",
                    venue="PONS_V2_CURVE",
                    lifecycle="bonding_curve",
                    regime="high_speculation",
                    risk_signature="clean",
                    flow_state="neutral",
                )
                adapter.store.db.execute(
                    "INSERT INTO robinhood_paper_trials VALUES (?,?,?,?)",
                    (index, entity, "PONS_V2_CURVE", "bonding_curve"),
                )
                adapter.store.db.execute(
                    "INSERT INTO robinhood_v5_trial_context VALUES (?,?,?,?,?,?)",
                    (
                        index,
                        "independent_entity",
                        "elite_entity_continuation",
                        "high_speculation",
                        "clean",
                        context_key,
                    ),
                )
                adapter.store.db.execute(
                    "INSERT INTO robinhood_paper_outcomes VALUES (?,?,?,?)",
                    (index, "release", index, value),
                )
                index += 1

    values, source = _rh_context_returns_v51(
        adapter,
        entity="entity:A",
        role="independent_entity",
        lane="elite_entity_continuation",
        venue="PONS_V2_CURVE",
        lifecycle="bonding_curve",
        regime="high_speculation",
        risk_signature="clean",
        flow_state="neutral",
    )
    assert source == "exact_entity_bootstrap"
    assert values == [-0.10] * 5


def test_robust_cost_ceiling_uses_trimmed_tail_adjusted_edge_not_raw_mean() -> None:
    profile = {
        "state": "promoted_positive_log_growth",
        "mean_return": 2.0,
        "trimmed_mean_ex_best": 0.10,
        "expected_shortfall_20": -0.10,
    }
    ceiling = robust_cost_ceiling(profile, 0.15)
    assert 0.15 < ceiling < 0.20


def test_production_robinhood_module_contains_guarded_v51_install_hook() -> None:
    path = Path(__file__).resolve().parents[1] / "src" / "solana_roi" / "robinhood_chain_paper.py"
    source = path.read_text(encoding="utf-8")
    assert 'getattr(_risk_v5, "_INSTALLED", False)' in source
    assert "install_risk_conditioned_alpha_v51()" in source
