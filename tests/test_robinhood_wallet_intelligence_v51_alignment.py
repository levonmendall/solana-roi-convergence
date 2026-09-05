from __future__ import annotations

import sqlite3

from solana_roi.robinhood_chain_paper import RobinhoodChainPaperPlane
from solana_roi import robinhood_pumpfun_wallet_intelligence as intelligence
from solana_roi import robinhood_wallet_intelligence_v51_alignment as alignment


def test_high_chase_and_late_forward_mark_is_strategy_observable_not_immediate_copyable() -> None:
    kwargs = {
        "copyable_price_eth": 1.25,
        "copyable_quote_wei": 1250.0,
        "chase_fraction": 0.25,
        "observation_lag_ms": 35_000.0,
    }
    assert alignment.strategy_observable(**kwargs) is True
    assert alignment.immediate_copyable(**kwargs) is False


def test_low_chase_fast_forward_mark_retains_immediate_copy_diagnostic() -> None:
    kwargs = {
        "copyable_price_eth": 1.10,
        "copyable_quote_wei": 1100.0,
        "chase_fraction": 0.10,
        "observation_lag_ms": 10_000.0,
    }
    assert alignment.strategy_observable(**kwargs) is True
    assert alignment.immediate_copyable(**kwargs) is True


def test_persisted_rows_separate_strategy_observability_from_legacy_immediate_copy() -> None:
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    db.execute(
        "CREATE TABLE robinhood_wallet_intelligence_forward ("
        "swap_id INTEGER PRIMARY KEY, copyable_price_eth REAL, copyable_quote_wei REAL, "
        "chase_fraction REAL, observation_lag_ms REAL, copyable INTEGER NOT NULL, "
        "immediate_copyable INTEGER NOT NULL DEFAULT 0)"
    )
    db.executemany(
        "INSERT INTO robinhood_wallet_intelligence_forward("
        "swap_id,copyable_price_eth,copyable_quote_wei,chase_fraction,observation_lag_ms,copyable,immediate_copyable) "
        "VALUES (?,?,?,?,?,?,?)",
        [
            (1, 1.25, 1250.0, 0.25, 35_000.0, 0, 0),
            (2, 1.10, 1100.0, 0.10, 10_000.0, 1, 0),
        ],
    )
    alignment._normalize_forward_rows_with_connection(db)
    rows = {
        int(row["swap_id"]): dict(row)
        for row in db.execute(
            "SELECT swap_id,copyable,immediate_copyable FROM robinhood_wallet_intelligence_forward ORDER BY swap_id"
        ).fetchall()
    }
    assert rows[1]["copyable"] == 1
    assert rows[1]["immediate_copyable"] == 0
    assert rows[2]["copyable"] == 1
    assert rows[2]["immediate_copyable"] == 1


def test_negative_fast_wallet_still_fails_profitability_even_when_mechanically_observable() -> None:
    rows: list[dict[str, object]] = []
    for index in range(30):
        token = f"token-{index}"
        rows.append(
            {
                "copyable": 1,
                "token": token,
                "side": "buy",
                "token_amount_raw": "100",
                "copyable_quote_wei": 100.0,
                "fee_or_tax_wei": "0",
            }
        )
        rows.append(
            {
                "copyable": 1,
                "token": token,
                "side": "sell",
                "token_amount_raw": "100",
                "copyable_quote_wei": 80.0,
                "fee_or_tax_wei": "0",
            }
        )
    metrics = intelligence._realized_copyable_metrics(rows)
    profile = {
        "entity_id": "0xentity",
        **metrics,
        "copyability_rate": 1.0,
        "manipulation_risk": 0.0,
        "side_wallet_risk": 0.0,
    }
    blockers = intelligence._evidence_blockers(profile)
    assert "copyable_return_not_positive" in blockers
    assert "geometric_growth_not_positive" in blockers


def test_telemetry_moves_15pct_20s_thresholds_under_non_authoritative_diagnostic() -> None:
    payload = alignment._decorate_universe_payload(
        {
            "pumpfun_forward_intelligence_parity": {
                "min_forward_closed_episodes": 30,
                "max_chase_fraction": 0.15,
                "max_observation_lag_seconds": 20.0,
            }
        }
    )
    parity = payload["pumpfun_forward_intelligence_parity"]
    assert "max_chase_fraction" not in parity
    assert "max_observation_lag_seconds" not in parity
    assert parity["chase_policy"] == "context_not_universal_veto"
    assert parity["latency_policy"] == "lane_x_latency_context_not_universal_veto"
    assert parity["teacher_promotion_uses_immediate_copy_gate"] is False
    diagnostic = parity["legacy_immediate_copy_diagnostic"]
    assert diagnostic["max_chase_fraction"] == 0.15
    assert diagnostic["max_observation_lag_seconds"] == 20.0
    assert diagnostic["promotion_authority"] is False


def test_production_composition_installs_wallet_intelligence_v51_alignment() -> None:
    assert bool(
        getattr(
            RobinhoodChainPaperPlane,
            "_roi_robinhood_wallet_intelligence_v51_alignment_installed",
            False,
        )
    )
    assert getattr(
        RobinhoodChainPaperPlane,
        "_roi_robinhood_wallet_intelligence_v51_alignment_version",
    ) == alignment.ALIGNMENT_VERSION
    assert intelligence._candidate_profile is alignment._candidate_profile_v51
    assert intelligence._intelligence_status is alignment._intelligence_status_v51
