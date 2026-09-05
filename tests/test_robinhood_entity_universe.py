from __future__ import annotations

from solana_roi import continuation_market_recalibration as continuation
from solana_roi.robinhood_chain_paper import RobinhoodChainPaperPlane
from solana_roi.robinhood_entity_universe import (
    MIN_CHALLENGER_SLOTS,
    MIN_MATURE_FORWARD_SAMPLES,
    TRACKING_CAPACITY_LIMIT,
    build_entity_universe,
)
from solana_roi.wallet_entity_universe_v4 import MIN_CHALLENGER_SLOTS as PUMPFUN_MIN_CHALLENGER_SLOTS


def _rows(entity: str, lane: str, regime: str, returns: list[float], token_prefix: str) -> list[dict[str, object]]:
    return [
        {
            "entity": entity,
            "lane": lane,
            "regime": regime,
            "venue": "PONS_V2_CURVE",
            "lifecycle": "bonding_curve",
            "token": f"{token_prefix}-{index}",
            "net_return": value,
        }
        for index, value in enumerate(returns)
    ]


def test_robinhood_uses_one_global_entity_universe_not_lane_or_regime_watchlists() -> None:
    assert TRACKING_CAPACITY_LIMIT == 12
    assert MIN_CHALLENGER_SLOTS == PUMPFUN_MIN_CHALLENGER_SLOTS == 4
    assert MIN_MATURE_FORWARD_SAMPLES == 5

    evidence: list[dict[str, object]] = []
    evidence += _rows(
        "0xaaa",
        "elite_entity_continuation",
        "neutral",
        [0.18, 0.15, 0.20, 0.17, 0.16, 0.19],
        "a-elite",
    )
    evidence += _rows(
        "0xaaa",
        "fomo_continuation",
        "broad_mania",
        [0.09, 0.11, 0.10, 0.08, 0.12, 0.10],
        "a-fomo",
    )
    evidence += _rows(
        "0xbbb",
        "creator_deployer_continuation",
        "high_speculation",
        [0.06, 0.05, 0.07, 0.04, 0.08, 0.05],
        "b",
    )
    research = [
        {
            "entity": "0xccc",
            "priority_research_challenger": True,
            "research_rank": 1,
            "trimmed_mean_120s_followthrough_ex_best_1_pct": 14.0,
            "marked_buy_observations": 8,
            "distinct_tokens": 5,
        }
    ]

    payload = build_entity_universe(evidence, research)

    assert payload["roster_key"] == "robinhood_chain_x_economic_entity"
    assert payload["lane_specific_watchlists"] is False
    assert payload["regime_specific_watchlists"] is False
    assert payload["roles_are_scores_not_rosters"] is True
    assert "lanes" not in payload
    assert "regimes" not in payload
    assert len(payload["high_priority_entities"]) == len(set(payload["high_priority_entities"]))
    assert "0xaaa" in payload["high_priority_entities"]
    assert "0xbbb" in payload["high_priority_entities"]
    assert "0xccc" in payload["high_priority_entities"]

    aaa = next(row for row in payload["current_role_for_high_priority_entity"] if row["entity"] == "0xaaa")
    assert aaa["current_role"] in {
        "scout_alpha",
        "momentum_alpha",
        "copyable_return_on_capital",
        "signal_decay",
    }
    assert aaa["forward_sample_count"] >= 6


def test_roles_and_regimes_are_diagnostics_on_the_same_global_entities() -> None:
    evidence: list[dict[str, object]] = []
    evidence += _rows(
        "0xaaa",
        "creator_deployer_continuation",
        "neutral",
        [0.12, 0.10, 0.11, 0.09, 0.13, 0.10],
        "a",
    )
    evidence += _rows(
        "0xaaa",
        "hazard_continuation",
        "high_speculation",
        [0.08, 0.07, 0.09, 0.06, 0.10, 0.07],
        "a-hazard",
    )

    payload = build_entity_universe(evidence)

    creator_leaders = payload["role_leaders"]["creator_alpha"]
    hazard_leaders = payload["role_leaders"]["distribution_warning_value"]
    assert creator_leaders[0]["entity"] == "0xaaa"
    assert hazard_leaders[0]["entity"] == "0xaaa"
    assert payload["regime_entity_value"]["neutral"][0]["entity"] == "0xaaa"
    assert payload["regime_entity_value"]["high_speculation"][0]["entity"] == "0xaaa"
    assert payload["high_priority_entities"].count("0xaaa") == 1


def test_production_composition_preserves_continuation_and_installs_global_universe() -> None:
    assert RobinhoodChainPaperPlane._v5_flow_metrics is continuation._rh_flow_without_sniper_cap
    assert bool(
        getattr(
            RobinhoodChainPaperPlane._v5_choose_lane_fraction,
            "_roi_robinhood_entity_universe",
            False,
        )
    )
    assert not bool(
        getattr(
            RobinhoodChainPaperPlane._v5_choose_lane_fraction,
            "_roi_robinhood_lane_specialist_watchlist",
            False,
        )
    )
    assert bool(getattr(RobinhoodChainPaperPlane._poll_once, "_roi_robinhood_entity_universe", False))
    assert bool(getattr(RobinhoodChainPaperPlane.status, "_roi_robinhood_entity_universe", False))
