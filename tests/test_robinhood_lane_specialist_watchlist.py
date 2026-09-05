from __future__ import annotations

from solana_roi import continuation_market_recalibration as continuation
from solana_roi.robinhood_chain_paper import RobinhoodChainPaperPlane
from solana_roi.robinhood_lane_specialist_watchlist import (
    WATCHLIST_MIN_FORWARD_SAMPLES,
    apply_lane_specialist_fraction,
    build_lane_specialist_watchlist,
)


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


def test_robinhood_watchlist_is_lane_first_and_aggregates_across_regimes() -> None:
    assert WATCHLIST_MIN_FORWARD_SAMPLES == 5
    rows = []
    rows += _rows(
        "0xaaa",
        "fomo_continuation",
        "neutral",
        [0.20, 0.18, 0.22],
        "a-neutral",
    )
    rows += _rows(
        "0xaaa",
        "fomo_continuation",
        "broad_mania",
        [0.16, 0.19, 0.17],
        "a-mania",
    )
    rows += _rows(
        "0xbbb",
        "fomo_continuation",
        "high_speculation",
        [0.08, 0.07, 0.09, 0.06, 0.10, 0.05],
        "b",
    )
    rows += _rows(
        "0xccc",
        "elite_entity_continuation",
        "weak_or_deteriorating",
        [0.12, 0.11, 0.10, 0.13, 0.09, 0.14],
        "c",
    )

    payload = build_lane_specialist_watchlist(rows)

    assert payload["regime_is_roster_dimension"] is False
    assert payload["regime_still_conditions_execution"] is True
    assert payload["ranking_objective"].startswith("robust_forward_roi_pct")

    fomo = next(item for item in payload["lanes"] if item["lane"] == "fomo_continuation")
    assert fomo["leaders"][0]["entity"] == "0xaaa"
    assert fomo["leaders"][0]["regimes_observed"] == ["broad_mania", "neutral"]
    assert fomo["leaders"][1]["entity"] == "0xbbb"

    elite = next(item for item in payload["lanes"] if item["lane"] == "elite_entity_continuation")
    assert elite["leaders"][0]["entity"] == "0xccc"


def test_lane_specialist_sizing_rewards_leaders_and_caps_challengers() -> None:
    assert apply_lane_specialist_fraction(
        0.05,
        {"state": "incumbent_tracking", "rank": 1},
    ) == 0.05
    assert apply_lane_specialist_fraction(
        0.05,
        {"state": "incumbent_tracking", "rank": 2},
    ) == 0.0425
    assert apply_lane_specialist_fraction(
        0.05,
        {"state": "challenger_tracking", "rank": 1},
    ) == 0.005
    assert apply_lane_specialist_fraction(
        0.05,
        {"state": "unranked_lane_challenger", "rank": None},
    ) == 0.005
    assert apply_lane_specialist_fraction(
        0.05,
        {"state": "lane_bootstrap_tracking", "rank": None},
    ) == 0.01


def test_production_composition_keeps_continuation_flow_and_installs_lane_roster() -> None:
    assert RobinhoodChainPaperPlane._v5_flow_metrics is continuation._rh_flow_without_sniper_cap
    assert bool(
        getattr(
            RobinhoodChainPaperPlane._v5_choose_lane_fraction,
            "_roi_robinhood_lane_specialist_watchlist",
            False,
        )
    )
    assert bool(
        getattr(
            RobinhoodChainPaperPlane._poll_once,
            "_roi_robinhood_lane_specialist_watchlist",
            False,
        )
    )
    assert bool(
        getattr(
            RobinhoodChainPaperPlane.status,
            "_roi_robinhood_lane_specialist_watchlist",
            False,
        )
    )
