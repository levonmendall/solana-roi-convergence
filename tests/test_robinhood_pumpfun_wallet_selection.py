from __future__ import annotations

from solana_roi.robinhood_chain_paper import RobinhoodChainPaperPlane
from solana_roi.robinhood_pumpfun_wallet_selection import (
    BROAD_SAMPLE_MODULUS,
    BROAD_SCAN_LIMIT,
    CURATED_RESEARCH_SEEDS,
    HISTORICAL_MAX_SWAPS,
    HISTORICAL_MIN_CLOSED_EPISODES,
    HISTORICAL_MIN_DISTINCT_TOKENS,
    HISTORICAL_MIN_PROFIT_FACTOR,
    HISTORICAL_MIN_RETURN_ON_CAPITAL,
    SELECTION_VERSION,
    build_quality_entity_universe,
)
from solana_roi.wallet_discovery import WalletDiscoveryPolicy


def _rows(entity: str, values: list[float]) -> list[dict[str, object]]:
    return [
        {
            "entity": entity,
            "lane": "elite_entity_continuation",
            "regime": "neutral",
            "venue": "PONS_V2_CURVE",
            "lifecycle": "bonding_curve",
            "token": f"token-{entity}-{index}",
            "net_return": value,
        }
        for index, value in enumerate(values)
    ]


def _candidate(entity: str, *, seed: bool = False, historical_roi_pct: float = 25.0) -> dict[str, object]:
    return {
        "entity": entity,
        "priority_research_challenger": True,
        "candidate_source": "curated_public_roi_seed" if seed else "pumpfun_equivalent_local_historical_screen",
        "seed_label": "research-seed" if seed else None,
        "seed_priority": 1 if seed else None,
        "historical_screen_passed": not seed,
        "historical_return_on_capital_pct": historical_roi_pct,
        "historical_profit_factor": 1.5,
        "marked_buy_observations": 0,
        "distinct_tokens": 6,
    }


def test_robinhood_selection_constants_match_pumpfun_discovery_policy() -> None:
    pump = WalletDiscoveryPolicy()
    assert BROAD_SAMPLE_MODULUS == pump.broad_sample_modulus == 20
    assert BROAD_SCAN_LIMIT == pump.broad_scan_limit == 600
    assert HISTORICAL_MAX_SWAPS == pump.historical_max_signatures == 120
    assert HISTORICAL_MIN_CLOSED_EPISODES == pump.historical_min_closed_episodes == 5
    assert HISTORICAL_MIN_DISTINCT_TOKENS == pump.historical_min_distinct_tokens == 5
    assert HISTORICAL_MIN_RETURN_ON_CAPITAL == pump.historical_min_return_on_capital == 0.05
    assert HISTORICAL_MIN_PROFIT_FACTOR == pump.historical_min_profit_factor == 1.05


def test_capacity_is_a_ceiling_and_negative_filler_wallets_do_not_fill_empty_slots() -> None:
    evidence: list[dict[str, object]] = []
    for index in range(12):
        evidence += _rows(f"0xnegative{index}", [-0.10, -0.12, -0.08, -0.15, -0.11, -0.09])
    research = [
        _candidate("0xquality1", historical_roi_pct=40.0),
        _candidate("0xquality2", historical_roi_pct=20.0),
    ]

    payload = build_quality_entity_universe(evidence, research, capacity=12)

    assert payload["tracking_capacity_is_ceiling_not_target"] is True
    assert payload["capacity_fill_required"] is False
    assert payload["empty_slots_allowed"] is True
    assert payload["quality_over_full_roster"] is True
    assert payload["high_priority_entities"] == ["0xquality1", "0xquality2"]
    assert payload["high_priority_entity_count"] == 2
    assert payload["unfilled_capacity"] == 10
    assert not any(entity.startswith("0xnegative") for entity in payload["high_priority_entities"])


def test_mature_positive_forward_wallet_can_join_without_a_seed_label() -> None:
    evidence = _rows("0xproven", [0.12, 0.10, 0.08, 0.11, 0.09, 0.13])
    payload = build_quality_entity_universe(evidence, [], capacity=12)
    assert payload["high_priority_entities"] == ["0xproven"]
    assert payload["current_role_for_high_priority_entity"][0]["selection_state"] == "mature_positive_forward_incumbent"


def test_mature_negative_seed_is_not_kept_just_to_fill_capacity() -> None:
    evidence = _rows("0xseed", [-0.10, -0.08, -0.12, -0.09, -0.11, -0.07])
    research = [_candidate("0xseed", seed=True)]
    payload = build_quality_entity_universe(evidence, research, capacity=12)
    assert payload["high_priority_entities"] == []
    assert payload["unfilled_capacity"] == 12


def test_public_roi_seeds_are_research_hypotheses_not_permanent_whitelist() -> None:
    assert len(CURATED_RESEARCH_SEEDS) == 6
    assert len({row["address"] for row in CURATED_RESEARCH_SEEDS}) == 6
    payload = build_quality_entity_universe([], [_candidate("0xseed", seed=True)], capacity=12)
    assert payload["named_seed_is_permanent_whitelist"] is False
    assert payload["historical_or_mark_evidence_has_paper_promotion_authority"] is False
    assert payload["provider_requests_added"] == 0


def test_production_composition_installs_pumpfun_selection_under_global_universe() -> None:
    assert SELECTION_VERSION == "robinhood-pumpfun-wallet-selection-v1"
    assert bool(getattr(RobinhoodChainPaperPlane, "_roi_robinhood_pumpfun_wallet_selection_installed", False))
    assert bool(getattr(RobinhoodChainPaperPlane._poll_once, "_roi_robinhood_entity_universe", False))
    assert bool(getattr(RobinhoodChainPaperPlane.status, "_roi_robinhood_entity_universe", False))
