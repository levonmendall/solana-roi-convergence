from __future__ import annotations

from solana_roi.robinhood_chain_paper import RobinhoodChainPaperPlane
from solana_roi.robinhood_pumpfun_wallet_intelligence import (
    INTELLIGENCE_VERSION,
    MAX_CHASE_FRACTION,
    MAX_OBSERVATION_LAG_SECONDS,
    MIN_FORWARD_CLOSED_EPISODES,
    MIN_RISK_COVERAGE_RATE,
    MIN_SUPERIORITY_RATIO,
    POLICY,
    _evidence_blockers,
    _overlap,
    _realized_copyable_metrics,
    build_intelligence_entity_universe,
)
from solana_roi.wallet_intelligence import WalletPromotionPolicy


def _candidate(
    actor: str,
    entity: str | None,
    *,
    eligible: bool,
    score: float,
    episodes: int = 30,
    previous: bool = False,
    signals: list[str] | None = None,
    seed: bool = False,
) -> dict[str, object]:
    blockers = [] if eligible else ["insufficient_forward_episodes"]
    return {
        "entity": actor,
        "actor": actor,
        "resolved_entity_id": entity,
        "economic_entity_resolved": bool(entity),
        "priority_research_challenger": bool(entity),
        "seed_label": "public-seed" if seed else None,
        "historical_screen_passed": not seed,
        "historical_return_on_capital_pct": 50.0,
        "trimmed_mean_120s_followthrough_ex_best_1_pct": 20.0,
        "marked_buy_observations": episodes,
        "distinct_tokens": 10,
        "forward_closed_episodes": episodes,
        "copyable_return_on_capital_pct": 25.0,
        "copyability_rate_pct": 90.0,
        "forward_max_drawdown_pct": 20.0,
        "risk_adjusted_copyable_score": score,
        "promotion_evidence_eligible": eligible,
        "promotion_blockers": blockers,
        "copyable_signal_keys": signals or [],
        "previously_selected": previous,
    }


def test_robinhood_forward_teacher_policy_matches_pumpfun_wallet_intelligence() -> None:
    pump = WalletPromotionPolicy()
    assert MIN_FORWARD_CLOSED_EPISODES == pump.min_forward_episodes == 30
    assert POLICY.min_copyability_rate == pump.min_copyability_rate == 0.80
    assert POLICY.max_manipulation_risk == pump.max_manipulation_risk == 0.10
    assert POLICY.max_side_wallet_risk == pump.max_side_wallet_risk == 0.10
    assert POLICY.max_drawdown == pump.max_drawdown == 0.60
    assert MIN_SUPERIORITY_RATIO == pump.min_superiority_ratio == 1.15
    assert MAX_CHASE_FRACTION == 0.15
    assert MAX_OBSERVATION_LAG_SECONDS == 20.0
    assert MIN_RISK_COVERAGE_RATE == 0.80


def test_copyable_metrics_require_actual_closed_forward_episodes() -> None:
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
                "copyable_quote_wei": 120.0,
                "fee_or_tax_wei": "0",
            }
        )
    profile = _realized_copyable_metrics(rows)
    assert profile["closed_episodes"] == 30
    assert profile["distinct_tokens"] == 30
    assert round(float(profile["copyable_return_on_capital"]), 6) == 0.20
    assert float(profile["geometric_growth"]) > 0.0
    assert float(profile["profit_factor"]) > 1.0


def test_unresolved_entity_fails_closed_even_with_good_returns() -> None:
    profile = {
        "entity_id": None,
        "closed_episodes": 30,
        "copyable_return_on_capital": 0.25,
        "geometric_growth": 0.10,
        "profit_factor": 2.0,
        "copyability_rate": 0.90,
        "manipulation_risk": 0.0,
        "side_wallet_risk": 0.0,
        "max_drawdown": 0.20,
    }
    assert "economic_entity_unresolved" in _evidence_blockers(profile)


def test_signal_overlap_penalizes_redundant_wallets() -> None:
    assert _overlap(["a", "b"], ["a", "b"]) == 1.0
    assert _overlap(["a", "b"], ["c", "d"]) == 0.0
    assert 0.0 < _overlap(["a", "b"], ["b", "c"]) < 1.0


def test_same_economic_entity_consumes_only_one_global_slot() -> None:
    research = [
        _candidate("0xactor1", "0xentity", eligible=True, score=2.0),
        _candidate("0xactor2", "0xentity", eligible=True, score=1.0),
        _candidate("0xactor3", "0xentity3", eligible=True, score=1.5),
    ]
    payload = build_intelligence_entity_universe([], research, capacity=12)
    assert payload["economic_entity_deduplication_before_slot"] is True
    assert payload["unresolved_raw_actor_can_consume_slot"] is False
    assert payload["selected_economic_entities"].count("0xentity") == 1
    assert "0xactor1" in payload["high_priority_entities"]
    assert "0xactor2" not in payload["high_priority_entities"]


def test_unresolved_actor_does_not_consume_capacity_but_resolved_seed_can_be_tracked_prospectively() -> None:
    research = [
        _candidate("0xunresolved", None, eligible=False, score=-999.0, episodes=0, seed=True),
        _candidate("0xseed", "0xseedentity", eligible=False, score=-999.0, episodes=0, seed=True),
    ]
    payload = build_intelligence_entity_universe([], research, capacity=12)
    assert "0xunresolved" not in payload["high_priority_entities"]
    assert "0xseed" in payload["high_priority_entities"]
    assert payload["high_priority_entity_count"] == 1
    assert payload["unfilled_capacity"] == 11


def test_challenger_must_clear_superiority_bar_to_replace_full_proven_incumbent_capacity() -> None:
    incumbent = _candidate("0xincumbent", "0xinc", eligible=True, score=1.0, previous=True)
    not_superior = _candidate("0xchallenger", "0xchall", eligible=True, score=1.10)
    payload = build_intelligence_entity_universe([], [incumbent, not_superior], capacity=1)
    assert payload["high_priority_entities"] == ["0xincumbent"]

    superior = _candidate("0xchallenger", "0xchall", eligible=True, score=1.20)
    payload2 = build_intelligence_entity_universe([], [incumbent, superior], capacity=1)
    assert payload2["high_priority_entities"] == ["0xchallenger"]


def test_production_composition_installs_full_intelligence_parity_without_strategy_authority() -> None:
    assert INTELLIGENCE_VERSION == "robinhood-pumpfun-wallet-intelligence-v1"
    assert bool(getattr(RobinhoodChainPaperPlane, "_roi_robinhood_pumpfun_wallet_selection_installed", False))
    assert bool(getattr(RobinhoodChainPaperPlane, "_roi_robinhood_pumpfun_wallet_intelligence_installed", False))
    assert bool(getattr(RobinhoodChainPaperPlane._poll_once, "_roi_robinhood_entity_universe", False))
    assert bool(getattr(RobinhoodChainPaperPlane.status, "_roi_robinhood_entity_universe", False))
