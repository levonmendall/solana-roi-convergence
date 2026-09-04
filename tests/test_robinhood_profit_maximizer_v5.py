from __future__ import annotations

from solana_roi.robinhood_chain_paper import RobinhoodChainPaperPlane
from solana_roi.robinhood_chain_profit_maximizer import (
    ROBINHOOD_V5_VERSION,
    RobinhoodProfitMaximizerMixin,
)
from solana_roi.risk_conditioned_alpha_v51 import ROBINHOOD_V51_VERSION


def test_v5_policy_overrides_legacy_entry_and_dispatches_settlement_by_evidence_version() -> None:
    assert issubclass(RobinhoodChainPaperPlane, RobinhoodProfitMaximizerMixin)
    assert RobinhoodChainPaperPlane._maybe_open_v3.__module__.endswith("robinhood_chain_profit_maximizer")
    # V5.1 intentionally supersedes only the Pons V2 entry path when the fully
    # composed production installer has already run in this process. Direct module
    # imports still expose the immutable base-v5 implementation.
    v2_module = RobinhoodChainPaperPlane._maybe_open_v2.__module__
    assert v2_module.endswith(("robinhood_chain_profit_maximizer", "risk_conditioned_alpha_v51"))
    # Settlement has a class-level compatibility dispatcher: v5/v5.1 trials use
    # learned exits while pre-v5 trials retain their exact historical reason semantics.
    assert RobinhoodChainPaperPlane._settle_one.__module__.endswith("robinhood_chain_paper")
    assert RobinhoodProfitMaximizerMixin._settle_one.__module__.endswith("robinhood_chain_profit_maximizer")


def test_active_robinhood_version_is_base_v5_or_explicit_v51_override() -> None:
    v2_module = RobinhoodChainPaperPlane._maybe_open_v2.__module__
    if v2_module.endswith("risk_conditioned_alpha_v51"):
        assert ROBINHOOD_V5_VERSION == ROBINHOOD_V51_VERSION
    else:
        assert ROBINHOOD_V5_VERSION == "robinhood-chain-risk-conditioned-v2"


def test_robinhood_regime_sizing_tightens_weak_and_mania_correlation_risk() -> None:
    assert RobinhoodProfitMaximizerMixin._v5_regime_multiplier("weak_or_deteriorating") == 0.50
    assert RobinhoodProfitMaximizerMixin._v5_regime_multiplier("neutral") == 1.0
    assert RobinhoodProfitMaximizerMixin._v5_regime_multiplier("high_speculation") > 1.0
    assert RobinhoodProfitMaximizerMixin._v5_regime_multiplier("broad_mania") < 1.0


def test_creator_lane_and_hazard_lane_are_first_class() -> None:
    dummy = object.__new__(RobinhoodProfitMaximizerMixin)
    lanes = dummy._v5_candidate_lanes(
        metrics={
            "trigger_is_creator": True,
            "independent_entities_60s": 3,
            "state": "active_fomo",
        },
        hazards=["creator_distributing"],
        lifecycle_progress=0.90,
    )
    assert "creator_deployer_continuation" in lanes
    assert "entity_flow_accumulation" in lanes
    assert "fomo_continuation" in lanes
    assert "lifecycle_transition_continuation" in lanes
    assert "hazard_continuation" in lanes
