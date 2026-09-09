from __future__ import annotations

from solana_roi.robinhood_chain_paper import RobinhoodChainPaperPlane
from solana_roi.robinhood_chain_profit_maximizer import (
    ROBINHOOD_V5_VERSION,
    RobinhoodProfitMaximizerMixin,
)
from solana_roi.risk_conditioned_alpha_v51 import ROBINHOOD_V51_VERSION


def test_v5_policy_overrides_legacy_entry_and_dispatches_settlement_by_evidence_version() -> None:
    assert issubclass(RobinhoodChainPaperPlane, RobinhoodProfitMaximizerMixin)
    # In fully composed v5.2 production, lifecycle validation is the final entry
    # owner. The exact predecessor remains reachable for lineage/compatibility.
    v3 = RobinhoodChainPaperPlane._maybe_open_v3
    if v3.__module__.endswith("v52_robinhood_position_lifecycle"):
        assert bool(getattr(v3, "_roi_v52_position_lifecycle", False)) is True
        assert callable(getattr(v3, "__wrapped__", None))
    else:
        assert v3.__module__.endswith("robinhood_chain_profit_maximizer")

    # V5.1 compatibility may supersede the Pons V2 substrate; v5.2 lifecycle may
    # then wrap that composed path without changing its durable storage label.
    v2 = RobinhoodChainPaperPlane._maybe_open_v2
    assert v2.__module__.endswith(
        ("robinhood_chain_profit_maximizer", "risk_conditioned_alpha_v51", "v52_robinhood_position_lifecycle")
    )
    if v2.__module__.endswith("v52_robinhood_position_lifecycle"):
        assert bool(getattr(v2, "_roi_v52_position_lifecycle", False)) is True
        assert callable(getattr(v2, "__wrapped__", None))

    # Settlement keeps the compatibility dispatcher beneath the v5.2 lifecycle
    # accounting owner; historical reason semantics remain available by evidence version.
    settle = RobinhoodChainPaperPlane._settle_one
    assert settle.__module__.endswith(("robinhood_chain_paper", "v52_robinhood_position_lifecycle"))
    if settle.__module__.endswith("v52_robinhood_position_lifecycle"):
        assert bool(getattr(settle, "_roi_v52_position_lifecycle", False)) is True
    assert RobinhoodProfitMaximizerMixin._settle_one.__module__.endswith("robinhood_chain_profit_maximizer")


def test_active_robinhood_version_is_base_v5_or_explicit_v51_override() -> None:
    v2_module = RobinhoodChainPaperPlane._maybe_open_v2.__module__
    if v2_module.endswith(("risk_conditioned_alpha_v51", "v52_robinhood_position_lifecycle")):
        # v5.2 intentionally retains the v5.1-compatible durable storage label;
        # economic authority comes from the v5.2 release/authority epoch.
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
