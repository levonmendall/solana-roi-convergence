from __future__ import annotations

from solana_roi.robinhood_chain_paper import RobinhoodChainPaperPlane
from solana_roi import robinhood_pumpfun_wallet_intelligence as intelligence
from solana_roi.robinhood_wallet_intelligence_policy import (
    MANIPULATION_BLOCKERS,
    POLICY_VERSION,
)


def test_creator_and_insider_are_context_not_blanket_manipulation_blockers() -> None:
    assert RobinhoodChainPaperPlane is not None
    assert POLICY_VERSION == "robinhood-wallet-intelligence-risk-policy-v1"
    assert "creator" not in MANIPULATION_BLOCKERS
    assert "insider" not in MANIPULATION_BLOCKERS
    assert set(MANIPULATION_BLOCKERS) == {
        "bundled_launch",
        "sniper_heavy",
        "common_funded_early_wallet_cluster",
        "scout_deployer_connection",
    }
    assert tuple(intelligence.MANIPULATION_TERMS) == MANIPULATION_BLOCKERS


def test_policy_is_installed_by_production_composition() -> None:
    assert bool(getattr(intelligence, "_roi_robinhood_wallet_intelligence_policy_installed", False))
