from solana_roi import continuation_market_recalibration as continuation
from solana_roi.robinhood_chain_paper import RobinhoodChainPaperPlane


def test_entity_discovery_does_not_replace_continuation_strategy_authority() -> None:
    assert RobinhoodChainPaperPlane._v5_flow_metrics is continuation._rh_flow_without_sniper_cap
    assert bool(getattr(RobinhoodChainPaperPlane, "_roi_robinhood_strategy_alignment_installed", False))
    assert bool(getattr(RobinhoodChainPaperPlane, "_roi_robinhood_strategy_alignment_composition_installed", False))
