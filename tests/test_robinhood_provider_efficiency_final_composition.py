from solana_roi import robinhood_catchup_capacity_repair as catchup
from solana_roi import robinhood_live_frontier_verification_repair as frontier
from solana_roi import robinhood_provider_efficiency_repair as efficiency
from solana_roi.robinhood_chain_paper import RobinhoodChainPaperPlane


def test_final_robinhood_production_composition_binds_combined_market_log_helper() -> None:
    """The helper must be active in the real final class, not only in isolated tests."""
    assert getattr(
        RobinhoodChainPaperPlane,
        "_roi_post177_forward_pipeline_composition_compat_installed",
        False,
    ) is True
    assert catchup._fetch_market_logs is efficiency._combined_fetch_market_logs
    assert frontier._fetch_market_logs is efficiency._combined_fetch_market_logs

    status = efficiency.status()
    assert status["installed"] is True
    assert status["composed_catchup_market_log_batching"] is True
    assert status["composed_live_frontier_market_log_batching"] is True
    assert status["block_coverage_reduced"] is False
    assert status["market_coverage_reduced"] is False
    assert status["strategy_thresholds_changed"] is False
    assert status["paper_only"] is True
    assert status["live_money_authority"] is False
