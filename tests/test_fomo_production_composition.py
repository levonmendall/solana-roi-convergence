from solana_roi.fomo_runtime_install import install_fomo_runtime
from solana_roi.profit_first_entity_final_research import FinalProfitFirstResearchAdapter
from solana_roi.wallet_context_router_precision_repair import install_wallet_context_router_precision_repair


def test_context_precision_install_composes_fomo_without_authority() -> None:
    install_wallet_context_router_precision_repair()
    assert bool(getattr(FinalProfitFirstResearchAdapter.observe, "_roi_fomo_runtime", False))
    assert bool(getattr(FinalProfitFirstResearchAdapter._manifest, "_roi_fomo_runtime", False))
    assert bool(getattr(FinalProfitFirstResearchAdapter._manifest, "_roi_wallet_context_precision_repair", False))
    install_fomo_runtime()
    assert bool(getattr(FinalProfitFirstResearchAdapter._manifest, "_roi_fomo_runtime", False))
