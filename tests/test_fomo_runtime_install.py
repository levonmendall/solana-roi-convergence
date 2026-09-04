from __future__ import annotations

from solana_roi.fomo_runtime_install import install_fomo_runtime
from solana_roi.profit_first_entity_final_research import FinalProfitFirstResearchAdapter


def test_fomo_runtime_install_is_shadow_only_and_idempotent() -> None:
    before_observe = FinalProfitFirstResearchAdapter.observe
    install_fomo_runtime()
    first_observe = FinalProfitFirstResearchAdapter.observe
    install_fomo_runtime()
    second_observe = FinalProfitFirstResearchAdapter.observe
    assert bool(getattr(first_observe, "_roi_fomo_runtime", False)) is True
    assert first_observe is second_observe
    assert first_observe is not before_observe or bool(getattr(before_observe, "_roi_fomo_runtime", False))
