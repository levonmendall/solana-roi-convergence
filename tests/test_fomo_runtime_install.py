from __future__ import annotations

from solana_roi import fomo_runtime_install as fomo_module
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


def test_fomo_runtime_does_not_rewrap_when_outer_wrapper_hides_marker(monkeypatch) -> None:
    install_fomo_runtime()
    inner_manifest = FinalProfitFirstResearchAdapter._manifest
    captured_original = fomo_module._ORIGINAL_MANIFEST

    def outer_manifest(self):
        return inner_manifest(self)

    assert bool(getattr(outer_manifest, "_roi_fomo_runtime", False)) is False
    monkeypatch.setattr(FinalProfitFirstResearchAdapter, "_manifest", outer_manifest)
    install_fomo_runtime()

    assert FinalProfitFirstResearchAdapter._manifest is outer_manifest
    assert fomo_module._ORIGINAL_MANIFEST is captured_original
