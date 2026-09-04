from solana_roi.fomo_continuation_shadow import FOMO_RESEARCH_VERSION


def test_fomo_shadow_version_is_research_only() -> None:
    assert FOMO_RESEARCH_VERSION.startswith("fomo-continuation-shadow")
