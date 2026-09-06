from __future__ import annotations

from solana_roi import cross_regime_paper_allocator as allocator


def _append_without_debt(grouped: dict[str, list[float]], metadata: dict[str, dict[str, str]], value: object) -> None:
    allocator._append(
        grouped,
        metadata,
        surface="FOMO",
        lane="independent_fomo_continuation",
        venue="PUMP_AMM",
        lifecycle="pump_amm_early_post_graduation_30_120s",
        regime="high_speculation",
        risk_signature="independent_market_flow",
        net_return=value,
    )


def test_105_legacy_allocator_helper_call_remains_compatible_and_preserves_total_loss() -> None:
    grouped: dict[str, list[float]] = {}
    metadata: dict[str, dict[str, str]] = {}
    _append_without_debt(grouped, metadata, -1.0)
    assert list(grouped.values()) == [[-1.0]]


def test_105_legacy_allocator_helper_call_never_imputes_invalid_return_to_zero() -> None:
    grouped: dict[str, list[float]] = {}
    metadata: dict[str, dict[str, str]] = {}
    _append_without_debt(grouped, metadata, float("nan"))
    assert grouped == {}
    assert len(metadata) == 1
