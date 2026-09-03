from __future__ import annotations


def test_production_installs_wallet_discovery_baseline_before_runtime_build() -> None:
    import solana_roi.production  # noqa: F401
    from solana_roi import runtime as runtime_module
    from solana_roi.config import BASELINE

    policy = runtime_module._wallet_discovery_policy()

    assert runtime_module.BASELINE is BASELINE
    assert policy.max_chase_fraction == BASELINE.max_chase_fraction
