from pathlib import Path


def test_production_authority_explicitly_composes_active_fomo_paper_strategy() -> None:
    source = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "solana_roi"
        / "v51_production_authority.py"
    ).read_text(encoding="utf-8")

    assert "from .fomo_paper_strategy import install_fomo_paper_strategy" in source
    assert "app.state.roi_fomo_paper_strategy_explicit = True" in source
    assert (
        '"fomo_paper_strategy_installation": '
        '"explicit_active_paper_entry_authority_before_terminal_exact_exit"'
    ) in source

    runtime_call = source.index("    install_fomo_runtime()")
    strategy_call = source.index("    install_fomo_paper_strategy()")
    measurement_call = source.index("    install_measurement_integrity()")
    hardening_call = source.index("    install_measurement_integrity_hardening()")
    lifecycle_call = source.index("    install_paper_lifecycle_runtime()")

    assert runtime_call < strategy_call < measurement_call < hardening_call < lifecycle_call


def test_fomo_observation_and_strategy_authority_remain_distinct() -> None:
    runtime_source = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "solana_roi"
        / "fomo_runtime_install.py"
    ).read_text(encoding="utf-8")
    strategy_source = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "solana_roi"
        / "fomo_paper_strategy.py"
    ).read_text(encoding="utf-8")

    # The observation runtime is the FOMO continuation shadow: paper-only,
    # no live-money authority, and explicitly unable to mutate active strategy.
    assert '"fomo_research_version": "fomo-continuation-shadow-v1"' in runtime_source
    assert '"fomo_lane": "fomo_continuation_shadow"' in runtime_source
    assert '"active_strategy_mutation_allowed": False' in runtime_source
    assert '"paper_only": True' in runtime_source
    assert '"live_money_authority": False' in runtime_source

    # Active FOMO paper authority is a separate module with the same safety boundary.
    assert "ACTIVE_FOMO_PAPER_STRATEGY_AUTHORITY = True" in strategy_source
    assert "PAPER_ONLY = True" in strategy_source
    assert "LIVE_MONEY_AUTHORITY = False" in strategy_source
    assert "SIGNING_AVAILABLE = False" in strategy_source
    assert "TRANSACTION_SUBMISSION_AVAILABLE = False" in strategy_source
