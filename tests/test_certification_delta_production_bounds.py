from __future__ import annotations

from solana_roi import certification_delta_production_bounds as bounds


def test_production_delta_bound_sets_safe_default_without_changing_authority(monkeypatch) -> None:
    monkeypatch.delenv("SOLANA_ROI_CERTIFICATION_DELTA_MAX_ROWS", raising=False)

    bounds.configure_production_certification_delta_bound()

    assert bounds.PRODUCTION_CERTIFICATION_DELTA_MAX_ROWS == 250
    assert __import__("os").environ["SOLANA_ROI_CERTIFICATION_DELTA_MAX_ROWS"] == "250"
    assert bounds.PAPER_ONLY is True
    assert bounds.LIVE_MONEY_AUTHORITY is False
    assert bounds.SIGNING_AVAILABLE is False
    assert bounds.TRANSACTION_SUBMISSION_AVAILABLE is False
    assert bounds.STRATEGY_THRESHOLDS_CHANGED is False
    assert bounds.CERTIFICATION_THRESHOLDS_CHANGED is False
    assert bounds.CONTINUITY_SEMANTICS_CHANGED is False


def test_production_delta_bound_preserves_explicit_operator_override(monkeypatch) -> None:
    monkeypatch.setenv("SOLANA_ROI_CERTIFICATION_DELTA_MAX_ROWS", "400")

    bounds.configure_production_certification_delta_bound()

    assert __import__("os").environ["SOLANA_ROI_CERTIFICATION_DELTA_MAX_ROWS"] == "400"
