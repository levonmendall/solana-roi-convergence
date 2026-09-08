from __future__ import annotations

import math

import pytest

from solana_roi import v52_continuation_capture as v52


def _observation(**overrides):
    values = {
        "source_signature": "sig-v52",
        "asset_id": "mint-v52",
        "surface": "PUMP_AMM",
        "lifecycle": "post_graduation_30_120s",
        "latency_seconds": 8.0,
        "chase_fraction": 0.12,
        "exact_entry_quote_available": True,
        "exact_exit_quote_available": True,
        "structurally_exitable": True,
        "residual_return_fraction": 0.18,
    }
    values.update(overrides)
    return v52.ContinuationObservation(**values)


def test_locked_v52_identity_and_incumbent_boundary() -> None:
    manifest = v52.safety_manifest()

    assert manifest["challenger_version"] == "roi-convergence-v5.2-continuation-capture-1"
    assert manifest["challenger_epoch"] == "v52-weekend-review-20260908"
    assert manifest["incumbent_version"] == "roi-convergence-v5.1-context-exactness-1"
    assert manifest["incumbent_remains_authoritative"] is True
    assert manifest["incumbent_authority_changed"] is False
    assert manifest["challenger_entry_authority"] is False
    assert manifest["historical_promotion_authority"] is False
    assert manifest["direct_promotion_from_weekend_review"] is False
    assert manifest["production_composition_hook"] is False


def test_paper_only_no_signing_submission_or_live_money() -> None:
    manifest = v52.safety_manifest()
    captured = v52.capture_continuation(_observation())

    for evidence in (manifest, captured):
        assert evidence["paper_only"] is True
        assert evidence["live_money_authority"] is False
        assert evidence["signing_available"] is False
        assert evidence["transaction_submission_available"] is False

    assert captured["research_only"] is True
    assert captured["entry_authority"] is False
    assert captured["direct_promotion_authority"] is False
    assert captured["incumbent_authority_changed"] is False


def test_inside_incumbent_window_is_still_only_v52_research() -> None:
    captured = v52.capture_continuation(
        _observation(latency_seconds=20.0, chase_fraction=0.40)
    )

    assert captured["classification"] == "continuation_candidate_observed"
    assert captured["latency_band"] == "10_20s"
    assert captured["chase_band"] == "25_40pct_challenger"
    assert captured["executable_snapshot"] is True
    assert captured["entry_authority"] is False
    assert captured["research_only"] is True


def test_post_20_second_observation_cannot_become_immediate_copy_authority() -> None:
    captured = v52.capture_continuation(_observation(latency_seconds=20.001))

    assert captured["classification"] == "challenger_observe_only"
    assert captured["latency_band"] == "20_60s_research_only"
    assert "post_20s_immediate_copy_window_research_only" in captured["reasons"]
    assert captured["entry_authority"] is False


def test_gt_40_percent_chase_is_observe_only() -> None:
    captured = v52.capture_continuation(_observation(chase_fraction=0.400001))

    assert captured["classification"] == "challenger_observe_only"
    assert captured["chase_band"] == "gt_40pct_observe_only"
    assert "gt_40pct_chase_observe_only" in captured["reasons"]
    assert captured["entry_authority"] is False


def test_exact_entry_and_exit_quotes_are_required_for_executable_snapshot() -> None:
    missing_entry = v52.capture_continuation(
        _observation(exact_entry_quote_available=False)
    )
    missing_exit = v52.capture_continuation(
        _observation(exact_exit_quote_available=False)
    )

    assert missing_entry["classification"] == "non_executable_quote_research_only"
    assert missing_entry["executable_snapshot"] is False
    assert "exact_entry_quote_missing" in missing_entry["reasons"]

    assert missing_exit["classification"] == "non_executable_quote_research_only"
    assert missing_exit["executable_snapshot"] is False
    assert "exact_exit_quote_missing" in missing_exit["reasons"]


def test_structural_exitability_remains_a_hard_measurement_requirement() -> None:
    captured = v52.capture_continuation(_observation(structurally_exitable=False))

    assert captured["classification"] == "structurally_unexitable_research_only"
    assert captured["executable_snapshot"] is False
    assert "structurally_unexitable" in captured["reasons"]
    assert captured["entry_authority"] is False


def test_supported_surfaces_remain_explicit_and_separate() -> None:
    for surface in ("PUMP_FUN", "PUMP_AMM", "PUMPSWAP", "RAYDIUM", "FOMO", "ROBINHOOD_CHAIN"):
        captured = v52.capture_continuation(_observation(surface=surface))
        assert captured["surface"] == surface
        assert captured["entry_authority"] is False


def test_invalid_identity_surface_and_numeric_context_fail_closed() -> None:
    with pytest.raises(ValueError, match="source_signature_missing"):
        v52.capture_continuation(_observation(source_signature=""))
    with pytest.raises(ValueError, match="surface_unsupported"):
        v52.capture_continuation(_observation(surface="UNKNOWN"))
    with pytest.raises(ValueError, match="latency_seconds_invalid"):
        v52.capture_continuation(_observation(latency_seconds=math.inf))
    with pytest.raises(ValueError, match="chase_fraction_invalid"):
        v52.capture_continuation(_observation(chase_fraction=-0.01))
