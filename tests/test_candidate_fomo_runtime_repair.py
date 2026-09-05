from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from solana_roi import candidate_fomo_runtime_repair as repair


def test_candidate_telemetry_excludes_non_candidate_scout_activity() -> None:
    payload = {
        "scout_candidate_continuity_repair": {
            "candidate_normalization_failed_session": 1906,
            "candidate_normalization_failure_reasons": {
                "supported_swap_source_missing": 1895,
                "economic_token_movement_missing": 2,
                "multiple_tracked_scout_accounts": 4,
                "semantic_directional_endpoint_missing": 5,
            },
        }
    }
    obj = SimpleNamespace(
        _roi_candidate_fomo_repair_pump_endpoint_fallback_attempts=5,
        _roi_candidate_fomo_repair_pump_endpoint_fallback_resolved=3,
        _roi_candidate_fomo_repair_pump_endpoint_fallback_fail_closed=2,
    )

    repair._reclassify_candidate_telemetry(payload, obj)
    status = payload["scout_candidate_continuity_repair"]

    assert status["candidate_normalization_failed_session_raw"] == 1906
    assert status["filtered_non_candidate_transactions_session"] == 1897
    assert status["candidate_normalization_failed_session"] == 9
    assert status["candidate_normalization_failure_reasons"] == {
        "multiple_tracked_scout_accounts": 4,
        "semantic_directional_endpoint_missing": 5,
    }
    assert status["pump_endpoint_fallback_attempts_session"] == 5
    assert status["pump_endpoint_fallback_resolved_session"] == 3
    assert status["pump_endpoint_fallback_fail_closed_session"] == 2


def test_pump_graph_fallback_uses_pr167_economic_actor_proof(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        repair,
        "_ORIGINAL_GRAPH_SWAP_FACTS",
        lambda result, *, wallet, source: (None, "semantic_graph_actor_legs_missing"),
    )
    monkeypatch.setattr(
        repair.economic,
        "_economic_movement",
        lambda result, wallet: (
            {
                "side": "buy",
                "token_mint": "TOKEN",
                "token_amount": 123.0,
                "native_amount_sol": 1.25,
            },
            None,
        ),
    )

    facts, error = repair._pump_graph_with_economic_endpoint(
        {}, wallet="SCOUT", source="PUMP_FUN"
    )

    assert error is None
    assert facts == ("buy", "TOKEN", 123.0, 1.25)


def test_pump_graph_fallback_never_broadens_other_venue_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called = False

    def economic_movement(result, wallet):
        nonlocal called
        called = True
        return None, "should_not_run"

    monkeypatch.setattr(
        repair,
        "_ORIGINAL_GRAPH_SWAP_FACTS",
        lambda result, *, wallet, source: (None, "semantic_graph_actor_legs_missing"),
    )
    monkeypatch.setattr(repair.economic, "_economic_movement", economic_movement)

    facts, error = repair._pump_graph_with_economic_endpoint(
        {}, wallet="SCOUT", source="RAYDIUM"
    )

    assert facts is None
    assert error == "semantic_graph_actor_legs_missing"
    assert called is False


def test_fomo_scanner_exposes_active_candidate_throughput() -> None:
    now = datetime(2026, 9, 5, 17, 30, tzinfo=timezone.utc)
    rows = []
    for index, wallet in enumerate(("A", "B", "C")):
        rows.append(
            {
                "signature": f"sig-{index}",
                "wallet": wallet,
                "token_mint": "TOKEN",
                "side": "buy",
                "token_amount": 100.0,
                "native_amount_sol": 1.0,
                "reference_price_sol": 0.01,
                "observed_at": (now - timedelta(seconds=5 - index)).isoformat(),
                "received_at": (now - timedelta(seconds=5 - index)).isoformat(),
                "source": "solana-direct:PUMP_AMM:buy",
            }
        )

    candidates, diagnostics = repair._fomo_scan_rows(rows, now=now)

    assert len(candidates) == 1
    assert candidates[0]["state"] == "active_fomo"
    assert diagnostics["rows_scanned"] == 3
    assert diagnostics["tokens_grouped"] == 1
    assert diagnostics["tokens_with_buy"] == 1
    assert diagnostics["tokens_with_short_buy"] == 1
    assert diagnostics["active_fomo_candidates"] == 1
    assert diagnostics["candidates_emitted"] == 1
    assert diagnostics["scanner_consuming_normalized_swaps"] is True


def test_fomo_scanner_reports_threshold_rejection_instead_of_opaque_zero() -> None:
    now = datetime(2026, 9, 5, 17, 30, tzinfo=timezone.utc)
    rows = [
        {
            "signature": "sig-one",
            "wallet": "A",
            "token_mint": "TOKEN",
            "side": "buy",
            "token_amount": 100.0,
            "native_amount_sol": 1.0,
            "reference_price_sol": 0.01,
            "observed_at": (now - timedelta(seconds=2)).isoformat(),
            "received_at": (now - timedelta(seconds=2)).isoformat(),
            "source": "solana-direct:PUMP_FUN:buy",
        }
    ]

    candidates, diagnostics = repair._fomo_scan_rows(rows, now=now)

    assert candidates == []
    assert diagnostics["rows_scanned"] == 1
    assert diagnostics["tokens_with_short_buy"] == 1
    assert diagnostics["rejected_min_buys"] == 1
    assert diagnostics["candidates_emitted"] == 0
