from __future__ import annotations

from pathlib import Path

from solana_roi.observation_store import ObservationEventStore
from solana_roi.v51_candidate_ledger import record_stage_event
from solana_roi import v51_robinhood_candidate_coverage as robinhood_coverage
from solana_roi.v51_counterfactual_extension import refresh_all_rejected_counterfactuals
from solana_roi.v51_cross_surface_proof import (
    combine_forward_proof_slo,
    combine_rejected_counterfactuals,
    combine_release_attestation,
)
from solana_roi.v51_forward_certification import compose_forward_certification


RELEASE = "c" * 40


def _transport_ready() -> dict:
    plane = {
        "runtime_ready": True,
        "blockers": [],
        "all_regimes_e2e_achievable": True,
        "all_regimes_e2e_proven": False,
    }
    return {
        "release_commit": RELEASE,
        "solana": dict(plane),
        "fomo": dict(plane),
        "robinhood": dict(plane),
        "overall": {
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        },
    }


def _base_evidence() -> dict:
    portfolio = {"overlapping_positions_share_one_capital_base": True}
    return {
        "forward_proof_slo": {
            "proof_state": "confirmed",
            "stage_events_last_60m": 2,
            "coverage_debt_count": 0,
        },
        "release_attestation": {"release_commit": RELEASE, "attested": True},
        "promotion_certification": {"families": {"PUMP_AMM": {"promotion_claim_valid": True}}},
        "rejected_counterfactuals": {
            "rejected_candidate_count": 0,
            "resolved_count": 0,
            "pending_count": 0,
            "resolved_positive_count": 0,
        },
        "hazard_calibration": {"observation_count": 0, "changes_current_hazard_multipliers": False},
        "cross_family_correlation": {"pairs": {}},
        "maturity_allocation_proof": {"families": {}},
        "portfolio_reconciliation": {
            "family_navs_are_not_summed_as_independent_capital": True,
            "audit_epoch_portfolio": dict(portfolio),
            "promotion_compatible_portfolio": dict(portfolio),
        },
    }


def test_43_cross_surface_counterfactuals_include_isolated_robinhood_pending() -> None:
    combined = combine_rejected_counterfactuals(
        {"rejected_candidate_count": 2, "resolved_count": 2, "pending_count": 0, "resolved_positive_count": 1},
        {"rejected_candidate_count": 13910, "resolved_count": 0, "pending_count": 13910, "resolved_positive_count": 0},
        robinhood_proof_state="confirmed",
    )
    evidence = _base_evidence()
    evidence["rejected_counterfactuals"] = combined
    result = compose_forward_certification(
        unified_status=_transport_ready(), evidence=evidence, expected_release_commit=RELEASE
    )
    check = result["checks"]["43_rejected_opportunity_counterfactuals"]
    assert check["rejected_candidate_count"] == 13912
    assert check["pending_count"] == 13910
    assert check["pass"] is False
    assert "rejected_opportunity_counterfactuals_pending" in result["blockers"]
    assert result["system_forward_certified"] is False


def test_40_cross_surface_slo_distinguishes_zero_flow_from_unavailable_measurement() -> None:
    slo = combine_forward_proof_slo(
        {"proof_state": "confirmed", "stage_events_last_60m": 0, "coverage_debt_count": 0},
        {"proof_state": "confirmed", "stage_events_last_60m": 0, "coverage_debt_count": 0},
        robinhood_proof_state="confirmed",
    )
    assert slo["proof_state"] == "confirmed"
    assert slo["stage_events_last_60m"] == 0
    evidence = _base_evidence()
    evidence["forward_proof_slo"] = slo
    result = compose_forward_certification(
        unified_status=_transport_ready(), evidence=evidence, expected_release_commit=RELEASE
    )
    assert result["hard_operational_gates_ok"] is True
    assert result["state"] == "collecting_forward_evidence"
    assert "no_recent_forward_candidate_stage_events" in result["blockers"]


def test_41_release_attestation_requires_local_and_isolated_robinhood_surfaces() -> None:
    combined = combine_release_attestation(
        {
            "release_commit": RELEASE,
            "measurement_epoch": "m",
            "attested": True,
            "surfaces": {"SOLANA": {"attested": True}, "FOMO": {"attested": True}},
        },
        {
            "release_commit": RELEASE,
            "measurement_epoch": "m",
            "attested": True,
            "surfaces": {"ROBINHOOD_CHAIN": {"attested": True}},
        },
        robinhood_proof_state="confirmed",
    )
    assert combined["attested"] is True
    assert set(combined["surfaces"]) == {"SOLANA", "FOMO", "ROBINHOOD_CHAIN"}

    stale = combine_release_attestation(
        combined,
        {"release_commit": RELEASE, "surfaces": {"ROBINHOOD_CHAIN": {"attested": True}}},
        robinhood_proof_state="stale",
    )
    assert stale["attested"] is False


def test_robinhood_candidate_coverage_uses_append_only_stage_event_writer() -> None:
    assert robinhood_coverage._record_stage is record_stage_event
    assert "append-only-stages" in robinhood_coverage.COVERAGE_VERSION


def _robinhood_ledger_schema(store: ObservationEventStore) -> None:
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE v51_robinhood_candidate_ledger ("
            "candidate_id TEXT PRIMARY KEY, release_commit TEXT, token TEXT, decision_reason TEXT, "
            "observed_at TEXT, updated_at TEXT, decision TEXT, market TEXT, venue TEXT, lifecycle TEXT, "
            "selected_lane TEXT, position_fraction REAL)"
        )
        for i in range(3):
            store.db.execute(
                "INSERT INTO v51_robinhood_candidate_ledger VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    f"rh-{i}", RELEASE, f"token-{i}", "failed_closed", "2026-09-05T00:00:00+00:00",
                    "2026-09-05T00:00:00+00:00", "paper_reject", f"market-{i}", "PONS_V2_CURVE",
                    "bonding_curve", None, 0.0,
                ),
            )


def test_robinhood_counterfactual_refresh_is_incremental_and_preserves_resolution(tmp_path: Path) -> None:
    store = ObservationEventStore(tmp_path / "counterfactual.sqlite3")
    try:
        _robinhood_ledger_schema(store)
        first = refresh_all_rejected_counterfactuals(store)
        assert first["robinhood_rejections_materialized"] == 3
        assert first["new_robinhood_rejections_examined"] == 3
        with store._lock, store.db:
            store.db.execute(
                "UPDATE v51_rejected_counterfactuals SET forward_net_return=0.25,"
                "resolution_source='future_mark',counterfactual_state='resolved_shadow_forward_outcome' "
                "WHERE surface='ROBINHOOD_CHAIN' AND candidate_id='rh-0'"
            )
        second = refresh_all_rejected_counterfactuals(store)
        assert second["robinhood_rejections_materialized"] == 0
        assert second["new_robinhood_rejections_examined"] == 0
        assert second["existing_counterfactual_rows_rewritten"] is False
        with store._lock:
            row = store.db.execute(
                "SELECT forward_net_return,resolution_source FROM v51_rejected_counterfactuals "
                "WHERE surface='ROBINHOOD_CHAIN' AND candidate_id='rh-0'"
            ).fetchone()
        assert float(row["forward_net_return"]) == 0.25
        assert row["resolution_source"] == "future_mark"
    finally:
        store.close()


def test_post_deploy_probe_default_timeout_covers_deep_proof_reads() -> None:
    probe = Path("tests/production_forward_proof_probe.py").read_text(encoding="utf-8")
    workflow = Path(".github/workflows/production-forward-proof.yml").read_text(encoding="utf-8")
    assert 'FORWARD_PROOF_HTTP_TIMEOUT_SECONDS", "60"' in probe
    assert "FORWARD_PROOF_HTTP_TIMEOUT_SECONDS: '60'" in workflow
