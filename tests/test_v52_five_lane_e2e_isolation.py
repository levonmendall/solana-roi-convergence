from __future__ import annotations

import sqlite3
import threading

from solana_roi.v52_five_lane_e2e_isolation import (
    EconomicEventObservation,
    reconcile_economic_events,
    run_batch3_five_lane_e2e_isolation,
    safety_manifest,
)


class Store:
    def __init__(self) -> None:
        self.db = sqlite3.connect(":memory:", check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self._lock = threading.RLock()


def test_batch3_five_lane_positive_negative_e2e_is_lossless_and_isolated() -> None:
    store = Store()
    report = run_batch3_five_lane_e2e_isolation(store)

    assert report["batch3_ready"] is True
    assert report["validation_errors"] == []
    assert report["canonical_lanes"] == [
        "pump_fun",
        "pump_amm",
        "raydium",
        "fomo",
        "robinhood",
    ]
    assert report["positive_case_count"] == 5
    assert report["negative_case_count"] == 5
    assert report["synthetic_candidate_count"] == 10
    assert report["unexplained_disappearance_count"] == 0
    assert report["batch2_candidate_accounting"]["accounted_candidate_count"] == 10
    assert report["batch2_candidate_accounting"]["terminal_candidate_count"] == 10
    assert report["batch2_candidate_accounting"]["certification_ready"] is True
    assert report["synthetic_certification_contribution_count"] == 0
    assert report["synthetic_profitability_contribution_count"] == 0
    assert report["canonical_statistics_contamination_count"] == 0
    assert report["fomo_shadow_non_authoritative"] is True
    assert report["robinhood_provenance_survives"] is True

    for lane, lane_report in report["lanes"].items():
        assert lane in report["canonical_lanes"]
        assert lane_report["positive_settlement_learning_verified"] is True
        assert lane_report["negative_rejection_accounted"] is True
        assert lane_report["positive_stages"][-2:] == ["settlement", "learning"]
        assert lane_report["negative_stages"][-1] == "position"
        assert "settlement" not in lane_report["negative_stages"]
        assert "learning" not in lane_report["negative_stages"]
        assert lane_report["synthetic_provenance_verified"] is True
        assert lane_report["certification_eligible_case_count"] == 0
        assert lane_report["profitability_statistics_contribution_count"] == 0


def test_cross_lane_economic_event_counts_once_and_preserves_lane_observability() -> None:
    observations = (
        EconomicEventObservation("same-solana-event", "pump_fun", "candidate-a"),
        EconomicEventObservation("same-solana-event", "pump_amm", "candidate-b"),
        EconomicEventObservation("ray-event", "raydium", "candidate-c"),
        EconomicEventObservation("fomo-event", "fomo", "candidate-d"),
        EconomicEventObservation("rh-event", "robinhood", "candidate-e"),
    )
    accounting = reconcile_economic_events(observations)

    assert accounting.validation_errors == ()
    assert accounting.lane_observation_count == 5
    assert accounting.unique_economic_event_count == 4
    assert accounting.cross_lane_event_count == 1
    assert accounting.event_lanes["same-solana-event"] == ("pump_amm", "pump_fun")
    assert all(accounting.per_lane_observation_count[lane] == 1 for lane in (
        "pump_fun", "pump_amm", "raydium", "fomo", "robinhood"
    ))
    assert accounting.economic_events_counted_once is True


def test_batch3_full_report_accepts_cross_lane_dedup_without_losing_lane_visibility() -> None:
    store = Store()
    observations = (
        EconomicEventObservation("shared-event", "pump_fun", "batch3-pump_fun-positive"),
        EconomicEventObservation("shared-event", "pump_amm", "batch3-pump_amm-positive"),
        EconomicEventObservation("ray-event", "raydium", "batch3-raydium-positive"),
        EconomicEventObservation("fomo-event", "fomo", "batch3-fomo-positive"),
        EconomicEventObservation("rh-event", "robinhood", "batch3-robinhood-positive"),
    )
    report = run_batch3_five_lane_e2e_isolation(
        store,
        economic_event_observations=observations,
    )

    assert report["batch3_ready"] is True
    event_report = report["economic_event_accounting"]
    assert event_report["lane_observation_count"] == 5
    assert event_report["unique_economic_event_count"] == 4
    assert event_report["cross_lane_event_count"] == 1
    assert event_report["economic_events_counted_once"] is True


def test_synthetic_candidate_in_canonical_statistics_table_fails_closed() -> None:
    store = Store()
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE risk_conditioned_alpha_v5_outcomes ("
            "id INTEGER PRIMARY KEY, source_signature TEXT NOT NULL, net_return REAL NOT NULL)"
        )
        store.db.execute(
            "INSERT INTO risk_conditioned_alpha_v5_outcomes(source_signature,net_return) VALUES (?,?)",
            ("batch3-pump_fun-positive", 0.99),
        )

    report = run_batch3_five_lane_e2e_isolation(store)

    assert report["batch3_ready"] is False
    assert "synthetic_canonical_statistics_contamination" in report["validation_errors"]
    assert report["canonical_statistics_contamination_count"] == 1
    assert report["synthetic_profitability_contribution_count"] == 1
    assert report["canonical_statistics_contamination"] == [
        "risk_conditioned_alpha_v5_outcomes:source_signature:batch3-pump_fun-positive"
    ]


def test_duplicate_exact_lane_observation_fails_economic_event_accounting_closed() -> None:
    duplicate = EconomicEventObservation("event-1", "pump_fun", "candidate-a")
    accounting = reconcile_economic_events((duplicate, duplicate))

    assert accounting.economic_events_counted_once is False
    assert accounting.lane_observation_count == 1
    assert accounting.unique_economic_event_count == 1
    assert accounting.validation_errors == (
        "observation[1]:duplicate_lane_observation:event-1:pump_fun:candidate-a",
    )


def test_batch3_safety_manifest_preserves_research_only_authority_boundary() -> None:
    manifest = safety_manifest()

    assert manifest["batch_version"] == "v52-batch3-five-lane-e2e-isolation-1"
    assert manifest["incumbent_remains_authoritative"] is True
    assert manifest["incumbent_authority_changed"] is False
    assert manifest["challenger_entry_authority"] is False
    assert manifest["research_only"] is True
    assert manifest["paper_only"] is True
    assert manifest["live_money_authority"] is False
    assert manifest["signing_available"] is False
    assert manifest["transaction_submission_available"] is False
    assert manifest["production_composition_hook"] is False
    assert manifest["positive_negative_e2e_required_per_lane"] is True
    assert manifest["synthetic_certification_contribution_required_zero"] is True
    assert manifest["synthetic_profitability_contribution_required_zero"] is True
    assert manifest["cross_lane_economic_event_dedup_required"] is True
    assert manifest["fomo_shadow_non_authoritative"] is True
    assert manifest["robinhood_provenance_required"] is True
    assert manifest["batch2_lossless_accounting_required"] is True
