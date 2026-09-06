from __future__ import annotations

import sqlite3
import threading
from types import SimpleNamespace

from fastapi import FastAPI

from solana_roi.v51_candidate_lane_accounting import (
    ACCOUNTING_VERSION,
    FIVE_LANES,
    build_five_lane_candidate_accounting,
)
from solana_roi import v51_phase14_api


ECONOMIC_EPOCH = "test-economic-epoch"
MEASUREMENT_EPOCH = "test-measurement-epoch"
AUTHORITY_ID = "test-authority"


class _Store:
    def __init__(self, *, schema: bool = True) -> None:
        self.db = sqlite3.connect(":memory:", check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        if schema:
            self.db.executescript(
                "CREATE TABLE v51_candidates ("
                "surface TEXT NOT NULL,candidate_id TEXT NOT NULL,venue TEXT,"
                "economic_epoch TEXT NOT NULL,measurement_epoch TEXT NOT NULL);"
                "CREATE TABLE v51_candidate_current_state ("
                "surface TEXT NOT NULL,candidate_id TEXT NOT NULL,stage TEXT NOT NULL,"
                "status TEXT NOT NULL,reason TEXT NOT NULL,economic_epoch TEXT NOT NULL,"
                "measurement_epoch TEXT NOT NULL);"
            )

    def candidate(self, candidate_id: str, venue: str) -> None:
        self.db.execute(
            "INSERT INTO v51_candidates(surface,candidate_id,venue,economic_epoch,measurement_epoch) "
            "VALUES ('SOLANA',?,?,?,?)",
            (candidate_id, venue, ECONOMIC_EPOCH, MEASUREMENT_EPOCH),
        )
        self.db.commit()

    def stage(
        self,
        surface: str,
        candidate_id: str,
        stage: str,
        status: str,
        reason: str = "test",
    ) -> None:
        self.db.execute(
            "INSERT INTO v51_candidate_current_state("
            "surface,candidate_id,stage,status,reason,economic_epoch,measurement_epoch) "
            "VALUES (?,?,?,?,?,?,?)",
            (
                surface,
                candidate_id,
                stage,
                status,
                reason,
                ECONOMIC_EPOCH,
                MEASUREMENT_EPOCH,
            ),
        )
        self.db.commit()


def _coverage(*, robinhood: dict | None = None, robinhood_state: str = "confirmed") -> dict:
    if robinhood is None and robinhood_state == "confirmed":
        robinhood = {
            "authority_id": AUTHORITY_ID,
            "economic_freeze_epoch": ECONOMIC_EPOCH,
            "canonical_candidate_count": 0,
            "explicit_rejection_count": 0,
            "settled_entry_count": 0,
            "pending_settlement_count": 0,
            "decision_coverage_debt_count": 0,
            "coverage_debt_count": 0,
            "coverage_complete": True,
            "stage_summary": {},
        }
    return {
        "authority_id": AUTHORITY_ID,
        "economic_freeze_epoch": ECONOMIC_EPOCH,
        "measurement_epoch": MEASUREMENT_EPOCH,
        "robinhood_proof_available": isinstance(robinhood, dict),
        "robinhood_proof_state": robinhood_state,
        "robinhood": robinhood,
    }


def test_five_lane_accounting_conserves_terminal_pending_and_debt() -> None:
    store = _Store()
    store.candidate("pump-reject", "PUMP_FUN")
    store.stage("SOLANA", "pump-reject", "position", "not_opened")

    store.candidate("amm-open", "PUMP_AMM")
    store.stage("SOLANA", "amm-open", "position", "paper_position_authorized")

    store.candidate("ray-debt", "RAYDIUM")
    store.stage("SOLANA", "ray-debt", "context", "coverage_debt")
    store.candidate("ray-settled", "RAYDIUM")
    store.stage("SOLANA", "ray-settled", "settlement", "complete")

    store.stage("FOMO", "fomo-reject", "candidate", "complete")
    store.stage("FOMO", "fomo-reject", "position", "not_opened")

    robinhood = {
        "authority_id": AUTHORITY_ID,
        "economic_freeze_epoch": ECONOMIC_EPOCH,
        "canonical_candidate_count": 2,
        "explicit_rejection_count": 1,
        "settled_entry_count": 1,
        "pending_settlement_count": 0,
        "decision_coverage_debt_count": 0,
        "coverage_debt_count": 0,
        "coverage_complete": True,
        "stage_summary": {"decision": {"complete": 2}},
    }
    payload = build_five_lane_candidate_accounting(store, merged_coverage=_coverage(robinhood=robinhood))

    assert payload["accounting_version"] == ACCOUNTING_VERSION
    assert tuple(payload["lanes"]) == FIVE_LANES
    assert payload["lanes"]["pump_fun"]["terminal_rejected_count"] == 1
    assert payload["lanes"]["pump_amm"]["valid_pending_candidate_count"] == 1
    assert payload["lanes"]["raydium"]["terminal_settled_count"] == 1
    assert payload["lanes"]["raydium"]["coverage_debt_candidate_count"] == 1
    assert payload["lanes"]["fomo"]["terminal_rejected_count"] == 1
    assert payload["lanes"]["robinhood"]["terminal_candidate_count"] == 2

    conservation = payload["candidate_conservation"]
    assert conservation["candidate_population_verifiable"] is True
    assert conservation["observed_candidate_count"] == 7
    assert conservation["terminal_candidate_count"] == 5
    assert conservation["valid_pending_candidate_count"] == 1
    assert conservation["coverage_debt_candidate_count"] == 1
    assert conservation["unexplained_candidate_count"] == 0
    assert conservation["conservation_delta"] == 0
    assert conservation["conserved"] is True
    assert conservation["reconciled"] is False
    assert payload["paper_only"] is True
    assert payload["live_money_authority"] is False
    assert payload["changes_strategy_authority"] is False
    assert payload["changes_economic_thresholds"] is False


def test_unexplained_candidate_is_explicit_and_never_disappears() -> None:
    store = _Store()
    store.candidate("pump-unexplained", "PUMP_FUN")

    payload = build_five_lane_candidate_accounting(store, merged_coverage=_coverage())
    lane = payload["lanes"]["pump_fun"]
    conservation = payload["candidate_conservation"]

    assert lane["observed_candidate_count"] == 1
    assert lane["unexplained_candidate_count"] == 1
    assert lane["conservation_delta"] == 0
    assert lane["conserved"] is True
    assert lane["reconciled"] is False
    assert conservation["observed_candidate_count"] == 1
    assert conservation["unexplained_candidate_count"] == 1
    assert conservation["conserved"] is True
    assert conservation["reconciled"] is False


def test_unknown_solana_lane_and_orphan_state_invalidate_full_population_totals() -> None:
    store = _Store()
    store.candidate("unknown-venue", "UNKNOWN_DEX")
    store.stage("SOLANA", "ghost", "position", "not_opened")

    payload = build_five_lane_candidate_accounting(store, merged_coverage=_coverage())
    anomalies = payload["classification_anomalies"]
    conservation = payload["candidate_conservation"]

    assert anomalies["unclassified_solana_candidate_count"] == 1
    assert anomalies["orphan_stage_state_count"] == 1
    assert conservation["all_lane_sources_verified"] is True
    assert conservation["candidate_population_verifiable"] is False
    assert conservation["observed_candidate_count"] is None
    assert conservation["conservation_delta"] is None
    assert conservation["conserved"] is False
    assert conservation["reconciled"] is False
    assert "unclassified_solana_candidates" in conservation["verification_blockers"]
    assert "orphan_candidate_stage_states" in conservation["verification_blockers"]


def test_missing_robinhood_proof_is_unable_to_verify_not_zero_activity() -> None:
    store = _Store()
    coverage = _coverage(robinhood=None, robinhood_state="unavailable")
    coverage["robinhood_proof_available"] = False
    coverage["robinhood"] = None

    payload = build_five_lane_candidate_accounting(store, merged_coverage=coverage)
    robinhood = payload["lanes"]["robinhood"]
    conservation = payload["candidate_conservation"]

    assert robinhood["verified"] is False
    assert robinhood["status"] == "unable_to_verify"
    assert robinhood["observed_candidate_count"] is None
    assert conservation["candidate_population_verifiable"] is False
    assert conservation["observed_candidate_count"] is None
    assert conservation["unverified_lanes"] == ["robinhood"]
    assert "unverified_lane_candidate_population" in conservation["verification_blockers"]


def test_unreadable_local_candidate_store_fails_closed() -> None:
    store = _Store(schema=False)
    payload = build_five_lane_candidate_accounting(store, merged_coverage=_coverage())

    assert payload["classification_anomalies"]["local_candidate_store_readable"] is False
    assert payload["lanes"]["pump_fun"]["verified"] is False
    assert payload["lanes"]["fomo"]["verified"] is False
    assert payload["candidate_conservation"]["candidate_population_verifiable"] is False
    assert payload["candidate_conservation"]["observed_candidate_count"] is None
    assert "local_candidate_store_unreadable" in payload["candidate_conservation"]["verification_blockers"]


def test_production_proof_exposes_five_lane_accounting_without_changing_authority(monkeypatch) -> None:
    store = _Store()
    store.candidate("pump-reject", "PUMP_FUN")
    store.stage("SOLANA", "pump-reject", "position", "not_opened")
    coverage = _coverage()
    coverage.update({"coverage_complete": True, "coverage_debt_count": 0, "proof_state": "confirmed", "stage_summary": {}})

    proof = {
        "state": "PARTIAL",
        "ready_for_forward_proof": False,
        "release": {"commit": "test-release"},
        "authority": {
            "economic_freeze_epoch": ECONOMIC_EPOCH,
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        },
        "strategy_evidence": {"promotion_certification": {}, "forward_certification": {}},
        "candidate_coverage": coverage,
        "operations_proof": {},
        "runtime": {"blockers": []},
    }

    monkeypatch.setattr(v51_phase14_api, "install_phase17_surface_attestation_hardening", lambda: None)
    monkeypatch.setattr(v51_phase14_api, "ensure_resource_pressure_sampler", lambda: None)
    monkeypatch.setattr(v51_phase14_api, "resource_pressure_snapshot", lambda: {"state": "ok"})
    monkeypatch.setattr(v51_phase14_api, "_isolated_robinhood_proof_state", lambda _provider: (None, "unavailable"))
    monkeypatch.setattr(
        v51_phase14_api,
        "build_phase17_profitability_certification",
        lambda *_args, **_kwargs: {
            "blockers": [],
            "measurement_epoch": MEASUREMENT_EPOCH,
            "execution_model_epoch": "test-execution-epoch",
        },
    )

    app = FastAPI()
    app.state.roi_v51_system_proof_precompute = lambda: proof
    runtime = SimpleNamespace(store=store)
    v51_phase14_api.install_phase14_profitability_certification(app, lambda: runtime)
    route = next(route for route in app.routes if getattr(route, "path", None) == "/v1/strategy/production-proof")
    payload = route.endpoint()

    accounting = payload["candidate_accounting"]
    assert accounting["accounting_version"] == ACCOUNTING_VERSION
    assert tuple(accounting["lane_accounting"]) == FIVE_LANES
    assert accounting["lane_accounting"]["pump_fun"]["terminal_rejected_count"] == 1
    assert accounting["candidate_conservation"]["observed_candidate_count"] == 1
    assert accounting["candidate_conservation"]["reconciled"] is True
    assert accounting["full_candidate_coverage"] is coverage
    assert payload["paper_only"] is True
    assert payload["live_money_authority"] is False
    assert payload["signing_available"] is False
    assert payload["transaction_submission_available"] is False
    assert payload["changes_strategy_authority"] is False
    assert payload["changes_economic_thresholds"] is False
