from __future__ import annotations

import json
import sqlite3
import threading

from solana_roi import v51_batch6_production_proof_release_gate as gate
from solana_roi.strategy_v51_authority import (
    AUTHORITY_ID,
    ECONOMIC_FREEZE_EPOCH,
    STRATEGY_VERSION,
    authority_fingerprint,
)
from solana_roi.v51_measurement_integrity import MEASUREMENT_EPOCH


class Store:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.db = sqlite3.connect(":memory:")
        self.db.row_factory = sqlite3.Row
        self.db.execute(
            "CREATE TABLE v51_release_attestation ("
            "release_commit TEXT NOT NULL, measurement_epoch TEXT NOT NULL, surface TEXT NOT NULL, "
            "attested INTEGER NOT NULL)"
        )


def _proof() -> dict:
    return {
        "state": "READY_FOR_FORWARD_PROOF",
        "ready_for_forward_proof": True,
        "release": {"exact_release_bound": True},
        "authority": {
            "authority_id": AUTHORITY_ID,
            "strategy_version": STRATEGY_VERSION,
            "economic_freeze_epoch": ECONOMIC_FREEZE_EPOCH,
            "authority_fingerprint": authority_fingerprint(),
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        },
        "runtime": {"surfaces": {"solana": {"ready": True}}},
    }


def _forward() -> dict:
    return {"checks": {"36_paper_only_safety_boundary": {"pass": True}}}


def _accounting() -> dict:
    return {
        "coverage_complete": True,
        "classification_anomalies": [],
        "candidate_conservation": {
            "population_verifiable": True,
            "conserved": True,
            "reconciled": True,
            "observed": 4,
            "terminal": 3,
            "valid_pending": 1,
            "coverage_debt": 0,
            "unexplained": 0,
            "conservation_delta": 0,
        },
    }


def _attest(store: Store, release: str) -> None:
    store.db.execute(
        "INSERT INTO v51_release_attestation(release_commit,measurement_epoch,surface,attested) "
        "VALUES (?,?,?,1)",
        (release, MEASUREMENT_EPOCH, "SOLANA"),
    )
    store.db.commit()


def test_batch6_gate_passes_only_exact_independently_configured_release_evidence(monkeypatch) -> None:
    store = Store()
    release = "a" * 40
    _attest(store, release)
    monkeypatch.setenv(gate.CANDIDATE_RELEASE_ENV, release)
    monkeypatch.setenv(gate.CONTINUATION_RELEASE_ENV, release)
    monkeypatch.setenv("RENDER_GIT_COMMIT", release)
    monkeypatch.setattr(gate, "promotion_records", lambda _store: [{"release_commit": release}])

    result = gate.build_batch6_production_proof_gate(
        store,
        system_proof=_proof(),
        candidate_accounting=_accounting(),
        forward_certification=_forward(),
    )

    assert result["pass"] is True
    assert result["verdict"] == "PASS"
    assert result["single_fail_closed_verdict"] is True
    assert result["economic_epoch"] == ECONOMIC_FREEZE_EPOCH
    assert result["measurement_epoch"] == MEASUREMENT_EPOCH
    assert result["batch6_starts_new_measurement_epoch"] is False
    candidate = result["assertions"]["candidate_release_exact"]
    continuation = result["assertions"]["continuation_release_exact"]
    assert candidate["expected_release_source"] == gate.CANDIDATE_RELEASE_ENV
    assert continuation["expected_release_source"] == gate.CONTINUATION_RELEASE_ENV
    assert candidate["global_release_fallback_allowed"] is False
    assert continuation["global_release_fallback_allowed"] is False
    assert result["assertions"]["release_contamination"]["pass"] is True
    assert result["assertions"]["candidate_conservation"]["no_disappeared_candidates"] is True
    assert result["artifact"]["persisted"] is True

    stored = store.db.execute(
        "SELECT verdict,report_json,paper_only,live_money_authority "
        "FROM v51_batch6_production_proof_artifacts WHERE proof_id=?",
        (result["proof_id"],),
    ).fetchone()
    assert stored is not None
    assert stored["verdict"] == "PASS"
    assert json.loads(stored["report_json"])["proof_id"] == result["proof_id"]
    assert stored["paper_only"] == 1
    assert stored["live_money_authority"] == 0


def test_batch6_gate_fails_closed_on_cross_release_contamination(monkeypatch) -> None:
    store = Store()
    expected = "a" * 40
    contaminated = "b" * 40
    _attest(store, expected)
    _attest(store, contaminated)
    monkeypatch.setenv(gate.CANDIDATE_RELEASE_ENV, expected)
    monkeypatch.setenv(gate.CONTINUATION_RELEASE_ENV, expected)
    monkeypatch.setenv("RENDER_GIT_COMMIT", expected)
    monkeypatch.setattr(
        gate,
        "promotion_records",
        lambda _store: [
            {"release_commit": expected},
            {"release_commit": contaminated},
        ],
    )

    result = gate.build_batch6_production_proof_gate(
        store,
        system_proof=_proof(),
        candidate_accounting=_accounting(),
        forward_certification=_forward(),
    )

    assert result["pass"] is False
    assert result["verdict"] == "FAIL_CLOSED"
    assert "candidate_release_exact" in result["blockers"]
    assert "continuation_release_exact" in result["blockers"]
    assert "release_contamination" in result["blockers"]
    assert result["assertions"]["release_contamination"]["cross_release_or_missing_provenance_can_certify"] is False
    assert result["artifact"]["persisted"] is True


def test_batch6_gate_never_falls_back_to_global_release_identity(monkeypatch) -> None:
    store = Store()
    release = "c" * 40
    _attest(store, release)
    monkeypatch.delenv(gate.CANDIDATE_RELEASE_ENV, raising=False)
    monkeypatch.delenv(gate.CONTINUATION_RELEASE_ENV, raising=False)
    monkeypatch.setenv("RENDER_GIT_COMMIT", release)
    monkeypatch.setenv("SOLANA_ROI_RELEASE_COMMIT", release)
    monkeypatch.setenv("GITHUB_SHA", release)
    monkeypatch.setattr(gate, "promotion_records", lambda _store: [{"release_commit": release}])

    result = gate.build_batch6_production_proof_gate(
        store,
        system_proof=_proof(),
        candidate_accounting=_accounting(),
        forward_certification=_forward(),
    )

    assert result["pass"] is False
    assert "independent_release_configuration" in result["blockers"]
    assert result["assertions"]["independent_release_configuration"]["shared_env_or_global_fallback_used"] is False
    assert result["assertions"]["candidate_release_exact"]["expected_release_sha"] is None
    assert result["assertions"]["continuation_release_exact"]["expected_release_sha"] is None


def test_batch6_gate_fails_closed_on_candidate_conservation_debt(monkeypatch) -> None:
    store = Store()
    release = "d" * 40
    _attest(store, release)
    monkeypatch.setenv(gate.CANDIDATE_RELEASE_ENV, release)
    monkeypatch.setenv(gate.CONTINUATION_RELEASE_ENV, release)
    monkeypatch.setenv("RENDER_GIT_COMMIT", release)
    monkeypatch.setattr(gate, "promotion_records", lambda _store: [{"release_commit": release}])
    accounting = _accounting()
    accounting["candidate_conservation"]["reconciled"] = False
    accounting["candidate_conservation"]["coverage_debt"] = 1
    accounting["candidate_conservation"]["conservation_delta"] = 1

    result = gate.build_batch6_production_proof_gate(
        store,
        system_proof=_proof(),
        candidate_accounting=accounting,
        forward_certification=_forward(),
    )

    assert result["pass"] is False
    assert "candidate_conservation" in result["blockers"]
    assert result["assertions"]["candidate_conservation"]["no_disappeared_candidates"] is False
    assert result["paper_only"] is True
    assert result["live_money_authority"] is False
