from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import datetime, timezone
from typing import Any

from .strategy_v51_authority import (
    AUTHORITY_ID,
    ECONOMIC_FREEZE_EPOCH,
    STRATEGY_VERSION,
    authority_fingerprint,
)
from .v51_evidence_analytics import promotion_records
from .v51_measurement_integrity import MEASUREMENT_EPOCH

BATCH6_PROOF_VERSION = "v51-batch6-production-proof-release-gate-v1"
CANDIDATE_RELEASE_ENV = "SOLANA_ROI_CANDIDATE_ALPHA_RELEASE_SHA"
CONTINUATION_RELEASE_ENV = "SOLANA_ROI_CONTINUATION_ALPHA_RELEASE_SHA"
_SHA_RE = re.compile(r"^[0-9a-f]{40}$")


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _sha(value: Any) -> str | None:
    text = str(value or "").strip().lower()
    return text if _SHA_RE.fullmatch(text) else None


def _expected_release(name: str) -> dict[str, Any]:
    raw = os.getenv(name)
    value = _sha(raw)
    return {
        "env": name,
        "release_sha": value,
        "configured": raw is not None and str(raw).strip() != "",
        "valid_full_sha": value is not None,
        "fallback_allowed": False,
    }


def _render_release() -> dict[str, Any]:
    raw = os.getenv("RENDER_GIT_COMMIT")
    value = _sha(raw)
    return {
        "source": "RENDER_GIT_COMMIT",
        "release_sha": value,
        "configured": raw is not None and str(raw).strip() != "",
        "valid_full_sha": value is not None,
        "fallback_allowed": False,
    }


def _table_exists(store: Any, table: str) -> bool:
    try:
        with store._lock:
            row = store.db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                (table,),
            ).fetchone()
        return row is not None
    except Exception:
        return False


def _candidate_release_evidence(store: Any) -> dict[str, Any]:
    table = "v51_release_attestation"
    if not _table_exists(store, table):
        return {
            "available": False,
            "measurement_epoch": MEASUREMENT_EPOCH,
            "row_count": 0,
            "release_counts": {},
            "surfaces": [],
            "unknown_release_count": 0,
            "reason": "release_attestation_table_unavailable",
        }
    try:
        with store._lock:
            rows = store.db.execute(
                "SELECT release_commit,surface,attested FROM v51_release_attestation "
                "WHERE measurement_epoch=? AND attested=1 ORDER BY release_commit,surface",
                (MEASUREMENT_EPOCH,),
            ).fetchall()
    except Exception as exc:
        return {
            "available": False,
            "measurement_epoch": MEASUREMENT_EPOCH,
            "row_count": 0,
            "release_counts": {},
            "surfaces": [],
            "unknown_release_count": 0,
            "reason": f"release_attestation_query_failed:{type(exc).__name__}",
        }
    counts: dict[str, int] = {}
    surfaces: set[str] = set()
    unknown = 0
    for row in rows:
        release = _sha(row["release_commit"] if hasattr(row, "keys") else row[0])
        surface = str(row["surface"] if hasattr(row, "keys") else row[1])
        surfaces.add(surface)
        if release is None:
            unknown += 1
        else:
            counts[release] = counts.get(release, 0) + 1
    return {
        "available": bool(rows),
        "measurement_epoch": MEASUREMENT_EPOCH,
        "row_count": len(rows),
        "release_counts": counts,
        "surfaces": sorted(surfaces),
        "unknown_release_count": unknown,
        "reason": None if rows else "no_attested_candidate_release_evidence",
    }


def _continuation_release_evidence(store: Any) -> dict[str, Any]:
    try:
        rows = promotion_records(store)
    except Exception as exc:
        return {
            "available": False,
            "measurement_epoch": MEASUREMENT_EPOCH,
            "row_count": 0,
            "release_counts": {},
            "unknown_release_count": 0,
            "reason": f"promotion_records_unavailable:{type(exc).__name__}",
        }
    counts: dict[str, int] = {}
    unknown = 0
    for row in rows:
        release = _sha(row.get("release_commit"))
        if release is None:
            unknown += 1
        else:
            counts[release] = counts.get(release, 0) + 1
    return {
        "available": bool(rows),
        "measurement_epoch": MEASUREMENT_EPOCH,
        "row_count": len(rows),
        "release_counts": counts,
        "unknown_release_count": unknown,
        "reason": None if rows else "no_current_attested_continuation_evidence",
    }


def _release_assertion(
    *,
    expected: dict[str, Any],
    deployed: dict[str, Any],
    evidence: dict[str, Any],
    lane: str,
) -> dict[str, Any]:
    expected_sha = expected.get("release_sha")
    deployed_sha = deployed.get("release_sha")
    release_counts = _dict(evidence.get("release_counts"))
    observed = sorted(str(value) for value in release_counts)
    unknown = int(evidence.get("unknown_release_count") or 0)
    evidence_exact = bool(
        expected_sha
        and evidence.get("available")
        and unknown == 0
        and observed == [expected_sha]
    )
    deployed_exact = bool(expected_sha and deployed_sha == expected_sha)
    passed = bool(
        expected.get("valid_full_sha")
        and deployed.get("valid_full_sha")
        and evidence_exact
        and deployed_exact
    )
    return {
        "lane": lane,
        "pass": passed,
        "expected_release_sha": expected_sha,
        "expected_release_source": expected.get("env"),
        "expected_release_independently_required": True,
        "global_release_fallback_allowed": False,
        "deployed_release_sha": deployed_sha,
        "deployed_release_source": deployed.get("source"),
        "deployed_exact_match": deployed_exact,
        "evidence_release_shas": observed,
        "evidence_release_counts": release_counts,
        "evidence_exact_match": evidence_exact,
        "unknown_release_count": unknown,
        "evidence_available": bool(evidence.get("available")),
        "evidence_reason": evidence.get("reason"),
        "measurement_epoch": MEASUREMENT_EPOCH,
    }


def _accounting_assertion(candidate_accounting: dict[str, Any]) -> dict[str, Any]:
    accounting = _dict(candidate_accounting)
    conservation = _dict(accounting.get("candidate_conservation"))
    anomalies_raw = accounting.get("classification_anomalies")

    # Canonical five-lane accounting uses explicit *_candidate_count names and
    # `candidate_population_verifiable`. Batch 6 originally consumed shorter
    # aliases that only existed in its unit-test fixture. Read the canonical schema
    # first and retain aliases solely for backward-compatible test/replay payloads.
    population_verifiable = bool(
        conservation.get("candidate_population_verifiable", conservation.get("population_verifiable"))
    )
    observed = conservation.get("observed_candidate_count", conservation.get("observed"))
    terminal = conservation.get("terminal_candidate_count", conservation.get("terminal"))
    valid_pending = conservation.get("valid_pending_candidate_count", conservation.get("valid_pending"))
    coverage_debt = conservation.get("coverage_debt_candidate_count", conservation.get("coverage_debt"))
    unexplained = conservation.get("unexplained_candidate_count", conservation.get("unexplained"))

    anomaly_blockers: list[str] = []
    if isinstance(anomalies_raw, list):
        anomaly_blockers = [str(value) for value in anomalies_raw if str(value)]
    elif isinstance(anomalies_raw, dict):
        if anomalies_raw.get("local_candidate_store_readable") is False:
            anomaly_blockers.append("local_candidate_store_unreadable")
        if int(anomalies_raw.get("unclassified_solana_candidate_count") or 0) > 0:
            anomaly_blockers.append("unclassified_solana_candidates")
        if int(anomalies_raw.get("orphan_stage_state_count") or 0) > 0:
            anomaly_blockers.append("orphan_candidate_stage_states")
        if bool(anomalies_raw.get("robinhood_count_inconsistency")):
            anomaly_blockers.append("robinhood_candidate_count_inconsistency")

    verification_blockers = conservation.get("verification_blockers")
    if isinstance(verification_blockers, list):
        anomaly_blockers.extend(
            str(value) for value in verification_blockers if str(value) and str(value) not in anomaly_blockers
        )

    passed = bool(
        accounting.get("coverage_complete")
        and population_verifiable
        and conservation.get("conserved")
        and conservation.get("reconciled")
        and int(coverage_debt or 0) == 0
        and int(unexplained or 0) == 0
        and not anomaly_blockers
    )
    return {
        "pass": passed,
        "coverage_complete": bool(accounting.get("coverage_complete")),
        "population_verifiable": population_verifiable,
        "conserved": bool(conservation.get("conserved")),
        "reconciled": bool(conservation.get("reconciled")),
        "observed": observed,
        "terminal": terminal,
        "valid_pending": valid_pending,
        "coverage_debt": coverage_debt,
        "unexplained": unexplained,
        "conservation_delta": conservation.get("conservation_delta"),
        "classification_anomalies": anomalies_raw if isinstance(anomalies_raw, (list, dict)) else {},
        "verification_blockers": anomaly_blockers,
        "no_disappeared_candidates": bool(
            conservation.get("reconciled")
            and int(unexplained or 0) == 0
        ),
    }


def _authority_assertion(system_proof: dict[str, Any]) -> dict[str, Any]:
    proof = _dict(system_proof)
    authority = _dict(proof.get("authority"))
    authority_id = authority.get("authority_id")
    strategy_version = authority.get("strategy_version")
    economic_epoch = authority.get("economic_freeze_epoch")
    passed = bool(
        authority_id == AUTHORITY_ID
        and strategy_version == STRATEGY_VERSION
        and economic_epoch == ECONOMIC_FREEZE_EPOCH
        and authority.get("authority_fingerprint") == authority_fingerprint()
        and bool(authority.get("paper_only"))
        and not bool(authority.get("live_money_authority"))
        and not bool(authority.get("signing_available"))
        and not bool(authority.get("transaction_submission_available"))
    )
    return {
        "pass": passed,
        "authority_id": authority_id,
        "expected_authority_id": AUTHORITY_ID,
        "strategy_version": strategy_version,
        "expected_strategy_version": STRATEGY_VERSION,
        "economic_epoch": economic_epoch,
        "expected_economic_epoch": ECONOMIC_FREEZE_EPOCH,
        "authority_fingerprint": authority.get("authority_fingerprint"),
        "expected_authority_fingerprint": authority_fingerprint(),
        "paper_only": bool(authority.get("paper_only")),
        "live_money_authority": bool(authority.get("live_money_authority")),
        "signing_available": bool(authority.get("signing_available")),
        "transaction_submission_available": bool(authority.get("transaction_submission_available")),
        "alternate_strategy_authority_allowed": False,
    }


def _topology_assertion(
    system_proof: dict[str, Any],
    forward_certification: dict[str, Any],
) -> dict[str, Any]:
    proof = _dict(system_proof)
    runtime = _dict(proof.get("runtime"))
    forward = _dict(forward_certification)
    checks = _dict(forward.get("checks"))
    safety = _dict(checks.get("36_paper_only_safety_boundary"))
    release = _dict(proof.get("release"))
    surfaces = runtime.get("surfaces")
    has_surfaces = isinstance(surfaces, dict) and bool(surfaces)
    safety_pass = bool(safety.get("pass"))
    runtime_not_degraded = str(proof.get("state") or "").upper() != "DEGRADED"
    global_release_bound = bool(release.get("exact_release_bound"))
    passed = bool(has_surfaces and safety_pass and runtime_not_degraded and global_release_bound)
    return {
        "pass": passed,
        "configured_surfaces_present": has_surfaces,
        "configured_surfaces": surfaces if isinstance(surfaces, dict) else {},
        "paper_only_safety_boundary_pass": safety_pass,
        "canonical_runtime_not_degraded": runtime_not_degraded,
        "underlying_system_proof_exact_release_bound": global_release_bound,
        "alternate_live_strategy_authority_detected": False if safety_pass else None,
        "shadow_or_live_money_authority_allowed": False,
        "lane_release_identity_is_not_inferred_from_underlying_global_release": True,
    }


def _epoch_assertion(system_proof: dict[str, Any]) -> dict[str, Any]:
    authority = _dict(_dict(system_proof).get("authority"))
    observed_economic = str(authority.get("economic_freeze_epoch") or "")
    passed = observed_economic == ECONOMIC_FREEZE_EPOCH
    return {
        "pass": passed,
        "economic_epoch": ECONOMIC_FREEZE_EPOCH,
        "observed_economic_epoch": observed_economic or None,
        "measurement_epoch": MEASUREMENT_EPOCH,
        "economic_epoch_advanced_by_batch6": False,
        "measurement_epoch_advanced_by_batch6": False,
        "batch6_starts_new_measurement_epoch": False,
    }


def _ensure_artifact_schema(store: Any) -> None:
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS v51_batch6_production_proof_artifacts ("
            "proof_id TEXT PRIMARY KEY, generated_at TEXT NOT NULL, "
            "candidate_expected_release_sha TEXT, continuation_expected_release_sha TEXT, "
            "economic_epoch TEXT NOT NULL, measurement_epoch TEXT NOT NULL, "
            "verdict TEXT NOT NULL, report_json TEXT NOT NULL, "
            "paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL)"
        )
        store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_v51_batch6_proof_generated "
            "ON v51_batch6_production_proof_artifacts(generated_at)"
        )


def _proof_id(core: dict[str, Any]) -> str:
    raw = json.dumps(core, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _persist_artifact(
    store: Any,
    *,
    proof_id: str,
    generated_at: str,
    candidate_expected_release_sha: str | None,
    continuation_expected_release_sha: str | None,
    verdict: str,
    report: dict[str, Any],
) -> None:
    _ensure_artifact_schema(store)
    payload = json.dumps(report, sort_keys=True, default=str)
    with store._lock, store.db:
        store.db.execute(
            "INSERT INTO v51_batch6_production_proof_artifacts("
            "proof_id,generated_at,candidate_expected_release_sha,continuation_expected_release_sha,"
            "economic_epoch,measurement_epoch,verdict,report_json,paper_only,live_money_authority) "
            "VALUES (?,?,?,?,?,?,?,?,1,0) "
            "ON CONFLICT(proof_id) DO UPDATE SET generated_at=excluded.generated_at,"
            "verdict=excluded.verdict,report_json=excluded.report_json,paper_only=1,live_money_authority=0",
            (
                proof_id,
                generated_at,
                candidate_expected_release_sha,
                continuation_expected_release_sha,
                ECONOMIC_FREEZE_EPOCH,
                MEASUREMENT_EPOCH,
                verdict,
                payload,
            ),
        )


def build_batch6_production_proof_gate(
    store: Any,
    *,
    system_proof: dict[str, Any],
    candidate_accounting: dict[str, Any],
    forward_certification: dict[str, Any],
) -> dict[str, Any]:
    generated_at = _utcnow()
    candidate_expected = _expected_release(CANDIDATE_RELEASE_ENV)
    continuation_expected = _expected_release(CONTINUATION_RELEASE_ENV)
    deployed = _render_release()
    candidate_evidence = _candidate_release_evidence(store)
    continuation_evidence = _continuation_release_evidence(store)

    candidate_release = _release_assertion(
        expected=candidate_expected,
        deployed=deployed,
        evidence=candidate_evidence,
        lane="candidate-alpha",
    )
    continuation_release = _release_assertion(
        expected=continuation_expected,
        deployed=deployed,
        evidence=continuation_evidence,
        lane="continuation-alpha",
    )
    accounting = _accounting_assertion(candidate_accounting)
    authority = _authority_assertion(system_proof)
    topology = _topology_assertion(system_proof, forward_certification)
    epochs = _epoch_assertion(system_proof)

    candidate_feasibility = {
        "pass": bool(candidate_evidence.get("available") and accounting.get("population_verifiable")),
        "current_attested_candidate_evidence_present": bool(candidate_evidence.get("available")),
        "candidate_population_verifiable": bool(accounting.get("population_verifiable")),
        "synthetic_or_replay_evidence_allowed": False,
    }
    continuation_feasibility = {
        "pass": bool(continuation_evidence.get("available")),
        "current_attested_promotion_or_settlement_evidence_present": bool(
            continuation_evidence.get("available")
        ),
        "current_attested_record_count": int(continuation_evidence.get("row_count") or 0),
        "synthetic_or_replay_evidence_allowed": False,
    }
    contamination = {
        "pass": bool(candidate_release["evidence_exact_match"] and continuation_release["evidence_exact_match"]),
        "candidate_lane_only_expected_release": candidate_release["evidence_exact_match"],
        "continuation_lane_only_expected_release": continuation_release["evidence_exact_match"],
        "candidate_unknown_release_count": candidate_release["unknown_release_count"],
        "continuation_unknown_release_count": continuation_release["unknown_release_count"],
        "cross_release_or_missing_provenance_can_certify": False,
    }
    independent_release_configuration = {
        "pass": bool(candidate_expected["valid_full_sha"] and continuation_expected["valid_full_sha"]),
        "candidate_source": CANDIDATE_RELEASE_ENV,
        "continuation_source": CONTINUATION_RELEASE_ENV,
        "candidate_expected_release_sha": candidate_expected["release_sha"],
        "continuation_expected_release_sha": continuation_expected["release_sha"],
        "same_sha_allowed_for_single_service_deployment": True,
        "shared_env_or_global_fallback_used": False,
    }

    assertions = {
        "independent_release_configuration": independent_release_configuration,
        "candidate_release_exact": candidate_release,
        "continuation_release_exact": continuation_release,
        "release_contamination": contamination,
        "candidate_feasibility": candidate_feasibility,
        "continuation_feasibility": continuation_feasibility,
        "candidate_conservation": accounting,
        "strategy_authority": authority,
        "production_topology": topology,
        "epochs": epochs,
    }
    blockers = [
        name
        for name, assertion in assertions.items()
        if not bool(_dict(assertion).get("pass"))
    ]
    core = {
        "version": BATCH6_PROOF_VERSION,
        "assertions": assertions,
        "blockers": blockers,
        "economic_epoch": ECONOMIC_FREEZE_EPOCH,
        "measurement_epoch": MEASUREMENT_EPOCH,
    }
    proof_id = _proof_id(core)
    passed = not blockers
    report = {
        "batch6_production_proof_version": BATCH6_PROOF_VERSION,
        "generated_at": generated_at,
        "proof_id": proof_id,
        "pass": passed,
        "verdict": "PASS" if passed else "FAIL_CLOSED",
        "single_fail_closed_verdict": True,
        "missing_or_unknown_evidence_fails_closed": True,
        "economic_epoch": ECONOMIC_FREEZE_EPOCH,
        "measurement_epoch": MEASUREMENT_EPOCH,
        "batch6_starts_new_measurement_epoch": False,
        "assertions": assertions,
        "blockers": blockers,
        "artifact": {
            "table": "v51_batch6_production_proof_artifacts",
            "proof_id": proof_id,
            "persisted": True,
        },
        "read_only_strategy_authority": True,
        "changes_strategy_authority": False,
        "changes_economic_thresholds": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }

    try:
        _persist_artifact(
            store,
            proof_id=proof_id,
            generated_at=generated_at,
            candidate_expected_release_sha=candidate_expected["release_sha"],
            continuation_expected_release_sha=continuation_expected["release_sha"],
            verdict=report["verdict"],
            report=report,
        )
    except Exception as exc:
        report["artifact"] = {
            "table": "v51_batch6_production_proof_artifacts",
            "proof_id": proof_id,
            "persisted": False,
            "error": type(exc).__name__,
        }
        if "proof_artifact_persistence" not in report["blockers"]:
            report["blockers"].append("proof_artifact_persistence")
        report["pass"] = False
        report["verdict"] = "FAIL_CLOSED"

    return report


def latest_batch6_proof_artifact(store: Any) -> dict[str, Any] | None:
    if not _table_exists(store, "v51_batch6_production_proof_artifacts"):
        return None
    try:
        with store._lock:
            row = store.db.execute(
                "SELECT report_json FROM v51_batch6_production_proof_artifacts "
                "ORDER BY generated_at DESC LIMIT 1"
            ).fetchone()
        if row is None:
            return None
        raw = row["report_json"] if hasattr(row, "keys") else row[0]
        payload = json.loads(str(raw))
        return payload if isinstance(payload, dict) else None
    except Exception:
        return None


__all__ = [
    "BATCH6_PROOF_VERSION",
    "CANDIDATE_RELEASE_ENV",
    "CONTINUATION_RELEASE_ENV",
    "build_batch6_production_proof_gate",
    "latest_batch6_proof_artifact",
]
