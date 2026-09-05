from __future__ import annotations

import json
from typing import Any

from . import v51_promotion_proof as promotion


SOURCE_VERSION = "v51-live-attestation-primary-sources-v1"
_ORIGINAL_SURFACE_PAYLOAD = promotion._surface_payload
_INSTALLED = False


def _table_exists(store: Any, table: str) -> bool:
    try:
        with store._lock:
            return store.db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (table,)
            ).fetchone() is not None
    except Exception:
        return False


def _columns(store: Any, table: str) -> set[str]:
    if not _table_exists(store, table):
        return set()
    with store._lock:
        return {str(row["name"]) for row in store.db.execute(f"PRAGMA table_info({table})").fetchall()}


def _solana_primary(store: Any, release: str) -> dict[str, Any]:
    measurement = promotion._measurement()
    candidates: list[dict[str, Any]] = []
    if _table_exists(store, "v51_candidates"):
        with store._lock:
            candidates = [dict(row) for row in store.db.execute(
                "SELECT candidate_id,trigger_wallet,raw_latency_ms FROM v51_candidates "
                "WHERE surface='SOLANA' AND release_commit=? AND measurement_epoch=?",
                (release, measurement.MEASUREMENT_EPOCH),
            ).fetchall()]
    candidate_ids = {str(row["candidate_id"]) for row in candidates}
    trial_ids: set[str] = set()
    execution_ids: set[str] = set()
    cols = _columns(store, "risk_conditioned_alpha_v5_trials")
    if {"release_commit", "source_signature"}.issubset(cols):
        executable_sql = (
            ",entry_executable,exit_executable" if {"entry_executable", "exit_executable"}.issubset(cols) else ""
        )
        with store._lock:
            trials = store.db.execute(
                f"SELECT source_signature{executable_sql} FROM risk_conditioned_alpha_v5_trials WHERE release_commit=?",
                (release,),
            ).fetchall()
        for row in trials:
            signature = str(row["source_signature"])
            trial_ids.add(signature)
            if executable_sql and bool(row["entry_executable"]) and bool(row["exit_executable"]):
                execution_ids.add(signature)
    wallet_targets = {str(row["candidate_id"]) for row in candidates if str(row.get("trigger_wallet") or "")}
    wallet_lineage: set[str] = set()
    if _table_exists(store, "v51_wallet_discovery_forward_lineage"):
        with store._lock:
            rows = store.db.execute(
                "SELECT source_candidate_id FROM v51_wallet_discovery_forward_lineage "
                "WHERE release_commit=? AND measurement_epoch=? AND source_candidate_id IS NOT NULL",
                (release, measurement.MEASUREMENT_EPOCH),
            ).fetchall()
        wallet_lineage = {str(row["source_candidate_id"]) for row in rows}
    candidate_count = len(candidate_ids)
    missing_context = candidate_ids - trial_ids
    latency_ok = candidate_count > 0 and all(row.get("raw_latency_ms") is not None for row in candidates)
    wallet_ok = wallet_targets.issubset(wallet_lineage)
    execution_ok = bool(candidate_ids & execution_ids)
    candidate_ok = candidate_count > 0 and not missing_context
    return {
        "surface": "SOLANA",
        "candidate_count": candidate_count,
        "context_trial_candidate_count": len(candidate_ids & trial_ids),
        "coverage_debt_count": len(missing_context),
        "execution_evidence_complete_count": len(candidate_ids & execution_ids),
        "wallet_candidate_count": len(wallet_targets),
        "wallet_lineage_count": len(wallet_targets & wallet_lineage),
        "candidate_coverage_valid": candidate_ok,
        "latency_measurement_valid": latency_ok,
        "execution_measurement_valid": execution_ok,
        "wallet_attribution_valid": wallet_ok,
        "fomo_measurement_valid": False,
        "robinhood_measurement_valid": False,
        "attested": candidate_ok and latency_ok and execution_ok and wallet_ok,
        "attestation_source": "primary_v51_candidates_plus_risk_conditioned_trials_plus_wallet_lineage",
        "requires_api_reconciliation": False,
    }


def _actionable_fomo_signatures(store: Any, release: str) -> set[str]:
    if not _table_exists(store, "fomo_shadow_observations"):
        return set()
    cols = _columns(store, "fomo_shadow_observations")
    if "source_signature" not in cols:
        return set()
    where = " WHERE release_commit=?" if "release_commit" in cols else ""
    params = (release,) if where else ()
    with store._lock:
        rows = store.db.execute(
            "SELECT source_signature,state_json FROM fomo_shadow_observations" + where,
            params,
        ).fetchall()
    result: set[str] = set()
    for row in rows:
        try:
            state = json.loads(str(row["state_json"] or "{}"))
        except Exception:
            state = {}
        if str(state.get("state") or "") in {"pre_fomo", "active_fomo"}:
            result.add(str(row["source_signature"]))
    return result


def _fomo_primary(store: Any, release: str) -> dict[str, Any]:
    candidates = _actionable_fomo_signatures(store, release)
    decisions: set[str] = set()
    executable: set[str] = set()
    cols = _columns(store, "fomo_paper_trials")
    if {"release_commit", "source_signature", "decision"}.issubset(cols):
        selected = ["source_signature", "decision"]
        if {"entry_executable", "exit_executable"}.issubset(cols):
            selected += ["entry_executable", "exit_executable"]
        with store._lock:
            rows = store.db.execute(
                f"SELECT {','.join(selected)} FROM fomo_paper_trials WHERE release_commit=?",
                (release,),
            ).fetchall()
        for row in rows:
            signature = str(row["source_signature"])
            if str(row["decision"] or ""):
                decisions.add(signature)
            if "entry_executable" in row.keys() and bool(row["entry_executable"]) and bool(row["exit_executable"]):
                executable.add(signature)
    debt = candidates - decisions
    candidate_ok = bool(candidates) and not debt
    execution_ok = bool(candidates & executable)
    return {
        "surface": "FOMO",
        "candidate_count": len(candidates),
        "terminal_decision_count": len(candidates & decisions),
        "coverage_debt_count": len(debt),
        "execution_evidence_complete_count": len(candidates & executable),
        "candidate_coverage_valid": candidate_ok,
        "latency_measurement_valid": True,
        "execution_measurement_valid": execution_ok,
        "wallet_attribution_valid": True,
        "fomo_measurement_valid": candidate_ok and execution_ok,
        "robinhood_measurement_valid": False,
        "attested": candidate_ok and execution_ok,
        "attestation_source": "primary_fomo_shadow_observations_plus_fomo_paper_trials",
        "requires_api_reconciliation": False,
    }


def _surface_payload_primary(store: Any, surface: str, release: str) -> dict[str, Any]:
    if surface == "SOLANA":
        return _solana_primary(store, release)
    if surface == "FOMO":
        return _fomo_primary(store, release)
    return _ORIGINAL_SURFACE_PAYLOAD(store, surface, release)


def install_primary_attestation_sources() -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    promotion._surface_payload = _surface_payload_primary  # type: ignore[assignment]
    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "version": SOURCE_VERSION,
        "installed": _INSTALLED,
        "solana_attestation_source": "canonical ingress candidates + canonical v5.1 trials + wallet lineage",
        "fomo_attestation_source": "forward FOMO observations + FOMO paper decisions",
        "robinhood_attestation_source": "isolated predecision candidate ledger",
        "requires_proof_api_poll_to_attest": False,
        "paper_only": True,
        "live_money_authority": False,
    }


__all__ = ["SOURCE_VERSION", "install_primary_attestation_sources", "status"]
