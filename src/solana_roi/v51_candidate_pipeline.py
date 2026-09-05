from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from .strategy_v51_authority import AUTHORITY_ID, ECONOMIC_FREEZE_EPOCH, PIPELINE_STAGES

PIPELINE_VERSION = "v51-candidate-pipeline-audit-v1"


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _table_exists(store: Any, table: str) -> bool:
    try:
        with store._lock:
            row = store.db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (table,)
            ).fetchone()
        return row is not None
    except Exception:
        return False


def _columns(store: Any, table: str) -> set[str]:
    if not _table_exists(store, table):
        return set()
    with store._lock:
        return {str(row["name"]) for row in store.db.execute(f"PRAGMA table_info({table})").fetchall()}


def _schema(store: Any) -> None:
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS v51_candidate_pipeline_audit ("
            "surface TEXT NOT NULL, candidate_id TEXT NOT NULL, release_commit TEXT, stage TEXT NOT NULL, "
            "stage_index INTEGER NOT NULL, status TEXT NOT NULL, reason TEXT NOT NULL, payload_json TEXT NOT NULL, "
            "observed_at TEXT NOT NULL, authority_id TEXT NOT NULL, economic_freeze_epoch TEXT NOT NULL, "
            "paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL, "
            "PRIMARY KEY(surface,candidate_id,stage))"
        )
        store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_v51_candidate_pipeline_stage "
            "ON v51_candidate_pipeline_audit(surface,stage,status,observed_at)"
        )


def _record(
    store: Any,
    *,
    surface: str,
    candidate_id: str,
    release_commit: str | None,
    stage: str,
    status: str,
    reason: str,
    payload: dict[str, Any] | None = None,
) -> None:
    if stage not in PIPELINE_STAGES:
        raise ValueError(f"unknown v5.1 pipeline stage: {stage}")
    _schema(store)
    with store._lock, store.db:
        store.db.execute(
            "INSERT OR REPLACE INTO v51_candidate_pipeline_audit("
            "surface,candidate_id,release_commit,stage,stage_index,status,reason,payload_json,observed_at,"
            "authority_id,economic_freeze_epoch,paper_only,live_money_authority) VALUES (?,?,?,?,?,?,?,?,?,?,?,1,0)",
            (
                surface,
                candidate_id,
                release_commit,
                stage,
                PIPELINE_STAGES.index(stage),
                status,
                reason,
                json.dumps(payload or {}, sort_keys=True, default=str),
                _utcnow(),
                AUTHORITY_ID,
                ECONOMIC_FREEZE_EPOCH,
            ),
        )


def _release_allowed(store: Any, release_commit: str | None) -> bool:
    if not release_commit or not _table_exists(store, "v51_economic_freeze_releases"):
        return False
    with store._lock:
        row = store.db.execute(
            "SELECT 1 FROM v51_economic_freeze_releases WHERE release_commit=? AND economic_freeze_epoch=? "
            "AND authority_id=? LIMIT 1",
            (release_commit, ECONOMIC_FREEZE_EPOCH, AUTHORITY_ID),
        ).fetchone()
    return row is not None


def _solana_candidates(store: Any) -> list[dict[str, Any]]:
    table = "wallet_discovery_forward_observations"
    cols = _columns(store, table)
    required = {"signature", "side"}
    if not required.issubset(cols):
        return []
    selected = [name for name in ("signature", "release_commit", "token_mint", "wallet", "side", "source", "copyable", "received_at") if name in cols]
    where = "LOWER(side)='buy'"
    if "copyable" in cols:
        where += " AND copyable=1"
    with store._lock:
        rows = store.db.execute(f"SELECT {','.join(selected)} FROM {table} WHERE {where} ORDER BY rowid").fetchall()
    return [dict(row) for row in rows]


def _reconcile_solana(store: Any) -> int:
    candidates = _solana_candidates(store)
    v5_cols = _columns(store, "risk_conditioned_alpha_v5_trials")
    outcome_cols = _columns(store, "risk_conditioned_alpha_v5_outcomes")
    unified_cols = _columns(store, "profit_first_final_trials")
    for candidate in candidates:
        signature = str(candidate.get("signature") or "")
        release = str(candidate.get("release_commit") or "") or None
        if release and not _release_allowed(store, release):
            continue
        _record(store, surface="SOLANA", candidate_id=signature, release_commit=release, stage="ingestion", status="complete", reason="forward_observation_persisted", payload=candidate)
        _record(store, surface="SOLANA", candidate_id=signature, release_commit=release, stage="candidate", status="complete", reason="copyable_buy_candidate", payload={"token_mint": candidate.get("token_mint"), "wallet": candidate.get("wallet"), "source": candidate.get("source")})
        trial = None
        if {"source_signature", "decision", "decision_reason"}.issubset(v5_cols):
            with store._lock:
                trial = store.db.execute(
                    "SELECT * FROM risk_conditioned_alpha_v5_trials WHERE source_signature=? "
                    + ("AND release_commit=? " if release and "release_commit" in v5_cols else "")
                    + "ORDER BY selected DESC,id DESC LIMIT 1",
                    ((signature, release) if release and "release_commit" in v5_cols else (signature,)),
                ).fetchone()
        if trial is None:
            _record(store, surface="SOLANA", candidate_id=signature, release_commit=release, stage="context", status="coverage_debt", reason="candidate_has_no_canonical_v51_context_record")
            continue
        trial_d = dict(trial)
        _record(store, surface="SOLANA", candidate_id=signature, release_commit=release, stage="context", status="complete", reason="risk_lane_venue_lifecycle_context_persisted", payload={k: trial_d.get(k) for k in ("lane", "venue", "lifecycle", "regime", "risk_signature", "context_key")})
        executable = bool(trial_d.get("entry_executable")) and bool(trial_d.get("exit_executable"))
        _record(store, surface="SOLANA", candidate_id=signature, release_commit=release, stage="execution_evidence", status="complete" if executable else "failed_closed", reason="amount_specific_entry_and_exit_evidence" if executable else "entry_or_exit_execution_evidence_incomplete", payload={k: trial_d.get(k) for k in ("round_trip_cost_fraction", "chase_band", "latency_band", "entry_executable", "exit_executable")})
        decision = str(trial_d.get("decision") or "unknown")
        reason = str(trial_d.get("decision_reason") or "unspecified")
        _record(store, surface="SOLANA", candidate_id=signature, release_commit=release, stage="decision", status="complete", reason=reason, payload={"decision": decision, "position_fraction": trial_d.get("position_fraction")})
        if decision.startswith("paper_enter") and int(trial_d.get("selected") or 0) == 1:
            _record(store, surface="SOLANA", candidate_id=signature, release_commit=release, stage="position", status="paper_position_authorized", reason="selected_v51_paper_entry", payload={"lane": trial_d.get("lane"), "position_fraction": trial_d.get("position_fraction")})
        else:
            _record(store, surface="SOLANA", candidate_id=signature, release_commit=release, stage="position", status="not_opened", reason=reason, payload={"decision": decision})
        outcome = None
        if {"source_signature", "net_return"}.issubset(outcome_cols):
            with store._lock:
                outcome = store.db.execute(
                    "SELECT * FROM risk_conditioned_alpha_v5_outcomes WHERE source_signature=? "
                    + ("AND release_commit=? " if release and "release_commit" in outcome_cols else "")
                    + "ORDER BY id DESC LIMIT 1",
                    ((signature, release) if release and "release_commit" in outcome_cols else (signature,)),
                ).fetchone()
        if outcome is not None:
            out = dict(outcome)
            _record(store, surface="SOLANA", candidate_id=signature, release_commit=release, stage="settlement", status="complete", reason=str(out.get("exit_reason") or "paper_settled"), payload={"net_return": out.get("net_return"), "settled_at": out.get("settled_at")})
            _record(store, surface="SOLANA", candidate_id=signature, release_commit=release, stage="learning", status="complete", reason="settled_outcome_available_to_frozen_epoch_learning", payload={"net_return": out.get("net_return")})
        elif decision.startswith("paper_enter"):
            _record(store, surface="SOLANA", candidate_id=signature, release_commit=release, stage="settlement", status="pending", reason="paper_position_not_yet_settled")
    return len(candidates)


def _reconcile_fomo(store: Any) -> int:
    if not _table_exists(store, "fomo_shadow_observations"):
        return 0
    cols = _columns(store, "fomo_shadow_observations")
    if "source_signature" not in cols:
        return 0
    with store._lock:
        observations = [dict(row) for row in store.db.execute("SELECT * FROM fomo_shadow_observations ORDER BY id").fetchall()]
    trial_cols = _columns(store, "fomo_paper_trials")
    outcome_cols = _columns(store, "fomo_paper_outcomes")
    count = 0
    for obs in observations:
        release = str(obs.get("release_commit") or "") or None
        if release and not _release_allowed(store, release):
            continue
        signature = str(obs.get("source_signature") or "")
        state = json.loads(str(obs.get("state_json") or "{}")) if obs.get("state_json") else {}
        fomo_state = str(state.get("state") or "unknown")
        if fomo_state not in {"pre_fomo", "active_fomo"}:
            continue
        count += 1
        _record(store, surface="FOMO", candidate_id=signature, release_commit=release, stage="ingestion", status="complete", reason="fomo_shadow_observation_persisted")
        _record(store, surface="FOMO", candidate_id=signature, release_commit=release, stage="candidate", status="complete", reason=fomo_state, payload={"venue": obs.get("venue"), "lifecycle": obs.get("lifecycle"), "regime": obs.get("regime")})
        _record(store, surface="FOMO", candidate_id=signature, release_commit=release, stage="context", status="complete", reason="clean_or_hazard_fomo_context_persisted", payload=state)
        trial = None
        if "source_signature" in trial_cols:
            with store._lock:
                trial = store.db.execute("SELECT * FROM fomo_paper_trials WHERE source_signature=? ORDER BY id DESC LIMIT 1", (signature,)).fetchone()
        if trial is None:
            _record(store, surface="FOMO", candidate_id=signature, release_commit=release, stage="decision", status="coverage_debt", reason="actionable_fomo_candidate_has_no_paper_decision")
            continue
        td = dict(trial)
        executable = bool(td.get("entry_executable")) and bool(td.get("exit_executable"))
        _record(store, surface="FOMO", candidate_id=signature, release_commit=release, stage="execution_evidence", status="complete" if executable else "failed_closed", reason="fomo_entry_exit_execution_evidence" if executable else "fomo_execution_incomplete")
        decision = str(td.get("decision") or "unknown")
        reason = str(td.get("decision_reason") or "unspecified")
        _record(store, surface="FOMO", candidate_id=signature, release_commit=release, stage="decision", status="complete", reason=reason, payload={"decision": decision, "position_fraction": td.get("position_fraction")})
        _record(store, surface="FOMO", candidate_id=signature, release_commit=release, stage="position", status="paper_position_authorized" if decision.startswith("paper_enter") else "not_opened", reason=reason)
        if "source_signature" in outcome_cols:
            with store._lock:
                out = store.db.execute("SELECT * FROM fomo_paper_outcomes WHERE source_signature=? ORDER BY id DESC LIMIT 1", (signature,)).fetchone()
            if out is not None:
                od = dict(out)
                _record(store, surface="FOMO", candidate_id=signature, release_commit=release, stage="settlement", status="complete", reason=str(od.get("exit_reason") or "paper_settled"), payload={"net_return": od.get("net_return")})
                _record(store, surface="FOMO", candidate_id=signature, release_commit=release, stage="learning", status="complete", reason="fomo_outcome_available_to_frozen_epoch_learning", payload={"net_return": od.get("net_return")})
    return count


def refresh_candidate_pipeline(store: Any) -> dict[str, Any]:
    _schema(store)
    solana = _reconcile_solana(store)
    fomo = _reconcile_fomo(store)
    with store._lock:
        rows = store.db.execute(
            "SELECT surface,stage,status,COUNT(*) AS count FROM v51_candidate_pipeline_audit "
            "WHERE economic_freeze_epoch=? GROUP BY surface,stage,status ORDER BY surface,stage,status",
            (ECONOMIC_FREEZE_EPOCH,),
        ).fetchall()
    summary: dict[str, dict[str, dict[str, int]]] = {}
    for row in rows:
        summary.setdefault(str(row["surface"]), {}).setdefault(str(row["stage"]), {})[str(row["status"])] = int(row["count"])
    coverage_debt = sum(
        int(statuses.get("coverage_debt", 0))
        for surface in summary.values()
        for statuses in surface.values()
    )
    return {
        "pipeline_version": PIPELINE_VERSION,
        "authority_id": AUTHORITY_ID,
        "economic_freeze_epoch": ECONOMIC_FREEZE_EPOCH,
        "pipeline_stages": list(PIPELINE_STAGES),
        "source_candidates_seen": {"solana": solana, "fomo": fomo},
        "stage_summary": summary,
        "coverage_debt_count": coverage_debt,
        "coverage_complete": coverage_debt == 0,
        "robinhood_detection_coverage": "reported_separately_until_pretrial_candidate_ledger_is_available; paper_trials_are_not_treated_as_proof_of_all_detected_opportunities",
        "paper_only": True,
        "live_money_authority": False,
    }


def record_seeded_stage(store: Any, candidate_id: str, stage: str, *, status: str = "complete", reason: str = "seeded_equivalence_test") -> None:
    _record(store, surface="SEEDED_E2E", candidate_id=candidate_id, release_commit="seeded", stage=stage, status=status, reason=reason)


__all__ = ["PIPELINE_VERSION", "refresh_candidate_pipeline", "record_seeded_stage"]
