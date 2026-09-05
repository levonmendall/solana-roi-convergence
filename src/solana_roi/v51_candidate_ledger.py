from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from .strategy_v51_authority import AUTHORITY_ID, ECONOMIC_FREEZE_EPOCH, PIPELINE_STAGES

LEDGER_VERSION = "v51-canonical-candidate-ledger-v1"


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _dump(value: Any) -> str:
    return json.dumps(value or {}, sort_keys=True, separators=(",", ":"), default=str)


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


def _columns(store: Any, table: str) -> set[str]:
    if not _table_exists(store, table):
        return set()
    with store._lock:
        return {str(row["name"]) for row in store.db.execute(f"PRAGMA table_info({table})").fetchall()}


def ensure_schema(store: Any) -> None:
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS v51_candidates ("
            "surface TEXT NOT NULL, candidate_id TEXT NOT NULL, source_signature TEXT NOT NULL, "
            "release_commit TEXT, authority_id TEXT NOT NULL, economic_epoch TEXT NOT NULL, "
            "measurement_epoch TEXT NOT NULL, execution_model_epoch TEXT NOT NULL, "
            "measurement_fingerprint TEXT NOT NULL, execution_model_fingerprint TEXT NOT NULL, "
            "source_transport TEXT NOT NULL, venue TEXT, token_mint TEXT, trigger_wallet TEXT, trigger_entity TEXT, "
            "slot_or_block INTEGER, market_observed_at TEXT, system_received_at TEXT, candidate_created_at TEXT NOT NULL, "
            "raw_chase_fraction REAL, raw_latency_ms REAL, reference_price REAL, lifecycle TEXT, payload_json TEXT NOT NULL, "
            "paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL, "
            "PRIMARY KEY(surface,candidate_id))"
        )
        store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_v51_candidates_measurement "
            "ON v51_candidates(surface,measurement_epoch,candidate_created_at)"
        )
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS v51_candidate_stage_events ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, surface TEXT NOT NULL, candidate_id TEXT NOT NULL, "
            "release_commit TEXT, stage TEXT NOT NULL, stage_index INTEGER NOT NULL, status TEXT NOT NULL, "
            "reason TEXT NOT NULL, payload_json TEXT NOT NULL, observed_at TEXT NOT NULL, authority_id TEXT NOT NULL, "
            "economic_epoch TEXT NOT NULL, measurement_epoch TEXT NOT NULL, execution_model_epoch TEXT NOT NULL, "
            "paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL)"
        )
        store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_v51_candidate_stage_events_candidate "
            "ON v51_candidate_stage_events(surface,candidate_id,id)"
        )
        store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_v51_candidate_stage_events_stage "
            "ON v51_candidate_stage_events(surface,stage,status,id)"
        )
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS v51_candidate_current_state ("
            "surface TEXT NOT NULL, candidate_id TEXT NOT NULL, release_commit TEXT, stage TEXT NOT NULL, "
            "stage_index INTEGER NOT NULL, status TEXT NOT NULL, reason TEXT NOT NULL, payload_json TEXT NOT NULL, "
            "observed_at TEXT NOT NULL, last_event_id INTEGER NOT NULL, authority_id TEXT NOT NULL, "
            "economic_epoch TEXT NOT NULL, measurement_epoch TEXT NOT NULL, execution_model_epoch TEXT NOT NULL, "
            "paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL, "
            "PRIMARY KEY(surface,candidate_id,stage))"
        )
        # Compatibility/current-state surface retained for existing seeded regressions and
        # older API consumers. It is no longer the audit history; immutable history lives
        # in v51_candidate_stage_events.
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS v51_candidate_pipeline_audit ("
            "surface TEXT NOT NULL, candidate_id TEXT NOT NULL, release_commit TEXT, stage TEXT NOT NULL, "
            "stage_index INTEGER NOT NULL, status TEXT NOT NULL, reason TEXT NOT NULL, payload_json TEXT NOT NULL, "
            "observed_at TEXT NOT NULL, authority_id TEXT NOT NULL, economic_freeze_epoch TEXT NOT NULL, "
            "paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL, "
            "PRIMARY KEY(surface,candidate_id,stage))"
        )


def _measurement() -> Any:
    from . import v51_measurement_integrity as measurement

    return measurement


def record_stage_event(
    store: Any,
    *,
    surface: str,
    candidate_id: str,
    release_commit: str | None,
    stage: str,
    status: str,
    reason: str,
    payload: dict[str, Any] | None = None,
    observed_at: str | None = None,
) -> bool:
    if stage not in PIPELINE_STAGES:
        raise ValueError(f"unknown v5.1 pipeline stage: {stage}")
    ensure_schema(store)
    measurement = _measurement()
    measurement.ensure_release_compatibility(store, release_commit)
    raw_payload = _dump(payload)
    at = observed_at or _utcnow()
    with store._lock, store.db:
        current = store.db.execute(
            "SELECT status,reason,payload_json,release_commit,measurement_epoch,execution_model_epoch "
            "FROM v51_candidate_current_state WHERE surface=? AND candidate_id=? AND stage=?",
            (surface, candidate_id, stage),
        ).fetchone()
        if current is not None and (
            str(current["status"]) == status
            and str(current["reason"]) == reason
            and str(current["payload_json"]) == raw_payload
            and str(current["release_commit"] or "") == str(release_commit or "")
            and str(current["measurement_epoch"]) == measurement.MEASUREMENT_EPOCH
            and str(current["execution_model_epoch"]) == measurement.EXECUTION_MODEL_EPOCH
        ):
            return False
        cursor = store.db.execute(
            "INSERT INTO v51_candidate_stage_events("
            "surface,candidate_id,release_commit,stage,stage_index,status,reason,payload_json,observed_at,"
            "authority_id,economic_epoch,measurement_epoch,execution_model_epoch,paper_only,live_money_authority) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,1,0)",
            (
                surface,
                candidate_id,
                release_commit,
                stage,
                PIPELINE_STAGES.index(stage),
                status,
                reason,
                raw_payload,
                at,
                AUTHORITY_ID,
                ECONOMIC_FREEZE_EPOCH,
                measurement.MEASUREMENT_EPOCH,
                measurement.EXECUTION_MODEL_EPOCH,
            ),
        )
        event_id = int(cursor.lastrowid)
        store.db.execute(
            "INSERT INTO v51_candidate_current_state("
            "surface,candidate_id,release_commit,stage,stage_index,status,reason,payload_json,observed_at,last_event_id,"
            "authority_id,economic_epoch,measurement_epoch,execution_model_epoch,paper_only,live_money_authority) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0) "
            "ON CONFLICT(surface,candidate_id,stage) DO UPDATE SET "
            "release_commit=excluded.release_commit,stage_index=excluded.stage_index,status=excluded.status,"
            "reason=excluded.reason,payload_json=excluded.payload_json,observed_at=excluded.observed_at,"
            "last_event_id=excluded.last_event_id,authority_id=excluded.authority_id,economic_epoch=excluded.economic_epoch,"
            "measurement_epoch=excluded.measurement_epoch,execution_model_epoch=excluded.execution_model_epoch,"
            "paper_only=1,live_money_authority=0",
            (
                surface,
                candidate_id,
                release_commit,
                stage,
                PIPELINE_STAGES.index(stage),
                status,
                reason,
                raw_payload,
                at,
                event_id,
                AUTHORITY_ID,
                ECONOMIC_FREEZE_EPOCH,
                measurement.MEASUREMENT_EPOCH,
                measurement.EXECUTION_MODEL_EPOCH,
            ),
        )
        store.db.execute(
            "INSERT INTO v51_candidate_pipeline_audit("
            "surface,candidate_id,release_commit,stage,stage_index,status,reason,payload_json,observed_at,"
            "authority_id,economic_freeze_epoch,paper_only,live_money_authority) VALUES (?,?,?,?,?,?,?,?,?,?,?,1,0) "
            "ON CONFLICT(surface,candidate_id,stage) DO UPDATE SET "
            "release_commit=excluded.release_commit,stage_index=excluded.stage_index,status=excluded.status,"
            "reason=excluded.reason,payload_json=excluded.payload_json,observed_at=excluded.observed_at,"
            "authority_id=excluded.authority_id,economic_freeze_epoch=excluded.economic_freeze_epoch,paper_only=1,live_money_authority=0",
            (
                surface,
                candidate_id,
                release_commit,
                stage,
                PIPELINE_STAGES.index(stage),
                status,
                reason,
                raw_payload,
                at,
                AUTHORITY_ID,
                ECONOMIC_FREEZE_EPOCH,
            ),
        )
    return True


def _venue_from_source(source: str) -> str | None:
    upper = str(source or "").upper()
    for venue in ("PUMP_AMM", "PUMP_FUN", "RAYDIUM"):
        if venue in upper:
            return venue
    return None


def record_solana_candidate(store: Any, swap: Any, *, release_commit: str | None = None) -> bool:
    """Persist one normalized scout candidate before risk/quote/strategy filtering.

    A failure here is intentionally allowed to propagate so candidate processing fails
    closed instead of silently creating an unaccounted opportunity.
    """
    ensure_schema(store)
    measurement = _measurement()
    release = release_commit or measurement.current_release_commit()
    measurement.ensure_release_compatibility(store, release)
    candidate_id = str(getattr(swap, "signature", "") or "")
    if not candidate_id:
        raise ValueError("canonical Solana candidate requires source signature")
    observed = getattr(swap, "observed_at", None)
    received = getattr(swap, "received_at", None)
    source = str(getattr(swap, "source", "") or "")
    payload = {
        "signature": candidate_id,
        "slot": getattr(swap, "slot", None),
        "wallet": getattr(swap, "wallet", None),
        "token_mint": getattr(swap, "token_mint", None),
        "side": getattr(swap, "side", None),
        "token_amount": getattr(swap, "token_amount", None),
        "native_amount_sol": getattr(swap, "native_amount_sol", None),
        "reference_price_sol": getattr(swap, "reference_price_sol", None),
        "source": source,
    }
    created_at = _utcnow()
    with store._lock, store.db:
        cursor = store.db.execute(
            "INSERT OR IGNORE INTO v51_candidates("
            "surface,candidate_id,source_signature,release_commit,authority_id,economic_epoch,measurement_epoch,"
            "execution_model_epoch,measurement_fingerprint,execution_model_fingerprint,source_transport,venue,"
            "token_mint,trigger_wallet,trigger_entity,slot_or_block,market_observed_at,system_received_at,candidate_created_at,"
            "raw_chase_fraction,raw_latency_ms,reference_price,lifecycle,payload_json,paper_only,live_money_authority) "
            "VALUES ('SOLANA',?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0)",
            (
                candidate_id,
                candidate_id,
                release,
                AUTHORITY_ID,
                ECONOMIC_FREEZE_EPOCH,
                measurement.MEASUREMENT_EPOCH,
                measurement.EXECUTION_MODEL_EPOCH,
                measurement.measurement_fingerprint(),
                measurement.execution_model_fingerprint(),
                "direct_solana_normalized_scout_pre_strategy",
                _venue_from_source(source),
                str(getattr(swap, "token_mint", "") or "") or None,
                str(getattr(swap, "wallet", "") or "") or None,
                None,
                int(getattr(swap, "slot", 0) or 0) or None,
                observed.isoformat() if hasattr(observed, "isoformat") else None,
                received.isoformat() if hasattr(received, "isoformat") else None,
                created_at,
                None,
                float(getattr(swap, "ingestion_latency_ms", 0.0) or 0.0),
                float(getattr(swap, "reference_price_sol", 0.0) or 0.0) or None,
                None,
                _dump(payload),
            ),
        )
        inserted = cursor.rowcount == 1
    if inserted:
        record_stage_event(
            store,
            surface="SOLANA",
            candidate_id=candidate_id,
            release_commit=release,
            stage="ingestion",
            status="complete",
            reason="canonical_normalized_scout_candidate_persisted_before_downstream_filters",
            payload=payload,
            observed_at=created_at,
        )
        record_stage_event(
            store,
            surface="SOLANA",
            candidate_id=candidate_id,
            release_commit=release,
            stage="candidate",
            status="complete",
            reason="canonical_v51_candidate_created",
            payload={"venue": _venue_from_source(source), "wallet": payload["wallet"], "token_mint": payload["token_mint"]},
            observed_at=created_at,
        )
    return inserted


def canonical_solana_candidates(store: Any) -> list[dict[str, Any]]:
    ensure_schema(store)
    measurement = _measurement()
    with store._lock:
        rows = store.db.execute(
            "SELECT candidate_id AS signature,release_commit,token_mint,trigger_wallet AS wallet,"
            "source_transport AS source,market_observed_at AS observed_at,system_received_at AS received_at,"
            "venue,raw_latency_ms FROM v51_candidates WHERE surface='SOLANA' AND measurement_epoch=? "
            "ORDER BY candidate_created_at,candidate_id",
            (measurement.MEASUREMENT_EPOCH,),
        ).fetchall()
    return [dict(row) for row in rows]


def _find_trial(store: Any, signature: str, release: str | None) -> Any | None:
    cols = _columns(store, "risk_conditioned_alpha_v5_trials")
    if not {"source_signature", "decision", "decision_reason"}.issubset(cols):
        return None
    sql = "SELECT * FROM risk_conditioned_alpha_v5_trials WHERE source_signature=? "
    params: tuple[Any, ...] = (signature,)
    if release and "release_commit" in cols:
        sql += "AND release_commit=? "
        params = (signature, release)
    sql += "ORDER BY selected DESC,id DESC LIMIT 1"
    with store._lock:
        return store.db.execute(sql, params).fetchone()


def _find_outcome(store: Any, signature: str, release: str | None) -> Any | None:
    cols = _columns(store, "risk_conditioned_alpha_v5_outcomes")
    if not {"source_signature", "net_return"}.issubset(cols):
        return None
    sql = "SELECT * FROM risk_conditioned_alpha_v5_outcomes WHERE source_signature=? "
    params: tuple[Any, ...] = (signature,)
    if release and "release_commit" in cols:
        sql += "AND release_commit=? "
        params = (signature, release)
    sql += "ORDER BY id DESC LIMIT 1"
    with store._lock:
        return store.db.execute(sql, params).fetchone()


def _reconcile_solana(store: Any) -> int:
    candidates = canonical_solana_candidates(store)
    for candidate in candidates:
        signature = str(candidate.get("signature") or "")
        release = str(candidate.get("release_commit") or "") or None
        trial = _find_trial(store, signature, release)
        if trial is None:
            record_stage_event(
                store,
                surface="SOLANA",
                candidate_id=signature,
                release_commit=release,
                stage="context",
                status="coverage_debt",
                reason="canonical_candidate_has_no_v51_context_record",
            )
            continue
        td = dict(trial)
        record_stage_event(
            store,
            surface="SOLANA",
            candidate_id=signature,
            release_commit=release,
            stage="context",
            status="complete",
            reason="risk_lane_venue_lifecycle_context_persisted",
            payload={k: td.get(k) for k in ("lane", "venue", "lifecycle", "regime", "risk_signature", "context_key")},
        )
        executable = bool(td.get("entry_executable")) and bool(td.get("exit_executable"))
        record_stage_event(
            store,
            surface="SOLANA",
            candidate_id=signature,
            release_commit=release,
            stage="execution_evidence",
            status="complete" if executable else "failed_closed",
            reason="amount_specific_entry_and_exit_evidence" if executable else "entry_or_exit_execution_evidence_incomplete",
            payload={k: td.get(k) for k in ("round_trip_cost_fraction", "chase_band", "latency_band", "entry_executable", "exit_executable")},
        )
        decision = str(td.get("decision") or "unknown")
        reason = str(td.get("decision_reason") or "unspecified")
        record_stage_event(
            store,
            surface="SOLANA",
            candidate_id=signature,
            release_commit=release,
            stage="decision",
            status="complete",
            reason=reason,
            payload={"decision": decision, "position_fraction": td.get("position_fraction")},
        )
        selected = decision.startswith("paper_enter") and int(td.get("selected") or 0) == 1
        record_stage_event(
            store,
            surface="SOLANA",
            candidate_id=signature,
            release_commit=release,
            stage="position",
            status="paper_position_authorized" if selected else "not_opened",
            reason="selected_v51_paper_entry" if selected else reason,
            payload={"decision": decision, "lane": td.get("lane"), "position_fraction": td.get("position_fraction")},
        )
        outcome = _find_outcome(store, signature, release)
        if outcome is not None:
            od = dict(outcome)
            record_stage_event(
                store,
                surface="SOLANA",
                candidate_id=signature,
                release_commit=release,
                stage="settlement",
                status="complete",
                reason=str(od.get("exit_reason") or "paper_settled"),
                payload={"net_return": od.get("net_return"), "settled_at": od.get("settled_at")},
            )
            record_stage_event(
                store,
                surface="SOLANA",
                candidate_id=signature,
                release_commit=release,
                stage="learning",
                status="complete",
                reason="settled_outcome_available_to_frozen_epoch_learning",
                payload={"net_return": od.get("net_return")},
            )
        elif selected:
            record_stage_event(
                store,
                surface="SOLANA",
                candidate_id=signature,
                release_commit=release,
                stage="settlement",
                status="pending",
                reason="paper_position_not_yet_settled",
            )
    return len(candidates)


def _reconcile_fomo_legacy(store: Any) -> int:
    # FOMO already has a primary shadow-observation source. Reuse its established
    # reconciliation logic while routing every stage write into the append-only v2
    # stage event recorder.
    from . import v51_candidate_pipeline as legacy

    legacy._record = record_stage_event  # type: ignore[assignment]
    return int(legacy._reconcile_fomo(store))


def refresh_candidate_pipeline(store: Any) -> dict[str, Any]:
    ensure_schema(store)
    solana = _reconcile_solana(store)
    fomo = _reconcile_fomo_legacy(store)
    measurement = _measurement()
    with store._lock:
        rows = store.db.execute(
            "SELECT surface,stage,status,COUNT(*) AS count FROM v51_candidate_current_state "
            "WHERE economic_epoch=? AND measurement_epoch=? GROUP BY surface,stage,status "
            "ORDER BY surface,stage,status",
            (ECONOMIC_FREEZE_EPOCH, measurement.MEASUREMENT_EPOCH),
        ).fetchall()
        watermark = store.db.execute(
            "SELECT MAX(id) AS max_id,MAX(observed_at) AS evidence_through FROM v51_candidate_stage_events "
            "WHERE economic_epoch=? AND measurement_epoch=?",
            (ECONOMIC_FREEZE_EPOCH, measurement.MEASUREMENT_EPOCH),
        ).fetchone()
    summary: dict[str, dict[str, dict[str, int]]] = {}
    for row in rows:
        summary.setdefault(str(row["surface"]), {}).setdefault(str(row["stage"]), {})[str(row["status"])] = int(row["count"])
    coverage_debt = sum(
        int(statuses.get("coverage_debt", 0))
        for surface in summary.values()
        for statuses in surface.values()
    )
    state = "confirmed" if coverage_debt == 0 else "partial"
    return {
        "pipeline_version": LEDGER_VERSION,
        "authority_id": AUTHORITY_ID,
        "economic_freeze_epoch": ECONOMIC_FREEZE_EPOCH,
        "measurement_epoch": measurement.MEASUREMENT_EPOCH,
        "execution_model_epoch": measurement.EXECUTION_MODEL_EPOCH,
        "measurement_fingerprint": measurement.measurement_fingerprint(),
        "execution_model_fingerprint": measurement.execution_model_fingerprint(),
        "pipeline_stages": list(PIPELINE_STAGES),
        "candidate_source_of_truth": "append_only_v51_candidates_at_normalized_scout_pre_strategy_boundary",
        "stage_history_source_of_truth": "append_only_v51_candidate_stage_events",
        "source_candidates_seen": {"solana": solana, "fomo": fomo},
        "stage_summary": summary,
        "coverage_debt_count": coverage_debt,
        "coverage_complete": coverage_debt == 0,
        "proof_state": state,
        "generated_at": _utcnow(),
        "evidence_through": watermark["evidence_through"] if watermark is not None else None,
        "evidence_event_id_through": int(watermark["max_id"] or 0) if watermark is not None else 0,
        "robinhood_detection_coverage": "merged_from_isolated_predecision_candidate_ledger",
        "paper_only": True,
        "live_money_authority": False,
    }


__all__ = [
    "LEDGER_VERSION",
    "canonical_solana_candidates",
    "ensure_schema",
    "record_solana_candidate",
    "record_stage_event",
    "refresh_candidate_pipeline",
]
