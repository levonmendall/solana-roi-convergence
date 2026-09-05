from __future__ import annotations

import copy
import threading
import time
from typing import Any

from .strategy_v51_authority import AUTHORITY_ID, ECONOMIC_FREEZE_EPOCH, PIPELINE_STAGES
from .v51_economic_certification import build_economic_certification
from .v51_execution_stress_diagnostics import build_execution_mechanism_stress

PROOF_CACHE_SECONDS = 30.0
_CACHE_LOCK = threading.Lock()
_CACHE: dict[str, Any] | None = None
_CACHE_MONOTONIC: float | None = None
_CACHE_STORE_ID: int | None = None


def _table_exists(store: Any, table: str) -> bool:
    try:
        with store._lock:
            return store.db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (table,)
            ).fetchone() is not None
    except Exception:
        return False


def _candidate_coverage(store: Any) -> dict[str, Any]:
    stage_summary: dict[str, dict[str, int]] = {}
    if _table_exists(store, "v51_candidate_pipeline_audit"):
        with store._lock:
            rows = store.db.execute(
                "SELECT stage,status,COUNT(*) AS count FROM v51_candidate_pipeline_audit "
                "WHERE surface='ROBINHOOD_CHAIN' AND economic_freeze_epoch=? "
                "GROUP BY stage,status ORDER BY stage,status",
                (ECONOMIC_FREEZE_EPOCH,),
            ).fetchall()
        for row in rows:
            stage_summary.setdefault(str(row["stage"]), {})[str(row["status"])] = int(row["count"])

    candidates = entries = rejections = settled = coarse_rejections = 0
    ledger_available = _table_exists(store, "v51_robinhood_candidate_ledger")
    if ledger_available:
        with store._lock:
            row = store.db.execute(
                "SELECT COUNT(*) AS candidates,"
                "SUM(CASE WHEN decision='paper_enter' THEN 1 ELSE 0 END) AS entries,"
                "SUM(CASE WHEN decision='paper_reject' THEN 1 ELSE 0 END) AS rejections,"
                "SUM(CASE WHEN decision_reason='preselection_policy_or_evidence_failed_closed_before_lane' THEN 1 ELSE 0 END) AS coarse "
                "FROM v51_robinhood_candidate_ledger WHERE economic_freeze_epoch=?",
                (ECONOMIC_FREEZE_EPOCH,),
            ).fetchone()
            candidates = int(row["candidates"] or 0) if row else 0
            entries = int(row["entries"] or 0) if row else 0
            rejections = int(row["rejections"] or 0) if row else 0
            coarse_rejections = int(row["coarse"] or 0) if row else 0
            if _table_exists(store, "robinhood_paper_outcomes"):
                settled_row = store.db.execute(
                    "SELECT COUNT(*) AS settled FROM v51_robinhood_candidate_ledger l "
                    "JOIN robinhood_paper_outcomes o ON o.trial_id=l.trial_id "
                    "WHERE l.economic_freeze_epoch=?",
                    (ECONOMIC_FREEZE_EPOCH,),
                ).fetchone()
                settled = int(settled_row["settled"] or 0) if settled_row else 0

    # Every concrete v2/v3 opportunity delivered by forward-only transport is now
    # registered before the production strategy can return. Every row must finish as
    # paper_enter or paper_reject. Coarse fail-closed reasons remain separately
    # visible so observability never claims more diagnostic precision than exists.
    decision_debt = max(0, candidates - entries - rejections)
    entry_settlement_debt = max(0, entries - settled)
    coverage_debt = decision_debt + (0 if ledger_available else 1)
    return {
        "surface": "ROBINHOOD_CHAIN",
        "authority_id": AUTHORITY_ID,
        "economic_freeze_epoch": ECONOMIC_FREEZE_EPOCH,
        "pipeline_stages": list(PIPELINE_STAGES),
        "pre_lane_candidate_ledger_available": ledger_available,
        "canonical_candidate_count": candidates,
        "paper_entry_count": entries,
        "explicit_rejection_count": rejections,
        "coarse_preselection_rejection_count": coarse_rejections,
        "settled_entry_count": settled,
        "pending_settlement_count": entry_settlement_debt,
        "decision_coverage_debt_count": decision_debt,
        "coverage_debt_count": coverage_debt,
        "coverage_complete": coverage_debt == 0,
        "stage_summary": stage_summary,
        "candidate_definition": (
            "every concrete forward-only Robinhood v2/v3 opportunity delivered to canonical _maybe_open_v2/_maybe_open_v3 "
            "before any strategy early return"
        ),
        "decision_attribution_mode": "every candidate receives paper_enter or explicit fail-closed paper_reject",
        "coarse_reason_policy": (
            "provider-dependent preselection gates are not re-run for telemetry; unresolved early rejection is explicitly "
            "classified coarse_no_duplicate_rpc instead of silently disappearing"
        ),
        "paper_only": True,
        "live_money_authority": False,
    }


def build_robinhood_proof(store: Any) -> dict[str, Any]:
    certification = build_economic_certification(store)
    unexpected = sorted(name for name in certification.get("families", {}) if name != "ROBINHOOD_CHAIN")
    return {
        "authority_id": AUTHORITY_ID,
        "economic_freeze_epoch": ECONOMIC_FREEZE_EPOCH,
        "economic_certification": certification,
        "candidate_coverage": _candidate_coverage(store),
        "execution_mechanism_stress": build_execution_mechanism_stress(store),
        "unexpected_non_robinhood_families": unexpected,
        "proof_store_role": "isolated_robinhood_chain_sqlite",
        "paper_only": True,
        "live_money_authority": False,
    }


def cached_robinhood_proof(store: Any, *, max_age_seconds: float = PROOF_CACHE_SECONDS) -> dict[str, Any]:
    global _CACHE, _CACHE_MONOTONIC, _CACHE_STORE_ID
    now = time.monotonic()
    store_id = id(store)
    with _CACHE_LOCK:
        if (
            _CACHE is not None
            and _CACHE_MONOTONIC is not None
            and _CACHE_STORE_ID == store_id
            and now - _CACHE_MONOTONIC <= max(0.0, float(max_age_seconds))
        ):
            return copy.deepcopy(_CACHE)
    payload = build_robinhood_proof(store)
    with _CACHE_LOCK:
        _CACHE = copy.deepcopy(payload)
        _CACHE_MONOTONIC = now
        _CACHE_STORE_ID = store_id
    return payload


__all__ = ["PROOF_CACHE_SECONDS", "build_robinhood_proof", "cached_robinhood_proof"]
