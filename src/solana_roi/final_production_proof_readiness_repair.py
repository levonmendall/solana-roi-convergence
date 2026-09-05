from __future__ import annotations

import asyncio
import json
import math
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from . import candidate_risk_quote_v4_handoff as candidate_v4
from . import direct_transaction as tx
from . import economic_signal_continuation_repair as economic
from . import scout_candidate_continuity_repair as scout
from .direct_solana import DirectSolanaIngestionPlane
from .profit_first_entity_final_research import _adapter

REPAIR_VERSION = "final-production-proof-readiness-v1"
PROBE_STAGE = "economic_current_context_probe"
PROBE_FRACTION = 0.005
CURRENT_REFERENCE_MAX_AGE_SECONDS = 300.0
PROBE_TASK_LIMIT = 8
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False

_ORIGINAL_NORMALIZE: Callable[..., Any] | None = None
_ORIGINAL_DIRECT_STATUS: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_ROBINHOOD_STATUS: Callable[..., dict[str, Any]] | None = None
_INSTALLED = False
_INSTALLED_AT: datetime | None = None


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_dt(value: Any) -> datetime | None:
    try:
        dt = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _table_exists(store: Any, name: str) -> bool:
    try:
        with store._lock:
            return store.db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (name,)
            ).fetchone() is not None
    except Exception:
        return False


def _inc(obj: Any, name: str) -> None:
    attr = f"_roi_final_proof_{name}"
    setattr(obj, attr, int(getattr(obj, attr, 0) or 0) + 1)


def _schema(plane: Any) -> None:
    with plane.store._lock, plane.store.db:
        plane.store.db.execute(
            "CREATE TABLE IF NOT EXISTS economic_current_context_probe_audit ("
            "signature TEXT PRIMARY KEY,token_mint TEXT NOT NULL,wallet TEXT NOT NULL,signal_observed_at TEXT NOT NULL,"
            "current_venue TEXT,reference_source TEXT,current_reference_price_sol REAL,reference_observed_at TEXT,"
            "reference_received_at TEXT,risk_complete INTEGER NOT NULL,risk_fresh INTEGER NOT NULL,"
            "canonical_quote_attempted INTEGER NOT NULL,canonical_quote_usable INTEGER NOT NULL,"
            "exact_entry_executable INTEGER NOT NULL,exact_exit_executable INTEGER NOT NULL,entry_price_sol REAL,"
            "exit_net_sol REAL,round_trip_cost_fraction REAL,zero_allocation INTEGER NOT NULL,state TEXT NOT NULL,"
            "reason TEXT NOT NULL,updated_at TEXT NOT NULL,paper_only INTEGER NOT NULL,live_money_authority INTEGER NOT NULL)"
        )


def _economic_unpriced_buy(plane: Any, signature: str) -> dict[str, Any] | None:
    if not _table_exists(plane.store, "scout_economic_movement_observations"):
        return None
    try:
        with plane.store._lock:
            row = plane.store.db.execute(
                "SELECT signature,wallet,token_mint,side,native_amount_sol,observed_at,received_at "
                "FROM scout_economic_movement_observations WHERE signature=? LIMIT 1", (signature,)
            ).fetchone()
        if row is None:
            return None
        result = dict(row)
        if str(result.get("side") or "").lower() != "buy":
            return None
        native = result.get("native_amount_sol")
        if native is not None and float(native) > 0.0:
            return None
        return result
    except Exception:
        return None


def _current_direct_reference(plane: Any, token: str, *, now: datetime | None = None) -> dict[str, Any] | None:
    current = now or _utcnow()
    lower = (current - timedelta(seconds=CURRENT_REFERENCE_MAX_AGE_SECONDS)).isoformat()
    try:
        with plane.store._lock:
            rows = plane.store.db.execute(
                "SELECT source,reference_price_sol,observed_at,received_at FROM normalized_swaps "
                "WHERE token_mint=? AND received_at>=? AND reference_price_sol>0 ORDER BY received_at DESC,id DESC LIMIT 128",
                (token, lower),
            ).fetchall()
    except Exception:
        return None
    for raw in rows:
        row = dict(raw)
        venue = economic._parse_direct_venue(str(row.get("source") or ""))
        if venue not in {"PUMP_FUN", "PUMP_AMM", "RAYDIUM"}:
            continue
        try:
            price = float(row["reference_price_sol"])
        except (KeyError, TypeError, ValueError):
            continue
        received = _parse_dt(row.get("received_at"))
        observed = _parse_dt(row.get("observed_at")) or received
        if received is None or observed is None or not math.isfinite(price) or price <= 0.0:
            continue
        if (current - received).total_seconds() > CURRENT_REFERENCE_MAX_AGE_SECONDS:
            continue
        return {"venue": venue, "source": str(row["source"]), "price": price, "observed_at": observed, "received_at": received}
    return None


def _record_probe(
    plane: Any,
    row: dict[str, Any],
    reference: dict[str, Any] | None,
    *,
    risk_complete: bool = False,
    risk_fresh: bool = False,
    quote_attempted: bool = False,
    quote_usable: bool = False,
    execution: dict[str, Any] | None = None,
    state: str,
    reason: str,
) -> None:
    _schema(plane)
    execution = execution or {}
    entry = execution.get("entry_price_sol")
    exit_net = execution.get("exit_net_sol")
    round_trip = execution.get("round_trip_cost_fraction")
    ref = reference or {}
    with plane.store._lock, plane.store.db:
        plane.store.db.execute(
            "INSERT OR REPLACE INTO economic_current_context_probe_audit("
            "signature,token_mint,wallet,signal_observed_at,current_venue,reference_source,current_reference_price_sol,"
            "reference_observed_at,reference_received_at,risk_complete,risk_fresh,canonical_quote_attempted,"
            "canonical_quote_usable,exact_entry_executable,exact_exit_executable,entry_price_sol,exit_net_sol,"
            "round_trip_cost_fraction,zero_allocation,state,reason,updated_at,paper_only,live_money_authority) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,?,?,?,1,0)",
            (
                str(row.get("signature") or ""), str(row.get("token_mint") or ""), str(row.get("wallet") or ""),
                str(row.get("observed_at") or ""), ref.get("venue"), ref.get("source"), ref.get("price"),
                ref.get("observed_at").isoformat() if ref.get("observed_at") else None,
                ref.get("received_at").isoformat() if ref.get("received_at") else None,
                int(risk_complete), int(risk_fresh), int(quote_attempted), int(quote_usable), int(entry is not None),
                int(exit_net is not None), float(entry) if entry is not None else None,
                float(exit_net) if exit_net is not None else None,
                float(round_trip) if round_trip is not None else None, state, reason, _utcnow().isoformat(),
            ),
        )


async def _fresh_risk(plane: Any, token: str) -> tuple[bool, bool]:
    service = getattr(plane, "service", None)
    refresh = getattr(getattr(service, "collectors", None), "refresh", None)
    if callable(refresh):
        try:
            await asyncio.wait_for(refresh(token, _utcnow(), current_swap=None), timeout=20.0)
        except asyncio.CancelledError:
            raise
        except Exception:
            _inc(plane, "risk_refresh_error")
    readiness_fn = getattr(service, "_risk_readiness", None)
    if not callable(readiness_fn):
        return False, False
    try:
        readiness = readiness_fn(token, _utcnow())
    except Exception:
        return False, False
    if not isinstance(readiness, dict):
        return False, False
    six_fn = getattr(service, "_six_dimensions_fresh", None)
    six = bool(six_fn(readiness)) if callable(six_fn) else bool(readiness.get("complete") and readiness.get("fresh"))
    return bool(readiness.get("complete") and six), bool(readiness.get("fresh") and six)


async def _probe_current_context(plane: Any, signature: str) -> None:
    row = _economic_unpriced_buy(plane, signature)
    if row is None:
        return
    reference = _current_direct_reference(plane, str(row.get("token_mint") or ""))
    if reference is None:
        _record_probe(plane, row, None, state="awaiting_current_direct_reference", reason="no_recent_direct_venue_reference_price")
        return

    risk_complete, risk_fresh = await _fresh_risk(plane, str(row["token_mint"]))
    execution = None
    discovery = candidate_v4._attached_discovery(plane)
    if discovery is not None:
        pseudo = {"token_mint": row["token_mint"], "observed_at": reference["observed_at"].isoformat(), "wallet_price_sol": reference["price"]}
        try:
            execution = await asyncio.wait_for(_adapter(discovery)._execution(pseudo, PROBE_FRACTION), timeout=15.0)
        except asyncio.CancelledError:
            raise
        except Exception:
            _inc(plane, "exact_execution_error")

    quote_attempted = False
    quote = None
    quote_fn = getattr(getattr(plane, "service", None), "_quote", None)
    if risk_complete and risk_fresh and callable(quote_fn):
        quote_attempted = True
        try:
            quote = await asyncio.wait_for(
                quote_fn(token_mint=row["token_mint"], stage=PROBE_STAGE, fraction=PROBE_FRACTION,
                         scout_reference_price_sol=reference["price"], trigger_observed_at=reference["observed_at"]),
                timeout=15.0,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            _inc(plane, "canonical_quote_error")
    quote_usable = bool(quote.get("usable")) if isinstance(quote, dict) else bool(getattr(quote, "usable", False)) if quote is not None else False
    entry_ok = bool(isinstance(execution, dict) and execution.get("entry_price_sol") is not None)
    exit_ok = bool(isinstance(execution, dict) and execution.get("exit_net_sol") is not None)

    if not risk_complete or not risk_fresh:
        state, reason = "awaiting_fresh_six_dimension_risk", "current_context_risk_incomplete_or_stale"
    elif not entry_ok:
        state, reason = "current_entry_route_unavailable", "amount_specific_current_entry_route_unavailable"
    elif not exit_ok:
        state, reason = "current_exit_route_unavailable", "amount_specific_current_exit_route_unavailable"
    elif not quote_attempted or quote is None:
        state, reason = "canonical_quote_failed_closed", "canonical_amount_specific_quote_or_unsigned_simulation_unavailable"
    else:
        state = "current_context_execution_proved" if quote_usable else "current_context_execution_observed_unusable"
        reason = "current_context_only_zero_allocation_proof"
    _record_probe(plane, row, reference, risk_complete=risk_complete, risk_fresh=risk_fresh,
                  quote_attempted=quote_attempted, quote_usable=quote_usable, execution=execution, state=state, reason=reason)


def _schedule_probe(plane: Any, signature: str) -> None:
    if _economic_unpriced_buy(plane, signature) is None:
        return
    active = getattr(plane, "_roi_final_proof_active", None)
    tasks = getattr(plane, "_roi_final_proof_tasks", None)
    if not isinstance(active, set):
        active = set(); setattr(plane, "_roi_final_proof_active", active)
    if not isinstance(tasks, set):
        tasks = set(); setattr(plane, "_roi_final_proof_tasks", tasks)
    tasks.difference_update({task for task in tasks if task.done()})
    if signature in active or len(tasks) >= PROBE_TASK_LIMIT:
        return
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return
    active.add(signature)
    task = asyncio.create_task(_probe_current_context(plane, signature), name=f"current-context-proof:{signature[:10]}")
    tasks.add(task)
    def done(completed: asyncio.Task[Any]) -> None:
        active.discard(signature); tasks.discard(completed)
        try:
            completed.exception()
        except BaseException:
            pass
    task.add_done_callback(done)


def _normalize_with_probe(result: Any, *, signature: str, trigger_received_at: datetime, source_hint: str | None = None) -> Any:
    if _ORIGINAL_NORMALIZE is None:
        raise RuntimeError("final proof repair not installed")
    normalized = _ORIGINAL_NORMALIZE(result, signature=signature, trigger_received_at=trigger_received_at, source_hint=source_hint)
    if normalized is None and source_hint is None:
        plane = scout._SCOUT_HYDRATION_PLANE.get()
        if plane is not None:
            _schedule_probe(plane, signature)
    return normalized


def _matching_existing_quote_without_probe_rows(obj: Any, swap: Any) -> dict[str, Any] | None:
    try:
        with obj.store._lock:
            rows = obj.store.db.execute(
                "SELECT token_mint,stage,effective_price_sol,scout_reference_price_sol,drift_fraction,received_at,"
                "chain_to_quote_ms,usable,reason FROM execution_quote_observations WHERE token_mint=? AND received_at>=? "
                "AND stage NOT LIKE 'economic_current_context_%' ORDER BY id DESC LIMIT 20",
                (swap.token_mint, swap.received_at.isoformat()),
            ).fetchall()
    except Exception:
        return None
    tolerance = max(1e-15, abs(float(swap.reference_price_sol)) * 1e-9)
    for raw in rows:
        row = dict(raw)
        try:
            if abs(float(row["scout_reference_price_sol"]) - float(swap.reference_price_sol)) > tolerance:
                continue
        except (TypeError, ValueError):
            continue
        row["usable"] = bool(row.get("usable")); return row
    return None


def _probe_counts(plane: Any) -> dict[str, Any]:
    empty = {"zero_allocation_probe_count": 0, "current_direct_reference_resolved": 0, "six_dimension_risk_complete_fresh": 0,
             "canonical_quote_attempted": 0, "canonical_quote_usable": 0, "exact_entry_executable": 0,
             "exact_exit_executable": 0, "current_context_execution_proved": 0, "states": {}}
    if not _table_exists(plane.store, "economic_current_context_probe_audit"):
        return empty
    try:
        with plane.store._lock:
            row = plane.store.db.execute(
                "SELECT COUNT(*) total,COALESCE(SUM(current_reference_price_sol IS NOT NULL),0) ref,"
                "COALESCE(SUM(risk_complete=1 AND risk_fresh=1),0) risk,COALESCE(SUM(canonical_quote_attempted=1),0) qa,"
                "COALESCE(SUM(canonical_quote_usable=1),0) qu,COALESCE(SUM(exact_entry_executable=1),0) ee,"
                "COALESCE(SUM(exact_exit_executable=1),0) ex,COALESCE(SUM(state='current_context_execution_proved'),0) proved "
                "FROM economic_current_context_probe_audit"
            ).fetchone()
            states = plane.store.db.execute("SELECT state,COUNT(*) n FROM economic_current_context_probe_audit GROUP BY state").fetchall()
        return {"zero_allocation_probe_count": int(row["total"]), "current_direct_reference_resolved": int(row["ref"]),
                "six_dimension_risk_complete_fresh": int(row["risk"]), "canonical_quote_attempted": int(row["qa"]),
                "canonical_quote_usable": int(row["qu"]), "exact_entry_executable": int(row["ee"]),
                "exact_exit_executable": int(row["ex"]), "current_context_execution_proved": int(row["proved"]),
                "states": {str(x["state"]): int(x["n"]) for x in states}}
    except Exception:
        return empty


def _recent_venue_counts(plane: Any) -> dict[str, dict[str, int]]:
    start = _INSTALLED_AT or (_utcnow() - timedelta(minutes=5))
    counts = {venue: Counter() for venue in ("PUMP_FUN", "PUMP_AMM", "RAYDIUM")}
    try:
        with plane.store._lock:
            rows = plane.store.db.execute("SELECT source,side,COUNT(*) n FROM normalized_swaps WHERE received_at>=? GROUP BY source,side", (start.isoformat(),)).fetchall()
        for row in rows:
            venue = economic._parse_direct_venue(str(row["source"] or ""))
            if venue in counts:
                n = int(row["n"] or 0); counts[venue][str(row["side"] or "unknown").lower()] += n; counts[venue]["total"] += n
    except Exception:
        pass
    return {venue: dict(value) for venue, value in counts.items()}


def _fomo_counts(plane: Any) -> dict[str, int]:
    if not _table_exists(plane.store, "independent_fomo_runtime"):
        return {}
    try:
        with plane.store._lock:
            row = plane.store.db.execute("SELECT value FROM independent_fomo_runtime WHERE key='diag:source_row_counts' LIMIT 1").fetchone()
        value = json.loads(str(row["value"])) if row else {}
        return {str(k): int(v or 0) for k, v in value.items()} if isinstance(value, dict) else {}
    except Exception:
        return {}


def _frontiers(plane: Any) -> dict[str, Any]:
    value = getattr(getattr(plane, "journal", None), "_roi_exact_durable_ws_frontiers", None)
    sources = sorted(source for source, row in (value.items() if isinstance(value, dict) else [])
                     if source in {"PUMP_FUN", "PUMP_AMM"} and isinstance(row, dict) and row.get("durable"))
    return {"sources": sources, "count": len(sources), "pump_fun": "PUMP_FUN" in sources, "pump_amm": "PUMP_AMM" in sources}


def _status_with_proof(self: Any) -> dict[str, Any]:
    if _ORIGINAL_DIRECT_STATUS is None:
        raise RuntimeError("final proof status not installed")
    payload = _ORIGINAL_DIRECT_STATUS(self)
    economic_counts = economic._economic_counts(self)
    payload["production_proof_readiness"] = {
        "installed": True, "version": REPAIR_VERSION,
        "original_transaction_price_evidence": {**economic_counts,
            "unpriced_original_economic_observations": max(0, int(economic_counts.get("durable_economic_observations", 0)) - int(economic_counts.get("priced_economic_observations", 0))),
            "current_context_probe_does_not_backfill_original_price": True},
        "current_executable_context": _probe_counts(self),
        "normalized_swap_rows_since_repair_start_by_venue": _recent_venue_counts(self),
        "fomo_normalized_source_row_counts": _fomo_counts(self),
        "exact_durable_websocket_frontier": _frontiers(self),
        "acceptance_contract": {"current_context_probe_is_zero_allocation": True, "probe_can_create_copyable_scout_authority": False,
            "probe_quote_rows_can_be_reused_for_candidate_authority": False, "fresh_six_dimension_risk_required_before_canonical_quote_probe": True,
            "certification_thresholds_changed": False, "hundred_sample_cohorts_remain_production_evidence_only": True},
        "paper_only": True, "live_money_authority": False, "signing_available": False, "transaction_submission_available": False,
    }
    return payload


def _sanitize_public_robinhood(value: Any) -> Any:
    if isinstance(value, str):
        return "robinhood_forward_frontier_not_ready" if value == "robinhood_not_caught_up_for_paper_decisions" else value
    if isinstance(value, list):
        return [_sanitize_public_robinhood(x) for x in value]
    if not isinstance(value, dict):
        return value
    output = {}
    for key, item in value.items():
        normalized = str(key).lower().replace("-", "_")
        if "catchup" in normalized or "caught_up" in normalized or normalized in {"historical_readiness", "historical_decision_readiness"}:
            continue
        output[key] = _sanitize_public_robinhood(item)
    return output


def _public_robinhood_status() -> dict[str, Any]:
    if _ORIGINAL_ROBINHOOD_STATUS is None:
        raise RuntimeError("public Robinhood status sanitizer not installed")
    payload = _sanitize_public_robinhood(_ORIGINAL_ROBINHOOD_STATUS())
    if not isinstance(payload, dict):
        payload = {}
    payload.setdefault("forward_frontier_ready", bool(payload.get("paper_decision_transport_ready")))
    payload["public_readiness_contract"] = {"version": REPAIR_VERSION, "decision_readiness_field": "paper_decision_transport_ready",
        "forward_frontier_field": "forward_frontier_ready", "historical_cursor_has_decision_authority": False,
        "archival_history_can_block_prospective_decisions": False, "obsolete_catchup_terminology_removed": True,
        "paper_only": True, "live_money_authority": False}
    return payload


def install_final_production_proof_readiness_repair() -> None:
    global _INSTALLED, _INSTALLED_AT, _ORIGINAL_NORMALIZE, _ORIGINAL_DIRECT_STATUS, _ORIGINAL_ROBINHOOD_STATUS
    if _INSTALLED:
        return
    _INSTALLED_AT = _utcnow()
    current = tx.normalize_standard_transaction
    if not getattr(current, "_roi_final_production_proof_readiness", False):
        _ORIGINAL_NORMALIZE = current; _normalize_with_probe.__dict__.update(getattr(current, "__dict__", {}))
        setattr(_normalize_with_probe, "_roi_final_production_proof_readiness", True); tx.normalize_standard_transaction = _normalize_with_probe
    current_status = DirectSolanaIngestionPlane.status
    if not getattr(current_status, "_roi_final_production_proof_readiness", False):
        _ORIGINAL_DIRECT_STATUS = current_status; _status_with_proof.__dict__.update(getattr(current_status, "__dict__", {}))
        setattr(_status_with_proof, "_roi_final_production_proof_readiness", True); DirectSolanaIngestionPlane.status = _status_with_proof  # type: ignore[method-assign]
    current_matcher = candidate_v4._matching_existing_quote
    if not getattr(current_matcher, "_roi_final_production_proof_readiness", False):
        setattr(_matching_existing_quote_without_probe_rows, "_roi_final_production_proof_readiness", True)
        candidate_v4._matching_existing_quote = _matching_existing_quote_without_probe_rows  # type: ignore[assignment]
    from . import robinhood_runtime_install as robinhood_runtime
    current_robinhood = robinhood_runtime._status
    if not getattr(current_robinhood, "_roi_final_production_proof_readiness", False):
        _ORIGINAL_ROBINHOOD_STATUS = current_robinhood
        setattr(_public_robinhood_status, "_roi_final_production_proof_readiness", True)
        robinhood_runtime._status = _public_robinhood_status
    _INSTALLED = True


__all__ = ["CURRENT_REFERENCE_MAX_AGE_SECONDS", "PROBE_FRACTION", "PROBE_STAGE", "REPAIR_VERSION",
           "_current_direct_reference", "_frontiers", "_fomo_counts", "_matching_existing_quote_without_probe_rows",
           "_probe_current_context", "_recent_venue_counts", "_sanitize_public_robinhood", "install_final_production_proof_readiness_repair"]
