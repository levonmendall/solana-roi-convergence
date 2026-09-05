from __future__ import annotations

import asyncio
import json
import math
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from functools import wraps
from typing import Any, Callable

from . import candidate_execution_evidence_plane as candidate_plane
from . import candidate_risk_window_repair as risk_window
from . import direct_transaction as tx
from . import post161_candidate_attribution_repair as post161
from . import risk_conditioned_alpha_v5 as v5
from . import risk_conditioned_alpha_v51 as v51
from . import scout_candidate_continuity_repair as scout
from . import semantic_candidate_attribution_architecture as semantic
from . import venue_native_candidate_graph_repair as venue
from .direct_solana import DirectSolanaIngestionPlane
from .ingestion import IngestionDecision, LAMPORTS_PER_SOL, NormalizedSwap
from .observation import TimedRiskCollectors, WSOL_MINT
from .profit_first_entity_final_research import FinalProfitFirstResearchAdapter
from .profit_first_entity_research import ProfitFirstResearchAdapter


REPAIR_VERSION = "economic-signal-continuation-v2"
ATTRIBUTION_VERSION = "economic-token-movement-before-venue-v1"
RISK_POLICY_VERSION = "mechanical-hard-stop-nonmechanical-shadow-v1"
ROUTER_OR_UNKNOWN_VENUE = "ROUTER_OR_UNKNOWN"
ROUTER_OR_UNKNOWN_LIFECYCLE = "router_or_unknown_venue"
IMMEDIATE_COPY_SECONDS = 20.0
CONFIRMED_CONTINUATION_SECONDS = 60.0
STRONG_CONTINUATION_SECONDS = 120.0
MATURE_CONTINUATION_SECONDS = 300.0
CANDIDATE_OPERATIONAL_TIMEOUT_SECONDS = 60.0
RISK_COLLECTION_OPERATIONAL_TIMEOUT_SECONDS = 20.0
CURRENT_VENUE_EVIDENCE_MAX_AGE_SECONDS = 300.0
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False

_ORIGINAL_NORMALIZER: Callable[..., Any] | None = None
_ORIGINAL_V5_PRE_CONTEXT: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_RESEARCH_RISK: Callable[..., Any] | None = None
_ORIGINAL_FINAL_BUY: Callable[..., Any] | None = None
_ORIGINAL_FINAL_STATUS: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_DIRECT_STATUS: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_TIMED_RISK_REFRESH: Callable[..., Any] | None = None
_ORIGINAL_V51_EXACT_SIZING: Callable[..., Any] | None = None
_INSTALLED = False

_SUPPORTED_VENUES = frozenset({"PUMP_FUN", "PUMP_AMM", "RAYDIUM"})
_MECHANICAL_HARD_STOPS = frozenset(v5.MECHANICAL_HARD_STOPS)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _inc(obj: Any, name: str, amount: int = 1) -> None:
    attr = f"_roi_economic_signal_{name}"
    setattr(obj, attr, int(getattr(obj, attr, 0) or 0) + int(amount))


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


def _economic_schema(plane: Any) -> None:
    store = getattr(plane, "store", None)
    if store is None:
        return
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS scout_economic_movement_observations ("
            "signature TEXT PRIMARY KEY, wallet TEXT NOT NULL, token_mint TEXT NOT NULL, side TEXT NOT NULL, "
            "token_amount REAL NOT NULL, native_amount_sol REAL, observed_at TEXT NOT NULL, received_at TEXT NOT NULL, "
            "original_source_hint TEXT, source_mode TEXT NOT NULL, direct_venue TEXT, venue_resolution_state TEXT NOT NULL, "
            "entry_authority INTEGER NOT NULL DEFAULT 0, architecture_version TEXT NOT NULL, created_at TEXT NOT NULL)"
        )
        store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_scout_economic_movement_token_time "
            "ON scout_economic_movement_observations(token_mint,received_at)"
        )


def _shadow_schema(adapter: Any) -> None:
    with adapter.store._lock, adapter.store.db:
        adapter.store.db.execute(
            "CREATE TABLE IF NOT EXISTS economic_signal_shadow_audit ("
            "release_commit TEXT NOT NULL, source_signature TEXT NOT NULL, token_mint TEXT NOT NULL, "
            "signal_origin_venue TEXT NOT NULL, resolved_current_venue TEXT, resolved_lifecycle TEXT, "
            "risk_complete INTEGER NOT NULL, risk_fresh INTEGER NOT NULL, reason TEXT NOT NULL, "
            "probe_position_fraction REAL NOT NULL, zero_allocation INTEGER NOT NULL, created_at TEXT NOT NULL, "
            "paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL, "
            "PRIMARY KEY(release_commit,source_signature))"
        )


def _material_owner_deltas(result: dict[str, Any], wallet: str) -> dict[str, float]:
    try:
        raw = tx._token_deltas_for_owner(result, wallet)
    except Exception:
        raw = {}
    return {
        str(mint): float(delta)
        for mint, delta in raw.items()
        if str(mint) != WSOL_MINT and math.isfinite(float(delta)) and abs(float(delta)) > 1e-18
    }


def _global_actor_transfer_flows(
    result: dict[str, Any], wallet: str
) -> tuple[dict[str, float], float]:
    """Aggregate only token/native transfers economically attributable to the scout.

    This is deliberately venue-agnostic. It proves the actor's movement first and
    leaves venue/lifecycle classification to a later, independent current-state step.
    """
    metadata = venue._token_account_metadata(result)
    keys = venue._account_keys(result)
    token_flow: dict[str, float] = defaultdict(float)
    quote_flow = 0.0
    for _parent, row in venue._walk_instruction_rows(result):
        transfer = venue._parsed_token_transfer(row, metadata)
        if transfer is None:
            transfer = post161._raw_token_transfer(row, keys=keys, metadata=metadata)
        if transfer is not None:
            source, destination, amount, mint, authority = transfer
            source_meta = metadata.get(source)
            destination_meta = metadata.get(destination)
            source_owner = str((source_meta or ("", "", 0))[0])
            destination_owner = str((destination_meta or ("", "", 0))[0])
            from_actor = source_owner == wallet or (not source_owner and authority == wallet)
            to_actor = destination_owner == wallet
            if not from_actor and not to_actor:
                continue
            signed = (float(amount) if to_actor else 0.0) - (float(amount) if from_actor else 0.0)
            if str(mint) == WSOL_MINT:
                quote_flow += signed
            else:
                token_flow[str(mint)] += signed
            continue

        native = venue._parsed_native_transfer(row)
        if native is None:
            native = post161._raw_native_transfer(row, keys=keys)
        if native is not None:
            source, destination, amount = native
            if destination == wallet:
                quote_flow += float(amount)
            if source == wallet:
                quote_flow -= float(amount)

    return (
        {mint: value for mint, value in token_flow.items() if abs(value) > 1e-18},
        float(quote_flow),
    )


def _economic_movement(
    result: Any, wallet: str
) -> tuple[dict[str, Any] | None, str | None]:
    if not isinstance(result, dict):
        return None, "invalid_transaction_result"
    meta = result.get("meta")
    if not isinstance(meta, dict) or meta.get("err") is not None:
        return None, "transaction_failed_or_meta_missing"
    if venue._wallet_account_index(result, wallet) is None:
        return None, "tracked_scout_account_index_missing"

    owner = _material_owner_deltas(result, wallet)
    transfer_flow, explicit_quote_flow = _global_actor_transfer_flows(result, wallet)

    # Final owner balance movement is the strongest actor endpoint proof when it is
    # unique. If it is ambiguous, do not use a transfer subset to guess around it.
    if len(owner) > 1:
        return None, "economic_multiple_token_endpoints"
    material = owner if len(owner) == 1 else transfer_flow
    material = {mint: value for mint, value in material.items() if abs(value) > 1e-18}
    if len(material) > 1:
        return None, "economic_multiple_token_endpoints"
    if len(material) != 1:
        return None, "economic_token_movement_missing"

    token_mint, token_delta = next(iter(material.items()))
    side = "buy" if token_delta > 0.0 else "sell"
    expected_quote_sign = -1.0 if side == "buy" else 1.0
    quote_flow = explicit_quote_flow

    # Balance-net fallback is fee-adjusted by the semantic helper. It is used only
    # to price an already-proven token movement; it never creates the movement fact.
    if quote_flow * expected_quote_sign <= 1e-18:
        wallet_index = venue._wallet_account_index(result, wallet)
        try:
            all_deltas = tx._token_deltas_for_owner(result, wallet)
        except Exception:
            all_deltas = {}
        native = (
            semantic._net_native_wsol_flow(
                result,
                wallet=wallet,
                wallet_index=int(wallet_index),
                deltas=all_deltas,
            )
            if wallet_index is not None
            else None
        )
        if native is not None and float(native) * expected_quote_sign > 1e-18:
            quote_flow = float(native)

    native_amount = abs(float(quote_flow)) if quote_flow * expected_quote_sign > 1e-18 else None
    return {
        "side": side,
        "token_mint": str(token_mint),
        "token_amount": abs(float(token_delta)),
        "native_amount_sol": native_amount,
        "movement_authority": "owner_token_delta" if owner else "actor_transfer_graph",
        "explicit_quote_flow": bool(explicit_quote_flow * expected_quote_sign > 1e-18),
    }, None


def _observed_at(result: dict[str, Any], trigger_received_at: datetime) -> datetime:
    try:
        block_time = int(result.get("blockTime") or 0)
    except (TypeError, ValueError):
        block_time = 0
    if block_time > 0:
        return datetime.fromtimestamp(block_time, tz=timezone.utc)
    return trigger_received_at


def _persist_economic_observation(
    plane: Any,
    *,
    signature: str,
    wallet: str,
    movement: dict[str, Any],
    observed_at: datetime,
    received_at: datetime,
    source_hint: str | None,
) -> None:
    _economic_schema(plane)
    direct_sources = set()
    try:
        direct_sources = set(venue._indexed_transaction_sources({}))
    except Exception:
        direct_sources = set()
    del direct_sources  # source authority is intentionally resolved independently below
    with plane.store._lock, plane.store.db:
        plane.store.db.execute(
            "INSERT OR IGNORE INTO scout_economic_movement_observations("
            "signature,wallet,token_mint,side,token_amount,native_amount_sol,observed_at,received_at,"
            "original_source_hint,source_mode,direct_venue,venue_resolution_state,entry_authority,architecture_version,created_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,NULL,'router_or_unknown_venue',0,?,?)",
            (
                signature,
                wallet,
                str(movement["token_mint"]),
                str(movement["side"]),
                float(movement["token_amount"]),
                movement.get("native_amount_sol"),
                observed_at.isoformat(),
                received_at.isoformat(),
                str(source_hint or "") or None,
                str(movement.get("movement_authority") or "economic_token_movement"),
                REPAIR_VERSION,
                _utcnow().isoformat(),
            ),
        )
    _inc(plane, "economic_observations")


def _economic_signal_normalizer(
    result: Any,
    *,
    signature: str,
    trigger_received_at: datetime,
    wallet: str,
    source_hint: str | None = None,
) -> tuple[NormalizedSwap | None, str | None]:
    if _ORIGINAL_NORMALIZER is None:
        raise RuntimeError("economic-signal normalizer is not installed")

    swap, error = _ORIGINAL_NORMALIZER(
        result,
        signature=signature,
        trigger_received_at=trigger_received_at,
        wallet=wallet,
        source_hint=source_hint,
    )
    plane = scout._SCOUT_HYDRATION_PLANE.get()
    if swap is not None:
        if plane is not None:
            _inc(plane, "direct_venue_preserved")
        return swap, error

    if str(error or "") not in {
        "supported_swap_source_missing",
        "source_hint_not_present",
        "semantic_unsupported_venue",
    }:
        return None, error

    movement, movement_error = _economic_movement(result, wallet)
    if movement is None:
        if plane is not None:
            _inc(plane, "economic_unresolved")
        return None, movement_error or error

    if not isinstance(result, dict):
        return None, "invalid_transaction_result"
    observed_at = _observed_at(result, trigger_received_at)
    if plane is not None:
        _persist_economic_observation(
            plane,
            signature=signature,
            wallet=wallet,
            movement=movement,
            observed_at=observed_at,
            received_at=trigger_received_at,
            source_hint=source_hint,
        )

    native_amount = movement.get("native_amount_sol")
    token_amount = float(movement.get("token_amount") or 0.0)
    if native_amount is None or float(native_amount) <= 0.0 or token_amount <= 0.0:
        if plane is not None:
            _inc(plane, "economic_price_unresolved")
        return None, "economic_movement_price_unresolved"

    try:
        slot = int(result["slot"])
    except (KeyError, TypeError, ValueError):
        return None, "slot_missing"

    recovered = NormalizedSwap(
        signature=signature,
        slot=slot,
        observed_at=observed_at,
        received_at=trigger_received_at,
        wallet=wallet,
        token_mint=str(movement["token_mint"]),
        side=str(movement["side"]),
        token_amount=token_amount,
        native_amount_sol=float(native_amount),
        reference_price_sol=float(native_amount) / token_amount,
        source=f"solana-direct:{ROUTER_OR_UNKNOWN_VENUE}:{movement['side']}",
    )
    if plane is not None:
        try:
            inserted = semantic._persist_opportunity(plane, recovered)
            semantic._persist_risk_readthrough(plane, recovered)
            if inserted:
                venue._schedule_prewarm(plane, recovered)
        except Exception:
            _inc(plane, "economic_ledger_errors")
            return None, "economic_candidate_ledger_persist_failed"
        _inc(plane, "router_unknown_normalized")
    return recovered, None


setattr(_economic_signal_normalizer, "_roi_economic_signal_continuation", True)
setattr(_economic_signal_normalizer, "_roi_post161_candidate_attribution", True)
setattr(_economic_signal_normalizer, "_roi_venue_native_candidate_graph", True)
setattr(_economic_signal_normalizer, "_roi_semantic_candidate_attribution", True)


def _parse_direct_venue(source: str) -> str | None:
    raw = str(source or "").upper()
    for candidate in ("PUMP_AMM", "RAYDIUM", "PUMP_FUN"):
        if candidate in raw:
            return candidate
    return None


def _resolve_current_venue_context(
    adapter: Any,
    row: dict[str, Any],
    *,
    as_of: datetime | None = None,
) -> tuple[str | None, str | None, str | None]:
    """Resolve *current* venue from independent durable market evidence.

    The scout's original route is not reused as venue authority. Exact Jupiter
    executability can prove current tradability, but it cannot prove an underlying
    venue, so an unresolved route stays shadow-only until direct venue evidence exists.
    """
    token = str(row.get("token_mint") or "")
    if not token:
        return None, None, None
    current = as_of or _utcnow()
    lower = (current - timedelta(seconds=CURRENT_VENUE_EVIDENCE_MAX_AGE_SECONDS)).isoformat()
    upper = current.isoformat()
    candidates: list[tuple[str, str, str]] = []
    try:
        with adapter.store._lock:
            rows = adapter.store.db.execute(
                "SELECT source,received_at FROM normalized_swaps WHERE token_mint=? AND received_at>=? AND received_at<=? "
                "ORDER BY received_at DESC,id DESC LIMIT 64",
                (token, lower, upper),
            ).fetchall()
        for raw in rows:
            venue_name = _parse_direct_venue(str(raw["source"] or ""))
            if venue_name:
                candidates.append((str(raw["received_at"]), venue_name, "normalized_swaps"))
    except Exception:
        pass
    if _table_exists(adapter.store, "semantic_candidate_events"):
        try:
            with adapter.store._lock:
                rows = adapter.store.db.execute(
                    "SELECT venue,observed_at FROM semantic_candidate_events WHERE token_mint=? AND observed_at>=? AND observed_at<=? "
                    "ORDER BY observed_at DESC LIMIT 64",
                    (token, lower, upper),
                ).fetchall()
            for raw in rows:
                venue_name = str(raw["venue"] or "").upper()
                if venue_name in _SUPPORTED_VENUES:
                    candidates.append((str(raw["observed_at"]), venue_name, "semantic_candidate_events"))
        except Exception:
            pass
    if not candidates:
        return None, ROUTER_OR_UNKNOWN_LIFECYCLE, None
    candidates.sort(key=lambda item: item[0], reverse=True)
    _when, venue_name, evidence = candidates[0]
    context_row = dict(row)
    context_row["source"] = f"solana-direct:{venue_name}:buy"
    context_row["received_at"] = upper
    try:
        lifecycle = v5._lifecycle(adapter, context_row, venue_name)
    except Exception:
        lifecycle = "current_venue_lifecycle_unresolved"
    return venue_name, lifecycle, evidence


def _risk_with_context_hazard(risk: dict[str, Any], hazard: str) -> dict[str, Any]:
    result = dict(risk)
    hazards = set(str(value) for value in result.get("hazards") or () if str(value))
    hazards.add(hazard)
    result["hazards"] = sorted(hazards)
    result["risk_signature"] = "+".join(sorted(hazards)) if hazards else "clean"
    severity = min(1.0, float(result.get("risk_severity") or 0.0) + 0.12)
    result["risk_severity"] = severity
    result["risk_severity_bin"] = (
        "low" if severity < 0.20 else "moderate" if severity < 0.45 else "high" if severity < 0.70 else "extreme"
    )
    return result


def _v5_pre_context_economic_first(
    adapter: Any,
    row: dict[str, Any],
    *,
    hard: Any,
    soft: Any,
    early_exit: float,
) -> dict[str, Any]:
    if _ORIGINAL_V5_PRE_CONTEXT is None:
        raise RuntimeError("economic-signal v5 context wrapper is not installed")
    pre = _ORIGINAL_V5_PRE_CONTEXT(adapter, row, hard=hard, soft=soft, early_exit=early_exit)
    if ROUTER_OR_UNKNOWN_VENUE not in str(row.get("source") or "").upper():
        return pre

    venue_name, lifecycle, evidence = _resolve_current_venue_context(adapter, row)
    pre["signal_origin_venue"] = ROUTER_OR_UNKNOWN_VENUE
    pre["current_venue_evidence_source"] = evidence
    pre["current_venue_resolved_independently"] = bool(venue_name)
    if venue_name:
        pre["venue"] = venue_name
        pre["lifecycle"] = lifecycle or "current_venue_lifecycle_unresolved"
    else:
        pre["venue"] = ROUTER_OR_UNKNOWN_VENUE
        pre["lifecycle"] = ROUTER_OR_UNKNOWN_LIFECYCLE
        pre["risk"] = _risk_with_context_hazard(dict(pre.get("risk") or {}), "route_venue_unresolved")
    return pre


def _risk_authority_state(adapter: Any, row: dict[str, Any]) -> tuple[bool, bool]:
    complete = bool(row.get("risk_complete"))
    fresh = complete
    token = str(row.get("token_mint") or "")
    if token and _table_exists(adapter.store, "risk_refresh_measurements"):
        try:
            with adapter.store._lock:
                latest = adapter.store.db.execute(
                    "SELECT complete,fresh FROM risk_refresh_measurements WHERE token_mint=? ORDER BY id DESC LIMIT 1",
                    (token,),
                ).fetchone()
            if latest is not None:
                complete = complete and bool(latest["complete"])
                fresh = complete and bool(latest["fresh"])
        except Exception:
            pass
    if token and _table_exists(adapter.store, "semantic_candidate_risk_state"):
        try:
            with adapter.store._lock:
                latest = adapter.store.db.execute(
                    "SELECT complete,fresh FROM semantic_candidate_risk_state WHERE token_mint=? ORDER BY assessed_at DESC LIMIT 1",
                    (token,),
                ).fetchone()
            if latest is not None:
                complete = complete and bool(latest["complete"])
                fresh = fresh and bool(latest["fresh"])
        except Exception:
            pass
    return bool(complete), bool(fresh)


async def _risk_unknown_is_not_liquidity_failure(
    self: ProfitFirstResearchAdapter,
    row: dict[str, Any],
    at: datetime,
) -> tuple[set[str], set[str], float]:
    """Keep proven mechanical failures hard while allowing unknown behavioral risk to shadow."""
    hard: set[str] = set()
    soft: set[str] = set()
    token = str(row.get("token_mint") or "")
    try:
        authority = self.store.latest_risk_evidence(token, "authority", as_of_received_at=at.isoformat())
        payload = dict(authority.get("payload") or {}) if authority else {}
        if payload.get("freeze_authority_active"):
            hard.add("authority_can_block_transfer_or_exit")
        if payload.get("mint_authority_active"):
            soft.add("mint_authority_active")
    except Exception:
        soft.add("authority_unknown")

    try:
        snapshot = await self.discovery.risk.snapshot(
            token,
            at,
            scout_wallet=str(row.get("wallet") or ""),
            scout_entity_id=self.discovery.entity_resolver.entity_id_for(
                str(row.get("wallet") or ""), fallback_entity_id=None, as_of=at
            ),
        )
    except Exception:
        snapshot = None

    if snapshot is None:
        soft.add("risk_bundle_incomplete")
        return hard, soft, 0.0
    if bool(getattr(snapshot, "unacceptable_liquidity", False)):
        hard.add("liquidity_unexitable")
    for name in (
        "bundled_launch",
        "sniper_heavy",
        "abnormal_sell_pressure",
        "common_funded_early_wallet_cluster",
        "scout_deployer_connection",
    ):
        if bool(getattr(snapshot, name, False)):
            soft.add(name)
    early_exit = 1.0 if bool(getattr(snapshot, "early_buyers_exiting", False)) else 0.0
    if early_exit:
        soft.add("early_buyers_exiting")
    if not bool(row.get("risk_complete")):
        soft.add("risk_bundle_incomplete")
    return hard, soft, early_exit


setattr(_risk_unknown_is_not_liquidity_failure, "_roi_economic_signal_continuation", True)


def _evaluation_lane(age_seconds: float) -> str:
    age = max(0.0, float(age_seconds))
    if age <= IMMEDIATE_COPY_SECONDS:
        return "immediate_copy"
    if age <= CONFIRMED_CONTINUATION_SECONDS:
        return "confirmed_continuation"
    if age <= STRONG_CONTINUATION_SECONDS:
        return "strong_continuation"
    if age <= MATURE_CONTINUATION_SECONDS:
        return "mature_continuation"
    return "fresh_signal_required"


async def _contextual_candidate_execution_worker(
    plane: DirectSolanaIngestionPlane,
    stop: asyncio.Event,
    worker_index: int,
) -> None:
    queue = candidate_plane._candidate_queue(plane)
    while not stop.is_set():
        try:
            _priority, _sequence, job = await asyncio.wait_for(queue.get(), timeout=0.25)
        except asyncio.TimeoutError:
            continue

        started_at = _utcnow()
        queue_wait_ms = max(0.0, (time.monotonic() - job.queued_monotonic) * 1000.0)
        candidate_plane._set_max(plane, "max_queue_wait_ms", queue_wait_ms)
        candidate_plane._set_active_workers(plane, candidate_plane._active_workers(plane) + 1)
        candidate_plane._inc(plane, "started")
        candidate_plane._STORAGE_PRESSURE.set()

        reason_token = candidate_plane.candidate_hotpath._CURRENT_HYDRATION_REASON.set(job.reason)
        trigger_token = candidate_plane.forward._CURRENT_TRIGGER_AT.set(job.swap.received_at)
        execution_token = candidate_plane._CANDIDATE_EXECUTION_CONTEXT.set(True)
        decision: IngestionDecision | None = None
        timed_out = False
        error_type: str | None = None
        try:
            age_seconds = max(0.0, (started_at - job.swap.observed_at).total_seconds())
            lane = _evaluation_lane(age_seconds)
            _inc(plane, f"lane_{lane}")
            if lane == "fresh_signal_required":
                decision = IngestionDecision(
                    signature=job.swap.signature,
                    token_mint=job.swap.token_mint,
                    wallet=job.swap.wallet,
                    decision="candidate_fresh_signal_required",
                    reason="original scout signal is older than five minutes; durable watch retained and a new prospective signal is required",
                    observed_at=job.swap.observed_at,
                    ingestion_latency_ms=job.swap.ingestion_latency_ms,
                )
                _inc(plane, "fresh_signal_required")
            elif candidate_plane._ORIGINAL_SERVICE_INGEST is None:
                error_type = "candidate_execution_delegate_unavailable"
                candidate_plane._inc(plane, "errors")
            else:
                try:
                    with candidate_plane.governor.rpc_workload(candidate_plane.candidate_priority.WORKLOAD_CANDIDATE):
                        decision = await asyncio.wait_for(
                            candidate_plane._ORIGINAL_SERVICE_INGEST(plane.service, job.swap),
                            timeout=CANDIDATE_OPERATIONAL_TIMEOUT_SECONDS,
                        )
                    candidate_plane._inc(plane, "completed")
                except asyncio.TimeoutError:
                    timed_out = True
                    error_type = "candidate_operational_timeout"
                    candidate_plane._inc(plane, "hard_timeouts")
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    error_type = type(exc).__name__
                    candidate_plane._inc(plane, "errors")

            completed_at = _utcnow()
            candidate_plane._set_max(
                plane,
                "max_end_to_end_ms",
                max(0.0, (completed_at - job.swap.observed_at).total_seconds() * 1000.0),
            )
            if timed_out or error_type is not None:
                try:
                    await asyncio.to_thread(
                        candidate_plane._record_terminal_failure_sync,
                        plane,
                        job,
                        started_at=started_at,
                        completed_at=completed_at,
                        failure_type=error_type or "candidate_execution_failed",
                    )
                    candidate_plane._inc(plane, "terminal_failures_accounted")
                except asyncio.CancelledError:
                    raise
                except Exception:
                    candidate_plane._inc(plane, "terminal_failure_accounting_errors")

            candidate_plane._schedule_snapshot(
                plane,
                job,
                started_at=started_at,
                completed_at=completed_at,
                queue_wait_ms=queue_wait_ms,
                decision=decision,
                timed_out=timed_out,
                error_type=error_type,
            )
        finally:
            candidate_plane._CANDIDATE_EXECUTION_CONTEXT.reset(execution_token)
            candidate_plane.forward._CURRENT_TRIGGER_AT.reset(trigger_token)
            candidate_plane.candidate_hotpath._CURRENT_HYDRATION_REASON.reset(reason_token)
            candidate_plane._set_active_workers(plane, candidate_plane._active_workers(plane) - 1)
            queue.task_done()
            candidate_plane._maybe_clear_storage_pressure(plane)


setattr(_contextual_candidate_execution_worker, "_roi_economic_signal_continuation", True)


async def _risk_refresh_context_not_expiration(
    self: TimedRiskCollectors,
    mint: str,
    at: datetime,
    *,
    current_swap: Any = None,
) -> None:
    if _ORIGINAL_TIMED_RISK_REFRESH is None:
        raise RuntimeError("contextual risk refresh is not installed")
    if not risk_window._eligible(self, current_swap):
        await _ORIGINAL_TIMED_RISK_REFRESH(self, mint, at, current_swap=current_swap)
        return

    trigger_observed_at = getattr(current_swap, "observed_at", at)
    trigger_received_at = getattr(current_swap, "received_at", at)
    started_at = self.now_fn()
    started_perf = self.perf_fn()
    ingestion_latency_ms = float(getattr(current_swap, "ingestion_latency_ms", 0.0) or 0.0)
    target_exceeded = (started_at - trigger_observed_at).total_seconds() > risk_window.CANDIDATE_PROCESSING_TARGET_SECONDS
    if target_exceeded:
        risk_window._inc(self, "processing_target_exceeded")

    unexpected_error: str | None = None
    operational_timeout = False
    try:
        await asyncio.wait_for(
            asyncio.gather(
                self.inner.refresh_coverage(mint, started_at, current_swap=current_swap),
                self.inner.refresh_candidate(mint, started_at, current_swap=current_swap),
            ),
            timeout=RISK_COLLECTION_OPERATIONAL_TIMEOUT_SECONDS,
        )
    except asyncio.TimeoutError:
        operational_timeout = True
        _inc(self, "risk_collection_operational_timeouts")
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        unexpected_error = f"{type(exc).__name__}:{str(exc)[:300]}"
        risk_window._inc(self, "unexpected_errors")

    completed_at = self.now_fn()
    try:
        raw = self.risk.readiness(mint, as_of=completed_at)
        readiness = dict(raw) if isinstance(raw, dict) else {"complete": False, "fresh": False}
    except Exception:
        readiness = {"complete": False, "fresh": False}
    end_to_end_ms = max(0.0, (completed_at - trigger_observed_at).total_seconds() * 1000.0)
    target_exceeded = target_exceeded or end_to_end_ms > risk_window.CANDIDATE_PROCESSING_TARGET_SECONDS * 1000.0
    readiness["candidate_processing_target_exceeded"] = bool(target_exceeded)
    readiness["candidate_processing_target_seconds"] = risk_window.CANDIDATE_PROCESSING_TARGET_SECONDS
    readiness["candidate_processing_target_is_not_entry_authority"] = True
    readiness["candidate_20s_is_immediate_copy_lane_not_expiration"] = True
    readiness["nonmechanical_incomplete_risk_shadow_eligible"] = not bool(readiness.get("complete") and readiness.get("fresh"))
    readiness["risk_collection_operational_timeout_seconds"] = RISK_COLLECTION_OPERATIONAL_TIMEOUT_SECONDS
    if operational_timeout:
        readiness["risk_collection_operational_timeout"] = True
    if unexpected_error:
        readiness["candidate_risk_collection_error"] = unexpected_error

    complete = bool(readiness.get("complete")) and bool(readiness.get("fresh"))
    fresh = bool(readiness.get("fresh"))
    if complete and target_exceeded:
        risk_window._inc(self, "late_but_complete")
    self.store.record_risk_refresh(
        token_mint=mint,
        trigger_observed_at=trigger_observed_at.isoformat(),
        trigger_received_at=trigger_received_at.isoformat(),
        started_at=started_at.isoformat(),
        completed_at=completed_at.isoformat(),
        elapsed_ms=max(0.0, (self.perf_fn() - started_perf) * 1000.0),
        ingestion_latency_ms=ingestion_latency_ms,
        end_to_end_ms=end_to_end_ms,
        complete=complete,
        fresh=fresh,
        readiness=readiness,
    )
    risk_window._inc(self, "measurements_recorded")


setattr(_risk_refresh_context_not_expiration, "_roi_economic_signal_continuation", True)


async def _v51_exact_sizing_defer_to_continuation(adapter: Any, row: dict[str, Any]) -> None:
    if _ORIGINAL_V51_EXACT_SIZING is None:
        raise RuntimeError("v5.1 exact-sizing continuation delegation is not installed")
    try:
        latency = float(row.get("observation_lag_ms") or 0.0) / 1000.0
    except (TypeError, ValueError):
        latency = 0.0
    try:
        chase = float(row.get("chase_fraction")) if row.get("chase_fraction") is not None else None
    except (TypeError, ValueError):
        chase = None
    if latency > IMMEDIATE_COPY_SECONDS or (chase is not None and math.isfinite(chase) and chase > 0.40):
        _inc(adapter, "v51_exact_sizing_delegated_to_continuation")
        return
    await _ORIGINAL_V51_EXACT_SIZING(adapter, row)


setattr(_v51_exact_sizing_defer_to_continuation, "_roi_economic_signal_continuation", True)


def _max_probe_fraction(adapter: Any, signature: str) -> float:
    try:
        with adapter.store._lock:
            row = adapter.store.db.execute(
                "SELECT MAX(position_fraction) FROM risk_conditioned_alpha_v5_trials WHERE release_commit=? AND source_signature=?",
                (adapter.release_commit, signature),
            ).fetchone()
        return max(0.0, float(row[0] or 0.0)) if row is not None else 0.0
    except Exception:
        return 0.0


def _enforce_zero_allocation_shadow(
    adapter: Any,
    row: dict[str, Any],
    *,
    reason: str,
    current_venue: str | None,
    lifecycle: str | None,
    risk_complete: bool,
    risk_fresh: bool,
) -> None:
    signature = str(row.get("signature") or "")
    if not signature:
        return
    _shadow_schema(adapter)
    probe_fraction = _max_probe_fraction(adapter, signature)
    now = _utcnow().isoformat()
    shadow_decision = "paper_enter_shadow_zero_allocation"
    with adapter.store._lock, adapter.store.db:
        adapter.store.db.execute(
            "UPDATE risk_conditioned_alpha_v5_trials SET selected=0,decision=?,decision_reason=?,position_fraction=0 "
            "WHERE release_commit=? AND source_signature=? AND decision LIKE 'paper_enter%'",
            (shadow_decision, f"zero_allocation_shadow:{reason}", adapter.release_commit, signature),
        )
        adapter.store.db.execute(
            "UPDATE profit_first_final_trials SET assigned_position_fraction=0 "
            "WHERE epoch_id=? AND source_signature=?",
            (adapter.epoch_id, signature),
        )
        if _table_exists(adapter.store, "continuation_recalibration_audit"):
            adapter.store.db.execute(
                "UPDATE continuation_recalibration_audit SET decision=?,reason=?,position_fraction=0 "
                "WHERE release_commit=? AND source_signature=?",
                ("paper_shadow_zero_allocation", reason, adapter.release_commit, signature),
            )
        adapter.store.db.execute(
            "INSERT OR REPLACE INTO economic_signal_shadow_audit("
            "release_commit,source_signature,token_mint,signal_origin_venue,resolved_current_venue,resolved_lifecycle,"
            "risk_complete,risk_fresh,reason,probe_position_fraction,zero_allocation,created_at,paper_only,live_money_authority) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,1,?,1,0)",
            (
                adapter.release_commit,
                signature,
                str(row.get("token_mint") or ""),
                ROUTER_OR_UNKNOWN_VENUE if ROUTER_OR_UNKNOWN_VENUE in str(row.get("source") or "").upper() else (_parse_direct_venue(str(row.get("source") or "")) or "UNKNOWN"),
                current_venue,
                lifecycle,
                1 if risk_complete else 0,
                1 if risk_fresh else 0,
                reason,
                probe_fraction,
                now,
            ),
        )
    _inc(adapter, "zero_allocation_shadows")


async def _buy_with_final_shadow_authority(self: Any, row: dict[str, Any]) -> None:
    if _ORIGINAL_FINAL_BUY is None:
        raise RuntimeError("economic-signal final buy authority is not installed")
    await _ORIGINAL_FINAL_BUY(self, row)
    if str(row.get("side") or "").lower() != "buy":
        return

    complete, fresh = _risk_authority_state(self, row)
    router_origin = ROUTER_OR_UNKNOWN_VENUE in str(row.get("source") or "").upper()
    current_venue: str | None = None
    lifecycle: str | None = None
    if router_origin:
        current_venue, lifecycle, _evidence = _resolve_current_venue_context(self, row)

    reason: str | None = None
    if not complete or not fresh:
        reason = "nonmechanical_risk_bundle_incomplete_or_stale"
    if router_origin and current_venue is None:
        reason = "router_signal_current_direct_venue_unresolved"
    if reason is None:
        _inc(self, "fully_authorized_contexts")
        return

    _enforce_zero_allocation_shadow(
        self,
        row,
        reason=reason,
        current_venue=current_venue,
        lifecycle=lifecycle,
        risk_complete=complete,
        risk_fresh=fresh,
    )


setattr(_buy_with_final_shadow_authority, "_roi_economic_signal_continuation", True)


def _economic_counts(plane: Any) -> dict[str, int]:
    try:
        _economic_schema(plane)
        with plane.store._lock:
            total = int(plane.store.db.execute("SELECT COUNT(*) FROM scout_economic_movement_observations").fetchone()[0])
            priced = int(plane.store.db.execute(
                "SELECT COUNT(*) FROM scout_economic_movement_observations WHERE native_amount_sol IS NOT NULL AND native_amount_sol>0"
            ).fetchone()[0])
        return {"durable_economic_observations": total, "priced_economic_observations": priced}
    except Exception:
        return {"durable_economic_observations": 0, "priced_economic_observations": 0}


def _direct_status_with_economic_signal(self: Any) -> dict[str, Any]:
    if _ORIGINAL_DIRECT_STATUS is None:
        raise RuntimeError("economic-signal direct status is not installed")
    payload = _ORIGINAL_DIRECT_STATUS(self)
    counts = _economic_counts(self)
    payload["economic_signal_continuation_repair"] = {
        "installed": True,
        "version": REPAIR_VERSION,
        "attribution_version": ATTRIBUTION_VERSION,
        "risk_policy_version": RISK_POLICY_VERSION,
        "candidate_attribution_order": [
            "prove_economic_token_movement_and_direction",
            "identify_token_and_actor",
            "resolve_direct_or_underlying_venue_when_proven",
            "retain_router_or_unknown_venue_observation_when_unresolved",
            "resolve_current_executable_venue_and_lifecycle_independently",
            "evaluate_v5.1_context",
        ],
        "router_or_unknown_observations_have_entry_authority": False,
        "current_venue_resolution_uses_original_router_as_authority": False,
        "direct_supported_venue_path_preserved": True,
        "economic_observations_session": int(getattr(self, "_roi_economic_signal_economic_observations", 0) or 0),
        "router_unknown_normalized_session": int(getattr(self, "_roi_economic_signal_router_unknown_normalized", 0) or 0),
        "economic_price_unresolved_session": int(getattr(self, "_roi_economic_signal_economic_price_unresolved", 0) or 0),
        "economic_unresolved_session": int(getattr(self, "_roi_economic_signal_economic_unresolved", 0) or 0),
        **counts,
        "time_policy": {
            "0_20s": "immediate_copy",
            "20_60s": "confirmed_continuation",
            "60_120s": "strong_continuation_reduced_sizing",
            "120_300s": "mature_continuation_fail_small",
            "gt_300s": "fresh_signal_required",
            "twenty_seconds_is_hard_expiration": False,
            "five_second_processing_target_is_entry_authority": False,
            "candidate_operational_timeout_seconds": CANDIDATE_OPERATIONAL_TIMEOUT_SECONDS,
        },
        "chase_policy": {
            "fifteen_percent_is_hard_veto": False,
            "bands": ["0_15pct", "15_25pct", "25_40pct", "40_75pct", "75_125pct", "gt_125pct"],
            "high_chase_requires_residual_continuation_confirmation": True,
            "sizing_reduces_as_chase_increases": True,
        },
        "risk_policy": {
            "mechanical_hard_stops": sorted(_MECHANICAL_HARD_STOPS),
            "missing_nonmechanical_risk_is_liquidity_failure": False,
            "incomplete_nonmechanical_risk_can_quote_and_shadow": True,
            "full_fresh_risk_required_for_nonzero_paper_allocation": True,
            "zero_allocation_shadow_outcomes_can_feed_learning": True,
            "certification_thresholds_changed": False,
        },
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }
    candidate = payload.get("candidate_execution_evidence_plane")
    if isinstance(candidate, dict):
        candidate.update(
            {
                "immediate_copy_lane_seconds": IMMEDIATE_COPY_SECONDS,
                "candidate_entry_window_is_hard_strategy_expiration": False,
                "operational_timeout_seconds": CANDIDATE_OPERATIONAL_TIMEOUT_SECONDS,
                "fresh_signal_required_after_seconds": MATURE_CONTINUATION_SECONDS,
            }
        )
    risk_payload = payload.get("candidate_risk_window")
    if isinstance(risk_payload, dict):
        risk_payload.update(
            {
                "entry_window_seconds": IMMEDIATE_COPY_SECONDS,
                "entry_window_is_hard_strategy_expiration": False,
                "risk_collection_operational_timeout_seconds": RISK_COLLECTION_OPERATIONAL_TIMEOUT_SECONDS,
                "incomplete_nonmechanical_risk_can_continue_to_shadow_learning": True,
            }
        )
    policy = payload.setdefault("provider_runtime_policy", {})
    if isinstance(policy, dict):
        policy.update(
            {
                "candidate_economic_movement_precedes_venue_authority": True,
                "router_or_unknown_venue_observation_preserved": True,
                "current_executable_venue_resolved_independently": True,
                "candidate_20s_hard_expiration_removed": True,
                "candidate_15pct_chase_hard_veto_removed": True,
                "nonmechanical_risk_incomplete_shadow_learning_allowed": True,
                "mechanical_hard_stops_preserved": True,
                "full_raw_market_scope_preserved": True,
                "paper_only_authority_unchanged": True,
                "signing_or_submission_available": False,
            }
        )
    return payload


setattr(_direct_status_with_economic_signal, "_roi_economic_signal_continuation", True)


def _final_status_with_economic_signal(self: Any) -> dict[str, Any]:
    if _ORIGINAL_FINAL_STATUS is None:
        raise RuntimeError("economic-signal final status is not installed")
    payload = _ORIGINAL_FINAL_STATUS(self)
    shadow_rows = 0
    try:
        _shadow_schema(self)
        with self.store._lock:
            shadow_rows = int(self.store.db.execute(
                "SELECT COUNT(*) FROM economic_signal_shadow_audit WHERE release_commit=?",
                (self.release_commit,),
            ).fetchone()[0])
    except Exception:
        pass
    payload["economic_signal_continuation_strategy"] = {
        "version": REPAIR_VERSION,
        "final_strategy_entry_window_seconds": None,
        "twenty_seconds_is_immediate_copy_lane_only": True,
        "fifteen_percent_chase_is_context_band_not_veto": True,
        "v5.1_exact_sizing_delegates_extended_context_to_continuation": True,
        "router_signal_requires_independent_current_venue_for_nonzero_allocation": True,
        "nonmechanical_risk_incomplete_can_shadow": True,
        "mechanical_hard_stops_preserved": True,
        "zero_allocation_shadow_rows": shadow_rows,
        "zero_allocation_shadows_session": int(getattr(self, "_roi_economic_signal_zero_allocation_shadows", 0) or 0),
        "fully_authorized_contexts_session": int(getattr(self, "_roi_economic_signal_fully_authorized_contexts", 0) or 0),
        "v51_extended_delegations_session": int(getattr(self, "_roi_economic_signal_v51_exact_sizing_delegated_to_continuation", 0) or 0),
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }
    return payload


setattr(_final_status_with_economic_signal, "_roi_economic_signal_continuation", True)


def install_economic_signal_continuation_repair() -> None:
    """Install the final Solana/FOMO candidate authority after PR165/continuation composition."""
    global _INSTALLED, _ORIGINAL_NORMALIZER, _ORIGINAL_V5_PRE_CONTEXT, _ORIGINAL_RESEARCH_RISK
    global _ORIGINAL_FINAL_BUY, _ORIGINAL_FINAL_STATUS, _ORIGINAL_DIRECT_STATUS
    global _ORIGINAL_TIMED_RISK_REFRESH, _ORIGINAL_V51_EXACT_SIZING
    if _INSTALLED:
        return

    current_normalizer = scout._normalize_tracked_wallet
    if not bool(getattr(current_normalizer, "_roi_economic_signal_continuation", False)):
        _ORIGINAL_NORMALIZER = current_normalizer
        try:
            _economic_signal_normalizer.__dict__.update(getattr(current_normalizer, "__dict__", {}))
        except Exception:
            pass
        scout._normalize_tracked_wallet = _economic_signal_normalizer  # type: ignore[assignment]

    _ORIGINAL_V5_PRE_CONTEXT = v5._v5_pre_context
    v5._v5_pre_context = _v5_pre_context_economic_first  # type: ignore[assignment]

    _ORIGINAL_RESEARCH_RISK = ProfitFirstResearchAdapter._risk
    ProfitFirstResearchAdapter._risk = _risk_unknown_is_not_liquidity_failure  # type: ignore[method-assign]

    _ORIGINAL_TIMED_RISK_REFRESH = TimedRiskCollectors.refresh
    TimedRiskCollectors.refresh = _risk_refresh_context_not_expiration  # type: ignore[method-assign]

    # The candidate runner now treats time as a strategy context. Twenty seconds is
    # the immediate-copy lane boundary, not the worker's remaining lifetime.
    candidate_plane._candidate_execution_worker = _contextual_candidate_execution_worker  # type: ignore[assignment]

    _ORIGINAL_V51_EXACT_SIZING = v51._repair_exact_sizing
    v51._repair_exact_sizing = _v51_exact_sizing_defer_to_continuation  # type: ignore[assignment]

    current_buy = FinalProfitFirstResearchAdapter._buy
    _ORIGINAL_FINAL_BUY = current_buy
    wrapped_buy = wraps(current_buy)(_buy_with_final_shadow_authority)
    try:
        wrapped_buy.__dict__.update(getattr(current_buy, "__dict__", {}))
    except Exception:
        pass
    setattr(wrapped_buy, "_roi_economic_signal_continuation", True)
    FinalProfitFirstResearchAdapter._buy = wrapped_buy  # type: ignore[method-assign]

    current_final_status = FinalProfitFirstResearchAdapter.status
    _ORIGINAL_FINAL_STATUS = current_final_status
    wrapped_final_status = wraps(current_final_status)(_final_status_with_economic_signal)
    try:
        wrapped_final_status.__dict__.update(getattr(current_final_status, "__dict__", {}))
    except Exception:
        pass
    setattr(wrapped_final_status, "_roi_economic_signal_continuation", True)
    FinalProfitFirstResearchAdapter.status = wrapped_final_status  # type: ignore[method-assign]

    current_direct_status = DirectSolanaIngestionPlane.status
    _ORIGINAL_DIRECT_STATUS = current_direct_status
    wrapped_direct_status = wraps(current_direct_status)(_direct_status_with_economic_signal)
    try:
        wrapped_direct_status.__dict__.update(getattr(current_direct_status, "__dict__", {}))
    except Exception:
        pass
    setattr(wrapped_direct_status, "_roi_economic_signal_continuation", True)
    DirectSolanaIngestionPlane.status = wrapped_direct_status  # type: ignore[method-assign]

    _INSTALLED = True


__all__ = [
    "ATTRIBUTION_VERSION",
    "CANDIDATE_OPERATIONAL_TIMEOUT_SECONDS",
    "CONFIRMED_CONTINUATION_SECONDS",
    "IMMEDIATE_COPY_SECONDS",
    "MATURE_CONTINUATION_SECONDS",
    "REPAIR_VERSION",
    "RISK_POLICY_VERSION",
    "ROUTER_OR_UNKNOWN_LIFECYCLE",
    "ROUTER_OR_UNKNOWN_VENUE",
    "STRONG_CONTINUATION_SECONDS",
    "_economic_movement",
    "_evaluation_lane",
    "_resolve_current_venue_context",
    "install_economic_signal_continuation_repair",
]
