from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import time
from collections import defaultdict, deque
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

from .observation import WSOL_MINT


EXECUTION_MODEL_EPOCH = "v51-execution-model-exact-exit-v2"
EXIT_EXECUTION_VERSION = "v51-exact-exit-execution-integrity-109-113-v1"
EXIT_RETRY_DELAYS_SECONDS = (0.0, 1.0, 2.0)
TERMINAL_LIQUIDATION_ASSUMPTION = "unliquidated_zero_recovery_for_risk_reporting_only_no_synthetic_fill"

_ORIGINAL_ROUTE: Callable[..., Any] | None = None
_ORIGINAL_SELL: Callable[..., Any] | None = None
_ORIGINAL_ANALYTICS_PROMOTION_RECORDS: Callable[..., Any] | None = None
_ORIGINAL_FILTER_SOLANA: Callable[..., Any] | None = None
_ORIGINAL_FILTER_FOMO: Callable[..., Any] | None = None
_INSTALLED = False


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def execution_model_fingerprint() -> str:
    payload = {
        "execution_model_epoch": EXECUTION_MODEL_EPOCH,
        "solana_entry": "amount_specific_jupiter_v2_order_unsigned_mainnet_simulation",
        "solana_exit": "actual_held_raw_amount_jupiter_v2_order_unsigned_mainnet_simulation",
        "exit_quote_fresh_at_each_due_exit": True,
        "exit_quote_amount_equals_actual_position_raw": True,
        "entry_route_reuse_for_exit": False,
        "synthetic_fixed_drag_exit_fill": False,
        "failed_exit_state": "paper_exit_execution_failed",
        "deterministic_retry_delays_seconds": list(EXIT_RETRY_DELAYS_SECONDS),
        "terminal_liquidation_assumption": TERMINAL_LIQUIDATION_ASSUMPTION,
        "live_submission": False,
        "signing": False,
    }
    return hashlib.sha256(_json(payload).encode()).hexdigest()


def _table_exists(store: Any, table: str) -> bool:
    try:
        with store._lock:
            return store.db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (table,)
            ).fetchone() is not None
    except Exception:
        return False


def ensure_schema(store: Any) -> None:
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS v51_exact_exit_attempts ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,release_commit TEXT NOT NULL,execution_model_epoch TEXT NOT NULL,"
            "source_signature TEXT NOT NULL,due_exit_signature TEXT NOT NULL,token_mint TEXT NOT NULL,"
            "actual_position_raw INTEGER NOT NULL,exit_quote_amount_raw INTEGER NOT NULL,exact_amount_match INTEGER NOT NULL,"
            "attempt_number INTEGER NOT NULL,first_exit_due_at TEXT NOT NULL,requested_at TEXT NOT NULL,completed_at TEXT NOT NULL,"
            "router TEXT,expected_output_raw INTEGER,minimum_output_raw INTEGER,route_hops_json TEXT,price_impact_fraction REAL,"
            "quote_age_ms REAL,token_account_requirements_json TEXT,transaction_built INTEGER NOT NULL,"
            "transaction_sha256 TEXT,transaction_size_bytes INTEGER,last_valid_block_height INTEGER,"
            "simulation_ok INTEGER NOT NULL,simulation_error_class TEXT,simulation_error TEXT,units_consumed INTEGER,"
            "simulation_slot INTEGER,route_valid INTEGER NOT NULL,token_restriction INTEGER NOT NULL,"
            "account_failure INTEGER NOT NULL,transfer_failure INTEGER NOT NULL,status TEXT NOT NULL,"
            "order_json TEXT,paper_only INTEGER NOT NULL,live_money_authority INTEGER NOT NULL,"
            "UNIQUE(release_commit,source_signature,due_exit_signature,attempt_number))"
        )
        store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_v51_exact_exit_attempts_position "
            "ON v51_exact_exit_attempts(release_commit,source_signature,id)"
        )
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS v51_exact_exit_state ("
            "release_commit TEXT NOT NULL,source_signature TEXT NOT NULL,execution_model_epoch TEXT NOT NULL,"
            "token_mint TEXT NOT NULL,actual_position_raw INTEGER NOT NULL,first_exit_due_at TEXT NOT NULL,"
            "retry_attempts INTEGER NOT NULL,route_ever_available INTEGER NOT NULL,last_due_exit_signature TEXT NOT NULL,"
            "last_status TEXT NOT NULL,last_attempt_at TEXT NOT NULL,eventual_exit_at TEXT,"
            "eventual_exit_output_raw INTEGER,terminal_liquidation_assumption TEXT,"
            "paper_only INTEGER NOT NULL,live_money_authority INTEGER NOT NULL,"
            "PRIMARY KEY(release_commit,source_signature))"
        )


def _release_epoch_map(store: Any) -> dict[str, str]:
    if not _table_exists(store, "v51_release_compatibility"):
        return {}
    with store._lock:
        rows = store.db.execute(
            "SELECT release_commit,execution_model_epoch FROM v51_release_compatibility"
        ).fetchall()
    return {str(row["release_commit"] or ""): str(row["execution_model_epoch"] or "") for row in rows}


def _current_epoch_promotion_records(store: Any) -> list[dict[str, Any]]:
    if _ORIGINAL_ANALYTICS_PROMOTION_RECORDS is None:
        raise RuntimeError("exact-exit promotion filter not installed")
    rows = list(_ORIGINAL_ANALYTICS_PROMOTION_RECORDS(store))
    epochs = _release_epoch_map(store)
    selected: list[dict[str, Any]] = []
    for raw in rows:
        row = dict(raw)
        release = str(row.get("release_commit") or "")
        surface = str(row.get("surface") or "")
        # Exact-exit semantics changed the Solana/FOMO measurement model. Old
        # release rows remain visible through audit surfaces but cannot promote.
        if surface in {"SOLANA", "SOLANA_ALPHA", "FOMO"}:
            actual_epoch = epochs.get(release, "")
            if actual_epoch != EXECUTION_MODEL_EPOCH:
                continue
            row["execution_model_epoch"] = actual_epoch
        selected.append(row)
    return selected


def _simulation_error_flags(error: Any) -> tuple[str | None, bool, bool, bool]:
    if error is None:
        return None, False, False, False
    text = _json(error) if not isinstance(error, str) else error
    lowered = text.lower()
    token_restriction = any(key in lowered for key in ("frozen", "token", "mint", "owner mismatch"))
    account_failure = any(key in lowered for key in ("account", "insufficientfunds", "insufficient funds"))
    transfer_failure = any(key in lowered for key in ("transfer", "custom program error", "instructionerror"))
    if token_restriction:
        kind = "token_restriction"
    elif account_failure:
        kind = "account_failure"
    elif transfer_failure:
        kind = "transfer_failure"
    else:
        kind = "simulation_failure"
    return kind, token_restriction, account_failure, transfer_failure


@dataclass
class ExitContext:
    adapter: Any
    row: dict[str, Any]
    positions_by_amount: dict[int, deque[str]] = field(default_factory=dict)
    first_due_by_source: dict[str, str] = field(default_factory=dict)


_CURRENT_EXIT: ContextVar[ExitContext | None] = ContextVar("v51_current_exact_exit", default=None)


def _pending_exit_context(adapter: Any, row: dict[str, Any]) -> ExitContext:
    token = str(row.get("token_mint") or "")
    observed = str(row.get("observed_at") or row.get("received_at") or _utcnow())
    ensure_schema(adapter.store)
    positions: dict[int, deque[str]] = defaultdict(deque)
    first_due: dict[str, str] = {}
    if _table_exists(adapter.store, "profit_first_final_trials") and _table_exists(adapter.store, "profit_first_final_outcomes"):
        with adapter.store._lock:
            rows = adapter.store.db.execute(
                "SELECT t.source_signature,MAX(COALESCE(t.entry_token_raw,0)) AS entry_token_raw,MIN(t.id) AS first_id "
                "FROM profit_first_final_trials t LEFT JOIN profit_first_final_outcomes o ON "
                "o.epoch_id=t.epoch_id AND o.source_signature=t.source_signature AND o.lane=t.lane "
                "WHERE t.epoch_id=? AND t.token_mint=? AND t.entry_executable=1 AND t.exit_executable=1 "
                "AND t.observed_at<? AND o.id IS NULL GROUP BY t.source_signature ORDER BY first_id",
                (adapter.epoch_id, token, observed),
            ).fetchall()
            state_rows = adapter.store.db.execute(
                "SELECT source_signature,first_exit_due_at FROM v51_exact_exit_state WHERE release_commit=? AND token_mint=?",
                (adapter.release_commit, token),
            ).fetchall()
        for state in state_rows:
            first_due[str(state["source_signature"])] = str(state["first_exit_due_at"])
        for item in rows:
            amount = int(item["entry_token_raw"] or 0)
            signature = str(item["source_signature"] or "")
            if amount > 0 and signature:
                positions[amount].append(signature)
                first_due.setdefault(signature, observed)
    return ExitContext(adapter=adapter, row=dict(row), positions_by_amount=dict(positions), first_due_by_source=first_due)


def _persist_attempt(
    adapter: Any,
    *,
    source_signature: str,
    due_exit_signature: str,
    token_mint: str,
    actual_position_raw: int,
    attempt_number: int,
    first_exit_due_at: str,
    requested_at: str,
    completed_at: str,
    order: dict[str, Any],
    transaction_built: bool,
    transaction_sha256: str | None,
    transaction_size_bytes: int | None,
    simulation_ok: bool,
    simulation_error: Any,
    units_consumed: int | None,
    simulation_slot: int | None,
    status: str,
    quote_age_ms: float,
) -> None:
    ensure_schema(adapter.store)
    router = str(order.get("router") or "unknown") if order else None
    out_raw = int(order["outAmount"]) if order.get("outAmount") is not None else None
    minimum_raw = None
    for key in ("otherAmountThreshold", "minimumOutAmount", "minOutAmount"):
        if order.get(key) is not None:
            try:
                minimum_raw = int(order[key])
            except (TypeError, ValueError):
                minimum_raw = None
            break
    raw_input = order.get("inAmount", order.get("inputAmount")) if order else None
    response_input = actual_position_raw
    if raw_input is not None:
        try:
            response_input = int(raw_input)
        except (TypeError, ValueError):
            response_input = -1
    exact = response_input == actual_position_raw
    route_plan = order.get("routePlan") if isinstance(order.get("routePlan"), list) else []
    price_impact = None
    for key in ("priceImpactPct", "priceImpact", "priceImpactFraction"):
        if order.get(key) is not None:
            try:
                value = float(order[key])
                price_impact = value / 100.0 if key == "priceImpactPct" and value > 1.0 else value
            except (TypeError, ValueError):
                pass
            break
    requirements = {
        "taker": os.getenv("SOLANA_ROI_SHADOW_WALLET_PUBLIC_KEY", "").strip() or None,
        "reported": order.get("tokenAccountRequirements"),
        "setup_instructions_reported": bool(order.get("setupInstructions")),
    }
    error_class, token_restriction, account_failure, transfer_failure = _simulation_error_flags(simulation_error)
    route_valid = bool(order and out_raw is not None and out_raw > 0 and exact)
    last_valid = None
    if order.get("lastValidBlockHeight") is not None:
        try:
            last_valid = int(order["lastValidBlockHeight"])
        except (TypeError, ValueError):
            pass
    with adapter.store._lock, adapter.store.db:
        adapter.store.db.execute(
            "INSERT OR REPLACE INTO v51_exact_exit_attempts("
            "release_commit,execution_model_epoch,source_signature,due_exit_signature,token_mint,actual_position_raw,"
            "exit_quote_amount_raw,exact_amount_match,attempt_number,first_exit_due_at,requested_at,completed_at,router,"
            "expected_output_raw,minimum_output_raw,route_hops_json,price_impact_fraction,quote_age_ms,"
            "token_account_requirements_json,transaction_built,transaction_sha256,transaction_size_bytes,last_valid_block_height,"
            "simulation_ok,simulation_error_class,simulation_error,units_consumed,simulation_slot,route_valid,token_restriction,"
            "account_failure,transfer_failure,status,order_json,paper_only,live_money_authority) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0)",
            (
                adapter.release_commit,EXECUTION_MODEL_EPOCH,source_signature,due_exit_signature,token_mint,actual_position_raw,
                actual_position_raw,1 if exact else 0,attempt_number,first_exit_due_at,requested_at,completed_at,router,
                out_raw,minimum_raw,_json(route_plan),price_impact,quote_age_ms,_json(requirements),1 if transaction_built else 0,
                transaction_sha256,transaction_size_bytes,last_valid,1 if simulation_ok else 0,error_class,
                None if simulation_error is None else _json(simulation_error)[:2000],units_consumed,simulation_slot,
                1 if route_valid else 0,1 if token_restriction else 0,1 if account_failure else 0,
                1 if transfer_failure else 0,status,_json(order)[:20000] if order else None,
            ),
        )
        previous = adapter.store.db.execute(
            "SELECT retry_attempts,route_ever_available,first_exit_due_at FROM v51_exact_exit_state "
            "WHERE release_commit=? AND source_signature=?",
            (adapter.release_commit, source_signature),
        ).fetchone()
        attempts = int(previous["retry_attempts"] or 0) + 1 if previous is not None else 1
        route_ever = bool(previous["route_ever_available"]) if previous is not None else False
        route_ever = route_ever or route_valid
        first_due = str(previous["first_exit_due_at"]) if previous is not None else first_exit_due_at
        successful = status == "paper_exit_executed_exact"
        terminal = None if successful else TERMINAL_LIQUIDATION_ASSUMPTION
        adapter.store.db.execute(
            "INSERT INTO v51_exact_exit_state("
            "release_commit,source_signature,execution_model_epoch,token_mint,actual_position_raw,first_exit_due_at,retry_attempts,"
            "route_ever_available,last_due_exit_signature,last_status,last_attempt_at,eventual_exit_at,eventual_exit_output_raw,"
            "terminal_liquidation_assumption,paper_only,live_money_authority) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0) "
            "ON CONFLICT(release_commit,source_signature) DO UPDATE SET "
            "execution_model_epoch=excluded.execution_model_epoch,actual_position_raw=excluded.actual_position_raw,"
            "retry_attempts=excluded.retry_attempts,route_ever_available=excluded.route_ever_available,"
            "last_due_exit_signature=excluded.last_due_exit_signature,last_status=excluded.last_status,last_attempt_at=excluded.last_attempt_at,"
            "eventual_exit_at=COALESCE(excluded.eventual_exit_at,v51_exact_exit_state.eventual_exit_at),"
            "eventual_exit_output_raw=COALESCE(excluded.eventual_exit_output_raw,v51_exact_exit_state.eventual_exit_output_raw),"
            "terminal_liquidation_assumption=excluded.terminal_liquidation_assumption,paper_only=1,live_money_authority=0",
            (
                adapter.release_commit,source_signature,EXECUTION_MODEL_EPOCH,token_mint,actual_position_raw,first_due,attempts,
                1 if route_ever else 0,due_exit_signature,status,completed_at,completed_at if successful else None,
                out_raw if successful else None,terminal,
            ),
        )


async def _exact_exit_route(adapter: Any, ctx: ExitContext, input_mint: str, output_mint: str, amount: int) -> dict[str, int] | None:
    candidates = ctx.positions_by_amount.get(int(amount))
    if not candidates:
        return None
    source_signature = candidates.popleft()
    due_exit_signature = str(ctx.row.get("signature") or "unknown-exit")
    due_at = str(ctx.row.get("observed_at") or ctx.row.get("received_at") or _utcnow())
    first_due = ctx.first_due_by_source.get(source_signature, due_at)
    client = adapter._client()
    taker = os.getenv("SOLANA_ROI_SHADOW_WALLET_PUBLIC_KEY", "").strip()
    api_key = os.getenv("JUPITER_API_KEY", "").strip()

    for index, delay in enumerate(EXIT_RETRY_DELAYS_SECONDS, start=1):
        if delay > 0:
            await asyncio.sleep(delay)
        requested_at = _utcnow()
        started = time.perf_counter()
        order: dict[str, Any] = {}
        tx_built = False
        tx_sha: str | None = None
        tx_size: int | None = None
        sim_ok = False
        sim_error: Any = None
        units: int | None = None
        slot: int | None = None
        status = "paper_exit_execution_failed"
        fee_lamports = 0
        try:
            if client is None or not taker or not api_key:
                raise RuntimeError("jupiter_exit_prerequisites_unavailable")
            response = await client.get(
                "https://api.jup.ag/swap/v2/order",
                params={"inputMint": input_mint, "outputMint": output_mint, "amount": str(amount), "taker": taker},
                headers={"x-api-key": api_key, "Accept": "application/json"},
            )
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                raise RuntimeError("jupiter_exit_order_not_object")
            order = dict(payload)
            transaction = order.get("transaction")
            if not isinstance(transaction, str) or not transaction or order.get("outAmount") is None:
                raise RuntimeError("jupiter_exit_order_unassembled")
            raw = base64.b64decode(transaction, validate=True)
            tx_built = True
            tx_sha = hashlib.sha256(raw).hexdigest()
            tx_size = len(raw)
            sim = await adapter.discovery.rpc.call(
                "simulateTransaction",
                [transaction,{"encoding":"base64","sigVerify":False,"replaceRecentBlockhash":True,"commitment":"processed"}],
            )
            if not isinstance(sim, dict):
                raise RuntimeError("exit_simulation_result_unavailable")
            context = sim.get("context")
            if isinstance(context, dict) and context.get("slot") is not None:
                slot = int(context["slot"])
            value = sim.get("value")
            if not isinstance(value, dict):
                raise RuntimeError("exit_simulation_value_unavailable")
            sim_error = value.get("err")
            if value.get("unitsConsumed") is not None:
                units = int(value["unitsConsumed"])
            sim_ok = sim_error is None
            fees = []
            for key in ("signatureFeeLamports","prioritizationFeeLamports","rentFeeLamports"):
                raw_fee = int(order.get(key) or 0)
                if raw_fee < 0:
                    raise RuntimeError("negative_exit_fee")
                fees.append(raw_fee)
            fee_lamports = sum(fees)
            response_input = order.get("inAmount", order.get("inputAmount"))
            exact = True if response_input is None else int(response_input) == int(amount)
            if sim_ok and exact and int(order["outAmount"]) > fee_lamports:
                status = "paper_exit_executed_exact"
        except Exception as exc:
            if sim_error is None:
                sim_error = f"{type(exc).__name__}:{exc}"

        completed_at = _utcnow()
        _persist_attempt(
            adapter,source_signature=source_signature,due_exit_signature=due_exit_signature,token_mint=input_mint,
            actual_position_raw=int(amount),attempt_number=index,first_exit_due_at=first_due,requested_at=requested_at,
            completed_at=completed_at,order=order,transaction_built=tx_built,transaction_sha256=tx_sha,
            transaction_size_bytes=tx_size,simulation_ok=sim_ok,simulation_error=sim_error,units_consumed=units,
            simulation_slot=slot,status=status,quote_age_ms=(time.perf_counter()-started)*1000.0,
        )
        if status == "paper_exit_executed_exact":
            return {"out_amount": int(order["outAmount"]), "fee_lamports": fee_lamports}
    return None


async def _route_with_exact_exit(self: Any, input_mint: str, output_mint: str, amount: int) -> dict[str, int] | None:
    if _ORIGINAL_ROUTE is None:
        raise RuntimeError("exact-exit route wrapper not installed")
    ctx = _CURRENT_EXIT.get()
    if ctx is None or str(input_mint) != str(ctx.row.get("token_mint") or "") or output_mint != WSOL_MINT:
        return await _ORIGINAL_ROUTE(self, input_mint, output_mint, amount)
    if amount <= 0:
        return None
    return await _exact_exit_route(self, ctx, input_mint, output_mint, int(amount))


def _mark_exact_exit_outcomes(adapter: Any, row: dict[str, Any]) -> None:
    # Persist explicit execution-model lineage on derived outcome tables when their
    # schemas support it. Release compatibility remains the canonical historical join.
    due_signature = str(row.get("signature") or "")
    if not due_signature:
        return
    with adapter.store._lock:
        successful = adapter.store.db.execute(
            "SELECT source_signature FROM v51_exact_exit_state WHERE release_commit=? AND last_due_exit_signature=? "
            "AND execution_model_epoch=? AND last_status='paper_exit_executed_exact'",
            (adapter.release_commit,due_signature,EXECUTION_MODEL_EPOCH),
        ).fetchall()
    signatures = [str(item["source_signature"]) for item in successful]
    if not signatures:
        return
    for table in ("profit_first_final_outcomes","risk_conditioned_alpha_v5_outcomes","fomo_paper_outcomes"):
        if not _table_exists(adapter.store, table):
            continue
        with adapter.store._lock, adapter.store.db:
            columns = {str(c["name"]) for c in adapter.store.db.execute(f"PRAGMA table_info({table})").fetchall()}
            if "execution_model_epoch" not in columns:
                adapter.store.db.execute(f"ALTER TABLE {table} ADD COLUMN execution_model_epoch TEXT")
            for signature in signatures:
                adapter.store.db.execute(
                    f"UPDATE {table} SET execution_model_epoch=? WHERE release_commit=? AND source_signature=? AND execution_model_epoch IS NULL",
                    (EXECUTION_MODEL_EPOCH,adapter.release_commit,signature),
                )


async def _sell_with_exact_exit(self: Any, row: dict[str, Any]) -> None:
    if _ORIGINAL_SELL is None:
        raise RuntimeError("exact-exit sell wrapper not installed")
    ctx = _pending_exit_context(self, row)
    token = _CURRENT_EXIT.set(ctx)
    try:
        await _ORIGINAL_SELL(self, row)
    finally:
        _CURRENT_EXIT.reset(token)
    _mark_exact_exit_outcomes(self, row)


def _solana_evidence_exact_epoch(adapter: Any, *, lane: str, pre: dict[str, Any], context_key: str) -> tuple[list[float], list[float]]:
    from . import risk_conditioned_alpha_v51 as v51
    from . import v51_consolidated_strategy as consolidated
    from . import v51_measurement_integrity as measurement
    from .v51_promotion_proof import cluster_rows, surface_attested

    release = getattr(adapter, "release_commit", None)
    consolidated._ensure_epoch(adapter.store, release)
    measurement.ensure_release_compatibility(adapter.store, release)
    if not surface_attested(adapter.store, "SOLANA", release_commit=release):
        return [], []
    parsed = v51._parse_context_key(context_key)
    entity = str(parsed.get("entity") or pre.get("trigger_entity") or "")
    risk_signature = str((pre.get("risk") or {}).get("risk_signature") or "clean")
    if not consolidated._table_exists(adapter.store, "risk_conditioned_alpha_v5_outcomes"):
        return [], []
    with adapter.store._lock:
        exact_rows = adapter.store.db.execute(
            "SELECT o.source_signature,o.net_return,o.token_mint,o.lifecycle,o.venue,o.settled_at FROM risk_conditioned_alpha_v5_outcomes o "
            "JOIN v51_economic_freeze_releases e ON e.release_commit=o.release_commit "
            "JOIN v51_release_compatibility m ON m.release_commit=o.release_commit AND m.measurement_epoch=? AND m.execution_model_epoch=? "
            "WHERE e.economic_freeze_epoch=? AND e.authority_id=? AND o.context_key=? ORDER BY o.id",
            (measurement.MEASUREMENT_EPOCH,EXECUTION_MODEL_EPOCH,measurement.ECONOMIC_FREEZE_EPOCH if hasattr(measurement,"ECONOMIC_FREEZE_EPOCH") else consolidated.ECONOMIC_FREEZE_EPOCH,consolidated.AUTHORITY_ID,context_key),
        ).fetchall()
        parent_rows = adapter.store.db.execute(
            "SELECT o.source_signature,o.net_return,o.token_mint,o.lifecycle,o.venue,o.settled_at FROM risk_conditioned_alpha_v5_outcomes o "
            "JOIN v51_economic_freeze_releases e ON e.release_commit=o.release_commit "
            "JOIN v51_release_compatibility m ON m.release_commit=o.release_commit AND m.measurement_epoch=? AND m.execution_model_epoch=? "
            "WHERE e.economic_freeze_epoch=? AND e.authority_id=? AND o.lane=? AND o.venue=? AND o.lifecycle=? "
            "AND o.risk_signature=? AND o.context_key LIKE ? AND o.context_key<>? ORDER BY o.id",
            (measurement.MEASUREMENT_EPOCH,EXECUTION_MODEL_EPOCH,consolidated.ECONOMIC_FREEZE_EPOCH,consolidated.AUTHORITY_ID,lane,
             str(pre.get("venue") or "UNKNOWN"),str(pre.get("lifecycle") or "unknown"),risk_signature,entity+"|%",context_key),
        ).fetchall()
    exact_raw = [dict(r) for r in consolidated._dedup(list(exact_rows), "source_signature")]
    exact = cluster_rows(exact_raw,family=f"SOLANA:{lane}",promotion_only=True)
    exact_clusters = {str(r["event_cluster_id"]) for r in exact}
    parent_raw = [dict(r) for r in consolidated._dedup(list(parent_rows), "source_signature")]
    parent = cluster_rows(parent_raw,family=f"SOLANA:{lane}",excluded_cluster_ids=exact_clusters,promotion_only=True)
    return [float(r["net_return"]) for r in exact],[float(r["net_return"]) for r in parent]


def _fomo_epoch_returns_exact(adapter: Any, *, wallet: str, venue: str, lifecycle: str, regime: str, hazard_signature: str) -> list[float]:
    from . import risk_conditioned_alpha_v5 as v5
    from . import risk_conditioned_alpha_v51 as v51
    from . import v51_consolidated_strategy as consolidated
    from . import v51_measurement_integrity as measurement
    from .v51_promotion_proof import cluster_rows, surface_attested

    release = getattr(adapter, "release_commit", None)
    consolidated._ensure_epoch(adapter.store, release)
    measurement.ensure_release_compatibility(adapter.store, release)
    if not surface_attested(adapter.store,"FOMO",release_commit=release):
        return []
    if not (consolidated._table_exists(adapter.store,"fomo_shadow_observations") and consolidated._table_exists(adapter.store,"fomo_shadow_outcomes")):
        return []
    try:
        with adapter.store._lock:
            rows = adapter.store.db.execute(
                "SELECT s.source_signature,s.token_mint,s.lifecycle,s.venue,s.observed_at AS settled_at,s.state_json,o.net_return,t.trigger_wallet "
                "FROM fomo_shadow_observations s JOIN v51_economic_freeze_releases e ON e.release_commit=s.release_commit "
                "JOIN v51_release_compatibility m ON m.release_commit=s.release_commit AND m.measurement_epoch=? AND m.execution_model_epoch=? "
                "JOIN profit_first_final_trials t ON t.release_commit=s.release_commit AND t.source_signature=s.source_signature AND t.lane='unified_profit_maximizer' "
                "JOIN fomo_shadow_outcomes o ON o.release_commit=s.release_commit AND o.source_signature=s.source_signature "
                "JOIN profit_first_final_outcomes x ON x.release_commit=s.release_commit AND x.source_signature=s.source_signature "
                "AND x.lane='unified_profit_maximizer' AND x.execution_model_epoch=? "
                "WHERE e.economic_freeze_epoch=? AND e.authority_id=? AND s.venue=? AND s.lifecycle=? AND s.regime=? ORDER BY s.id",
                (measurement.MEASUREMENT_EPOCH,EXECUTION_MODEL_EPOCH,EXECUTION_MODEL_EPOCH,consolidated.ECONOMIC_FREEZE_EPOCH,consolidated.AUTHORITY_ID,venue,lifecycle,regime),
            ).fetchall()
    except Exception:
        return []
    selected: list[dict[str,Any]] = []
    for row in consolidated._dedup(list(rows),"source_signature"):
        if str(row["trigger_wallet"] or "") != wallet:
            continue
        state = v5._safe_json(row["state_json"])
        if str(state.get("state") or "") not in {"pre_fomo","active_fomo"}:
            continue
        if v51.fomo_hazard_signature(state) != hazard_signature:
            continue
        value = v5._finite(row["net_return"])
        if value is not None:
            item = dict(row); item["net_return"] = float(value); selected.append(item)
    clusters = cluster_rows(selected,family="FOMO",promotion_only=True)
    return [float(r["net_return"]) for r in clusters]


def status(store: Any | None = None) -> dict[str, Any]:
    result: dict[str, Any] = {
        "version": EXIT_EXECUTION_VERSION,"execution_model_epoch":EXECUTION_MODEL_EPOCH,
        "execution_model_fingerprint":execution_model_fingerprint(),"exact_held_size_exit_quote_required":True,
        "unsigned_jupiter_exit_order_required":True,"unsigned_exit_simulation_required":True,
        "failed_exit_state":"paper_exit_execution_failed","synthetic_fixed_drag_exit_fill_authority":False,
        "retry_delays_seconds":list(EXIT_RETRY_DELAYS_SECONDS),"historical_execution_epoch_promotion_authority":False,
        "paper_only":True,"live_money_authority":False,"signing_available":False,"transaction_submission_available":False,
    }
    if store is not None:
        ensure_schema(store)
        with store._lock:
            attempts = store.db.execute("SELECT COUNT(*) FROM v51_exact_exit_attempts WHERE execution_model_epoch=?",(EXECUTION_MODEL_EPOCH,)).fetchone()[0]
            successes = store.db.execute("SELECT COUNT(*) FROM v51_exact_exit_state WHERE execution_model_epoch=? AND last_status='paper_exit_executed_exact'",(EXECUTION_MODEL_EPOCH,)).fetchone()[0]
            failures = store.db.execute("SELECT COUNT(*) FROM v51_exact_exit_state WHERE execution_model_epoch=? AND last_status='paper_exit_execution_failed'",(EXECUTION_MODEL_EPOCH,)).fetchone()[0]
        result.update({"exit_attempt_rows":int(attempts or 0),"successful_exact_exit_positions":int(successes or 0),"currently_failed_exit_positions":int(failures or 0)})
    return result


def install_exact_exit_execution_integrity() -> None:
    global _INSTALLED,_ORIGINAL_ROUTE,_ORIGINAL_SELL,_ORIGINAL_ANALYTICS_PROMOTION_RECORDS,_ORIGINAL_FILTER_SOLANA,_ORIGINAL_FILTER_FOMO
    if _INSTALLED:
        return
    from . import v51_cross_surface_proof as cross_surface
    from . import v51_evidence_analytics as analytics
    from . import v51_measurement_compatibility_filters as filters
    from . import v51_measurement_integrity as measurement
    from .profit_first_entity_final_research import FinalProfitFirstResearchAdapter
    from .profit_first_entity_research import ProfitFirstResearchAdapter

    measurement.EXECUTION_MODEL_EPOCH = EXECUTION_MODEL_EPOCH
    measurement.execution_model_fingerprint = execution_model_fingerprint  # type: ignore[assignment]

    release = measurement.current_release_commit()
    if release:
        measurement._compat_schema_store = getattr(measurement,"_compat_schema_store",None)  # compatibility marker only

    _ORIGINAL_ROUTE = ProfitFirstResearchAdapter._route
    ProfitFirstResearchAdapter._route = _route_with_exact_exit  # type: ignore[method-assign]
    _ORIGINAL_SELL = FinalProfitFirstResearchAdapter._sell
    FinalProfitFirstResearchAdapter._sell = _sell_with_exact_exit  # type: ignore[method-assign]

    _ORIGINAL_ANALYTICS_PROMOTION_RECORDS = analytics.promotion_records
    analytics.promotion_records = _current_epoch_promotion_records  # type: ignore[assignment]
    cross_surface.promotion_records = _current_epoch_promotion_records  # type: ignore[assignment]

    _ORIGINAL_FILTER_SOLANA = filters._solana_evidence_compatible
    _ORIGINAL_FILTER_FOMO = filters._fomo_epoch_returns_compatible
    filters._solana_evidence_compatible = _solana_evidence_exact_epoch  # type: ignore[assignment]
    filters._fomo_epoch_returns_compatible = _fomo_epoch_returns_exact  # type: ignore[assignment]

    _INSTALLED = True


__all__ = ["EXECUTION_MODEL_EPOCH","EXIT_EXECUTION_VERSION","EXIT_RETRY_DELAYS_SECONDS","TERMINAL_LIQUIDATION_ASSUMPTION","ensure_schema","execution_model_fingerprint","install_exact_exit_execution_integrity","status"]
