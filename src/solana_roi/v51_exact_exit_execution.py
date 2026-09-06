from __future__ import annotations

import base64
import hashlib
import json
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from .observation import WSOL_MINT
from .quote import LAMPORTS_PER_SOL


EXACT_EXIT_EXECUTION_MODEL_EPOCH = "v51-execution-model-exact-exit-v2"
LEGACY_EXIT_EXECUTION_MODEL_EPOCH = "legacy-pre-exact-exit-v1"
EXIT_RETRY_ELAPSED_SECONDS = (0, 10, 30, 60, 120, 300)
TERMINAL_LIQUIDATION_ASSUMPTION = "total_loss_after_300s_without_executable_exact_exit"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False

_ORIGINAL_SELL: Callable[..., Any] | None = None
_ORIGINAL_OBSERVE: Callable[..., Any] | None = None
_ORIGINAL_STATUS: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_MANIFEST: Callable[..., dict[str, Any]] | None = None
_INSTALLED = False


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _dump(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _table_exists(adapter: Any, table: str) -> bool:
    with adapter.store._lock:
        row = adapter.store.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (table,),
        ).fetchone()
    return row is not None


def _ensure_schema(adapter: Any) -> None:
    with adapter.store._lock, adapter.store.db:
        adapter.store.db.execute(
            "CREATE TABLE IF NOT EXISTS profit_first_final_exit_liquidations ("
            "epoch_id TEXT NOT NULL, release_commit TEXT NOT NULL, execution_model_epoch TEXT NOT NULL, "
            "position_scope TEXT NOT NULL, source_signature TEXT NOT NULL, token_mint TEXT NOT NULL, "
            "actual_position_raw INTEGER NOT NULL, entry_cost_sol REAL NOT NULL, position_fraction REAL NOT NULL, "
            "exit_signal_signature TEXT NOT NULL, exit_reason TEXT NOT NULL, exit_features_json TEXT NOT NULL, "
            "first_exit_due_at TEXT NOT NULL, last_attempt_at TEXT, attempt_count INTEGER NOT NULL, next_retry_at TEXT, "
            "status TEXT NOT NULL, eventual_exit_net_sol REAL, settled_at TEXT, terminal_assumption TEXT, "
            "paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL, "
            "PRIMARY KEY(epoch_id,position_scope,source_signature))"
        )
        adapter.store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_profit_first_exit_liquidations_due ON "
            "profit_first_final_exit_liquidations(epoch_id,status,next_retry_at)"
        )
        adapter.store.db.execute(
            "CREATE TABLE IF NOT EXISTS profit_first_final_exit_execution_attempts ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, epoch_id TEXT NOT NULL, release_commit TEXT NOT NULL, "
            "execution_model_epoch TEXT NOT NULL, position_scope TEXT NOT NULL, source_signature TEXT NOT NULL, "
            "exit_signal_signature TEXT NOT NULL, token_mint TEXT NOT NULL, actual_position_raw INTEGER NOT NULL, "
            "quote_input_raw INTEGER NOT NULL, amount_match INTEGER NOT NULL, first_exit_due_at TEXT NOT NULL, "
            "attempt_number INTEGER NOT NULL, attempted_at TEXT NOT NULL, next_retry_at TEXT, status TEXT NOT NULL, "
            "router TEXT, expected_output_lamports INTEGER, minimum_output_lamports INTEGER, route_hops_json TEXT NOT NULL, "
            "price_impact_pct REAL, quote_age_ms REAL NOT NULL, token_account_requirements_json TEXT NOT NULL, "
            "transaction_built INTEGER NOT NULL, transaction_sha256 TEXT, transaction_size_bytes INTEGER, "
            "last_valid_block_height INTEGER, simulation_ok INTEGER NOT NULL, simulation_error_class TEXT, "
            "units_consumed INTEGER, simulation_slot INTEGER, logs_count INTEGER NOT NULL, route_valid INTEGER NOT NULL, "
            "token_restriction INTEGER NOT NULL, account_failure INTEGER NOT NULL, transfer_failure INTEGER NOT NULL, "
            "signature_fee_lamports INTEGER NOT NULL, prioritization_fee_lamports INTEGER NOT NULL, "
            "rent_fee_lamports INTEGER NOT NULL, total_fee_lamports INTEGER NOT NULL, error TEXT, terminal_assumption TEXT, "
            "paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL, "
            "UNIQUE(epoch_id,position_scope,source_signature,attempt_number))"
        )
        adapter.store.db.execute(
            "CREATE TABLE IF NOT EXISTS profit_first_final_outcome_execution_models ("
            "epoch_id TEXT NOT NULL, source_signature TEXT NOT NULL, lane TEXT NOT NULL, "
            "execution_model_epoch TEXT NOT NULL, exit_attempt_id INTEGER, position_scope TEXT NOT NULL, "
            "created_at TEXT NOT NULL, PRIMARY KEY(epoch_id,source_signature,lane))"
        )
        adapter.store.db.execute(
            "CREATE TABLE IF NOT EXISTS fomo_paper_outcome_execution_models ("
            "release_commit TEXT NOT NULL, source_signature TEXT NOT NULL, execution_model_epoch TEXT NOT NULL, "
            "exit_attempt_id INTEGER, actual_position_raw INTEGER NOT NULL, created_at TEXT NOT NULL, "
            "PRIMARY KEY(release_commit,source_signature))"
        )
        adapter.store.db.execute(
            "CREATE TABLE IF NOT EXISTS risk_conditioned_alpha_v5_outcome_execution_models ("
            "release_commit TEXT NOT NULL, source_signature TEXT NOT NULL, lane TEXT NOT NULL, "
            "execution_model_epoch TEXT NOT NULL, created_at TEXT NOT NULL, "
            "PRIMARY KEY(release_commit,source_signature,lane))"
        )


def _fee(order: dict[str, Any], key: str) -> int:
    try:
        return max(0, int(order.get(key) or 0))
    except (TypeError, ValueError):
        return 0


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _float_or_none(value: Any) -> float | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return numeric if numeric == numeric and numeric not in (float("inf"), float("-inf")) else None


def _classify_simulation_failure(err: Any, logs: list[Any]) -> tuple[str | None, bool, bool, bool]:
    if err is None:
        return None, False, False, False
    text = (str(err) + " " + " ".join(str(item) for item in logs)).lower()
    token_restriction = any(
        marker in text
        for marker in (
            "frozen",
            "freeze authority",
            "transfer hook",
            "non-transferable",
            "token restriction",
            "restricted transfer",
        )
    )
    account_failure = any(
        marker in text
        for marker in (
            "accountnotfound",
            "account not found",
            "invalid account",
            "invalidaccountdata",
            "owner mismatch",
            "insufficient funds",
            "insufficientfunds",
        )
    )
    transfer_failure = "transfer" in text and any(marker in text for marker in ("fail", "error", "denied", "restricted"))
    if token_restriction:
        error_class = "token_restriction"
    elif account_failure:
        error_class = "account_failure"
    elif transfer_failure:
        error_class = "transfer_failure"
    else:
        error_class = "simulation_error"
    return error_class, token_restriction, account_failure, transfer_failure


def _route_hops(order: dict[str, Any]) -> list[Any]:
    for key in ("routePlan", "route_plan", "routes"):
        value = order.get(key)
        if isinstance(value, list):
            return value
    route = order.get("route")
    return route if isinstance(route, list) else []


async def observe_exact_exit_order(adapter: Any, *, token_mint: str, actual_position_raw: int) -> dict[str, Any]:
    """Build and simulate one unsigned Jupiter sell for the exact held raw amount.

    The function contains no signer and no submission path. A successful result
    requires the order amount to equal the held amount, an assembled transaction,
    and a successful mainnet ``simulateTransaction`` with signature verification
    disabled.
    """

    started = time.perf_counter()
    attempted_at = _utcnow()
    amount = max(0, int(actual_position_raw))
    result: dict[str, Any] = {
        "attempted_at": attempted_at.isoformat(),
        "actual_position_raw": amount,
        "quote_input_raw": amount,
        "amount_match": amount > 0,
        "router": None,
        "expected_output_lamports": None,
        "minimum_output_lamports": None,
        "route_hops": [],
        "price_impact_pct": None,
        "quote_age_ms": 0.0,
        "token_account_requirements": {},
        "transaction_built": False,
        "transaction_sha256": None,
        "transaction_size_bytes": None,
        "last_valid_block_height": None,
        "simulation_ok": False,
        "simulation_error_class": None,
        "units_consumed": None,
        "simulation_slot": None,
        "logs_count": 0,
        "route_valid": False,
        "token_restriction": False,
        "account_failure": False,
        "transfer_failure": False,
        "signature_fee_lamports": 0,
        "prioritization_fee_lamports": 0,
        "rent_fee_lamports": 0,
        "total_fee_lamports": 0,
        "exit_net_sol": None,
        "error": None,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
        "execution_model_epoch": EXACT_EXIT_EXECUTION_MODEL_EPOCH,
    }
    if amount <= 0:
        result["error"] = "invalid_actual_position_raw"
        return result

    client = adapter.execution._client()
    taker = os.getenv("SOLANA_ROI_SHADOW_WALLET_PUBLIC_KEY", "").strip()
    api_key = os.getenv("JUPITER_API_KEY", "").strip()
    if client is None or not taker or not api_key:
        result["error"] = "exact_exit_credentials_or_shadow_wallet_unavailable"
        return result

    try:
        response = await client.get(
            "https://api.jup.ag/swap/v2/order",
            params={
                "inputMint": token_mint,
                "outputMint": WSOL_MINT,
                "amount": str(amount),
                "taker": taker,
            },
            headers={"x-api-key": api_key, "Accept": "application/json"},
        )
        response.raise_for_status()
        order = response.json()
        if not isinstance(order, dict):
            raise RuntimeError("Jupiter exit order response is not an object")

        response_received = time.perf_counter()
        result["router"] = str(order.get("router") or "unknown")
        result["expected_output_lamports"] = _int_or_none(order.get("outAmount"))
        result["minimum_output_lamports"] = _int_or_none(
            order.get("otherAmountThreshold") if order.get("otherAmountThreshold") is not None else order.get("minOutAmount")
        )
        result["route_hops"] = _route_hops(order)
        result["price_impact_pct"] = _float_or_none(order.get("priceImpactPct"))
        result["last_valid_block_height"] = _int_or_none(order.get("lastValidBlockHeight"))
        result["signature_fee_lamports"] = _fee(order, "signatureFeeLamports")
        result["prioritization_fee_lamports"] = _fee(order, "prioritizationFeeLamports")
        result["rent_fee_lamports"] = _fee(order, "rentFeeLamports")
        result["total_fee_lamports"] = (
            int(result["signature_fee_lamports"])
            + int(result["prioritization_fee_lamports"])
            + int(result["rent_fee_lamports"])
        )
        result["token_account_requirements"] = {
            "taker": taker,
            "input_mint": token_mint,
            "output_mint": WSOL_MINT,
            "order_reported_requirements": order.get("tokenAccountRequirements") or order.get("accountRequirements"),
            "rent_fee_lamports": result["rent_fee_lamports"],
        }
        transaction = order.get("transaction")
        expected = result["expected_output_lamports"]
        if not isinstance(transaction, str) or not transaction:
            result["error"] = str(order.get("errorMessage") or order.get("error") or "Jupiter exit transaction unavailable")[:1000]
            result["quote_age_ms"] = max(0.0, (time.perf_counter() - response_received) * 1000.0)
            return result
        raw_tx = base64.b64decode(transaction, validate=True)
        if not raw_tx:
            raise RuntimeError("Jupiter exit transaction decoded empty")
        result["transaction_built"] = True
        result["transaction_size_bytes"] = len(raw_tx)
        result["transaction_sha256"] = hashlib.sha256(raw_tx).hexdigest()
        result["route_valid"] = bool(expected is not None and int(expected) > 0)

        simulation = await adapter.discovery.rpc.call(
            "simulateTransaction",
            [
                transaction,
                {
                    "encoding": "base64",
                    "sigVerify": False,
                    "replaceRecentBlockhash": True,
                    "commitment": "processed",
                },
            ],
        )
        value = simulation.get("value") if isinstance(simulation, dict) else None
        context = simulation.get("context") if isinstance(simulation, dict) else None
        if isinstance(context, dict) and context.get("slot") is not None:
            result["simulation_slot"] = _int_or_none(context.get("slot"))
        if not isinstance(value, dict):
            raise RuntimeError("exit simulateTransaction value unavailable")
        logs = value.get("logs") if isinstance(value.get("logs"), list) else []
        result["logs_count"] = len(logs)
        result["units_consumed"] = _int_or_none(value.get("unitsConsumed"))
        err = value.get("err")
        error_class, token_restriction, account_failure, transfer_failure = _classify_simulation_failure(err, logs)
        result["simulation_error_class"] = error_class
        result["token_restriction"] = token_restriction
        result["account_failure"] = account_failure
        result["transfer_failure"] = transfer_failure
        result["simulation_ok"] = err is None
        if err is not None:
            result["error"] = _dump(err)[:1000]
        if result["simulation_ok"] and result["route_valid"] and result["amount_match"]:
            net_lamports = int(expected or 0) - int(result["total_fee_lamports"])
            if net_lamports > 0:
                result["exit_net_sol"] = net_lamports / LAMPORTS_PER_SOL
        result["quote_age_ms"] = max(0.0, (time.perf_counter() - response_received) * 1000.0)
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}:{exc}"[:1000]
    result["total_latency_ms"] = max(0.0, (time.perf_counter() - started) * 1000.0)
    return result


def _retry_at(first_due: datetime, attempt_number: int) -> datetime | None:
    # attempt 1 is t+0. After the sixth attempt (t+300) the deterministic
    # liquidation window is exhausted and a conservative total-loss assumption
    # becomes the terminal paper measurement.
    if attempt_number >= len(EXIT_RETRY_ELAPSED_SECONDS):
        return None
    return first_due + timedelta(seconds=EXIT_RETRY_ELAPSED_SECONDS[attempt_number])


def _record_attempt(
    adapter: Any,
    liquidation: dict[str, Any],
    evidence: dict[str, Any],
    *,
    attempt_number: int,
    next_retry_at: datetime | None,
    status: str,
    terminal_assumption: str | None = None,
) -> int:
    _ensure_schema(adapter)
    with adapter.store._lock, adapter.store.db:
        cursor = adapter.store.db.execute(
            "INSERT OR IGNORE INTO profit_first_final_exit_execution_attempts("
            "epoch_id,release_commit,execution_model_epoch,position_scope,source_signature,exit_signal_signature,token_mint,"
            "actual_position_raw,quote_input_raw,amount_match,first_exit_due_at,attempt_number,attempted_at,next_retry_at,status,"
            "router,expected_output_lamports,minimum_output_lamports,route_hops_json,price_impact_pct,quote_age_ms,"
            "token_account_requirements_json,transaction_built,transaction_sha256,transaction_size_bytes,last_valid_block_height,"
            "simulation_ok,simulation_error_class,units_consumed,simulation_slot,logs_count,route_valid,token_restriction,"
            "account_failure,transfer_failure,signature_fee_lamports,prioritization_fee_lamports,rent_fee_lamports,total_fee_lamports,"
            "error,terminal_assumption,paper_only,live_money_authority) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0)",
            (
                adapter.epoch_id,
                adapter.release_commit,
                EXACT_EXIT_EXECUTION_MODEL_EPOCH,
                str(liquidation["position_scope"]),
                str(liquidation["source_signature"]),
                str(liquidation["exit_signal_signature"]),
                str(liquidation["token_mint"]),
                int(liquidation["actual_position_raw"]),
                int(evidence.get("quote_input_raw") or 0),
                1 if bool(evidence.get("amount_match")) else 0,
                str(liquidation["first_exit_due_at"]),
                int(attempt_number),
                str(evidence.get("attempted_at") or _utcnow().isoformat()),
                next_retry_at.isoformat() if next_retry_at else None,
                status,
                evidence.get("router"),
                evidence.get("expected_output_lamports"),
                evidence.get("minimum_output_lamports"),
                _dump(evidence.get("route_hops") or []),
                evidence.get("price_impact_pct"),
                float(evidence.get("quote_age_ms") or 0.0),
                _dump(evidence.get("token_account_requirements") or {}),
                1 if bool(evidence.get("transaction_built")) else 0,
                evidence.get("transaction_sha256"),
                evidence.get("transaction_size_bytes"),
                evidence.get("last_valid_block_height"),
                1 if bool(evidence.get("simulation_ok")) else 0,
                evidence.get("simulation_error_class"),
                evidence.get("units_consumed"),
                evidence.get("simulation_slot"),
                int(evidence.get("logs_count") or 0),
                1 if bool(evidence.get("route_valid")) else 0,
                1 if bool(evidence.get("token_restriction")) else 0,
                1 if bool(evidence.get("account_failure")) else 0,
                1 if bool(evidence.get("transfer_failure")) else 0,
                int(evidence.get("signature_fee_lamports") or 0),
                int(evidence.get("prioritization_fee_lamports") or 0),
                int(evidence.get("rent_fee_lamports") or 0),
                int(evidence.get("total_fee_lamports") or 0),
                str(evidence.get("error") or "")[:1000] or None,
                terminal_assumption,
            ),
        )
        row = adapter.store.db.execute(
            "SELECT id FROM profit_first_final_exit_execution_attempts WHERE epoch_id=? AND position_scope=? "
            "AND source_signature=? AND attempt_number=?",
            (adapter.epoch_id, str(liquidation["position_scope"]), str(liquidation["source_signature"]), int(attempt_number)),
        ).fetchone()
    return int(row["id"]) if row is not None else int(cursor.lastrowid or 0)


def _upsert_liquidation(adapter: Any, payload: dict[str, Any]) -> dict[str, Any]:
    _ensure_schema(adapter)
    with adapter.store._lock, adapter.store.db:
        adapter.store.db.execute(
            "INSERT OR IGNORE INTO profit_first_final_exit_liquidations("
            "epoch_id,release_commit,execution_model_epoch,position_scope,source_signature,token_mint,actual_position_raw,"
            "entry_cost_sol,position_fraction,exit_signal_signature,exit_reason,exit_features_json,first_exit_due_at,last_attempt_at,"
            "attempt_count,next_retry_at,status,eventual_exit_net_sol,settled_at,terminal_assumption,paper_only,live_money_authority) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,NULL,0,?,'exit_due',NULL,NULL,NULL,1,0)",
            (
                adapter.epoch_id,
                adapter.release_commit,
                EXACT_EXIT_EXECUTION_MODEL_EPOCH,
                payload["position_scope"],
                payload["source_signature"],
                payload["token_mint"],
                int(payload["actual_position_raw"]),
                float(payload["entry_cost_sol"]),
                float(payload["position_fraction"]),
                payload["exit_signal_signature"],
                payload["exit_reason"],
                payload["exit_features_json"],
                payload["first_exit_due_at"],
                payload["first_exit_due_at"],
            ),
        )
        row = adapter.store.db.execute(
            "SELECT * FROM profit_first_final_exit_liquidations WHERE epoch_id=? AND position_scope=? AND source_signature=?",
            (adapter.epoch_id, payload["position_scope"], payload["source_signature"]),
        ).fetchone()
    if row is None:
        raise RuntimeError("exact exit liquidation state unavailable")
    return dict(row)


def _final_trials(adapter: Any, source_signature: str) -> list[dict[str, Any]]:
    with adapter.store._lock:
        rows = adapter.store.db.execute(
            "SELECT * FROM profit_first_final_trials WHERE epoch_id=? AND source_signature=? ORDER BY id",
            (adapter.epoch_id, source_signature),
        ).fetchall()
    return [dict(row) for row in rows]


def _record_outcome_model(adapter: Any, *, source_signature: str, lane: str, attempt_id: int, scope: str) -> None:
    with adapter.store._lock, adapter.store.db:
        adapter.store.db.execute(
            "INSERT OR REPLACE INTO profit_first_final_outcome_execution_models("
            "epoch_id,source_signature,lane,execution_model_epoch,exit_attempt_id,position_scope,created_at) VALUES (?,?,?,?,?,?,?)",
            (
                adapter.epoch_id,
                source_signature,
                lane,
                EXACT_EXIT_EXECUTION_MODEL_EPOCH,
                int(attempt_id) if attempt_id > 0 else None,
                scope,
                _utcnow().isoformat(),
            ),
        )


def _settle_final(
    adapter: Any,
    liquidation: dict[str, Any],
    *,
    attempt_id: int,
    exit_net_sol: float,
    terminal: bool,
) -> None:
    from .profit_first_entity_final import FinalForwardOutcome, UNIFIED_LANE
    from .profit_first_entity_final_research import FINAL_STRATEGY_VERSION, _context

    trials = _final_trials(adapter, str(liquidation["source_signature"]))
    if not trials:
        return
    entry_cost_sol = float(liquidation["entry_cost_sol"])
    if entry_cost_sol <= 0:
        return
    net_return = -1.0 if terminal else float(exit_net_sol) / entry_cost_sol - 1.0
    exit_signature = (
        f"paper_exit_terminal:{liquidation['source_signature']}"
        if terminal
        else f"paper_exit_exact:{liquidation['exit_signal_signature']}:{liquidation['attempt_count']}"
    )
    reason = str(liquidation["exit_reason"])
    if terminal:
        reason = reason + ":terminal_unexitable_total_loss"
    now = _utcnow().isoformat()
    inserted: list[Any] = []
    with adapter.store._lock, adapter.store.db:
        for trial in trials:
            cursor = adapter.store.db.execute(
                "INSERT OR IGNORE INTO profit_first_final_outcomes("
                "epoch_id,release_commit,strategy_version,source_signature,exit_signature,token_mint,trigger_wallet,lane,context_json,"
                "entry_observed_at,exit_observed_at,signal_to_entry_seconds,position_fraction,entry_cost_sol,exit_net_sol,net_return,"
                "evidence_phase,exit_reason,exit_features_json,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    adapter.epoch_id,
                    adapter.release_commit,
                    FINAL_STRATEGY_VERSION,
                    str(liquidation["source_signature"]),
                    exit_signature,
                    str(liquidation["token_mint"]),
                    str(trial["trigger_wallet"]),
                    str(trial["lane"]),
                    trial["context_json"],
                    str(trial["observed_at"]),
                    now,
                    float(trial["signal_to_entry_seconds"]),
                    float(trial["assigned_position_fraction"]),
                    entry_cost_sol,
                    float(exit_net_sol),
                    net_return,
                    "forward",
                    reason,
                    str(liquidation["exit_features_json"]),
                    now,
                ),
            )
            _record_outcome_model(
                adapter,
                source_signature=str(liquidation["source_signature"]),
                lane=str(trial["lane"]),
                attempt_id=attempt_id,
                scope="final",
            )
            if cursor.rowcount == 1 and trial["context_json"] is not None and str(trial["lane"]) != UNIFIED_LANE:
                inserted.append(
                    FinalForwardOutcome(
                        context=_context(str(trial["context_json"])),
                        net_return=net_return,
                        source_signature=str(liquidation["source_signature"]),
                        release_commit=adapter.release_commit,
                        observed_at=str(trial["observed_at"]),
                        signal_to_entry_seconds=float(trial["signal_to_entry_seconds"]),
                        position_fraction=float(trial["assigned_position_fraction"]),
                        evidence_phase="forward",
                        exit_reason=reason,
                    )
                )
    for outcome in inserted:
        adapter.ledger.add(outcome)
    _sync_v5_exact_outcomes(adapter, str(liquidation["source_signature"]), exit_signature, net_return, reason)


def _settle_fomo(
    adapter: Any,
    liquidation: dict[str, Any],
    *,
    attempt_id: int,
    exit_net_sol: float,
    terminal: bool,
) -> None:
    if not _table_exists(adapter, "fomo_paper_trials") or not _table_exists(adapter, "fomo_paper_outcomes"):
        return
    with adapter.store._lock:
        trial = adapter.store.db.execute(
            "SELECT * FROM fomo_paper_trials WHERE release_commit=? AND source_signature=? AND decision LIKE 'paper_enter_%' LIMIT 1",
            (adapter.release_commit, str(liquidation["source_signature"])),
        ).fetchone()
    if trial is None:
        return
    entry_cost_sol = float(liquidation["entry_cost_sol"])
    if entry_cost_sol <= 0:
        return
    net_return = -1.0 if terminal else float(exit_net_sol) / entry_cost_sol - 1.0
    fraction = float(trial["position_fraction"])
    exit_signature = (
        f"paper_fomo_exit_terminal:{liquidation['source_signature']}"
        if terminal
        else f"paper_fomo_exit_exact:{liquidation['exit_signal_signature']}:{liquidation['attempt_count']}"
    )
    reason = str(liquidation["exit_reason"])
    if terminal:
        reason = reason + ":terminal_unexitable_total_loss"
    now = _utcnow().isoformat()
    with adapter.store._lock, adapter.store.db:
        adapter.store.db.execute(
            "INSERT OR IGNORE INTO fomo_paper_outcomes("
            "release_commit,strategy_version,source_signature,exit_signature,token_mint,trigger_wallet,venue,lifecycle,regime,"
            "fomo_state,position_fraction,net_return,paper_return_contribution,exit_reason,settled_at,paper_only,live_money_authority) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0)",
            (
                adapter.release_commit,
                str(trial["strategy_version"]),
                str(liquidation["source_signature"]),
                exit_signature,
                str(trial["token_mint"]),
                str(trial["trigger_wallet"]),
                str(trial["venue"]),
                str(trial["lifecycle"]),
                str(trial["regime"]),
                str(trial["fomo_state"]),
                fraction,
                net_return,
                fraction * net_return,
                reason,
                now,
            ),
        )
        adapter.store.db.execute(
            "INSERT OR REPLACE INTO fomo_paper_outcome_execution_models("
            "release_commit,source_signature,execution_model_epoch,exit_attempt_id,actual_position_raw,created_at) VALUES (?,?,?,?,?,?)",
            (
                adapter.release_commit,
                str(liquidation["source_signature"]),
                EXACT_EXIT_EXECUTION_MODEL_EPOCH,
                int(attempt_id) if attempt_id > 0 else None,
                int(liquidation["actual_position_raw"]),
                now,
            ),
        )


def _sync_v5_exact_outcomes(adapter: Any, source_signature: str, exit_signature: str, net_return: float, exit_reason: str) -> None:
    if not _table_exists(adapter, "risk_conditioned_alpha_v5_trials") or not _table_exists(adapter, "risk_conditioned_alpha_v5_outcomes"):
        return
    with adapter.store._lock:
        rows = adapter.store.db.execute(
            "SELECT * FROM risk_conditioned_alpha_v5_trials WHERE release_commit=? AND source_signature=? AND decision LIKE 'paper_enter%'",
            (adapter.release_commit, source_signature),
        ).fetchall()
    now = _utcnow().isoformat()
    with adapter.store._lock, adapter.store.db:
        for raw in rows:
            row = dict(raw)
            adapter.store.db.execute(
                "INSERT OR IGNORE INTO risk_conditioned_alpha_v5_outcomes("
                "release_commit,strategy_version,source_signature,exit_signature,token_mint,lane,venue,lifecycle,regime,risk_signature,"
                "context_key,position_fraction,net_return,exit_reason,settled_at,paper_only,live_money_authority) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0)",
                (
                    adapter.release_commit,
                    str(row["strategy_version"]),
                    source_signature,
                    exit_signature,
                    str(row["token_mint"]),
                    str(row["lane"]),
                    str(row["venue"]),
                    str(row["lifecycle"]),
                    str(row["regime"]),
                    str(row["risk_signature"]),
                    str(row["context_key"]),
                    float(row["position_fraction"]),
                    float(net_return),
                    exit_reason,
                    now,
                ),
            )
            adapter.store.db.execute(
                "INSERT OR REPLACE INTO risk_conditioned_alpha_v5_outcome_execution_models("
                "release_commit,source_signature,lane,execution_model_epoch,created_at) VALUES (?,?,?,?,?)",
                (adapter.release_commit, source_signature, str(row["lane"]), EXACT_EXIT_EXECUTION_MODEL_EPOCH, now),
            )


async def _attempt_liquidation(adapter: Any, liquidation: dict[str, Any]) -> None:
    attempt_number = int(liquidation.get("attempt_count") or 0) + 1
    first_due = datetime.fromisoformat(str(liquidation["first_exit_due_at"]))
    evidence = await observe_exact_exit_order(
        adapter,
        token_mint=str(liquidation["token_mint"]),
        actual_position_raw=int(liquidation["actual_position_raw"]),
    )
    executable = bool(
        evidence.get("amount_match")
        and evidence.get("transaction_built")
        and evidence.get("route_valid")
        and evidence.get("simulation_ok")
        and evidence.get("exit_net_sol") is not None
        and float(evidence["exit_net_sol"]) > 0.0
    )
    next_retry = None if executable else _retry_at(first_due, attempt_number)
    terminal = bool(not executable and next_retry is None)
    status = "paper_exit_executed" if executable else ("paper_exit_terminal_unexitable" if terminal else "paper_exit_execution_failed")
    attempt_id = _record_attempt(
        adapter,
        liquidation,
        evidence,
        attempt_number=attempt_number,
        next_retry_at=next_retry,
        status=status,
        terminal_assumption=TERMINAL_LIQUIDATION_ASSUMPTION if terminal else None,
    )
    settled_at = _utcnow().isoformat() if executable or terminal else None
    eventual_exit = float(evidence["exit_net_sol"]) if executable else (0.0 if terminal else None)
    with adapter.store._lock, adapter.store.db:
        adapter.store.db.execute(
            "UPDATE profit_first_final_exit_liquidations SET last_attempt_at=?,attempt_count=?,next_retry_at=?,status=?,"
            "eventual_exit_net_sol=?,settled_at=?,terminal_assumption=? WHERE epoch_id=? AND position_scope=? AND source_signature=?",
            (
                str(evidence.get("attempted_at") or _utcnow().isoformat()),
                attempt_number,
                next_retry.isoformat() if next_retry else None,
                status,
                eventual_exit,
                settled_at,
                TERMINAL_LIQUIDATION_ASSUMPTION if terminal else None,
                adapter.epoch_id,
                str(liquidation["position_scope"]),
                str(liquidation["source_signature"]),
            ),
        )
    liquidation = dict(liquidation)
    liquidation["attempt_count"] = attempt_number
    if executable or terminal:
        if str(liquidation["position_scope"]) == "fomo":
            _settle_fomo(adapter, liquidation, attempt_id=attempt_id, exit_net_sol=float(eventual_exit or 0.0), terminal=terminal)
        else:
            _settle_final(adapter, liquidation, attempt_id=attempt_id, exit_net_sol=float(eventual_exit or 0.0), terminal=terminal)
    try:
        adapter.store.append(
            status,
            str(evidence.get("attempted_at") or _utcnow().isoformat()),
            {
                "execution_model_epoch": EXACT_EXIT_EXECUTION_MODEL_EPOCH,
                "position_scope": str(liquidation["position_scope"]),
                "source_signature": str(liquidation["source_signature"]),
                "token_mint": str(liquidation["token_mint"]),
                "actual_position_raw": int(liquidation["actual_position_raw"]),
                "exit_quote_amount": int(evidence.get("quote_input_raw") or 0),
                "amount_match": bool(evidence.get("amount_match")),
                "attempt_number": attempt_number,
                "next_retry_at": next_retry.isoformat() if next_retry else None,
                "simulation_ok": bool(evidence.get("simulation_ok")),
                "terminal_assumption": TERMINAL_LIQUIDATION_ASSUMPTION if terminal else None,
                "paper_only": True,
                "live_money_authority": False,
                "signing_available": False,
                "transaction_submission_available": False,
            },
        )
    except Exception:
        pass


def _fomo_liquidation_payload(adapter: Any, final_trial: dict[str, Any], base: dict[str, Any]) -> dict[str, Any] | None:
    if not _table_exists(adapter, "fomo_paper_trials"):
        return None
    with adapter.store._lock:
        raw = adapter.store.db.execute(
            "SELECT * FROM fomo_paper_trials WHERE release_commit=? AND source_signature=? AND decision LIKE 'paper_enter_%' LIMIT 1",
            (adapter.release_commit, str(final_trial["source_signature"])),
        ).fetchone()
    if raw is None:
        return None
    trial = dict(raw)
    fomo_fraction = float(trial.get("position_fraction") or 0.0)
    base_fraction = float(final_trial.get("assigned_position_fraction") or 0.0)
    base_raw = int(final_trial.get("entry_token_raw") or 0)
    quote_input = int(final_trial.get("quote_input_lamports") or 0)
    entry_fee = int(final_trial.get("entry_fee_lamports") or 0)
    if fomo_fraction <= 0 or base_fraction <= 0 or base_raw <= 0 or quote_input <= 0:
        return None
    scale = fomo_fraction / base_fraction
    actual_raw = max(1, int(round(base_raw * scale)))
    base_entry_cost = (quote_input + entry_fee) / LAMPORTS_PER_SOL
    return {
        **base,
        "position_scope": "fomo",
        "actual_position_raw": actual_raw,
        "entry_cost_sol": base_entry_cost * scale,
        "position_fraction": fomo_fraction,
    }


async def _start_due_liquidations(adapter: Any, *, trials: list[dict[str, Any]], row: dict[str, Any], exit_reason: str, features_json: str) -> None:
    if not trials:
        return
    first = trials[0]
    token_raw = int(first.get("entry_token_raw") or 0)
    quote_input = int(first.get("quote_input_lamports") or 0)
    entry_fee = int(first.get("entry_fee_lamports") or 0)
    if token_raw <= 0 or quote_input <= 0:
        return
    first_due = _utcnow().isoformat()
    base = {
        "source_signature": str(first["source_signature"]),
        "token_mint": str(first["token_mint"]),
        "exit_signal_signature": str(row.get("signature") or "paper-exit-signal"),
        "exit_reason": exit_reason,
        "exit_features_json": features_json,
        "first_exit_due_at": first_due,
    }
    final_payload = {
        **base,
        "position_scope": "final",
        "actual_position_raw": token_raw,
        "entry_cost_sol": (quote_input + entry_fee) / LAMPORTS_PER_SOL,
        "position_fraction": float(first.get("assigned_position_fraction") or 0.0),
    }
    final_state = _upsert_liquidation(adapter, final_payload)
    if str(final_state["status"]) == "exit_due":
        await _attempt_liquidation(adapter, final_state)

    fomo_payload = _fomo_liquidation_payload(adapter, first, base)
    if fomo_payload is not None:
        fomo_state = _upsert_liquidation(adapter, fomo_payload)
        if str(fomo_state["status"]) == "exit_due":
            await _attempt_liquidation(adapter, fomo_state)


async def _sell_exact(self: Any, row: dict[str, Any]) -> None:
    from .profit_first_entity_final import ExitFeatures

    _ensure_schema(self)
    at = datetime.fromisoformat(str(row["received_at"]))
    token_mint, seller = str(row["token_mint"]), str(row["wallet"])
    with self.store._lock:
        candidates = self.store.db.execute(
            "SELECT t.* FROM profit_first_final_trials t LEFT JOIN profit_first_final_outcomes o ON "
            "o.epoch_id=t.epoch_id AND o.source_signature=t.source_signature AND o.lane=t.lane "
            "WHERE t.epoch_id=? AND t.token_mint=? AND t.entry_executable=1 AND t.exit_executable=1 "
            "AND t.observed_at<? AND o.id IS NULL ORDER BY t.id",
            (self.epoch_id, token_mint, str(row["observed_at"])),
        ).fetchall()
    groups: dict[str, list[dict[str, Any]]] = {}
    for candidate in candidates:
        groups.setdefault(str(candidate["source_signature"]), []).append(dict(candidate))
    for _entry_signature, trials in groups.items():
        first = trials[0]
        opportunity = json.loads(str(first["opportunity_json"]))
        creator_wallet = self.execution._deployer(token_mint, at)
        seller_entity, current_creator_entity = self._seller_entity(seller, creator_wallet, at)
        creator_entity = opportunity.get("creator_entity") or current_creator_entity
        features = ExitFeatures(
            creator_distribution=bool(creator_entity and seller_entity == creator_entity),
            linked_entity_distribution=bool(creator_entity and seller_entity == creator_entity and seller != creator_wallet),
            early_holder_exit_fraction=float(opportunity.get("early_buyer_exit_fraction") or 0.0),
            successful_scout_exit=seller == str(first["trigger_wallet"]),
            buy_sell_flow_reversal=self._flow_reversed(token_mint, at),
        )
        signal = self.strategy.exit_model.evaluate(features)
        now = _utcnow().isoformat()
        features_json = _dump(features.__dict__ if hasattr(features, "__dict__") else {
            "creator_distribution": features.creator_distribution,
            "linked_entity_distribution": features.linked_entity_distribution,
            "early_holder_exit_fraction": features.early_holder_exit_fraction,
            "successful_scout_exit": features.successful_scout_exit,
            "buy_sell_flow_reversal": features.buy_sell_flow_reversal,
        })
        signal_json = _dump(signal.__dict__ if hasattr(signal, "__dict__") else {
            "should_exit": signal.should_exit,
            "reasons": list(signal.reasons),
        })
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "INSERT OR IGNORE INTO profit_first_final_exit_signals("
                "epoch_id,token_mint,source_signature,seller_wallet,observed_at,features_json,signal_json,created_at) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (self.epoch_id, token_mint, str(row["signature"]), seller, str(row["observed_at"]), features_json, signal_json, now),
            )
        if not signal.should_exit and seller != str(first["trigger_wallet"]):
            continue
        exit_reason = "dynamic_exit_alpha:" + ",".join(signal.reasons) if signal.should_exit else "trigger_wallet_exit_baseline"
        await _start_due_liquidations(self, trials=trials, row=row, exit_reason=exit_reason, features_json=features_json)


async def _retry_due(self: Any) -> None:
    _ensure_schema(self)
    now = _utcnow()
    with self.store._lock:
        rows = self.store.db.execute(
            "SELECT * FROM profit_first_final_exit_liquidations WHERE epoch_id=? AND status='paper_exit_execution_failed' "
            "AND next_retry_at IS NOT NULL AND next_retry_at<=? ORDER BY next_retry_at LIMIT 32",
            (self.epoch_id, now.isoformat()),
        ).fetchall()
    for row in rows:
        await _attempt_liquidation(self, dict(row))


async def _observe_with_exact_exit(self: Any, signature: str) -> None:
    if _ORIGINAL_OBSERVE is None:
        raise RuntimeError("exact exit execution model missing original observe")
    await _ORIGINAL_OBSERVE(self, signature)
    await _retry_due(self)


def _status_with_exact_exit(self: Any) -> dict[str, Any]:
    if _ORIGINAL_STATUS is None:
        raise RuntimeError("exact exit execution model missing original status")
    payload = _ORIGINAL_STATUS(self)
    _ensure_schema(self)
    with self.store._lock:
        attempts = self.store.db.execute(
            "SELECT COUNT(*) total,SUM(CASE WHEN status='paper_exit_executed' THEN 1 ELSE 0 END) executed,"
            "SUM(CASE WHEN status='paper_exit_execution_failed' THEN 1 ELSE 0 END) failed,"
            "SUM(CASE WHEN status='paper_exit_terminal_unexitable' THEN 1 ELSE 0 END) terminal "
            "FROM profit_first_final_exit_execution_attempts WHERE epoch_id=? AND execution_model_epoch=?",
            (self.epoch_id, EXACT_EXIT_EXECUTION_MODEL_EPOCH),
        ).fetchone()
        pending = int(self.store.db.execute(
            "SELECT COUNT(*) FROM profit_first_final_exit_liquidations WHERE epoch_id=? AND status='paper_exit_execution_failed'",
            (self.epoch_id,),
        ).fetchone()[0])
    payload["exact_exit_execution"] = {
        "execution_model_epoch": EXACT_EXIT_EXECUTION_MODEL_EPOCH,
        "measurement_compatibility_change": True,
        "economic_strategy_changed": False,
        "exact_held_size_required": True,
        "exit_quote_amount_must_equal_actual_position_units": True,
        "fresh_jupiter_sell_order_required": True,
        "unsigned_exit_simulation_required": True,
        "signing_available": False,
        "transaction_submission_available": False,
        "paper_only": True,
        "live_money_authority": False,
        "retry_elapsed_seconds": list(EXIT_RETRY_ELAPSED_SECONDS),
        "terminal_liquidation_assumption": TERMINAL_LIQUIDATION_ASSUMPTION,
        "attempt_count": int(attempts["total"] or 0) if attempts else 0,
        "executed_attempt_count": int(attempts["executed"] or 0) if attempts else 0,
        "failed_attempt_count": int(attempts["failed"] or 0) if attempts else 0,
        "terminal_unexitable_count": int(attempts["terminal"] or 0) if attempts else 0,
        "pending_liquidation_count": pending,
        "legacy_outcomes_pool_with_exact_epoch": False,
    }
    return payload


def _manifest_with_exact_exit(self: Any) -> dict[str, Any]:
    if _ORIGINAL_MANIFEST is None:
        raise RuntimeError("exact exit execution model missing original manifest")
    payload = _ORIGINAL_MANIFEST(self)
    payload.update(
        {
            "execution_model_epoch": EXACT_EXIT_EXECUTION_MODEL_EPOCH,
            "exit_logic": "fresh exact-held-size Jupiter sell order plus unsigned mainnet simulation with deterministic failed-exit liquidation",
            "exit_quote_amount_must_equal_actual_position_units": True,
            "entry_route_reuse_for_exit_forbidden": True,
            "arbitrary_standard_exit_notional_forbidden": True,
            "exit_transaction_unsigned": True,
            "exit_transaction_submission_available": False,
            "failed_exit_state": "paper_exit_execution_failed",
            "exit_retry_elapsed_seconds": list(EXIT_RETRY_ELAPSED_SECONDS),
            "terminal_liquidation_assumption": TERMINAL_LIQUIDATION_ASSUMPTION,
            "old_execution_model_outcomes_promotion_compatible": False,
            "economic_strategy_changed_by_execution_model": False,
        }
    )
    return payload


def install_exact_exit_execution_model() -> None:
    """Install Repairs 109-113 into the existing execution-realism composition.

    This deliberately replaces the old final research sell implementation rather
    than wrapping and then calling it: the legacy method could settle from a quote
    and silently discard failed exits, which is precisely the behavior this epoch
    forbids.
    """

    global _ORIGINAL_SELL, _ORIGINAL_OBSERVE, _ORIGINAL_STATUS, _ORIGINAL_MANIFEST, _INSTALLED
    if _INSTALLED:
        return
    from .profit_first_entity_final_research import FinalProfitFirstResearchAdapter

    _ORIGINAL_SELL = FinalProfitFirstResearchAdapter._sell
    _ORIGINAL_OBSERVE = FinalProfitFirstResearchAdapter.observe
    _ORIGINAL_STATUS = FinalProfitFirstResearchAdapter.status
    _ORIGINAL_MANIFEST = FinalProfitFirstResearchAdapter._manifest

    setattr(_sell_exact, "_roi_exact_exit_execution_v2", True)
    setattr(_observe_with_exact_exit, "_roi_exact_exit_execution_v2", True)
    setattr(_status_with_exact_exit, "_roi_exact_exit_execution_v2", True)
    setattr(_manifest_with_exact_exit, "_roi_exact_exit_execution_v2", True)
    FinalProfitFirstResearchAdapter._sell = _sell_exact  # type: ignore[method-assign]
    FinalProfitFirstResearchAdapter.observe = _observe_with_exact_exit  # type: ignore[method-assign]
    FinalProfitFirstResearchAdapter.status = _status_with_exact_exit  # type: ignore[method-assign]
    FinalProfitFirstResearchAdapter._manifest = _manifest_with_exact_exit  # type: ignore[method-assign]
    _INSTALLED = True


__all__ = [
    "EXACT_EXIT_EXECUTION_MODEL_EPOCH",
    "EXIT_RETRY_ELAPSED_SECONDS",
    "LEGACY_EXIT_EXECUTION_MODEL_EPOCH",
    "LIVE_MONEY_AUTHORITY",
    "PAPER_ONLY",
    "SIGNING_AVAILABLE",
    "TERMINAL_LIQUIDATION_ASSUMPTION",
    "TRANSACTION_SUBMISSION_AVAILABLE",
    "install_exact_exit_execution_model",
    "observe_exact_exit_order",
]
