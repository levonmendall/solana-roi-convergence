from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from typing import Any, Callable

from . import direct_transaction as tx
from . import scout_candidate_continuity_repair as scout
from . import semantic_candidate_attribution_architecture as semantic
from .direct_solana import DirectSolanaIngestionPlane
from .ingestion import LAMPORTS_PER_SOL, NormalizedSwap
from .observation import WSOL_MINT
from .source_coverage import FROZEN_SUPPORTED_PROGRAM_IDS_BY_SOURCE

REPAIR_VERSION = "venue-native-instruction-transfer-graph-v2"
PREWARM_VERSION = "durable-opportunity-risk-prewarm-v1"
PREWARM_TIMEOUT_SECONDS = 12.0
PREWARM_MIN_INTERVAL_SECONDS = 30.0
PREWARM_CONCURRENCY = 2
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False

_PROGRAM_IDS_BY_SOURCE = {
    str(source): frozenset(str(program_id) for program_id in ids)
    for source, ids in FROZEN_SUPPORTED_PROGRAM_IDS_BY_SOURCE
}
_PROGRAM_SOURCE_BY_ID = {
    program_id: source for source, ids in _PROGRAM_IDS_BY_SOURCE.items() for program_id in ids
}
_ORIGINAL_STATUS: Callable[..., dict[str, Any]] | None = None


def _inc(plane: Any, name: str, amount: int = 1) -> None:
    attr = f"_roi_venue_graph_{name}"
    setattr(plane, attr, int(getattr(plane, attr, 0) or 0) + int(amount))


def _account_keys(result: dict[str, Any]) -> list[str]:
    return tx._account_keys(result)


def _instruction_program_id(row: dict[str, Any], keys: list[str]) -> str | None:
    direct = str(row.get("programId") or "")
    if direct:
        return direct
    try:
        index = int(row.get("programIdIndex"))
    except (TypeError, ValueError):
        return None
    return keys[index] if 0 <= index < len(keys) and keys[index] else None


def _walk_instruction_rows(result: dict[str, Any]) -> list[tuple[int | None, dict[str, Any]]]:
    rows: list[tuple[int | None, dict[str, Any]]] = []
    transaction = result.get("transaction")
    message = transaction.get("message") if isinstance(transaction, dict) else None
    top = message.get("instructions") if isinstance(message, dict) else []
    if isinstance(top, list):
        rows.extend((index, row) for index, row in enumerate(top) if isinstance(row, dict))
    meta = result.get("meta")
    groups = meta.get("innerInstructions") if isinstance(meta, dict) else []
    if isinstance(groups, list):
        for group in groups:
            if not isinstance(group, dict):
                continue
            try:
                parent = int(group.get("index"))
            except (TypeError, ValueError):
                parent = None
            children = group.get("instructions")
            if isinstance(children, list):
                rows.extend((parent, row) for row in children if isinstance(row, dict))
    return rows


def _indexed_transaction_sources(result: dict[str, Any]) -> set[str]:
    keys = _account_keys(result)
    sources = set(tx.transaction_sources(result))
    for _parent, row in _walk_instruction_rows(result):
        program_id = _instruction_program_id(row, keys)
        source = _PROGRAM_SOURCE_BY_ID.get(str(program_id or ""))
        if source:
            sources.add(source)
    return sources


def _source_for_transaction(result: dict[str, Any], source_hint: str | None) -> tuple[str | None, str | None]:
    source, error = scout._source_for_transaction(result, source_hint)
    if source is not None:
        return source, None
    sources = _indexed_transaction_sources(result)
    hint = str(source_hint or "").upper()
    if hint:
        if hint not in _PROGRAM_IDS_BY_SOURCE:
            return None, "unsupported_source_hint"
        return (hint, None) if hint in sources else (None, "source_hint_not_present")
    if len(sources) == 1:
        return next(iter(sources)), None
    if not sources:
        return None, error or "supported_swap_source_missing"
    return None, "multiple_supported_swap_sources"


def _token_account_metadata(result: dict[str, Any]) -> dict[str, tuple[str, str, int]]:
    keys = _account_keys(result)
    meta = result.get("meta")
    if not isinstance(meta, dict):
        return {}
    out: dict[str, tuple[str, str, int]] = {}
    for field in ("preTokenBalances", "postTokenBalances"):
        for row in meta.get(field) or []:
            if not isinstance(row, dict):
                continue
            try:
                index = int(row.get("accountIndex"))
            except (TypeError, ValueError):
                continue
            if not (0 <= index < len(keys)):
                continue
            account = keys[index]
            owner = str(row.get("owner") or "")
            mint = str(row.get("mint") or "")
            ui = row.get("uiTokenAmount")
            try:
                decimals = int(ui.get("decimals") or 0) if isinstance(ui, dict) else 0
            except (TypeError, ValueError):
                decimals = 0
            if account and owner and mint:
                out[account] = (owner, mint, decimals)
    return out


def _parsed_token_transfer(
    row: dict[str, Any], metadata: dict[str, tuple[str, str, int]]
) -> tuple[str, str, float] | None:
    parsed = row.get("parsed")
    if not isinstance(parsed, dict) or str(parsed.get("type") or "") not in {"transfer", "transferChecked"}:
        return None
    info = parsed.get("info")
    if not isinstance(info, dict):
        return None
    source = str(info.get("source") or "")
    destination = str(info.get("destination") or "")
    if not source or not destination:
        return None
    amount: float | None = None
    token_amount = info.get("tokenAmount")
    if isinstance(token_amount, dict):
        try:
            raw = int(str(token_amount.get("amount") or "0"))
            decimals = int(token_amount.get("decimals") or 0)
            amount = raw / (10 ** decimals)
        except (TypeError, ValueError):
            amount = None
    if amount is None:
        try:
            raw = int(str(info.get("amount") or "0"))
            account_meta = metadata.get(source) or metadata.get(destination)
            decimals = int(account_meta[2]) if account_meta is not None else 0
            amount = raw / (10 ** decimals)
        except (TypeError, ValueError):
            amount = None
    if amount is None or amount <= 0:
        return None
    return source, destination, amount


def _parsed_native_transfer(row: dict[str, Any]) -> tuple[str, str, float] | None:
    parsed = row.get("parsed")
    if not isinstance(parsed, dict) or str(parsed.get("type") or "") != "transfer":
        return None
    info = parsed.get("info")
    if not isinstance(info, dict) or "lamports" not in info:
        return None
    source = str(info.get("source") or "")
    destination = str(info.get("destination") or "")
    try:
        lamports = int(str(info.get("lamports") or "0"))
    except (TypeError, ValueError):
        return None
    if not source or not destination or lamports <= 0:
        return None
    return source, destination, lamports / LAMPORTS_PER_SOL


def _venue_instruction_groups(result: dict[str, Any], source: str) -> list[list[dict[str, Any]]]:
    keys = _account_keys(result)
    target_programs = _PROGRAM_IDS_BY_SOURCE.get(source, frozenset())
    transaction = result.get("transaction")
    message = transaction.get("message") if isinstance(transaction, dict) else None
    top = message.get("instructions") if isinstance(message, dict) else []
    meta = result.get("meta")
    inners = meta.get("innerInstructions") if isinstance(meta, dict) else []
    by_parent: dict[int, list[dict[str, Any]]] = {}
    if isinstance(inners, list):
        for group in inners:
            if not isinstance(group, dict):
                continue
            try:
                parent = int(group.get("index"))
            except (TypeError, ValueError):
                continue
            children = group.get("instructions")
            if isinstance(children, list):
                by_parent[parent] = [row for row in children if isinstance(row, dict)]
    groups: list[list[dict[str, Any]]] = []
    if isinstance(top, list):
        for index, row in enumerate(top):
            if not isinstance(row, dict):
                continue
            program_id = _instruction_program_id(row, keys)
            if program_id in target_programs:
                groups.append([row, *by_parent.get(index, [])])
    if not groups:
        for _parent, children in by_parent.items():
            if any(_instruction_program_id(row, keys) in target_programs for row in children):
                groups.append(children)
    return groups


def _select_quote_amount(amounts: list[float]) -> float | None:
    values = sorted((float(value) for value in amounts if float(value) > 0), reverse=True)
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    if all(abs(value - values[0]) <= max(1e-12, values[0] * 1e-6) for value in values[1:]):
        return values[0]
    if values[1] <= values[0] * 0.20:
        return values[0]
    return None


def _graph_quote_side_and_amount(
    result: dict[str, Any], *, wallet: str, source: str, token_mint: str, token_delta: float
) -> tuple[str, float] | None:
    metadata = _token_account_metadata(result)
    side = "buy" if token_delta > 0 else "sell"
    quote_amounts: list[float] = []
    proven_token_leg = False
    for group in _venue_instruction_groups(result, source):
        group_quote: list[float] = []
        group_token = False
        for row in group:
            transfer = _parsed_token_transfer(row, metadata)
            if transfer is not None:
                from_account, to_account, amount = transfer
                from_meta = metadata.get(from_account)
                to_meta = metadata.get(to_account)
                mint = str((from_meta or to_meta or ("", "", 0))[1])
            else:
                native = _parsed_native_transfer(row)
                if native is not None:
                    from_account, to_account, amount = native
                    if (side == "buy" and from_account == wallet) or (side == "sell" and to_account == wallet):
                        group_quote.append(amount)
                continue
            if mint == token_mint:
                if side == "buy" and to_meta is not None and to_meta[0] == wallet:
                    group_token = True
                elif side == "sell" and from_meta is not None and from_meta[0] == wallet:
                    group_token = True
            elif mint == WSOL_MINT:
                group_quote.append(amount)
        if group_token:
            proven_token_leg = True
            quote_amounts.extend(group_quote)
    if not proven_token_leg:
        return None
    quote = _select_quote_amount(quote_amounts)
    return (side, quote) if quote is not None else None


def _decode_supported_venue(
    result: Any, *, signature: str, trigger_received_at: datetime, wallet: str, source: str
) -> tuple[NormalizedSwap | None, str | None]:
    if not isinstance(result, dict):
        return None, "invalid_transaction_result"
    if source not in _PROGRAM_IDS_BY_SOURCE:
        return None, "semantic_unsupported_venue"
    meta = result.get("meta")
    if not isinstance(meta, dict) or meta.get("err") is not None:
        return None, "transaction_failed_or_meta_missing"
    wallet_index = scout._wallet_account_index(result, wallet)
    if wallet_index is None:
        return None, "tracked_scout_account_index_missing"
    deltas = tx._token_deltas_for_owner(result, wallet)
    native = semantic._net_native_wsol_flow(result, wallet=wallet, wallet_index=wallet_index, deltas=deltas)
    if native is not None and abs(native) > 1e-18:
        side = "buy" if native < 0 else "sell"
        endpoint, error = semantic._directional_endpoint(deltas, side=side)
        if endpoint is None:
            return None, error
        token_mint, token_delta = endpoint
        native_amount = abs(float(native))
    else:
        material = [
            (mint, float(delta)) for mint, delta in deltas.items()
            if mint != WSOL_MINT and abs(float(delta)) > 1e-18
        ]
        positive = [(mint, delta) for mint, delta in material if delta > 1e-18]
        negative = [(mint, delta) for mint, delta in material if delta < -1e-18]
        endpoints = positive if len(positive) == 1 else negative if len(negative) == 1 else []
        if len(endpoints) != 1:
            return None, "semantic_multiple_directional_endpoints" if material else "semantic_directional_endpoint_missing"
        token_mint, token_delta = endpoints[0]
        graph = _graph_quote_side_and_amount(
            result, wallet=wallet, source=source, token_mint=token_mint, token_delta=token_delta
        )
        if graph is None:
            return None, "semantic_native_wsol_direction_ambiguous"
        side, native_amount = graph
        if (side == "buy") != (token_delta > 0):
            return None, "semantic_native_wsol_direction_ambiguous"
    token_amount = abs(float(token_delta))
    if token_amount <= 0 or native_amount <= 0:
        return None, "semantic_amount_nonpositive"
    try:
        slot = int(result["slot"])
    except (KeyError, TypeError, ValueError):
        return None, "slot_missing"
    try:
        block_time = int(result.get("blockTime") or 0)
    except (TypeError, ValueError):
        block_time = 0
    observed_at = datetime.fromtimestamp(block_time, tz=timezone.utc) if block_time > 0 else trigger_received_at
    return NormalizedSwap(
        signature=signature, slot=slot, observed_at=observed_at, received_at=trigger_received_at,
        wallet=wallet, token_mint=str(token_mint), side=side, token_amount=token_amount,
        native_amount_sol=native_amount, reference_price_sol=native_amount / token_amount,
        source=f"solana-direct:{source}:{side}",
    ), None


def _prewarm_tasks(plane: Any) -> set[asyncio.Task[Any]]:
    value = getattr(plane, "_roi_venue_graph_prewarm_tasks", None)
    if isinstance(value, set):
        return value
    value = set()
    setattr(plane, "_roi_venue_graph_prewarm_tasks", value)
    return value


def _prewarm_last(plane: Any) -> dict[str, float]:
    value = getattr(plane, "_roi_venue_graph_prewarm_last", None)
    if isinstance(value, dict):
        return value
    value = {}
    setattr(plane, "_roi_venue_graph_prewarm_last", value)
    return value


def _prewarm_sem(plane: Any) -> asyncio.Semaphore:
    value = getattr(plane, "_roi_venue_graph_prewarm_sem", None)
    if isinstance(value, asyncio.Semaphore):
        return value
    value = asyncio.Semaphore(PREWARM_CONCURRENCY)
    setattr(plane, "_roi_venue_graph_prewarm_sem", value)
    return value


async def _prewarm_after_immediate_window(plane: Any, swap: NormalizedSwap, key: str) -> None:
    delay = max(0.0, (swap.observed_at.timestamp() + semantic.IMMEDIATE_ENTRY_WINDOW_SECONDS_UNCHANGED) - time.time())
    if delay:
        await asyncio.sleep(delay)
    async with _prewarm_sem(plane):
        collectors = getattr(getattr(plane, "service", None), "collectors", None)
        inner = getattr(collectors, "inner", None)
        coverage = getattr(inner, "refresh_coverage", None)
        candidate = getattr(inner, "refresh_candidate", None)
        try:
            now = datetime.now(timezone.utc)

            async def run() -> None:
                if callable(coverage):
                    await coverage(swap.token_mint, now, current_swap=swap)
                if callable(candidate):
                    await candidate(swap.token_mint, now, current_swap=swap)

            await asyncio.wait_for(run(), timeout=PREWARM_TIMEOUT_SECONDS)
            semantic._persist_risk_readthrough(plane, swap, as_of=datetime.now(timezone.utc))
            _inc(plane, "prewarm_completed")
        except asyncio.TimeoutError:
            _inc(plane, "prewarm_timeouts")
        except asyncio.CancelledError:
            raise
        except Exception:
            _inc(plane, "prewarm_errors")
        finally:
            _prewarm_last(plane)[key] = time.monotonic()


def _schedule_prewarm(plane: Any, swap: NormalizedSwap) -> None:
    key = f"{swap.token_mint}:{semantic._venue(swap)}"
    last = _prewarm_last(plane).get(key)
    if last is not None and time.monotonic() - last < PREWARM_MIN_INTERVAL_SECONDS:
        _inc(plane, "prewarm_rate_skips")
        return
    try:
        task = asyncio.create_task(
            _prewarm_after_immediate_window(plane, swap, key),
            name=f"candidate-prewarm:{swap.token_mint[:8]}",
        )
    except RuntimeError:
        return
    tasks = _prewarm_tasks(plane)
    tasks.add(task)
    _inc(plane, "prewarm_scheduled")
    task.add_done_callback(lambda completed: tasks.discard(completed))


def _normalize_tracked_wallet_v2(
    result: Any, *, signature: str, trigger_received_at: datetime, wallet: str, source_hint: str | None = None
) -> tuple[NormalizedSwap | None, str | None]:
    plane = scout._SCOUT_HYDRATION_PLANE.get()
    if plane is not None:
        _inc(plane, "attempts")
    if not isinstance(result, dict):
        return None, "invalid_transaction_result"
    source, error = _source_for_transaction(result, source_hint)
    if source is None:
        if plane is not None:
            _inc(plane, "source_failures")
        return None, error
    if plane is not None and source not in tx.transaction_sources(result):
        _inc(plane, "indexed_source_recoveries")
    native_before = None
    if plane is not None:
        try:
            deltas = tx._token_deltas_for_owner(result, wallet)
            wallet_index = scout._wallet_account_index(result, wallet)
            native_before = (
                semantic._net_native_wsol_flow(result, wallet=wallet, wallet_index=int(wallet_index), deltas=deltas)
                if wallet_index is not None
                else None
            )
            if native_before is None or abs(native_before) <= 1e-18:
                _inc(plane, "graph_attempts")
        except Exception:
            pass
    swap, error = _decode_supported_venue(
        result, signature=signature, trigger_received_at=trigger_received_at, wallet=wallet, source=source
    )
    if swap is None:
        if plane is not None:
            _inc(plane, "failures")
            if error == "semantic_native_wsol_direction_ambiguous":
                _inc(plane, "graph_unresolved")
        return None, error
    if plane is not None:
        if native_before is None or abs(native_before) <= 1e-18:
            _inc(plane, "graph_resolved")
        try:
            inserted = semantic._persist_opportunity(plane, swap)
        except Exception:
            _inc(plane, "ledger_persistence_errors")
            return None, "semantic_candidate_ledger_persist_failed"
        semantic._persist_risk_readthrough(plane, swap)
        if inserted:
            _schedule_prewarm(plane, swap)
        _inc(plane, "decoded")
    return swap, None


setattr(_normalize_tracked_wallet_v2, "_roi_venue_native_candidate_graph", True)


def _status_v2(self: DirectSolanaIngestionPlane) -> dict[str, Any]:
    if _ORIGINAL_STATUS is None:
        raise RuntimeError("venue-native graph repair not installed")
    payload = _ORIGINAL_STATUS(self)
    payload["venue_native_candidate_graph_repair"] = {
        "installed": True,
        "version": REPAIR_VERSION,
        "prewarm_version": PREWARM_VERSION,
        "program_id_index_resolution_supported": True,
        "same_venue_instruction_transfer_graph_supported": True,
        "sponsored_or_relayed_scout_swap_supported": True,
        "wallet_native_balance_required_when_quote_graph_proven": False,
        "comparable_multiple_quote_legs_fail_closed": True,
        "attempts_session": int(getattr(self, "_roi_venue_graph_attempts", 0) or 0),
        "decoded_session": int(getattr(self, "_roi_venue_graph_decoded", 0) or 0),
        "graph_attempts_session": int(getattr(self, "_roi_venue_graph_graph_attempts", 0) or 0),
        "graph_resolved_session": int(getattr(self, "_roi_venue_graph_graph_resolved", 0) or 0),
        "graph_unresolved_session": int(getattr(self, "_roi_venue_graph_graph_unresolved", 0) or 0),
        "indexed_source_recoveries_session": int(getattr(self, "_roi_venue_graph_indexed_source_recoveries", 0) or 0),
        "prewarm_scheduled_session": int(getattr(self, "_roi_venue_graph_prewarm_scheduled", 0) or 0),
        "prewarm_completed_session": int(getattr(self, "_roi_venue_graph_prewarm_completed", 0) or 0),
        "prewarm_timeouts_session": int(getattr(self, "_roi_venue_graph_prewarm_timeouts", 0) or 0),
        "prewarm_errors_session": int(getattr(self, "_roi_venue_graph_prewarm_errors", 0) or 0),
        "continuation_prewarm_starts_after_immediate_window": True,
        "continuation_prewarm_has_entry_authority": False,
        "immediate_entry_window_seconds_unchanged": semantic.IMMEDIATE_ENTRY_WINDOW_SECONDS_UNCHANGED,
        "candidate_processing_target_seconds_unchanged": 5.0,
        "max_chase_fraction_unchanged": semantic.MAX_CHASE_FRACTION_UNCHANGED,
        "strategy_thresholds_changed": False,
        "full_market_scope_reduced": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }
    policy = payload.setdefault("provider_runtime_policy", {})
    if isinstance(policy, dict):
        policy.update(
            {
                "venue_native_instruction_transfer_graph_candidate_attribution": True,
                "program_id_index_resolution_supported": True,
                "continuation_risk_prewarm_after_immediate_window": True,
                "continuation_risk_prewarm_has_entry_authority": False,
                "candidate_entry_window_unchanged": True,
                "strategy_thresholds_unchanged": True,
                "full_raw_market_scope_preserved": True,
                "paper_only_authority_unchanged": True,
                "signing_or_submission_available": False,
            }
        )
    return payload


setattr(_status_v2, "_roi_venue_native_candidate_graph", True)


def install_venue_native_candidate_graph_repair() -> None:
    global _ORIGINAL_STATUS
    current = scout._normalize_tracked_wallet
    if not bool(getattr(current, "_roi_venue_native_candidate_graph", False)):
        scout._normalize_tracked_wallet = _normalize_tracked_wallet_v2  # type: ignore[assignment]
    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_venue_native_candidate_graph", False)):
        _ORIGINAL_STATUS = current_status
        try:
            _status_v2.__dict__.update(getattr(current_status, "__dict__", {}))
        except Exception:
            pass
        DirectSolanaIngestionPlane.status = _status_v2  # type: ignore[method-assign]


__all__ = [
    "REPAIR_VERSION",
    "PREWARM_VERSION",
    "_decode_supported_venue",
    "_indexed_transaction_sources",
    "_normalize_tracked_wallet_v2",
    "_source_for_transaction",
    "_venue_instruction_groups",
    "install_venue_native_candidate_graph_repair",
]
