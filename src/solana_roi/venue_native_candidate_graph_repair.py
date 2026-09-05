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

REPAIR_VERSION = "venue-native-graph-first-attribution-v3"
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


def _raw_account_keys(result: dict[str, Any]) -> list[str]:
    return tx._account_keys(result)


def _loaded_address_keys(result: dict[str, Any]) -> list[str]:
    meta = result.get("meta")
    loaded = meta.get("loadedAddresses") if isinstance(meta, dict) else None
    if not isinstance(loaded, dict):
        return []
    out: list[str] = []
    for field in ("writable", "readonly"):
        values = loaded.get(field)
        if not isinstance(values, list):
            continue
        for value in values:
            if isinstance(value, str):
                out.append(value)
            elif isinstance(value, dict):
                out.append(str(value.get("pubkey") or ""))
            else:
                out.append("")
    return out


def _account_keys(result: dict[str, Any]) -> list[str]:
    # Standard RPC versioned transactions index pre/post balances over static keys
    # followed by loaded writable + readonly address-table entries. Preserve that
    # canonical order so token-account ownership is available to the venue graph.
    return [*_raw_account_keys(result), *_loaded_address_keys(result)]


def _wallet_account_index(result: dict[str, Any], wallet: str) -> int | None:
    keys = _account_keys(result)
    try:
        return keys.index(wallet)
    except ValueError:
        return None


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


def _raw_token_amount(row: dict[str, Any]) -> tuple[int, int] | None:
    ui = row.get("uiTokenAmount")
    if not isinstance(ui, dict):
        return None
    try:
        amount = int(str(ui.get("amount") or "0"))
        decimals = int(ui.get("decimals") or 0)
    except (TypeError, ValueError):
        return None
    if decimals < 0 or decimals > 18:
        return None
    return amount, decimals


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
            raw = _raw_token_amount(row)
            decimals = int(raw[1]) if raw is not None else 0
            if account and mint:
                prior = out.get(account, ("", "", decimals))
                out[account] = (owner or prior[0], mint or prior[1], decimals)

    # New ATAs and temporary token accounts can be created during the same swap.
    # Parsed create/initialize instructions provide point-in-time owner/mint hints
    # even when one side of the token-balance metadata is absent.
    for _parent, row in _walk_instruction_rows(result):
        parsed = row.get("parsed")
        if not isinstance(parsed, dict):
            continue
        info = parsed.get("info")
        if not isinstance(info, dict):
            continue
        account = str(info.get("account") or info.get("newAccount") or "")
        owner = str(info.get("owner") or info.get("wallet") or "")
        mint = str(info.get("mint") or "")
        if account and (owner or mint):
            prior = out.get(account, ("", "", 0))
            out[account] = (owner or prior[0], mint or prior[1], prior[2])
    return out


def _parsed_token_transfer(
    row: dict[str, Any], metadata: dict[str, tuple[str, str, int]]
) -> tuple[str, str, float, str, str] | None:
    parsed = row.get("parsed")
    if not isinstance(parsed, dict) or str(parsed.get("type") or "") not in {"transfer", "transferChecked"}:
        return None
    info = parsed.get("info")
    if not isinstance(info, dict) or "lamports" in info:
        return None
    source = str(info.get("source") or "")
    destination = str(info.get("destination") or "")
    if not source or not destination:
        return None
    source_meta = metadata.get(source)
    destination_meta = metadata.get(destination)
    mint = str(info.get("mint") or (source_meta or destination_meta or ("", "", 0))[1] or "")
    authority = str(info.get("authority") or info.get("owner") or "")
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
            account_meta = source_meta or destination_meta
            decimals = int(account_meta[2]) if account_meta is not None else 0
            amount = raw / (10 ** decimals)
        except (TypeError, ValueError):
            amount = None
    if amount is None or amount <= 0 or not mint:
        return None
    return source, destination, float(amount), mint, authority


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
            if _instruction_program_id(row, keys) in target_programs:
                groups.append([row, *by_parent.get(index, [])])
    if not groups:
        for children in by_parent.values():
            if any(_instruction_program_id(row, keys) in target_programs for row in children):
                groups.append(children)
    return groups


def _graph_swap_facts(
    result: dict[str, Any], *, wallet: str, source: str
) -> tuple[tuple[str, str, float, float] | None, str | None]:
    metadata = _token_account_metadata(result)
    candidates: list[tuple[str, str, float, float]] = []
    saw_actor_token_leg = False
    saw_ambiguous_actor_flow = False

    for group in _venue_instruction_groups(result, source):
        token_flow: dict[str, float] = {}
        wsol_flow = 0.0
        native_flow = 0.0
        for row in group:
            transfer = _parsed_token_transfer(row, metadata)
            if transfer is not None:
                from_account, to_account, amount, mint, authority = transfer
                from_meta = metadata.get(from_account)
                to_meta = metadata.get(to_account)
                from_owner = str((from_meta or ("", "", 0))[0])
                to_owner = str((to_meta or ("", "", 0))[0])
                from_actor = from_owner == wallet or (not from_owner and authority == wallet)
                to_actor = to_owner == wallet
                if not from_actor and not to_actor:
                    continue
                if mint == WSOL_MINT:
                    if to_actor:
                        wsol_flow += amount
                    if from_actor:
                        wsol_flow -= amount
                else:
                    saw_actor_token_leg = True
                    if to_actor:
                        token_flow[mint] = token_flow.get(mint, 0.0) + amount
                    if from_actor:
                        token_flow[mint] = token_flow.get(mint, 0.0) - amount
                continue

            native = _parsed_native_transfer(row)
            if native is not None:
                from_account, to_account, amount = native
                # If SOL funds a wallet-owned temporary token account, the later
                # WSOL transfer represents the same quote value. Track SOL here but
                # prefer proven WSOL flow below to avoid double counting wrapping.
                if to_account == wallet:
                    native_flow += amount
                if from_account == wallet:
                    native_flow -= amount

        material = [(mint, flow) for mint, flow in token_flow.items() if abs(flow) > 1e-18]
        if len(material) > 1:
            saw_ambiguous_actor_flow = True
            continue
        if len(material) != 1:
            continue
        token_mint, token_flow_amount = material[0]
        side = "buy" if token_flow_amount > 0 else "sell"
        quote_flow = wsol_flow if abs(wsol_flow) > 1e-18 else native_flow
        expected_quote_sign = -1.0 if side == "buy" else 1.0
        if quote_flow * expected_quote_sign <= 1e-18:
            # A close-account unwrap or provider encoding can omit a parsed native
            # transfer. Use the authoritative wallet native/WSOL net only as a quote
            # fallback after the venue graph has already proven the traded token.
            wallet_index = _wallet_account_index(result, wallet)
            deltas = tx._token_deltas_for_owner(result, wallet)
            native = (
                semantic._net_native_wsol_flow(result, wallet=wallet, wallet_index=wallet_index, deltas=deltas)
                if wallet_index is not None
                else None
            )
            if native is None or float(native) * expected_quote_sign <= 1e-18:
                continue
            quote_flow = float(native)
        candidates.append((side, token_mint, abs(float(token_flow_amount)), abs(float(quote_flow))))

    if saw_ambiguous_actor_flow:
        return None, "semantic_multiple_directional_endpoints"
    if not candidates:
        return None, "semantic_graph_actor_legs_missing" if not saw_actor_token_leg else "semantic_native_wsol_direction_ambiguous"

    identities = {(side, mint) for side, mint, _token, _quote in candidates}
    if len(identities) != 1:
        return None, "semantic_multiple_directional_endpoints"
    side, mint = next(iter(identities))
    token_amount = sum(token for candidate_side, candidate_mint, token, _quote in candidates if (candidate_side, candidate_mint) == (side, mint))
    native_amount = sum(quote for candidate_side, candidate_mint, _token, quote in candidates if (candidate_side, candidate_mint) == (side, mint))
    if token_amount <= 0 or native_amount <= 0:
        return None, "semantic_amount_nonpositive"
    return (side, mint, token_amount, native_amount), None


def _balance_fallback(
    result: dict[str, Any], *, wallet: str
) -> tuple[tuple[str, str, float, float] | None, str | None]:
    wallet_index = _wallet_account_index(result, wallet)
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
        return (side, str(token_mint), abs(float(token_delta)), abs(float(native))), None
    material = [(mint, float(delta)) for mint, delta in deltas.items() if mint != WSOL_MINT and abs(float(delta)) > 1e-18]
    if not material:
        return None, "semantic_directional_endpoint_missing"
    positive = [(mint, delta) for mint, delta in material if delta > 1e-18]
    negative = [(mint, delta) for mint, delta in material if delta < -1e-18]
    endpoints = positive if len(positive) == 1 else negative if len(negative) == 1 else []
    if len(endpoints) != 1:
        return None, "semantic_multiple_directional_endpoints"
    return None, "semantic_native_wsol_direction_ambiguous"


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
    if _wallet_account_index(result, wallet) is None:
        return None, "tracked_scout_account_index_missing"

    graph, graph_error = _graph_swap_facts(result, wallet=wallet, source=source)
    used_graph = graph is not None
    if graph is None:
        if graph_error == "semantic_multiple_directional_endpoints":
            return None, graph_error
        graph, fallback_error = _balance_fallback(result, wallet=wallet)
        if graph is None:
            # Keep the pre-v3 canonical failure vocabulary when there is no proven
            # wallet token endpoint at all; this makes prospective improvement easy
            # to measure against the exact 76/76 production boundary.
            if graph_error == "semantic_graph_actor_legs_missing":
                return None, fallback_error or "semantic_directional_endpoint_missing"
            return None, graph_error or fallback_error

    side, token_mint, token_amount, native_amount = graph
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
    swap = NormalizedSwap(
        signature=signature,
        slot=slot,
        observed_at=observed_at,
        received_at=trigger_received_at,
        wallet=wallet,
        token_mint=str(token_mint),
        side=side,
        token_amount=float(token_amount),
        native_amount_sol=float(native_amount),
        reference_price_sol=float(native_amount) / float(token_amount),
        source=f"solana-direct:{source}:{side}",
    )
    setattr(swap, "_roi_graph_first", used_graph) if hasattr(swap, "__dict__") else None
    return swap, None


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
        task = asyncio.create_task(_prewarm_after_immediate_window(plane, swap, key), name=f"candidate-prewarm:{swap.token_mint[:8]}")
    except RuntimeError:
        return
    tasks = _prewarm_tasks(plane)
    tasks.add(task)
    _inc(plane, "prewarm_scheduled")
    task.add_done_callback(lambda completed: tasks.discard(completed))


def _normalize_tracked_wallet_v2(
    result: Any, *, signature: str, trigger_received_at: datetime, wallet: str, source_hint: str | None = None
) -> tuple[NormalizedSwap | None, str | None]:
    # Function name is retained for composition compatibility; behavior is v3.
    plane = scout._SCOUT_HYDRATION_PLANE.get()
    if plane is not None:
        _inc(plane, "attempts")
        _inc(plane, "graph_attempts")
    if not isinstance(result, dict):
        return None, "invalid_transaction_result"
    source, error = _source_for_transaction(result, source_hint)
    if source is None:
        if plane is not None:
            _inc(plane, "source_failures")
        return None, error
    if plane is not None and source not in tx.transaction_sources(result):
        _inc(plane, "indexed_source_recoveries")
    loaded_count = len(_loaded_address_keys(result))
    if plane is not None and loaded_count:
        _inc(plane, "loaded_address_transactions")

    swap, error = _decode_supported_venue(
        result, signature=signature, trigger_received_at=trigger_received_at, wallet=wallet, source=source
    )
    if swap is None:
        if plane is not None:
            _inc(plane, "failures")
            _inc(plane, "graph_unresolved")
        return None, error

    # Determine telemetry authority without changing the candidate itself.
    balance_graph, _ = _balance_fallback(result, wallet=wallet)
    if plane is not None:
        if balance_graph is None:
            _inc(plane, "graph_resolved")
            _inc(plane, "graph_first_resolved_without_balance_endpoint")
        else:
            _inc(plane, "graph_or_balance_resolved")
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
setattr(_normalize_tracked_wallet_v2, "_roi_semantic_candidate_attribution", True)


def _status_v2(self: DirectSolanaIngestionPlane) -> dict[str, Any]:
    if _ORIGINAL_STATUS is None:
        raise RuntimeError("venue-native graph repair not installed")
    payload = _ORIGINAL_STATUS(self)
    payload["venue_native_candidate_graph_repair"] = {
        "installed": True,
        "version": REPAIR_VERSION,
        "prewarm_version": PREWARM_VERSION,
        "program_id_index_resolution_supported": True,
        "address_lookup_table_account_resolution_supported": True,
        "same_venue_instruction_transfer_graph_supported": True,
        "graph_first_before_owner_balance_delta": True,
        "final_wallet_token_delta_required_for_candidate": False,
        "temporary_account_owner_hints_supported": True,
        "split_quote_fee_legs_aggregated_by_actor_flow": True,
        "sponsored_or_relayed_scout_swap_supported": True,
        "wallet_native_balance_required_when_quote_graph_proven": False,
        "multiple_actor_token_endpoints_fail_closed": True,
        "attempts_session": int(getattr(self, "_roi_venue_graph_attempts", 0) or 0),
        "decoded_session": int(getattr(self, "_roi_venue_graph_decoded", 0) or 0),
        "graph_attempts_session": int(getattr(self, "_roi_venue_graph_graph_attempts", 0) or 0),
        "graph_resolved_session": int(getattr(self, "_roi_venue_graph_graph_resolved", 0) or 0),
        "graph_first_resolved_without_balance_endpoint_session": int(getattr(self, "_roi_venue_graph_graph_first_resolved_without_balance_endpoint", 0) or 0),
        "graph_or_balance_resolved_session": int(getattr(self, "_roi_venue_graph_graph_or_balance_resolved", 0) or 0),
        "graph_unresolved_session": int(getattr(self, "_roi_venue_graph_graph_unresolved", 0) or 0),
        "loaded_address_transactions_session": int(getattr(self, "_roi_venue_graph_loaded_address_transactions", 0) or 0),
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
                "venue_graph_is_primary_candidate_attribution_authority": True,
                "final_wallet_token_delta_required_for_candidate": False,
                "address_lookup_table_account_resolution_supported": True,
                "temporary_token_account_owner_hints_supported": True,
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
    "_account_keys",
    "_decode_supported_venue",
    "_graph_swap_facts",
    "_indexed_transaction_sources",
    "_normalize_tracked_wallet_v2",
    "_source_for_transaction",
    "_token_account_metadata",
    "_venue_instruction_groups",
    "install_venue_native_candidate_graph_repair",
]
