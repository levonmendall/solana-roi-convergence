from __future__ import annotations

import asyncio
import json
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Callable

from . import scout_candidate_continuity_repair as scout
from . import venue_native_candidate_graph_repair as venue
from .direct_solana import DirectSolanaIngestionPlane
from .ingestion import LAMPORTS_PER_SOL
from .observation import WSOL_MINT
from .pump_raw import _base58_decode

REPAIR_VERSION = "post161-scout-attribution-observability-v1"
RAW_TRANSFER_DECODER_VERSION = "compiled-spl-system-transfer-v1"
DIAGNOSTIC_VERSION = "sanitized-scout-failure-shape-v1"
TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
SYSTEM_PROGRAM_ID = "11111111111111111111111111111111"
MAX_DIAGNOSTIC_ROWS = 256
MAX_DIAGNOSTIC_TASKS = 4
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False

_ORIGINAL_NORMALIZE: Callable[..., Any] | None = None
_ORIGINAL_STATUS: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_GRAPH: Callable[..., Any] | None = None


def _inc(plane: Any, name: str, amount: int = 1) -> None:
    attr = f"_roi_post161_{name}"
    setattr(plane, attr, int(getattr(plane, attr, 0) or 0) + int(amount))


def _instruction_accounts(row: dict[str, Any], keys: list[str]) -> list[str]:
    values = row.get("accounts")
    if not isinstance(values, list):
        return []
    out: list[str] = []
    for value in values:
        if isinstance(value, int):
            out.append(keys[value] if 0 <= value < len(keys) else "")
            continue
        if isinstance(value, str):
            try:
                index = int(value)
            except ValueError:
                out.append(value)
            else:
                out.append(keys[index] if 0 <= index < len(keys) else "")
            continue
        if isinstance(value, dict):
            out.append(str(value.get("pubkey") or ""))
            continue
        out.append("")
    return out


def _compiled_data(row: dict[str, Any]) -> bytes | None:
    data = row.get("data")
    if not isinstance(data, str) or not data:
        return None
    try:
        return _base58_decode(data)
    except (TypeError, ValueError):
        return None


def _raw_token_transfer(
    row: dict[str, Any],
    *,
    keys: list[str],
    metadata: dict[str, tuple[str, str, int]],
) -> tuple[str, str, float, str, str] | None:
    if venue._instruction_program_id(row, keys) != TOKEN_PROGRAM_ID:
        return None
    payload = _compiled_data(row)
    accounts = _instruction_accounts(row, keys)
    if payload is None or not payload or len(accounts) < 3:
        return None
    opcode = int(payload[0])
    if opcode == 3:  # SPL Token Transfer
        if len(payload) < 9:
            return None
        source, destination, authority = accounts[0], accounts[1], accounts[2]
        source_meta = metadata.get(source)
        destination_meta = metadata.get(destination)
        account_meta = source_meta or destination_meta
        if account_meta is None:
            return None
        mint = str(account_meta[1] or "")
        decimals = int(account_meta[2])
        raw_amount = int.from_bytes(payload[1:9], "little", signed=False)
    elif opcode == 12:  # SPL Token TransferChecked
        if len(payload) < 10 or len(accounts) < 4:
            return None
        source, mint, destination, authority = accounts[0], accounts[1], accounts[2], accounts[3]
        raw_amount = int.from_bytes(payload[1:9], "little", signed=False)
        decimals = int(payload[9])
    else:
        return None
    if not source or not destination or not mint or raw_amount <= 0 or decimals < 0 or decimals > 18:
        return None
    return source, destination, raw_amount / (10 ** decimals), mint, authority


def _raw_native_transfer(row: dict[str, Any], *, keys: list[str]) -> tuple[str, str, float] | None:
    if venue._instruction_program_id(row, keys) != SYSTEM_PROGRAM_ID:
        return None
    payload = _compiled_data(row)
    accounts = _instruction_accounts(row, keys)
    if payload is None or len(payload) < 12 or len(accounts) < 2:
        return None
    opcode = int.from_bytes(payload[:4], "little", signed=False)
    if opcode != 2:  # SystemInstruction::Transfer
        return None
    lamports = int.from_bytes(payload[4:12], "little", signed=False)
    if lamports <= 0 or not accounts[0] or not accounts[1]:
        return None
    return accounts[0], accounts[1], lamports / LAMPORTS_PER_SOL


def _graph_swap_facts_v4(
    result: dict[str, Any], *, wallet: str, source: str
) -> tuple[tuple[str, str, float, float] | None, str | None]:
    metadata = venue._token_account_metadata(result)
    keys = venue._account_keys(result)
    candidates: list[tuple[str, str, float, float]] = []
    saw_actor_token_leg = False
    saw_ambiguous_actor_flow = False
    raw_transfer_used = False

    for group in venue._venue_instruction_groups(result, source):
        token_flow: dict[str, float] = {}
        wsol_flow = 0.0
        native_flow = 0.0
        for row in group:
            transfer = venue._parsed_token_transfer(row, metadata)
            if transfer is None:
                transfer = _raw_token_transfer(row, keys=keys, metadata=metadata)
                raw_transfer_used = raw_transfer_used or transfer is not None
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

            native = venue._parsed_native_transfer(row)
            if native is None:
                native = _raw_native_transfer(row, keys=keys)
                raw_transfer_used = raw_transfer_used or native is not None
            if native is not None:
                from_account, to_account, amount = native
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
            wallet_index = venue._wallet_account_index(result, wallet)
            deltas = venue.tx._token_deltas_for_owner(result, wallet)
            native = (
                venue.semantic._net_native_wsol_flow(
                    result, wallet=wallet, wallet_index=wallet_index, deltas=deltas
                )
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
    token_amount = sum(
        token
        for candidate_side, candidate_mint, token, _quote in candidates
        if (candidate_side, candidate_mint) == (side, mint)
    )
    native_amount = sum(
        quote
        for candidate_side, candidate_mint, _token, quote in candidates
        if (candidate_side, candidate_mint) == (side, mint)
    )
    if token_amount <= 0 or native_amount <= 0:
        return None, "semantic_amount_nonpositive"
    plane = scout._SCOUT_HYDRATION_PLANE.get()
    if plane is not None and raw_transfer_used:
        _inc(plane, "raw_transfer_graph_resolved")
    return (side, mint, token_amount, native_amount), None


def _diagnostic_facts(
    result: Any, *, wallet: str, source_hint: str | None, reason: str
) -> dict[str, Any]:
    if not isinstance(result, dict):
        return {"reason": reason, "shape": "invalid_result"}
    keys = venue._account_keys(result)
    metadata = venue._token_account_metadata(result)
    source, source_error = venue._source_for_transaction(result, source_hint)
    source_name = source or str(source_hint or "") or "unknown"
    groups = venue._venue_instruction_groups(result, source) if source else []
    transaction = result.get("transaction")
    message = transaction.get("message") if isinstance(transaction, dict) else None
    header = message.get("header") if isinstance(message, dict) else None
    required_signers = int(header.get("numRequiredSignatures") or 0) if isinstance(header, dict) else 0
    wallet_index = venue._wallet_account_index(result, wallet)
    wallet_signer = wallet_index is not None and wallet_index < required_signers

    parsed_token = raw_token = parsed_native = raw_native = 0
    token_program_raw_opcodes: Counter[str] = Counter()
    supported_wallet_positions: list[list[int]] = []
    instruction_programs: Counter[str] = Counter()
    for group in groups:
        group_positions: list[int] = []
        for row in group:
            program_id = str(venue._instruction_program_id(row, keys) or "")
            if program_id:
                instruction_programs[program_id] += 1
            accounts = _instruction_accounts(row, keys)
            group_positions.extend(index for index, account in enumerate(accounts) if account == wallet)
            if venue._parsed_token_transfer(row, metadata) is not None:
                parsed_token += 1
            elif _raw_token_transfer(row, keys=keys, metadata=metadata) is not None:
                raw_token += 1
            if venue._parsed_native_transfer(row) is not None:
                parsed_native += 1
            elif _raw_native_transfer(row, keys=keys) is not None:
                raw_native += 1
            if program_id == TOKEN_PROGRAM_ID:
                payload = _compiled_data(row)
                if payload:
                    token_program_raw_opcodes[str(int(payload[0]))] += 1
        supported_wallet_positions.append(sorted(set(group_positions)))

    deltas = venue.tx._token_deltas_for_owner(result, wallet)
    material_owner_deltas = {
        mint: float(delta)
        for mint, delta in deltas.items()
        if mint != WSOL_MINT and abs(float(delta)) > 1e-18
    }
    meta = result.get("meta") if isinstance(result.get("meta"), dict) else {}
    wallet_owned_accounts = sum(1 for owner, _mint, _decimals in metadata.values() if owner == wallet)
    payload = {
        "reason": reason,
        "source": source_name,
        "source_error": source_error,
        "wallet_static_or_loaded_index": wallet_index,
        "wallet_signer": wallet_signer,
        "supported_instruction_group_count": len(groups),
        "wallet_positions_in_supported_groups": supported_wallet_positions,
        "wallet_owned_token_account_count": wallet_owned_accounts,
        "owner_material_token_delta_count": len(material_owner_deltas),
        "owner_material_token_delta_signs": sorted(1 if value > 0 else -1 for value in material_owner_deltas.values()),
        "parsed_token_transfer_count": parsed_token,
        "raw_token_transfer_count": raw_token,
        "parsed_native_transfer_count": parsed_native,
        "raw_native_transfer_count": raw_native,
        "token_program_raw_opcodes": dict(sorted(token_program_raw_opcodes.items())),
        "loaded_address_count": len(venue._loaded_address_keys(result)),
        "pre_token_balance_rows": len(meta.get("preTokenBalances") or []),
        "post_token_balance_rows": len(meta.get("postTokenBalances") or []),
        "instruction_program_counts": dict(sorted(instruction_programs.items())),
    }
    shape_basis = {
        "source": payload["source"],
        "reason": reason,
        "groups": payload["supported_instruction_group_count"],
        "wallet_positions": payload["wallet_positions_in_supported_groups"],
        "owned_accounts": wallet_owned_accounts,
        "owner_deltas": payload["owner_material_token_delta_count"],
        "parsed_token": parsed_token,
        "raw_token": raw_token,
        "parsed_native": parsed_native,
        "raw_native": raw_native,
        "loaded": payload["loaded_address_count"],
    }
    payload["shape"] = json.dumps(shape_basis, sort_keys=True, separators=(",", ":"))
    return payload


def _diagnostic_tasks(plane: Any) -> set[asyncio.Task[Any]]:
    tasks = getattr(plane, "_roi_post161_diagnostic_tasks", None)
    if isinstance(tasks, set):
        return tasks
    tasks = set()
    setattr(plane, "_roi_post161_diagnostic_tasks", tasks)
    return tasks


def _persist_diagnostic_sync(
    plane: Any,
    *,
    signature: str,
    wallet: str,
    source: str,
    reason: str,
    facts: dict[str, Any],
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with plane.store._lock, plane.store.db:
        plane.store.db.execute(
            "CREATE TABLE IF NOT EXISTS scout_attribution_failure_diagnostics ("
            "signature TEXT PRIMARY KEY, observed_at TEXT NOT NULL, wallet TEXT NOT NULL, "
            "source TEXT NOT NULL, reason TEXT NOT NULL, shape TEXT NOT NULL, facts_json TEXT NOT NULL, "
            "diagnostic_version TEXT NOT NULL)"
        )
        plane.store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_scout_attribution_failure_observed "
            "ON scout_attribution_failure_diagnostics(observed_at)"
        )
        plane.store.db.execute(
            "INSERT OR IGNORE INTO scout_attribution_failure_diagnostics("
            "signature,observed_at,wallet,source,reason,shape,facts_json,diagnostic_version) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (
                signature,
                now,
                wallet,
                source,
                reason,
                str(facts.get("shape") or "unknown")[:2000],
                json.dumps(facts, sort_keys=True, separators=(",", ":"))[:12000],
                DIAGNOSTIC_VERSION,
            ),
        )
        plane.store.db.execute(
            "DELETE FROM scout_attribution_failure_diagnostics WHERE signature IN ("
            "SELECT signature FROM scout_attribution_failure_diagnostics "
            "ORDER BY observed_at DESC LIMIT -1 OFFSET ?)",
            (MAX_DIAGNOSTIC_ROWS,),
        )
    _inc(plane, "diagnostic_persisted")


def _record_shape_in_memory(plane: Any, facts: dict[str, Any], reason: str) -> None:
    shapes = getattr(plane, "_roi_post161_shape_counts", None)
    if not isinstance(shapes, Counter):
        shapes = Counter()
        setattr(plane, "_roi_post161_shape_counts", shapes)
    reasons = getattr(plane, "_roi_post161_reason_counts", None)
    if not isinstance(reasons, Counter):
        reasons = Counter()
        setattr(plane, "_roi_post161_reason_counts", reasons)
    shapes[str(facts.get("shape") or "unknown")] += 1
    reasons[reason] += 1


def _schedule_diagnostic(
    plane: Any,
    *,
    result: Any,
    signature: str,
    wallet: str,
    source_hint: str | None,
    reason: str,
) -> None:
    facts = _diagnostic_facts(result, wallet=wallet, source_hint=source_hint, reason=reason)
    _record_shape_in_memory(plane, facts, reason)
    _inc(plane, "diagnostic_observed")
    tasks = _diagnostic_tasks(plane)
    if len(tasks) >= MAX_DIAGNOSTIC_TASKS:
        _inc(plane, "diagnostic_persistence_backpressure")
        return
    try:
        task = asyncio.create_task(
            asyncio.to_thread(
                _persist_diagnostic_sync,
                plane,
                signature=signature,
                wallet=wallet,
                source=str(facts.get("source") or source_hint or "unknown"),
                reason=reason,
                facts=facts,
            ),
            name=f"scout-attribution-diag:{signature[:10]}",
        )
    except RuntimeError:
        _inc(plane, "diagnostic_persistence_unavailable")
        return
    tasks.add(task)
    task.add_done_callback(lambda completed: tasks.discard(completed))


def _normalize_with_post161_evidence(
    result: Any,
    *,
    signature: str,
    trigger_received_at: datetime,
    wallet: str,
    source_hint: str | None = None,
) -> tuple[Any, str | None]:
    if _ORIGINAL_NORMALIZE is None:
        raise RuntimeError("post161 candidate attribution repair not installed")
    swap, error = _ORIGINAL_NORMALIZE(
        result,
        signature=signature,
        trigger_received_at=trigger_received_at,
        wallet=wallet,
        source_hint=source_hint,
    )
    plane = scout._SCOUT_HYDRATION_PLANE.get()
    if swap is not None:
        if plane is not None:
            _inc(plane, "normalized")
        return swap, error
    if plane is not None:
        _schedule_diagnostic(
            plane,
            result=result,
            signature=signature,
            wallet=wallet,
            source_hint=source_hint,
            reason=str(error or "unknown"),
        )
    return None, error


setattr(_normalize_with_post161_evidence, "_roi_post161_candidate_attribution", True)
setattr(_normalize_with_post161_evidence, "_roi_venue_native_candidate_graph", True)
setattr(_normalize_with_post161_evidence, "_roi_semantic_candidate_attribution", True)


def _status_with_post161(original: Callable[[Any], dict[str, Any]]) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        shapes = getattr(self, "_roi_post161_shape_counts", Counter())
        reasons = getattr(self, "_roi_post161_reason_counts", Counter())
        top_shapes = [
            {"shape": shape, "count": int(count)}
            for shape, count in (shapes.most_common(10) if isinstance(shapes, Counter) else [])
        ]
        payload["post161_candidate_attribution_repair"] = {
            "installed": True,
            "version": REPAIR_VERSION,
            "raw_transfer_decoder_version": RAW_TRANSFER_DECODER_VERSION,
            "diagnostic_version": DIAGNOSTIC_VERSION,
            "compiled_spl_token_transfer_supported": True,
            "compiled_spl_token_transfer_checked_supported": True,
            "compiled_system_transfer_supported": True,
            "raw_transfer_graph_resolved_session": int(getattr(self, "_roi_post161_raw_transfer_graph_resolved", 0) or 0),
            "normalized_session": int(getattr(self, "_roi_post161_normalized", 0) or 0),
            "diagnostic_observed_session": int(getattr(self, "_roi_post161_diagnostic_observed", 0) or 0),
            "diagnostic_persisted_session": int(getattr(self, "_roi_post161_diagnostic_persisted", 0) or 0),
            "diagnostic_persistence_backpressure_session": int(getattr(self, "_roi_post161_diagnostic_persistence_backpressure", 0) or 0),
            "failure_reason_counts_session": dict(reasons) if isinstance(reasons, Counter) else {},
            "top_failure_shapes_session": top_shapes,
            "diagnostic_rows_bounded": MAX_DIAGNOSTIC_ROWS,
            "raw_transaction_payload_persisted": False,
            "credential_or_secret_material_persisted": False,
            "diagnostics_have_entry_authority": False,
            "storage_pressure_flag_semantics": "candidate_or_candidate_hydration_activity_backpressure_not_disk_full_proof",
            "storage_capacity_or_retention_changed": False,
            "candidate_processing_target_seconds_unchanged": 5.0,
            "candidate_entry_window_seconds_unchanged": 20.0,
            "max_chase_fraction_unchanged": 0.15,
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
                    "compiled_spl_and_system_transfer_candidate_decoding": True,
                    "failed_scout_attribution_shape_diagnostics": True,
                    "failed_scout_diagnostic_payload_sanitized_and_bounded": True,
                    "diagnostic_lane_has_entry_authority": False,
                    "candidate_thresholds_unchanged": True,
                }
            )
        return payload

    try:
        status.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(status, "_roi_post161_candidate_attribution", True)
    return status


def install_post161_candidate_attribution_repair() -> None:
    global _ORIGINAL_NORMALIZE, _ORIGINAL_STATUS, _ORIGINAL_GRAPH
    current_graph = venue._graph_swap_facts
    if not bool(getattr(current_graph, "_roi_post161_candidate_attribution", False)):
        _ORIGINAL_GRAPH = current_graph
        setattr(_graph_swap_facts_v4, "_roi_post161_candidate_attribution", True)
        venue._graph_swap_facts = _graph_swap_facts_v4  # type: ignore[assignment]

    current_normalize = scout._normalize_tracked_wallet
    if not bool(getattr(current_normalize, "_roi_post161_candidate_attribution", False)):
        _ORIGINAL_NORMALIZE = current_normalize
        try:
            _normalize_with_post161_evidence.__dict__.update(getattr(current_normalize, "__dict__", {}))
        except Exception:
            pass
        scout._normalize_tracked_wallet = _normalize_with_post161_evidence  # type: ignore[assignment]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_post161_candidate_attribution", False)):
        _ORIGINAL_STATUS = current_status
        DirectSolanaIngestionPlane.status = _status_with_post161(current_status)  # type: ignore[method-assign]


__all__ = [
    "DIAGNOSTIC_VERSION",
    "RAW_TRANSFER_DECODER_VERSION",
    "REPAIR_VERSION",
    "_diagnostic_facts",
    "_graph_swap_facts_v4",
    "_raw_native_transfer",
    "_raw_token_transfer",
    "install_post161_candidate_attribution_repair",
]
