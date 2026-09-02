from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .ingestion import LAMPORTS_PER_SOL, NormalizedSwap
from .observation import WSOL_MINT
from .source_coverage import FROZEN_SUPPORTED_PROGRAM_IDS_BY_SOURCE


PROGRAM_SOURCE_BY_ID: dict[str, str] = {
    program_id: source
    for source, program_ids in FROZEN_SUPPORTED_PROGRAM_IDS_BY_SOURCE
    for program_id in program_ids
}


def _account_keys(result: dict[str, Any]) -> list[str]:
    transaction = result.get("transaction")
    message = transaction.get("message") if isinstance(transaction, dict) else None
    rows = message.get("accountKeys") if isinstance(message, dict) else None
    keys: list[str] = []
    for row in rows or []:
        if isinstance(row, str):
            keys.append(row)
        elif isinstance(row, dict):
            keys.append(str(row.get("pubkey") or ""))
        else:
            keys.append("")
    return keys


def _instruction_program_ids(result: dict[str, Any]) -> set[str]:
    ids: set[str] = set()
    transaction = result.get("transaction")
    message = transaction.get("message") if isinstance(transaction, dict) else None
    top = message.get("instructions") if isinstance(message, dict) else []
    meta = result.get("meta")
    inner_groups = meta.get("innerInstructions") if isinstance(meta, dict) else []

    def walk(rows: Any) -> None:
        if not isinstance(rows, list):
            return
        for row in rows:
            if not isinstance(row, dict):
                continue
            program_id = str(row.get("programId") or "")
            if program_id:
                ids.add(program_id)
            nested = row.get("innerInstructions")
            if isinstance(nested, list):
                walk(nested)

    walk(top)
    if isinstance(inner_groups, list):
        for group in inner_groups:
            if isinstance(group, dict):
                walk(group.get("instructions"))
    return ids


def transaction_sources(result: dict[str, Any]) -> set[str]:
    mentioned = set(_account_keys(result)) | _instruction_program_ids(result)
    return {PROGRAM_SOURCE_BY_ID[program_id] for program_id in mentioned if program_id in PROGRAM_SOURCE_BY_ID}


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


def _token_deltas_for_owner(result: dict[str, Any], owner: str) -> dict[str, float]:
    meta = result.get("meta")
    if not isinstance(meta, dict):
        return {}
    before: dict[tuple[int, str], tuple[int, int]] = {}
    after: dict[tuple[int, str], tuple[int, int]] = {}
    for row in meta.get("preTokenBalances") or []:
        if not isinstance(row, dict) or str(row.get("owner") or "") != owner:
            continue
        mint = str(row.get("mint") or "")
        raw = _raw_token_amount(row)
        if not mint or raw is None:
            continue
        before[(int(row.get("accountIndex") or 0), mint)] = raw
    for row in meta.get("postTokenBalances") or []:
        if not isinstance(row, dict) or str(row.get("owner") or "") != owner:
            continue
        mint = str(row.get("mint") or "")
        raw = _raw_token_amount(row)
        if not mint or raw is None:
            continue
        after[(int(row.get("accountIndex") or 0), mint)] = raw
    deltas: dict[str, float] = {}
    for key in set(before) | set(after):
        left_amount, left_decimals = before.get(key, (0, after.get(key, (0, 0))[1]))
        right_amount, right_decimals = after.get(key, (0, left_decimals))
        if left_decimals != right_decimals:
            continue
        scale = 10 ** right_decimals
        delta = (right_amount - left_amount) / scale
        if abs(delta) > 0:
            deltas[key[1]] = deltas.get(key[1], 0.0) + delta
    return deltas


def _fee_payer(result: dict[str, Any]) -> tuple[str, int] | None:
    transaction = result.get("transaction")
    message = transaction.get("message") if isinstance(transaction, dict) else None
    rows = message.get("accountKeys") if isinstance(message, dict) else None
    if not isinstance(rows, list) or not rows:
        return None
    for index, row in enumerate(rows):
        if isinstance(row, dict) and bool(row.get("signer")):
            pubkey = str(row.get("pubkey") or "")
            if pubkey:
                return pubkey, index
    first = rows[0]
    if isinstance(first, str) and first:
        return first, 0
    if isinstance(first, dict):
        pubkey = str(first.get("pubkey") or "")
        return (pubkey, 0) if pubkey else None
    return None


def normalize_standard_transaction(
    result: Any,
    *,
    signature: str,
    trigger_received_at: datetime,
    source_hint: str | None = None,
) -> NormalizedSwap | None:
    """Normalize a simple SOL/WSOL <-> SPL swap from standard RPC metadata.

    The parser intentionally fails closed for multi-token/ambiguous transactions.
    It does not need provider-specific enhanced transaction interpretation.
    """

    if not isinstance(result, dict):
        return None
    meta = result.get("meta")
    if not isinstance(meta, dict) or meta.get("err") is not None:
        return None
    payer = _fee_payer(result)
    if payer is None:
        return None
    wallet, payer_index = payer
    sources = transaction_sources(result)
    hint = str(source_hint or "").upper()
    if hint:
        if hint not in {source for source, _ids in FROZEN_SUPPORTED_PROGRAM_IDS_BY_SOURCE}:
            return None
        if sources and hint not in sources:
            return None
        source = hint
    elif len(sources) == 1:
        source = next(iter(sources))
    else:
        return None

    deltas = _token_deltas_for_owner(result, wallet)
    wsol_delta = float(deltas.pop(WSOL_MINT, 0.0))
    material = [(mint, delta) for mint, delta in deltas.items() if abs(delta) > 1e-18]
    if len(material) != 1:
        return None
    token_mint, token_delta = material[0]

    pre_balances = meta.get("preBalances")
    post_balances = meta.get("postBalances")
    try:
        pre_lamports = int(pre_balances[payer_index])
        post_lamports = int(post_balances[payer_index])
        fee_lamports = int(meta.get("fee") or 0)
    except (IndexError, TypeError, ValueError):
        return None
    # Add the transaction fee back to the payer's native delta so the execution
    # price measures the swap rather than the network fee. Persistent WSOL flow
    # is included as another representation of native SOL.
    native_change_sol = (post_lamports - pre_lamports + fee_lamports) / LAMPORTS_PER_SOL + wsol_delta
    if token_delta > 0 and native_change_sol < 0:
        side = "buy"
    elif token_delta < 0 and native_change_sol > 0:
        side = "sell"
    else:
        return None
    token_amount = abs(float(token_delta))
    native_amount = abs(float(native_change_sol))
    if token_amount <= 0 or native_amount <= 0:
        return None

    try:
        slot = int(result["slot"])
    except (KeyError, TypeError, ValueError):
        return None
    try:
        block_time = int(result.get("blockTime") or 0)
    except (TypeError, ValueError):
        block_time = 0
    observed_at = (
        datetime.fromtimestamp(block_time, tz=timezone.utc)
        if block_time > 0
        else trigger_received_at
    )
    return NormalizedSwap(
        signature=signature,
        slot=slot,
        observed_at=observed_at,
        received_at=trigger_received_at,
        wallet=wallet,
        token_mint=token_mint,
        side=side,
        token_amount=token_amount,
        native_amount_sol=native_amount,
        reference_price_sol=native_amount / token_amount,
        source=f"solana-direct:{source}:{side}",
    )
