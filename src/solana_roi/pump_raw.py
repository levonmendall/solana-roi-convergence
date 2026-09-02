from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .ingestion import LAMPORTS_PER_SOL, NormalizedSwap

PUMP_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
WSOL_MINT = "So11111111111111111111111111111111111111112"

_BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_BUY_DISCRIMINATORS = {
    bytes([102, 6, 61, 18, 1, 218, 235, 234]),
    bytes([56, 252, 116, 8, 158, 223, 205, 95]),
}
_SELL_DISCRIMINATOR = bytes([51, 230, 133, 164, 1, 127, 131, 173])


def _base58_decode(value: str) -> bytes:
    number = 0
    for char in value:
        index = _BASE58_ALPHABET.find(char)
        if index < 0:
            raise ValueError("invalid base58 character")
        number = number * 58 + index
    payload = number.to_bytes((number.bit_length() + 7) // 8, "big") if number else b""
    leading_zeroes = len(value) - len(value.lstrip("1"))
    return b"\x00" * leading_zeroes + payload


def _ui_amount(row: dict[str, Any]) -> float | None:
    ui = row.get("uiTokenAmount")
    if not isinstance(ui, dict):
        return None
    try:
        amount = float(ui.get("amount") or 0.0)
        decimals = int(ui.get("decimals") or 0)
    except (TypeError, ValueError):
        return None
    scaled = amount / (10 ** decimals)
    return scaled if scaled >= 0 else None


class PumpFunRawWebhookParser:
    """Fail-closed Pump bonding-curve parser for Helius raw webhooks.

    Helius Enhanced currently has no PUMP_FUN source parser. Raw webhooks expose
    exact Solana transactions, so this parser recognizes only official Pump buy
    and sell instruction discriminators and derives one unambiguous fee-payer
    SOL/SPL balance change. Multi-mint or contradictory flows are discarded.
    """

    @staticmethod
    def looks_raw(payload: Any) -> bool:
        rows = payload if isinstance(payload, list) else [payload]
        return any(
            isinstance(row, dict)
            and isinstance(row.get("meta"), dict)
            and isinstance(row.get("transaction"), dict)
            and row.get("blockTime") is not None
            for row in rows
        )

    @staticmethod
    def _account_keys(tx: dict[str, Any]) -> list[str]:
        transaction = tx.get("transaction")
        message = transaction.get("message") if isinstance(transaction, dict) else None
        static = message.get("accountKeys") if isinstance(message, dict) else None
        keys = [str(value) for value in static] if isinstance(static, list) else []
        meta = tx.get("meta")
        loaded = meta.get("loadedAddresses") if isinstance(meta, dict) else None
        if isinstance(loaded, dict):
            for name in ("writable", "readonly"):
                rows = loaded.get(name)
                if isinstance(rows, list):
                    keys.extend(str(value) for value in rows)
        return keys

    @staticmethod
    def _instructions(tx: dict[str, Any]) -> list[dict[str, Any]]:
        transaction = tx.get("transaction")
        message = transaction.get("message") if isinstance(transaction, dict) else None
        result = [row for row in (message.get("instructions") if isinstance(message, dict) else []) or [] if isinstance(row, dict)]
        meta = tx.get("meta")
        for group in (meta.get("innerInstructions") if isinstance(meta, dict) else []) or []:
            if not isinstance(group, dict):
                continue
            result.extend(row for row in (group.get("instructions") or []) if isinstance(row, dict))
        return result

    @classmethod
    def _trade_side(cls, tx: dict[str, Any], keys: list[str]) -> str | None:
        sides: set[str] = set()
        for instruction in cls._instructions(tx):
            try:
                program_index = int(instruction["programIdIndex"])
            except (KeyError, TypeError, ValueError):
                continue
            if program_index < 0 or program_index >= len(keys) or keys[program_index] != PUMP_PROGRAM_ID:
                continue
            data = instruction.get("data")
            if not isinstance(data, str) or not data:
                continue
            try:
                discriminator = _base58_decode(data)[:8]
            except ValueError:
                continue
            if discriminator in _BUY_DISCRIMINATORS:
                sides.add("buy")
            elif discriminator == _SELL_DISCRIMINATOR:
                sides.add("sell")
        return next(iter(sides)) if len(sides) == 1 else None

    @staticmethod
    def _owned_totals(rows: Any, wallet: str) -> tuple[dict[str, float], set[int]]:
        totals: dict[str, float] = {}
        account_indexes: set[int] = set()
        for row in rows or []:
            if not isinstance(row, dict) or str(row.get("owner") or "") != wallet:
                continue
            mint = str(row.get("mint") or "")
            amount = _ui_amount(row)
            if not mint or amount is None:
                continue
            totals[mint] = totals.get(mint, 0.0) + amount
            try:
                account_indexes.add(int(row["accountIndex"]))
            except (KeyError, TypeError, ValueError):
                pass
        return totals, account_indexes

    @classmethod
    def _parse_one(cls, tx: dict[str, Any], received_at: datetime) -> NormalizedSwap | None:
        meta = tx.get("meta")
        transaction = tx.get("transaction")
        if not isinstance(meta, dict) or not isinstance(transaction, dict) or meta.get("err") is not None:
            return None
        keys = cls._account_keys(tx)
        if not keys or PUMP_PROGRAM_ID not in keys:
            return None
        side = cls._trade_side(tx, keys)
        if side is None:
            return None
        wallet = keys[0]
        signatures = transaction.get("signatures")
        signature = str(signatures[0]) if isinstance(signatures, list) and signatures else ""
        try:
            slot = int(tx["slot"])
            block_time = int(tx["blockTime"])
        except (KeyError, TypeError, ValueError):
            return None
        if not signature or block_time <= 0:
            return None

        pre_tokens, pre_indexes = cls._owned_totals(meta.get("preTokenBalances"), wallet)
        post_tokens, post_indexes = cls._owned_totals(meta.get("postTokenBalances"), wallet)
        mints = set(pre_tokens) | set(post_tokens)
        deltas = {
            mint: post_tokens.get(mint, 0.0) - pre_tokens.get(mint, 0.0)
            for mint in mints
            if mint != WSOL_MINT
        }
        material = [(mint, delta) for mint, delta in deltas.items() if abs(delta) > 1e-18]
        if len(material) != 1:
            return None
        token_mint, token_delta = material[0]
        if (side == "buy" and token_delta <= 0) or (side == "sell" and token_delta >= 0):
            return None

        pre_balances = meta.get("preBalances")
        post_balances = meta.get("postBalances")
        if not isinstance(pre_balances, list) or not isinstance(post_balances, list) or not pre_balances or not post_balances:
            return None
        try:
            native_delta = float(post_balances[0]) - float(pre_balances[0]) + float(meta.get("fee") or 0.0)
        except (TypeError, ValueError, IndexError):
            return None

        # Remove rent effects from fee-payer-owned token accounts. Creating an
        # ATA debits the fee payer and credits that token account; closing one
        # does the reverse. Adding the owned-account lamport delta isolates the
        # trade SOL flow without treating rent as execution price.
        for index in pre_indexes | post_indexes:
            if index == 0 or index < 0 or index >= len(pre_balances) or index >= len(post_balances):
                continue
            try:
                native_delta += float(post_balances[index]) - float(pre_balances[index])
            except (TypeError, ValueError):
                return None

        if (side == "buy" and native_delta >= 0) or (side == "sell" and native_delta <= 0):
            return None
        native_amount_sol = abs(native_delta) / LAMPORTS_PER_SOL
        token_amount = abs(token_delta)
        if native_amount_sol <= 0 or token_amount <= 0:
            return None
        observed_at = datetime.fromtimestamp(block_time, tz=timezone.utc)
        return NormalizedSwap(
            signature=signature,
            slot=slot,
            observed_at=observed_at,
            received_at=received_at,
            wallet=wallet,
            token_mint=token_mint,
            side=side,
            token_amount=token_amount,
            native_amount_sol=native_amount_sol,
            reference_price_sol=native_amount_sol / token_amount,
            source=f"helius-raw-webhook:PUMP_FUN:{side}",
        )

    def parse(self, payload: Any, *, received_at: datetime | None = None) -> list[NormalizedSwap]:
        at = received_at or datetime.now(timezone.utc)
        rows = payload if isinstance(payload, list) else [payload]
        result: list[NormalizedSwap] = []
        for tx in rows:
            if not isinstance(tx, dict):
                continue
            normalized = self._parse_one(tx, at)
            if normalized is not None:
                result.append(normalized)
        return result
