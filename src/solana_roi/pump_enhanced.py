from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from typing import Any, Iterable

from .ingestion import HeliusEnhancedWebhookParser, NormalizedSwap
from .pump_raw import PUMP_PROGRAM_ID

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


def _walk_instructions(rows: Any) -> Iterable[dict[str, Any]]:
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        yield row
        nested = row.get("innerInstructions")
        if isinstance(nested, list):
            yield from _walk_instructions(nested)


class PumpFunEnhancedWebhookParser:
    """Classify Pump.fun bonding-curve trades from Enhanced instruction data.

    Enhanced transactions include programId + raw instruction data even when the
    human-readable source parser reports UNKNOWN. We trust only the frozen Pump
    program id and the official buy/buy_exact_sol_in/sell discriminators. The
    existing enhanced normalizer still derives the actual SOL/SPL amounts; this
    adapter only supplies a verified source/side when Helius did not.
    """

    def __init__(self) -> None:
        self.base = HeliusEnhancedWebhookParser()

    @staticmethod
    def trade_side(tx: dict[str, Any]) -> str | None:
        sides: set[str] = set()
        for instruction in _walk_instructions(tx.get("instructions")):
            if str(instruction.get("programId") or "") != PUMP_PROGRAM_ID:
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

    def parse(self, payload: Any, *, received_at: datetime | None = None) -> list[NormalizedSwap]:
        at = received_at or datetime.now(timezone.utc)
        transactions = payload if isinstance(payload, list) else [payload]
        result: list[NormalizedSwap] = []
        for tx in transactions:
            if not isinstance(tx, dict) or tx.get("transactionError"):
                continue
            side = self.trade_side(tx)
            if side is None:
                continue
            adapted = dict(tx)
            adapted["source"] = "PUMP_FUN"
            tx_type = str(adapted.get("type") or "").upper()
            if tx_type not in self.base.SWAP_TYPES | self.base.DIRECT_TRADE_TYPES:
                adapted["type"] = side.upper()
            rows = self.base.parse(adapted, received_at=at)
            if len(rows) != 1 or rows[0].side != side:
                continue
            result.append(
                replace(
                    rows[0],
                    source=f"helius-enhanced-webhook:PUMP_FUN:{side}",
                )
            )
        return result
