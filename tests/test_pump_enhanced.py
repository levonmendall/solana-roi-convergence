from __future__ import annotations

from datetime import datetime, timezone

from solana_roi.pump_enhanced import PumpFunEnhancedWebhookParser
from solana_roi.pump_raw import PUMP_PROGRAM_ID

_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_BUY = bytes([102, 6, 61, 18, 1, 218, 235, 234])
_SELL = bytes([51, 230, 133, 164, 1, 127, 131, 173])


def _b58(data: bytes) -> str:
    number = int.from_bytes(data, "big")
    encoded = ""
    while number:
        number, rem = divmod(number, 58)
        encoded = _ALPHABET[rem] + encoded
    leading = len(data) - len(data.lstrip(b"\x00"))
    return "1" * leading + (encoded or "")


def _tx(*, instructions, tx_type="UNKNOWN"):
    wallet = "wallet-1"
    mint = "mint-1"
    return {
        "type": tx_type,
        "source": "UNKNOWN",
        "signature": "sig-1",
        "slot": 123,
        "timestamp": 1788318000,
        "feePayer": wallet,
        "instructions": instructions,
        "tokenTransfers": [
            {
                "fromUserAccount": "curve",
                "toUserAccount": wallet,
                "mint": mint,
                "tokenAmount": 1000.0,
            }
        ],
        "nativeTransfers": [
            {
                "fromUserAccount": wallet,
                "toUserAccount": "curve",
                "amount": 1_000_000_000,
            }
        ],
        "events": {},
    }


def test_unknown_enhanced_pump_buy_is_locally_classified_and_normalized():
    payload = _tx(
        instructions=[{"programId": PUMP_PROGRAM_ID, "data": _b58(_BUY + b"payload")}],
    )
    rows = PumpFunEnhancedWebhookParser().parse(
        payload,
        received_at=datetime(2026, 9, 2, tzinfo=timezone.utc),
    )
    assert len(rows) == 1
    row = rows[0]
    assert row.side == "buy"
    assert row.token_mint == "mint-1"
    assert row.native_amount_sol == 1.0
    assert row.token_amount == 1000.0
    assert row.source == "helius-enhanced-webhook:PUMP_FUN:buy"


def test_nested_pump_instruction_is_detected():
    payload = _tx(
        instructions=[
            {
                "programId": "outer-program",
                "data": "1",
                "innerInstructions": [
                    {"programId": PUMP_PROGRAM_ID, "data": _b58(_BUY + b"payload")},
                ],
            }
        ],
    )
    rows = PumpFunEnhancedWebhookParser().parse(payload)
    assert len(rows) == 1
    assert rows[0].source.startswith("helius-enhanced-webhook:PUMP_FUN:")


def test_conflicting_pump_buy_and_sell_instructions_fail_closed():
    payload = _tx(
        instructions=[
            {"programId": PUMP_PROGRAM_ID, "data": _b58(_BUY + b"payload")},
            {"programId": PUMP_PROGRAM_ID, "data": _b58(_SELL + b"payload")},
        ],
    )
    assert PumpFunEnhancedWebhookParser().parse(payload) == []


def test_unrelated_program_cannot_claim_pump_fun_source():
    payload = _tx(
        instructions=[{"programId": "not-pump", "data": _b58(_BUY + b"payload")}],
    )
    assert PumpFunEnhancedWebhookParser().parse(payload) == []
