from __future__ import annotations

from datetime import datetime, timezone

from solana_roi.pump_raw import PUMP_PROGRAM_ID, PumpFunRawWebhookParser

_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
BUY = bytes([102, 6, 61, 18, 1, 218, 235, 234])
SELL = bytes([51, 230, 133, 164, 1, 127, 131, 173])


def _b58(data: bytes) -> str:
    number = int.from_bytes(data, "big")
    encoded = ""
    while number:
        number, remainder = divmod(number, 58)
        encoded = _ALPHABET[remainder] + encoded
    leading = len(data) - len(data.lstrip(b"\x00"))
    return "1" * leading + (encoded or ("1" if not leading else ""))


def _raw_tx(*, side: str = "buy", second_mint: bool = False):
    wallet = "wallet"
    mint = "token-mint"
    discriminator = BUY if side == "buy" else SELL
    pre_token = 0 if side == "buy" else 1_000_000_000
    post_token = 1_000_000_000 if side == "buy" else 500_000_000
    trade_lamports = 1_000_000_000 if side == "buy" else 500_000_000
    fee = 5_000
    pre_wallet = 10_000_000_000
    post_wallet = (
        pre_wallet - trade_lamports - fee
        if side == "buy"
        else pre_wallet + trade_lamports - fee
    )
    pre_balances = [pre_wallet, 2_039_280, 1]
    post_balances = [post_wallet, 2_039_280, 1]
    pre_tokens = [{
        "accountIndex": 1,
        "mint": mint,
        "owner": wallet,
        "uiTokenAmount": {"amount": str(pre_token), "decimals": 6},
    }]
    post_tokens = [{
        "accountIndex": 1,
        "mint": mint,
        "owner": wallet,
        "uiTokenAmount": {"amount": str(post_token), "decimals": 6},
    }]
    if second_mint:
        pre_tokens.append({
            "accountIndex": 1,
            "mint": "other-mint",
            "owner": wallet,
            "uiTokenAmount": {"amount": "0", "decimals": 6},
        })
        post_tokens.append({
            "accountIndex": 1,
            "mint": "other-mint",
            "owner": wallet,
            "uiTokenAmount": {"amount": "1000000", "decimals": 6},
        })
    return {
        "slot": 123,
        "blockTime": 1_788_316_800,
        "meta": {
            "err": None,
            "fee": fee,
            "preBalances": pre_balances,
            "postBalances": post_balances,
            "preTokenBalances": pre_tokens,
            "postTokenBalances": post_tokens,
            "innerInstructions": [],
        },
        "transaction": {
            "signatures": [f"sig-{side}"],
            "message": {
                "accountKeys": [wallet, "token-account", PUMP_PROGRAM_ID],
                "instructions": [{
                    "programIdIndex": 2,
                    "accounts": [0, 1],
                    "data": _b58(discriminator),
                }],
            },
        },
    }


def test_raw_pump_buy_normalizes_exact_fee_payer_balance_change():
    parser = PumpFunRawWebhookParser()
    received = datetime(2026, 9, 2, 2, 0, tzinfo=timezone.utc)
    rows = parser.parse([_raw_tx(side="buy")], received_at=received)
    assert len(rows) == 1
    swap = rows[0]
    assert swap.side == "buy"
    assert swap.wallet == "wallet"
    assert swap.token_mint == "token-mint"
    assert swap.token_amount == 1000.0
    assert swap.native_amount_sol == 1.0
    assert swap.reference_price_sol == 0.001
    assert swap.source == "helius-raw-webhook:PUMP_FUN:buy"


def test_raw_pump_sell_normalizes_and_multimint_fails_closed():
    parser = PumpFunRawWebhookParser()
    rows = parser.parse([_raw_tx(side="sell")])
    assert len(rows) == 1
    assert rows[0].side == "sell"
    assert rows[0].token_amount == 500.0
    assert rows[0].native_amount_sol == 0.5
    assert parser.parse([_raw_tx(side="buy", second_mint=True)]) == []


def test_raw_transaction_without_official_pump_trade_discriminator_is_ignored():
    payload = _raw_tx(side="buy")
    payload["transaction"]["message"]["instructions"][0]["data"] = _b58(b"notpump!")
    assert PumpFunRawWebhookParser().parse([payload]) == []
