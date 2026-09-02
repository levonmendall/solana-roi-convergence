from __future__ import annotations

from datetime import datetime, timezone

from solana_roi.direct_transaction import normalize_standard_transaction
from solana_roi.observation import WSOL_MINT
from solana_roi.source_coverage import FROZEN_SUPPORTED_PROGRAM_IDS_BY_SOURCE


PUMP_FUN = FROZEN_SUPPORTED_PROGRAM_IDS_BY_SOURCE[0][1][0]
PUMP_AMM = FROZEN_SUPPORTED_PROGRAM_IDS_BY_SOURCE[1][1][0]
RAYDIUM = FROZEN_SUPPORTED_PROGRAM_IDS_BY_SOURCE[2][1][0]
PAYER = "11111111111111111111111111111112"
TOKEN = "So11111111111111111111111111111111111111111"
TOKEN_2 = "So11111111111111111111111111111111111111113"


def _token_row(index: int, mint: str, owner: str, amount: int, decimals: int = 3):
    return {
        "accountIndex": index,
        "mint": mint,
        "owner": owner,
        "uiTokenAmount": {"amount": str(amount), "decimals": decimals},
    }


def _tx(*, program: str, pre_lamports: int, post_lamports: int, fee: int, pre_tokens, post_tokens, extra_programs=()):
    keys = [
        {"pubkey": PAYER, "signer": True, "writable": True},
        {"pubkey": program, "signer": False, "writable": False},
        *({"pubkey": value, "signer": False, "writable": False} for value in extra_programs),
    ]
    return {
        "slot": 123456,
        "blockTime": 1_788_321_600,
        "transaction": {"message": {"accountKeys": keys, "instructions": []}},
        "meta": {
            "err": None,
            "fee": fee,
            "preBalances": [pre_lamports] + [0] * (len(keys) - 1),
            "postBalances": [post_lamports] + [0] * (len(keys) - 1),
            "preTokenBalances": pre_tokens,
            "postTokenBalances": post_tokens,
            "innerInstructions": [],
        },
    }


def test_direct_pump_fun_buy_normalizes_from_authoritative_balance_deltas():
    fee = 5_000
    result = _tx(
        program=PUMP_FUN,
        pre_lamports=10_000_000_000,
        post_lamports=8_999_995_000,
        fee=fee,
        pre_tokens=[_token_row(2, TOKEN, PAYER, 0)],
        post_tokens=[_token_row(2, TOKEN, PAYER, 1_000_000)],
    )
    received = datetime(2026, 9, 2, tzinfo=timezone.utc)
    swap = normalize_standard_transaction(result, signature="pump-buy", trigger_received_at=received)
    assert swap is not None
    assert swap.source == "solana-direct:PUMP_FUN:buy"
    assert swap.side == "buy"
    assert swap.wallet == PAYER
    assert swap.token_mint == TOKEN
    assert swap.token_amount == 1000.0
    assert swap.native_amount_sol == 1.0
    assert swap.reference_price_sol == 0.001


def test_direct_raydium_sell_normalizes_and_removes_network_fee_from_price():
    fee = 5_000
    result = _tx(
        program=RAYDIUM,
        pre_lamports=8_000_000_000,
        post_lamports=8_999_995_000,
        fee=fee,
        pre_tokens=[_token_row(2, TOKEN, PAYER, 1_000_000)],
        post_tokens=[_token_row(2, TOKEN, PAYER, 0)],
    )
    swap = normalize_standard_transaction(
        result,
        signature="ray-sell",
        trigger_received_at=datetime(2026, 9, 2, tzinfo=timezone.utc),
    )
    assert swap is not None
    assert swap.source == "solana-direct:RAYDIUM:sell"
    assert swap.side == "sell"
    assert swap.native_amount_sol == 1.0


def test_persistent_wsol_delta_is_treated_as_native_leg_without_provider_enrichment():
    fee = 5_000
    result = _tx(
        program=PUMP_AMM,
        pre_lamports=8_000_000_000,
        post_lamports=7_999_995_000,
        fee=fee,
        pre_tokens=[
            _token_row(2, WSOL_MINT, PAYER, 1_000_000_000, decimals=9),
            _token_row(3, TOKEN, PAYER, 0),
        ],
        post_tokens=[
            _token_row(2, WSOL_MINT, PAYER, 0, decimals=9),
            _token_row(3, TOKEN, PAYER, 1_000_000),
        ],
    )
    swap = normalize_standard_transaction(
        result,
        signature="wsol-buy",
        trigger_received_at=datetime(2026, 9, 2, tzinfo=timezone.utc),
    )
    assert swap is not None
    assert swap.source == "solana-direct:PUMP_AMM:buy"
    assert swap.native_amount_sol == 1.0


def test_multi_token_transaction_fails_closed():
    result = _tx(
        program=PUMP_FUN,
        pre_lamports=10_000_000_000,
        post_lamports=8_999_995_000,
        fee=5_000,
        pre_tokens=[_token_row(2, TOKEN, PAYER, 0), _token_row(3, TOKEN_2, PAYER, 0)],
        post_tokens=[_token_row(2, TOKEN, PAYER, 500_000), _token_row(3, TOKEN_2, PAYER, 500_000)],
    )
    assert normalize_standard_transaction(
        result,
        signature="ambiguous",
        trigger_received_at=datetime(2026, 9, 2, tzinfo=timezone.utc),
    ) is None


def test_multi_source_transaction_requires_explicit_matching_source_hint():
    result = _tx(
        program=PUMP_FUN,
        extra_programs=(RAYDIUM,),
        pre_lamports=10_000_000_000,
        post_lamports=8_999_995_000,
        fee=5_000,
        pre_tokens=[_token_row(2, TOKEN, PAYER, 0)],
        post_tokens=[_token_row(2, TOKEN, PAYER, 1_000_000)],
    )
    received = datetime(2026, 9, 2, tzinfo=timezone.utc)
    assert normalize_standard_transaction(result, signature="multi", trigger_received_at=received) is None
    hinted = normalize_standard_transaction(
        result,
        signature="multi",
        trigger_received_at=received,
        source_hint="PUMP_FUN",
    )
    assert hinted is not None
    assert hinted.source == "solana-direct:PUMP_FUN:buy"
    assert normalize_standard_transaction(
        result,
        signature="wrong-hint",
        trigger_received_at=received,
        source_hint="PUMP_AMM",
    ) is None
