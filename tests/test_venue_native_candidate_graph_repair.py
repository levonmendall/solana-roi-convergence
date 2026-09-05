from __future__ import annotations

from datetime import datetime, timezone

from solana_roi.ingestion import LAMPORTS_PER_SOL
from solana_roi.observation import WSOL_MINT
from solana_roi.source_coverage import FROZEN_SUPPORTED_PROGRAM_IDS_BY_SOURCE
from solana_roi.venue_native_candidate_graph_repair import (
    REPAIR_VERSION,
    _decode_supported_venue,
    _indexed_transaction_sources,
)

SCOUT = "Scout111111111111111111111111111111111111111"
SPONSOR = "Sponsor111111111111111111111111111111111111"
TARGET = "Target1111111111111111111111111111111111111"
TARGET_ACCOUNT = "TargetAcct111111111111111111111111111111111"
WSOL_ACCOUNT = "WsolAcct11111111111111111111111111111111111"
VAULT_TARGET = "VaultTarget111111111111111111111111111111111"
VAULT_WSOL = "VaultWsol11111111111111111111111111111111111"
RAYDIUM = dict(FROZEN_SUPPORTED_PROGRAM_IDS_BY_SOURCE)["RAYDIUM"][0]
TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"


def _balance(index: int, mint: str, owner: str, amount: int, decimals: int = 0) -> dict:
    return {
        "accountIndex": index,
        "mint": mint,
        "owner": owner,
        "uiTokenAmount": {"amount": str(amount), "decimals": decimals},
    }


def _transfer(source: str, destination: str, amount: int, decimals: int = 0) -> dict:
    return {
        "program": "spl-token",
        "programId": TOKEN_PROGRAM,
        "parsed": {
            "type": "transferChecked",
            "info": {
                "source": source,
                "destination": destination,
                "tokenAmount": {"amount": str(amount), "decimals": decimals},
            },
        },
    }


def _sponsored_buy(*, quote_amounts: list[int] | None = None) -> dict:
    quote_amounts = quote_amounts or [1_250_000_000]
    keys = [SPONSOR, SCOUT, TARGET_ACCOUNT, WSOL_ACCOUNT, VAULT_TARGET, VAULT_WSOL, RAYDIUM, TOKEN_PROGRAM]
    inner = [_transfer(VAULT_TARGET, TARGET_ACCOUNT, 1000)]
    inner.extend(_transfer(WSOL_ACCOUNT, VAULT_WSOL, amount, 9) for amount in quote_amounts)
    return {
        "slot": 555,
        "blockTime": 1_800_000_000,
        "transaction": {"message": {"accountKeys": keys, "instructions": [{"programIdIndex": 6, "accounts": []}]}},
        "meta": {
            "err": None,
            "fee": 5000,
            "preBalances": [10 * LAMPORTS_PER_SOL, 8 * LAMPORTS_PER_SOL, 0, 0, 0, 0, 0, 0],
            "postBalances": [10 * LAMPORTS_PER_SOL - 5000, 8 * LAMPORTS_PER_SOL, 0, 0, 0, 0, 0, 0],
            "preTokenBalances": [
                _balance(2, TARGET, SCOUT, 0),
                _balance(3, WSOL_MINT, SCOUT, 2_000_000_000, 9),
                _balance(4, TARGET, "vault-owner", 1000000),
                _balance(5, WSOL_MINT, "vault-owner", 10_000_000_000, 9),
            ],
            "postTokenBalances": [
                _balance(2, TARGET, SCOUT, 1000),
                _balance(3, WSOL_MINT, SCOUT, 2_000_000_000, 9),
                _balance(4, TARGET, "vault-owner", 999000),
                _balance(5, WSOL_MINT, "vault-owner", 11_250_000_000, 9),
            ],
            "innerInstructions": [{"index": 0, "instructions": inner}],
        },
    }


def test_program_id_index_recovers_supported_source():
    assert _indexed_transaction_sources(_sponsored_buy()) == {"RAYDIUM"}


def test_sponsored_swap_uses_same_venue_transfer_graph_when_wallet_native_is_flat():
    swap, error = _decode_supported_venue(
        _sponsored_buy(),
        signature="sig-graph",
        trigger_received_at=datetime.fromtimestamp(1_800_000_000.25, tz=timezone.utc),
        wallet=SCOUT,
        source="RAYDIUM",
    )
    assert error is None
    assert swap is not None
    assert swap.side == "buy"
    assert swap.token_mint == TARGET
    assert swap.token_amount == 1000.0
    assert abs(swap.native_amount_sol - 1.25) < 1e-12


def test_comparable_multiple_quote_legs_remain_fail_closed():
    swap, error = _decode_supported_venue(
        _sponsored_buy(quote_amounts=[1_250_000_000, 1_000_000_000]),
        signature="sig-ambiguous",
        trigger_received_at=datetime.fromtimestamp(1_800_000_000.25, tz=timezone.utc),
        wallet=SCOUT,
        source="RAYDIUM",
    )
    assert swap is None
    assert error == "semantic_native_wsol_direction_ambiguous"


def test_repair_contract_is_explicit_and_v2():
    assert REPAIR_VERSION == "venue-native-instruction-transfer-graph-v2"
