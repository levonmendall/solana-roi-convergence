from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone

from solana_roi.ingestion import LAMPORTS_PER_SOL
from solana_roi.observation import WSOL_MINT
from solana_roi.source_coverage import FROZEN_SUPPORTED_PROGRAM_IDS_BY_SOURCE
from solana_roi.venue_native_candidate_graph_repair import (
    REPAIR_VERSION,
    _account_keys,
    _decode_supported_venue,
    _indexed_transaction_sources,
)

SCOUT = "Scout111111111111111111111111111111111111111"
SPONSOR = "Sponsor111111111111111111111111111111111111"
TARGET = "Target1111111111111111111111111111111111111"
SECOND_TARGET = "SecondTarget11111111111111111111111111111111"
TARGET_ACCOUNT = "TargetAcct111111111111111111111111111111111"
SECOND_ACCOUNT = "SecondAcct111111111111111111111111111111111"
OTHER_ACCOUNT = "OtherAcct1111111111111111111111111111111111"
WSOL_ACCOUNT = "WsolAcct11111111111111111111111111111111111"
VAULT_TARGET = "VaultTarget111111111111111111111111111111111"
VAULT_SECOND = "VaultSecond111111111111111111111111111111111"
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


def _transfer(
    source: str,
    destination: str,
    amount: int,
    decimals: int = 0,
    *,
    mint: str | None = None,
    authority: str | None = None,
) -> dict:
    info = {
        "source": source,
        "destination": destination,
        "tokenAmount": {"amount": str(amount), "decimals": decimals},
    }
    if mint is not None:
        info["mint"] = mint
    if authority is not None:
        info["authority"] = authority
    return {
        "program": "spl-token",
        "programId": TOKEN_PROGRAM,
        "parsed": {"type": "transferChecked", "info": info},
    }


def _sponsored_buy(*, quote_amounts: list[int] | None = None) -> dict:
    quote_amounts = quote_amounts or [1_250_000_000]
    keys = [SPONSOR, SCOUT, TARGET_ACCOUNT, WSOL_ACCOUNT, VAULT_TARGET, VAULT_WSOL, RAYDIUM, TOKEN_PROGRAM]
    inner = [_transfer(VAULT_TARGET, TARGET_ACCOUNT, 1000, mint=TARGET)]
    inner.extend(
        _transfer(WSOL_ACCOUNT, VAULT_WSOL, amount, 9, mint=WSOL_MINT, authority=SCOUT)
        for amount in quote_amounts
    )
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
                _balance(4, TARGET, "vault-owner", 1_000_000),
                _balance(5, WSOL_MINT, "vault-owner", 10_000_000_000, 9),
            ],
            "postTokenBalances": [
                _balance(2, TARGET, SCOUT, 1000),
                _balance(3, WSOL_MINT, SCOUT, 2_000_000_000, 9),
                _balance(4, TARGET, "vault-owner", 999_000),
                _balance(5, WSOL_MINT, "vault-owner", 11_250_000_000, 9),
            ],
            "innerInstructions": [{"index": 0, "instructions": inner}],
        },
    }


def _decode(result: dict, signature: str = "sig-graph"):
    return _decode_supported_venue(
        result,
        signature=signature,
        trigger_received_at=datetime.fromtimestamp(1_800_000_000.25, tz=timezone.utc),
        wallet=SCOUT,
        source="RAYDIUM",
    )


def test_program_id_index_recovers_supported_source():
    assert _indexed_transaction_sources(_sponsored_buy()) == {"RAYDIUM"}


def test_sponsored_swap_uses_same_venue_transfer_graph_when_wallet_native_is_flat():
    swap, error = _decode(_sponsored_buy())
    assert error is None
    assert swap is not None
    assert swap.side == "buy"
    assert swap.token_mint == TARGET
    assert swap.token_amount == 1000.0
    assert abs(swap.native_amount_sol - 1.25) < 1e-12


def test_graph_first_decodes_venue_trade_even_when_final_wallet_token_delta_is_zero():
    result = _sponsored_buy()
    # The scout receives TARGET inside the Raydium instruction, then transfers the
    # same amount later in the transaction. Final owner balance is unchanged, so the
    # legacy final-balance heuristic has no directional endpoint.
    result["transaction"]["message"]["accountKeys"].append(OTHER_ACCOUNT)
    result["transaction"]["message"]["instructions"].append(
        _transfer(TARGET_ACCOUNT, OTHER_ACCOUNT, 1000, mint=TARGET, authority=SCOUT)
    )
    result["meta"]["preBalances"].append(0)
    result["meta"]["postBalances"].append(0)
    result["meta"]["preTokenBalances"][0] = _balance(2, TARGET, SCOUT, 1000)
    result["meta"]["postTokenBalances"][0] = _balance(2, TARGET, SCOUT, 1000)
    result["meta"]["postTokenBalances"].append(_balance(8, TARGET, "other-owner", 1000))

    swap, error = _decode(result, "sig-net-zero")
    assert error is None
    assert swap is not None
    assert swap.side == "buy"
    assert swap.token_mint == TARGET
    assert swap.token_amount == 1000.0
    assert abs(swap.native_amount_sol - 1.25) < 1e-12


def test_loaded_address_table_token_accounts_are_part_of_graph_identity():
    result = _sponsored_buy()
    # Keep only static transaction keys in the message and move token/vault accounts
    # into meta.loadedAddresses, preserving Solana's canonical writable/readonly order.
    result["transaction"]["message"]["accountKeys"] = [SPONSOR, SCOUT, RAYDIUM, TOKEN_PROGRAM]
    result["transaction"]["message"]["instructions"] = [{"programIdIndex": 2, "accounts": []}]
    result["meta"]["loadedAddresses"] = {
        "writable": [TARGET_ACCOUNT, WSOL_ACCOUNT, VAULT_TARGET, VAULT_WSOL],
        "readonly": [],
    }
    result["meta"]["preTokenBalances"] = [
        _balance(4, TARGET, SCOUT, 0),
        _balance(5, WSOL_MINT, SCOUT, 2_000_000_000, 9),
        _balance(6, TARGET, "vault-owner", 1_000_000),
        _balance(7, WSOL_MINT, "vault-owner", 10_000_000_000, 9),
    ]
    result["meta"]["postTokenBalances"] = [
        _balance(4, TARGET, SCOUT, 1000),
        _balance(5, WSOL_MINT, SCOUT, 2_000_000_000, 9),
        _balance(6, TARGET, "vault-owner", 999_000),
        _balance(7, WSOL_MINT, "vault-owner", 11_250_000_000, 9),
    ]
    result["meta"]["preBalances"] = [10 * LAMPORTS_PER_SOL, 8 * LAMPORTS_PER_SOL, 0, 0, 0, 0, 0, 0]
    result["meta"]["postBalances"] = [10 * LAMPORTS_PER_SOL - 5000, 8 * LAMPORTS_PER_SOL, 0, 0, 0, 0, 0, 0]

    assert _account_keys(result)[4:] == [TARGET_ACCOUNT, WSOL_ACCOUNT, VAULT_TARGET, VAULT_WSOL]
    swap, error = _decode(result, "sig-alt")
    assert error is None
    assert swap is not None
    assert swap.token_mint == TARGET
    assert abs(swap.native_amount_sol - 1.25) < 1e-12


def test_split_wallet_quote_and_fee_legs_are_aggregated_not_misclassified_ambiguous():
    swap, error = _decode(_sponsored_buy(quote_amounts=[1_250_000_000, 50_000_000]), "sig-split")
    assert error is None
    assert swap is not None
    assert swap.side == "buy"
    assert abs(swap.native_amount_sol - 1.30) < 1e-12


def test_multiple_actor_token_endpoints_remain_fail_closed():
    result = deepcopy(_sponsored_buy())
    result["transaction"]["message"]["accountKeys"].extend([SECOND_ACCOUNT, VAULT_SECOND])
    result["meta"]["preBalances"].extend([0, 0])
    result["meta"]["postBalances"].extend([0, 0])
    result["meta"]["preTokenBalances"].extend(
        [_balance(8, SECOND_TARGET, SCOUT, 0), _balance(9, SECOND_TARGET, "vault-owner", 5000)]
    )
    result["meta"]["postTokenBalances"].extend(
        [_balance(8, SECOND_TARGET, SCOUT, 250), _balance(9, SECOND_TARGET, "vault-owner", 4750)]
    )
    result["meta"]["innerInstructions"][0]["instructions"].insert(
        1, _transfer(VAULT_SECOND, SECOND_ACCOUNT, 250, mint=SECOND_TARGET)
    )

    swap, error = _decode(result, "sig-multi")
    assert swap is None
    assert error == "semantic_multiple_directional_endpoints"


def test_repair_contract_is_explicit_and_v3():
    assert REPAIR_VERSION == "venue-native-graph-first-attribution-v3"
