from __future__ import annotations

from datetime import datetime, timezone

from solana_roi.ingestion import LAMPORTS_PER_SOL
from solana_roi.observation import WSOL_MINT
from solana_roi.post161_candidate_attribution_repair import (
    DIAGNOSTIC_VERSION,
    RAW_TRANSFER_DECODER_VERSION,
    REPAIR_VERSION,
    _diagnostic_facts,
    install_post161_candidate_attribution_repair,
)
from solana_roi.source_coverage import FROZEN_SUPPORTED_PROGRAM_IDS_BY_SOURCE
from solana_roi import venue_native_candidate_graph_repair as venue

SCOUT = "Scout111111111111111111111111111111111111111"
SPONSOR = "Sponsor111111111111111111111111111111111111"
TARGET = "Target1111111111111111111111111111111111111"
TARGET_ACCOUNT = "TargetAcct111111111111111111111111111111111"
WSOL_ACCOUNT = "WsolAcct11111111111111111111111111111111111"
VAULT_TARGET = "VaultTarget111111111111111111111111111111111"
VAULT_WSOL = "VaultWsol11111111111111111111111111111111111"
RAYDIUM = dict(FROZEN_SUPPORTED_PROGRAM_IDS_BY_SOURCE)["RAYDIUM"][0]
TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"

_BASE58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def _b58encode(payload: bytes) -> str:
    zeroes = len(payload) - len(payload.lstrip(b"\x00"))
    value = int.from_bytes(payload, "big")
    chars: list[str] = []
    while value:
        value, remainder = divmod(value, 58)
        chars.append(_BASE58[remainder])
    return "1" * zeroes + ("".join(reversed(chars)) or ("" if zeroes else "1"))


def _balance(index: int, mint: str, owner: str, amount: int, decimals: int = 0) -> dict:
    return {
        "accountIndex": index,
        "mint": mint,
        "owner": owner,
        "uiTokenAmount": {"amount": str(amount), "decimals": decimals},
    }


def _raw_transfer_checked(accounts: list[int], amount: int, decimals: int) -> dict:
    payload = bytes([12]) + int(amount).to_bytes(8, "little") + bytes([decimals])
    return {"programIdIndex": 7, "accounts": accounts, "data": _b58encode(payload)}


def _raw_sponsored_buy() -> dict:
    keys = [
        SPONSOR,
        SCOUT,
        TARGET_ACCOUNT,
        WSOL_ACCOUNT,
        VAULT_TARGET,
        VAULT_WSOL,
        RAYDIUM,
        TOKEN_PROGRAM,
        TARGET,
        WSOL_MINT,
    ]
    return {
        "slot": 777,
        "blockTime": 1_800_000_000,
        "transaction": {
            "message": {
                "accountKeys": keys,
                "header": {"numRequiredSignatures": 2},
                "instructions": [{"programIdIndex": 6, "accounts": [1, 2, 3, 4, 5]}],
            }
        },
        "meta": {
            "err": None,
            "fee": 5000,
            "preBalances": [10 * LAMPORTS_PER_SOL, 8 * LAMPORTS_PER_SOL, 0, 0, 0, 0, 0, 0, 0, 0],
            "postBalances": [10 * LAMPORTS_PER_SOL - 5000, 8 * LAMPORTS_PER_SOL, 0, 0, 0, 0, 0, 0, 0, 0],
            "preTokenBalances": [
                _balance(2, TARGET, SCOUT, 0),
                _balance(3, WSOL_MINT, SCOUT, 2_000_000_000, 9),
                _balance(4, TARGET, "vault-owner", 1_000_000),
                _balance(5, WSOL_MINT, "vault-owner", 10_000_000_000, 9),
            ],
            "postTokenBalances": [
                _balance(2, TARGET, SCOUT, 1000),
                _balance(3, WSOL_MINT, SCOUT, 750_000_000, 9),
                _balance(4, TARGET, "vault-owner", 999_000),
                _balance(5, WSOL_MINT, "vault-owner", 11_250_000_000, 9),
            ],
            "innerInstructions": [
                {
                    "index": 0,
                    "instructions": [
                        _raw_transfer_checked([4, 8, 2, 4], 1000, 0),
                        _raw_transfer_checked([3, 9, 5, 1], 1_250_000_000, 9),
                    ],
                }
            ],
        },
    }


def test_compiled_spl_transfers_recover_supported_venue_candidate() -> None:
    install_post161_candidate_attribution_repair()
    swap, error = venue._decode_supported_venue(
        _raw_sponsored_buy(),
        signature="raw-spl-swap",
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


def test_failure_diagnostic_is_shape_only_and_has_no_raw_payload() -> None:
    result = _raw_sponsored_buy()
    result["meta"]["innerInstructions"] = []
    facts = _diagnostic_facts(
        result,
        wallet=SCOUT,
        source_hint="RAYDIUM",
        reason="semantic_directional_endpoint_missing",
    )
    assert facts["source"] == "RAYDIUM"
    assert facts["wallet_signer"] is True
    assert facts["supported_instruction_group_count"] == 1
    assert "transaction" not in facts
    assert "meta" not in facts
    assert "shape" in facts


def test_post161_contract_preserves_strategy_boundaries() -> None:
    assert REPAIR_VERSION == "post161-scout-attribution-observability-v1"
    assert RAW_TRANSFER_DECODER_VERSION == "compiled-spl-system-transfer-v1"
    assert DIAGNOSTIC_VERSION == "sanitized-scout-failure-shape-v1"
