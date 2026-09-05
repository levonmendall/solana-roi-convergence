from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from solana_roi import economic_signal_continuation_repair as repair
from solana_roi import venue_native_candidate_graph_repair as venue


WALLET = "Scout1111111111111111111111111111111111111"
TOKEN_A = "TokenA1111111111111111111111111111111111111"
TOKEN_B = "TokenB1111111111111111111111111111111111111"
TOKEN_ACCOUNT_A = "AtaA11111111111111111111111111111111111111"
TOKEN_ACCOUNT_B = "AtaB11111111111111111111111111111111111111"
PUMP_FUN = next(iter(venue._PROGRAM_IDS_BY_SOURCE["PUMP_FUN"]))


def _amount(value: int) -> dict:
    return {"amount": str(value), "decimals": 0, "uiAmount": float(value)}


def _result(
    *,
    token_rows: list[tuple[str, str, int, int]],
    pre_lamports: int = 1_000_000_000,
    post_lamports: int = 900_000_000,
    fee: int = 0,
    include_pump_key_only: bool = False,
) -> dict:
    keys = [WALLET]
    pre_token = []
    post_token = []
    for account, mint, before, after in token_rows:
        index = len(keys)
        keys.append(account)
        pre_token.append(
            {"accountIndex": index, "mint": mint, "owner": WALLET, "uiTokenAmount": _amount(before)}
        )
        post_token.append(
            {"accountIndex": index, "mint": mint, "owner": WALLET, "uiTokenAmount": _amount(after)}
        )
    if include_pump_key_only:
        keys.append(PUMP_FUN)
    balances = [pre_lamports] + [2_000_000 for _ in keys[1:]]
    post_balances = [post_lamports] + [2_000_000 for _ in keys[1:]]
    return {
        "slot": 123,
        "blockTime": 1_788_625_600,
        "transaction": {
            "message": {
                "accountKeys": keys,
                "header": {"numRequiredSignatures": 1},
                "instructions": [],
            }
        },
        "meta": {
            "err": None,
            "fee": fee,
            "preBalances": balances,
            "postBalances": post_balances,
            "preTokenBalances": pre_token,
            "postTokenBalances": post_token,
            "innerInstructions": [],
        },
    }


def test_economic_movement_precedes_venue_and_recovers_router_buy() -> None:
    result = _result(token_rows=[(TOKEN_ACCOUNT_A, TOKEN_A, 0, 100)])

    movement, error = repair._economic_movement(result, WALLET)

    assert error is None
    assert movement is not None
    assert movement["side"] == "buy"
    assert movement["token_mint"] == TOKEN_A
    assert movement["token_amount"] == 100.0
    assert movement["native_amount_sol"] == pytest.approx(0.1)
    assert movement["movement_authority"] == "owner_token_delta"


def test_account_key_only_pump_reference_without_economic_movement_stays_non_candidate() -> None:
    result = _result(token_rows=[], include_pump_key_only=True)

    movement, error = repair._economic_movement(result, WALLET)

    assert movement is None
    assert error == "economic_token_movement_missing"


def test_multiple_owner_token_endpoints_fail_closed() -> None:
    result = _result(
        token_rows=[
            (TOKEN_ACCOUNT_A, TOKEN_A, 0, 100),
            (TOKEN_ACCOUNT_B, TOKEN_B, 0, 50),
        ]
    )

    movement, error = repair._economic_movement(result, WALLET)

    assert movement is None
    assert error == "economic_multiple_token_endpoints"


def test_router_unknown_normalizer_is_priced_from_proven_actor_flow(monkeypatch: pytest.MonkeyPatch) -> None:
    result = _result(token_rows=[(TOKEN_ACCOUNT_A, TOKEN_A, 0, 100)])
    monkeypatch.setattr(
        repair,
        "_ORIGINAL_NORMALIZER",
        lambda *args, **kwargs: (None, "supported_swap_source_missing"),
    )

    swap, error = repair._economic_signal_normalizer(
        result,
        signature="sig-router",
        trigger_received_at=datetime.fromtimestamp(1_788_625_601, tz=timezone.utc),
        wallet=WALLET,
        source_hint=None,
    )

    assert error is None
    assert swap is not None
    assert swap.side == "buy"
    assert swap.token_mint == TOKEN_A
    assert swap.native_amount_sol == pytest.approx(0.1)
    assert swap.reference_price_sol == pytest.approx(0.001)
    assert repair.ROUTER_OR_UNKNOWN_VENUE in swap.source


def test_economic_signal_without_quote_value_is_retained_but_not_normalized(monkeypatch: pytest.MonkeyPatch) -> None:
    result = _result(
        token_rows=[(TOKEN_ACCOUNT_A, TOKEN_A, 0, 100)],
        pre_lamports=1_000_000_000,
        post_lamports=1_000_000_000,
    )
    monkeypatch.setattr(
        repair,
        "_ORIGINAL_NORMALIZER",
        lambda *args, **kwargs: (None, "supported_swap_source_missing"),
    )

    swap, error = repair._economic_signal_normalizer(
        result,
        signature="sig-unpriced",
        trigger_received_at=datetime.fromtimestamp(1_788_625_601, tz=timezone.utc),
        wallet=WALLET,
        source_hint=None,
    )

    assert swap is None
    assert error == "economic_movement_price_unresolved"


def test_time_is_context_not_twenty_second_expiration() -> None:
    assert repair._evaluation_lane(5.0) == "immediate_copy"
    assert repair._evaluation_lane(20.0) == "immediate_copy"
    assert repair._evaluation_lane(20.1) == "confirmed_continuation"
    assert repair._evaluation_lane(60.1) == "strong_continuation"
    assert repair._evaluation_lane(120.1) == "mature_continuation"
    assert repair._evaluation_lane(300.1) == "fresh_signal_required"
    assert repair.CANDIDATE_OPERATIONAL_TIMEOUT_SECONDS > repair.IMMEDIATE_COPY_SECONDS


class _Store:
    def latest_risk_evidence(self, *args, **kwargs):
        return None


class _EntityResolver:
    def entity_id_for(self, *args, **kwargs):
        return None


class _RiskMissing:
    async def snapshot(self, *args, **kwargs):
        return None


class _RiskUnexitable:
    async def snapshot(self, *args, **kwargs):
        return SimpleNamespace(
            unacceptable_liquidity=True,
            bundled_launch=False,
            sniper_heavy=False,
            abnormal_sell_pressure=False,
            common_funded_early_wallet_cluster=False,
            scout_deployer_connection=False,
            early_buyers_exiting=False,
        )


def _research_adapter_with_risk(risk) -> SimpleNamespace:
    return SimpleNamespace(
        store=_Store(),
        discovery=SimpleNamespace(risk=risk, entity_resolver=_EntityResolver()),
    )


def test_missing_risk_bundle_is_soft_unknown_not_fake_liquidity_failure() -> None:
    adapter = _research_adapter_with_risk(_RiskMissing())
    hard, soft, early_exit = asyncio.run(
        repair._risk_unknown_is_not_liquidity_failure(
            adapter,
            {"token_mint": TOKEN_A, "wallet": WALLET, "risk_complete": 0},
            datetime.now(timezone.utc),
        )
    )

    assert "liquidity_unexitable" not in hard
    assert "risk_bundle_incomplete" in soft
    assert early_exit == 0.0


def test_proven_unexitable_liquidity_remains_mechanical_hard_stop() -> None:
    adapter = _research_adapter_with_risk(_RiskUnexitable())
    hard, soft, _early_exit = asyncio.run(
        repair._risk_unknown_is_not_liquidity_failure(
            adapter,
            {"token_mint": TOKEN_A, "wallet": WALLET, "risk_complete": 0},
            datetime.now(timezone.utc),
        )
    )

    assert "liquidity_unexitable" in hard
    assert "risk_bundle_incomplete" in soft


def test_repair_contract_preserves_paper_only_and_mechanical_stops() -> None:
    assert repair.IMMEDIATE_COPY_SECONDS == 20.0
    assert repair.MATURE_CONTINUATION_SECONDS == 300.0
    assert "liquidity_unexitable" in repair._MECHANICAL_HARD_STOPS
    assert "authority_can_block_transfer_or_exit" in repair._MECHANICAL_HARD_STOPS
    assert repair.PAPER_ONLY is True
    assert repair.LIVE_MONEY_AUTHORITY is False
    assert repair.SIGNING_AVAILABLE is False
    assert repair.TRANSACTION_SUBMISSION_AVAILABLE is False
