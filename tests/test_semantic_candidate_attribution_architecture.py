from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from solana_roi.ingestion import LAMPORTS_PER_SOL, NormalizedSwap
from solana_roi.semantic_candidate_attribution_architecture import (
    ARCHITECTURE_VERSION,
    ATTRIBUTION_VERSION,
    IMMEDIATE_ENTRY_WINDOW_SECONDS_UNCHANGED,
    _decode_supported_venue,
    _persist_opportunity,
    _persist_risk_readthrough,
    _semantic_normalize_tracked_wallet,
)
from solana_roi.source_coverage import FROZEN_SUPPORTED_PROGRAM_IDS_BY_SOURCE

SCOUT = "Scout111111111111111111111111111111111111111"
SPONSOR = "Sponsor111111111111111111111111111111111111"
TARGET = "Target1111111111111111111111111111111111111"
AUX = "Auxiliary11111111111111111111111111111111111"
TARGET_ACCOUNT = "TargetAcct111111111111111111111111111111111"
AUX_ACCOUNT = "AuxAcct111111111111111111111111111111111111"
RAYDIUM = dict(FROZEN_SUPPORTED_PROGRAM_IDS_BY_SOURCE)["RAYDIUM"][0]


class _Store:
    def __init__(self) -> None:
        self.db = sqlite3.connect(":memory:")
        self._lock = threading.RLock()


class _RiskProvider:
    def readiness(self, token_mint, *, as_of):
        assert token_mint == TARGET
        assert isinstance(as_of, datetime)
        return {
            "complete": True,
            "fresh": True,
            "fresh_dimensions": {
                "authority": True,
                "liquidity": True,
                "launch": True,
                "flow": True,
                "funding": True,
                "deployer": True,
            },
        }


def _plane(*, risk_provider=None):
    store = _Store()
    return SimpleNamespace(
        store=store,
        service=SimpleNamespace(store=store, risk_provider=risk_provider),
    )


def _token_balance(index: int, mint: str, owner: str, amount: int) -> dict:
    return {
        "accountIndex": index,
        "mint": mint,
        "owner": owner,
        "uiTokenAmount": {"amount": str(amount), "decimals": 0},
    }


def _result(
    *,
    scout_pre_sol: float,
    scout_post_sol: float,
    target_pre: int,
    target_post: int,
    aux_pre: int,
    aux_post: int,
    fee_payer: str = SPONSOR,
    fee_lamports: int = 5000,
    block_time: int = 1_800_000_000,
) -> dict:
    if fee_payer == SCOUT:
        keys = [
            {"pubkey": SCOUT, "signer": True},
            {"pubkey": SPONSOR, "signer": False},
            {"pubkey": TARGET_ACCOUNT, "signer": False},
            {"pubkey": AUX_ACCOUNT, "signer": False},
            {"pubkey": RAYDIUM, "signer": False},
        ]
        pre_balances = [
            int(scout_pre_sol * LAMPORTS_PER_SOL),
            10 * LAMPORTS_PER_SOL,
            0,
            0,
            0,
        ]
        post_balances = [
            int(scout_post_sol * LAMPORTS_PER_SOL) - fee_lamports,
            10 * LAMPORTS_PER_SOL,
            0,
            0,
            0,
        ]
    else:
        keys = [
            {"pubkey": SPONSOR, "signer": True},
            {"pubkey": SCOUT, "signer": True},
            {"pubkey": TARGET_ACCOUNT, "signer": False},
            {"pubkey": AUX_ACCOUNT, "signer": False},
            {"pubkey": RAYDIUM, "signer": False},
        ]
        pre_balances = [
            10 * LAMPORTS_PER_SOL,
            int(scout_pre_sol * LAMPORTS_PER_SOL),
            0,
            0,
            0,
        ]
        post_balances = [
            10 * LAMPORTS_PER_SOL - fee_lamports,
            int(scout_post_sol * LAMPORTS_PER_SOL),
            0,
            0,
            0,
        ]
    return {
        "slot": 123,
        "blockTime": block_time,
        "transaction": {"message": {"accountKeys": keys, "instructions": []}},
        "meta": {
            "err": None,
            "fee": fee_lamports,
            "preBalances": pre_balances,
            "postBalances": post_balances,
            "preTokenBalances": [
                _token_balance(2, TARGET, SCOUT, target_pre),
                _token_balance(3, AUX, SCOUT, aux_pre),
            ],
            "postTokenBalances": [
                _token_balance(2, TARGET, SCOUT, target_post),
                _token_balance(3, AUX, SCOUT, aux_post),
            ],
        },
    }


def _decode(result: dict, *, signature: str = "sig"):
    return _decode_supported_venue(
        result,
        signature=signature,
        trigger_received_at=datetime.fromtimestamp(1_800_000_000.25, tz=timezone.utc),
        wallet=SCOUT,
        source="RAYDIUM",
    )


def test_buy_resolves_positive_endpoint_despite_opposite_auxiliary_delta():
    swap, error = _decode(
        _result(
            scout_pre_sol=8.0,
            scout_post_sol=7.0,
            target_pre=0,
            target_post=1000,
            aux_pre=1000,
            aux_post=500,
        )
    )
    assert error is None
    assert swap is not None
    assert (swap.side, swap.token_mint, swap.token_amount, swap.native_amount_sol) == (
        "buy",
        TARGET,
        1000.0,
        1.0,
    )


def test_sell_resolves_negative_endpoint_despite_opposite_auxiliary_delta():
    swap, error = _decode(
        _result(
            scout_pre_sol=8.0,
            scout_post_sol=9.0,
            target_pre=1000,
            target_post=0,
            aux_pre=0,
            aux_post=500,
        )
    )
    assert error is None
    assert swap is not None
    assert (swap.side, swap.token_mint, swap.token_amount, swap.native_amount_sol) == (
        "sell",
        TARGET,
        1000.0,
        1.0,
    )


def test_multiple_same_direction_endpoints_fail_closed():
    swap, error = _decode(
        _result(
            scout_pre_sol=8.0,
            scout_post_sol=7.0,
            target_pre=0,
            target_post=1000,
            aux_pre=0,
            aux_post=500,
        )
    )
    assert swap is None
    assert error == "semantic_multiple_directional_endpoints"


def test_missing_native_or_wsol_direction_fails_closed():
    swap, error = _decode(
        _result(
            scout_pre_sol=8.0,
            scout_post_sol=8.0,
            target_pre=0,
            target_post=1000,
            aux_pre=1000,
            aux_post=500,
        )
    )
    assert swap is None
    assert error == "semantic_native_wsol_direction_ambiguous"


def test_scout_fee_adjustment_applies_only_when_scout_is_fee_payer():
    swap, error = _decode(
        _result(
            scout_pre_sol=8.0,
            scout_post_sol=7.0,
            target_pre=0,
            target_post=1000,
            aux_pre=0,
            aux_post=0,
            fee_payer=SCOUT,
            fee_lamports=5000,
        )
    )
    assert error is None
    assert swap is not None
    assert abs(swap.native_amount_sol - 1.0) < 1e-12


def _swap(signature: str, when: datetime) -> NormalizedSwap:
    return NormalizedSwap(
        signature=signature,
        slot=1,
        observed_at=when,
        received_at=when + timedelta(milliseconds=100),
        wallet=SCOUT,
        token_mint=TARGET,
        side="buy",
        token_amount=1000.0,
        native_amount_sol=1.0,
        reference_price_sol=0.001,
        source="solana-direct:RAYDIUM:buy",
    )


def test_durable_opportunity_reuses_mint_venue_and_later_signal_gets_fresh_clock():
    plane = _plane()
    first = datetime(2026, 9, 4, 10, 0, tzinfo=timezone.utc)
    later = first + timedelta(minutes=20)
    assert _persist_opportunity(plane, _swap("sig-1", first)) is True
    assert _persist_opportunity(plane, _swap("sig-1", first)) is False
    assert _persist_opportunity(plane, _swap("sig-2", later)) is True
    with plane.store._lock:
        row = plane.store.db.execute(
            "SELECT first_seen,last_seen,last_signature,signal_count,latest_immediate_deadline,"
            "continuation_eligible,entry_authority,architecture_version "
            "FROM semantic_candidate_opportunities WHERE token_mint=? AND venue='RAYDIUM'",
            (TARGET,),
        ).fetchone()
    assert row == (
        first.isoformat(),
        later.isoformat(),
        "sig-2",
        2,
        (later + timedelta(seconds=IMMEDIATE_ENTRY_WINDOW_SECONDS_UNCHANGED)).isoformat(),
        1,
        0,
        ARCHITECTURE_VERSION,
    )


def test_risk_state_is_readthrough_only_and_never_entry_authority():
    plane = _plane(risk_provider=_RiskProvider())
    when = datetime(2026, 9, 4, 10, 0, tzinfo=timezone.utc)
    swap = _swap("sig-risk", when)
    _persist_opportunity(plane, swap)
    _persist_risk_readthrough(plane, swap)
    with plane.store._lock:
        row = plane.store.db.execute(
            "SELECT complete,fresh,fresh_dimensions_json,entry_authority,architecture_version "
            "FROM semantic_candidate_risk_state WHERE token_mint=? AND venue='RAYDIUM'",
            (TARGET,),
        ).fetchone()
    assert row is not None
    assert row[0:2] == (1, 1)
    assert '"authority":true' in row[2]
    assert row[3:] == (0, ARCHITECTURE_VERSION)


def test_semantic_normalizer_persists_proven_scout_fact_before_candidate_return():
    from solana_roi import scout_candidate_continuity_repair as scout

    plane = _plane(risk_provider=_RiskProvider())
    result = _result(
        scout_pre_sol=8.0,
        scout_post_sol=7.0,
        target_pre=0,
        target_post=1000,
        aux_pre=1000,
        aux_post=500,
    )
    token = scout._SCOUT_HYDRATION_PLANE.set(plane)
    try:
        swap, error = _semantic_normalize_tracked_wallet(
            result,
            signature="sig-semantic",
            trigger_received_at=datetime.fromtimestamp(1_800_000_000.25, tz=timezone.utc),
            wallet=SCOUT,
            source_hint=None,
        )
    finally:
        scout._SCOUT_HYDRATION_PLANE.reset(token)
    assert error is None
    assert swap is not None
    with plane.store._lock:
        event = plane.store.db.execute(
            "SELECT attribution_method FROM semantic_candidate_events WHERE signature='sig-semantic'"
        ).fetchone()
    assert event == (ATTRIBUTION_VERSION,)
