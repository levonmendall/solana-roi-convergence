from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from solana_roi.ingestion import LAMPORTS_PER_SOL, NormalizedSwap
from solana_roi.observation import WSOL_MINT
from solana_roi.semantic_candidate_attribution_architecture import (
    ARCHITECTURE_VERSION,
    ATTRIBUTION_VERSION,
    IMMEDIATE_ENTRY_WINDOW_SECONDS_UNCHANGED,
    _decode_supported_venue,
    _persist_opportunity,
    _persist_risk_readthrough,
    _semantic_normalize_tracked_wallet,
    install_semantic_candidate_attribution_architecture,
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
    service = SimpleNamespace(store=store, risk_provider=risk_provider)
    return SimpleNamespace(store=store, service=service)


def _token_balance(account_index: int, mint: str, owner: str, amount: float) -> dict:
    return {
        "accountIndex": account_index,
        "mint": mint,
        "owner": owner,
        "uiTokenAmount": {"uiAmount": amount},
    }


def _result(
    *,
    scout_pre_sol: float,
    scout_post_sol: float,
    target_pre: float,
    target_post: float,
    aux_pre: float,
    aux_post: float,
    fee_payer: str = SPONSOR,
    fee_lamports: int = 5000,
    signature: str = "sig-1",
    block_time: float = 1_800_000_000.0,
) -> dict:
    keys = [
        {"pubkey": fee_payer, "signer": True},
        {"pubkey": SCOUT, "signer": True},
        {"pubkey": TARGET_ACCOUNT, "signer": False},
        {"pubkey": AUX_ACCOUNT, "signer": False},
        {"pubkey": RAYDIUM, "signer": False},
    ]
    pre_balances = [10 * LAMPORTS_PER_SOL, scout_pre_sol * LAMPORTS_PER_SOL, 0, 0, 0]
    post_balances = [
        10 * LAMPORTS_PER_SOL - fee_lamports,
        scout_post_sol * LAMPORTS_PER_SOL,
        0,
        0,
        0,
    ]
    if fee_payer == SCOUT:
        keys[0] = {"pubkey": SCOUT, "signer": True}
        keys[1] = {"pubkey": SPONSOR, "signer": False}
        pre_balances[0] = scout_pre_sol * LAMPORTS_PER_SOL
        post_balances[0] = scout_post_sol * LAMPORTS_PER_SOL - fee_lamports
        pre_balances[1] = 10 * LAMPORTS_PER_SOL
        post_balances[1] = 10 * LAMPORTS_PER_SOL
        target_owner_index = 0
    else:
        target_owner_index = 1
    return {
        "slot": 123,
        "blockTime": block_time,
        "transaction": {
            "signatures": [signature],
            "message": {
                "accountKeys": keys,
                "instructions": [],
            },
        },
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


def test_buy_uses_native_direction_then_positive_endpoint_not_single_delta_assumption():
    received = datetime.fromtimestamp(1_800_000_000.25, tz=timezone.utc)
    result = _result(
        scout_pre_sol=8.0,
        scout_post_sol=7.0,
        target_pre=0.0,
        target_post=1000.0,
        aux_pre=1000.0,
        aux_post=500.0,
    )
    swap, error = _decode_supported_venue(
        result,
        wallet=SCOUT,
        received_at=received,
        source_hint="RAYDIUM",
        source="RAYDIUM",
    )
    assert error is None
    assert swap is not None
    assert swap.side == "buy"
    assert swap.token_mint == TARGET
    assert swap.token_amount == 1000.0
    assert swap.native_amount_sol == 1.0
    assert swap.source == "solana-direct:RAYDIUM:buy"


def test_sell_uses_native_direction_then_negative_endpoint():
    received = datetime.fromtimestamp(1_800_000_000.25, tz=timezone.utc)
    result = _result(
        scout_pre_sol=8.0,
        scout_post_sol=9.0,
        target_pre=1000.0,
        target_post=0.0,
        aux_pre=0.0,
        aux_post=500.0,
    )
    swap, error = _decode_supported_venue(
        result,
        wallet=SCOUT,
        received_at=received,
        source_hint="RAYDIUM",
        source="RAYDIUM",
    )
    assert error is None
    assert swap is not None
    assert swap.side == "sell"
    assert swap.token_mint == TARGET
    assert swap.token_amount == 1000.0
    assert swap.native_amount_sol == 1.0


def test_multiple_same_direction_endpoints_fail_closed():
    received = datetime.fromtimestamp(1_800_000_000.25, tz=timezone.utc)
    result = _result(
        scout_pre_sol=8.0,
        scout_post_sol=7.0,
        target_pre=0.0,
        target_post=1000.0,
        aux_pre=0.0,
        aux_post=500.0,
    )
    swap, error = _decode_supported_venue(
        result,
        wallet=SCOUT,
        received_at=received,
        source_hint="RAYDIUM",
        source="RAYDIUM",
    )
    assert swap is None
    assert error == "semantic_multiple_directional_endpoints"


def test_native_direction_missing_fails_closed():
    received = datetime.fromtimestamp(1_800_000_000.25, tz=timezone.utc)
    result = _result(
        scout_pre_sol=8.0,
        scout_post_sol=8.0,
        target_pre=0.0,
        target_post=1000.0,
        aux_pre=1000.0,
        aux_post=500.0,
    )
    swap, error = _decode_supported_venue(
        result,
        wallet=SCOUT,
        received_at=received,
        source_hint="RAYDIUM",
        source="RAYDIUM",
    )
    assert swap is None
    assert error == "semantic_native_wsol_direction_ambiguous"


def test_scout_fee_is_removed_only_when_scout_is_actual_fee_payer():
    received = datetime.fromtimestamp(1_800_000_000.25, tz=timezone.utc)
    result = _result(
        scout_pre_sol=8.0,
        scout_post_sol=7.0,
        target_pre=0.0,
        target_post=1000.0,
        aux_pre=0.0,
        aux_post=0.0,
        fee_payer=SCOUT,
        fee_lamports=5000,
    )
    swap, error = _decode_supported_venue(
        result,
        wallet=SCOUT,
        received_at=received,
        source_hint="RAYDIUM",
        source="RAYDIUM",
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
        ingestion_latency_ms=100.0,
        source="solana-direct:RAYDIUM:buy",
    )


def test_durable_mint_venue_ledger_reuses_opportunity_and_refreshes_prospective_clock():
    plane = _plane()
    first = datetime(2026, 9, 4, 10, 0, tzinfo=timezone.utc)
    second = first + timedelta(minutes=20)
    assert _persist_opportunity(plane, _swap("sig-1", first)) is True
    assert _persist_opportunity(plane, _swap("sig-1", first)) is False
    assert _persist_opportunity(plane, _swap("sig-2", second)) is True

    with plane.store._lock:
        row = plane.store.db.execute(
            "SELECT first_seen,last_seen,last_signature,signal_count,latest_immediate_deadline,"
            "continuation_eligible,entry_authority,architecture_version "
            "FROM semantic_candidate_opportunities WHERE token_mint=? AND venue=?",
            (TARGET, "RAYDIUM"),
        ).fetchone()
    assert row is not None
    assert row[0] == first.isoformat()
    assert row[1] == second.isoformat()
    assert row[2] == "sig-2"
    assert row[3] == 2
    assert row[4] == (second + timedelta(seconds=IMMEDIATE_ENTRY_WINDOW_SECONDS_UNCHANGED)).isoformat()
    assert row[5] == 1
    assert row[6] == 0
    assert row[7] == ARCHITECTURE_VERSION


def test_risk_state_is_readthrough_only_and_has_no_entry_authority():
    plane = _plane(risk_provider=_RiskProvider())
    when = datetime(2026, 9, 4, 10, 0, tzinfo=timezone.utc)
    swap = _swap("sig-risk", when)
    _persist_opportunity(plane, swap)
    _persist_risk_readthrough(plane, swap)
    with plane.store._lock:
        row = plane.store.db.execute(
            "SELECT complete,fresh,fresh_dimensions_json,entry_authority,architecture_version "
            "FROM semantic_candidate_risk_state WHERE token_mint=? AND venue=?",
            (TARGET, "RAYDIUM"),
        ).fetchone()
    assert row is not None
    assert row[0] == 1
    assert row[1] == 1
    assert '"authority":true' in row[2]
    assert row[3] == 0
    assert row[4] == ARCHITECTURE_VERSION


def test_semantic_normalizer_persists_before_returning_candidate_fact():
    from solana_roi import scout_candidate_continuity_repair as scout

    plane = _plane(risk_provider=_RiskProvider())
    received = datetime.fromtimestamp(1_800_000_000.25, tz=timezone.utc)
    result = _result(
        scout_pre_sol=8.0,
        scout_post_sol=7.0,
        target_pre=0.0,
        target_post=1000.0,
        aux_pre=1000.0,
        aux_post=500.0,
        signature="sig-semantic",
    )
    token = scout._SCOUT_HYDRATION_PLANE.set(plane)
    try:
        swap, error = _semantic_normalize_tracked_wallet(
            result,
            wallet=SCOUT,
            received_at=received,
            source_hint="RAYDIUM",
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


def test_installer_preserves_paper_only_and_existing_scout_marker():
    from solana_roi.direct_solana import DirectSolanaIngestionPlane

    install_semantic_candidate_attribution_architecture()
    assert getattr(DirectSolanaIngestionPlane.status, "_roi_semantic_candidate_attribution", False) is True
    assert getattr(DirectSolanaIngestionPlane.status, "_roi_scout_candidate_continuity", False) is True
