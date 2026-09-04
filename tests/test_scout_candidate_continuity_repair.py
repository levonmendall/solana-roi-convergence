from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

from solana_roi import scout_candidate_continuity_repair as repair
from solana_roi.direct_solana import WatchTarget
from solana_roi.source_coverage import FROZEN_SUPPORTED_PROGRAM_IDS_BY_SOURCE


RAYDIUM = FROZEN_SUPPORTED_PROGRAM_IDS_BY_SOURCE[2][1][0]
SPONSOR = "11111111111111111111111111111112"
SCOUT = "11111111111111111111111111111113"
OTHER_SCOUT = "11111111111111111111111111111114"
TOKEN_ACCOUNT = "11111111111111111111111111111115"
TOKEN = "So11111111111111111111111111111111111111111"


def _token_row(index: int, mint: str, owner: str, amount: int, decimals: int = 3):
    return {
        "accountIndex": index,
        "mint": mint,
        "owner": owner,
        "uiTokenAmount": {"amount": str(amount), "decimals": decimals},
    }


def _sponsored_scout_buy(*, second_scout: bool = False):
    keys = [
        {"pubkey": SPONSOR, "signer": True, "writable": True},
        {"pubkey": SCOUT, "signer": True, "writable": True},
        {"pubkey": RAYDIUM, "signer": False, "writable": False},
        {"pubkey": TOKEN_ACCOUNT, "signer": False, "writable": True},
    ]
    if second_scout:
        keys.append({"pubkey": OTHER_SCOUT, "signer": True, "writable": False})
    pre = [10_000_000_000, 8_000_000_000, 0, 0] + ([0] if second_scout else [])
    post = [9_999_995_000, 7_000_000_000, 0, 0] + ([0] if second_scout else [])
    return {
        "slot": 444_000_000,
        "blockTime": 1_788_321_600,
        "transaction": {"message": {"accountKeys": keys, "instructions": []}},
        "meta": {
            "err": None,
            "fee": 5_000,
            "preBalances": pre,
            "postBalances": post,
            "preTokenBalances": [_token_row(3, TOKEN, SCOUT, 0)],
            "postTokenBalances": [_token_row(3, TOKEN, SCOUT, 1_000_000)],
            "innerInstructions": [],
        },
    }


def test_exact_tracked_scout_is_selected_in_sponsored_transaction():
    result = _sponsored_scout_buy()
    wallet, error = repair._tracked_scout_wallet(result, (SCOUT, OTHER_SCOUT))
    assert error is None
    assert wallet == SCOUT

    swap, error = repair._normalize_tracked_wallet(
        result,
        signature="sponsored-scout-buy",
        trigger_received_at=datetime(2026, 9, 4, tzinfo=timezone.utc),
        wallet=SCOUT,
    )
    assert error is None
    assert swap is not None
    assert swap.wallet == SCOUT
    assert swap.side == "buy"
    assert swap.source == "solana-direct:RAYDIUM:buy"
    assert swap.token_mint == TOKEN
    assert swap.native_amount_sol == 1.0
    # The sponsor's 5,000-lamport transaction fee must not be added to the scout's
    # native balance delta.
    assert swap.reference_price_sol == 0.001


def test_multiple_configured_scout_signers_fail_closed():
    wallet, error = repair._tracked_scout_wallet(
        _sponsored_scout_buy(second_scout=True),
        (SCOUT, OTHER_SCOUT),
    )
    assert wallet is None
    assert error == "multiple_tracked_scout_signers"


def test_scout_targets_gain_burst_recovery_without_changing_program_scope():
    scout = WatchTarget(kind="scout", address=SCOUT, source_hint=None)
    assert repair._burst_sensitive_target(scout) is True
    assert repair._scout_source_key(scout) == f"SCOUT:{SCOUT}"
    assert repair.RECOVERABILITY_LEASE_SECONDS_UNCHANGED == 12.0
    assert repair.HARD_RECOVERY_BOUND_UNCHANGED == "3x1000"
    assert repair.CANDIDATE_PROCESSING_TARGET_SECONDS_UNCHANGED == 5.0
    assert repair.CANDIDATE_ENTRY_WINDOW_SECONDS_UNCHANGED == 20.0
    assert repair.MAX_CHASE_FRACTION_UNCHANGED == 0.15


def test_pre_persistence_v4_schedule_is_deferred_to_candidate_worker():
    calls: list[tuple[object, str]] = []
    old = repair._ORIGINAL_HANDOFF_SCHEDULE
    repair._ORIGINAL_HANDOFF_SCHEDULE = lambda obj, signature: calls.append((obj, signature))
    plane = SimpleNamespace()
    plane.service = SimpleNamespace(_roi_candidate_execution_plane=plane)
    token = repair._SCOUT_HYDRATION_PLANE.set(plane)
    try:
        repair._defer_pre_persistence_v4_handoff(plane, "sig")
    finally:
        repair._SCOUT_HYDRATION_PLANE.reset(token)
        repair._ORIGINAL_HANDOFF_SCHEDULE = old
    assert calls == []
    assert plane._roi_scout_candidate_continuity_v4_handoff_deferred_until_candidate_execution == 1


def test_candidate_worker_schedules_v4_only_after_ingest_returns():
    events: list[str] = []
    old_ingest = repair._ORIGINAL_EXECUTION_INGEST
    old_schedule = repair._ORIGINAL_HANDOFF_SCHEDULE

    async def ingest(service, swap):
        events.append("persisted")
        return SimpleNamespace(decision="record_only")

    repair._ORIGINAL_EXECUTION_INGEST = ingest
    repair._ORIGINAL_HANDOFF_SCHEDULE = lambda plane, signature: events.append(f"handoff:{signature}")
    plane = SimpleNamespace()
    service = SimpleNamespace(_roi_candidate_execution_plane=plane)
    swap = SimpleNamespace(signature="durable-sig")
    try:
        decision = asyncio.run(repair._candidate_ingest_then_v4_handoff(service, swap))
    finally:
        repair._ORIGINAL_EXECUTION_INGEST = old_ingest
        repair._ORIGINAL_HANDOFF_SCHEDULE = old_schedule
    assert decision.decision == "record_only"
    assert events == ["persisted", "handoff:durable-sig"]
    assert plane._roi_scout_candidate_continuity_v4_handoff_after_durable_candidate_execution == 1


def test_paper_and_strategy_boundaries_are_unchanged():
    assert repair.PAPER_ONLY is True
    assert repair.LIVE_MONEY_AUTHORITY is False
    assert repair.SIGNING_AVAILABLE is False
    assert repair.TRANSACTION_SUBMISSION_AVAILABLE is False
