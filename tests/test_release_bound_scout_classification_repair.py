from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from solana_roi import certification_failure_accounting_repair as accounting
from solana_roi import release_bound_scout_classification_repair as repair
from solana_roi import scout_candidate_continuity_repair as scout
from solana_roi.observation import LatencyCertificationGate
from solana_roi.observation_store import ObservationEventStore
from solana_roi.source_coverage import FROZEN_SUPPORTED_PROGRAM_IDS_BY_SOURCE


SCOUT = "11111111111111111111111111111113"
RAYDIUM = FROZEN_SUPPORTED_PROGRAM_IDS_BY_SOURCE[2][1][0]


def _now() -> datetime:
    return datetime.now(timezone.utc)


def test_pre_release_trigger_reaped_after_boundary_is_not_new_release_failure(tmp_path):
    store = ObservationEventStore(tmp_path / "release-bound.sqlite3")
    start = _now()
    stale_trigger = start - timedelta(minutes=2)
    fresh_trigger = start + timedelta(seconds=1)

    repair._record_release_bound_failure(
        store,
        signature="stale-sig",
        trigger_received_at=stale_trigger,
        reason="frozen_scout_processed_trigger",
        outcome="expired_before_entry",
        failed_at=start + timedelta(seconds=5),
    )
    repair._record_release_bound_failure(
        store,
        signature="fresh-sig",
        trigger_received_at=fresh_trigger,
        reason="frozen_scout_processed_trigger",
        outcome="expired_before_entry",
        failed_at=fresh_trigger + timedelta(seconds=21),
    )

    rows = repair._release_bound_failure_rows(store, since=start)
    assert [row["signature"] for row in rows] == ["fresh-sig"]
    store.close()


def test_latency_gate_uses_original_trigger_epoch_not_failure_time(tmp_path):
    store = ObservationEventStore(tmp_path / "latency-bound.sqlite3")
    start = _now()
    repair._record_release_bound_failure(
        store,
        signature="old-release-sig",
        trigger_received_at=start - timedelta(minutes=1),
        reason="frozen_scout_live_poll_trigger",
        outcome="expired_before_entry",
        failed_at=start + timedelta(seconds=2),
    )

    old_status = accounting._ORIGINAL_LATENCY_STATUS
    accounting._ORIGINAL_LATENCY_STATUS = lambda _self, limit=500: {
        "certified": True,
        "requirements": {},
    }
    try:
        gate = LatencyCertificationGate(store, prospective_start_at=start)
        status = repair._latency_status_release_bound(gate)
    finally:
        accounting._ORIGINAL_LATENCY_STATUS = old_status

    assert status["certified"] is True
    assert status["candidate_sampling_complete"] is True
    assert status["unclassified_scout_trigger_expiry_count"] == 0
    boundary = status["release_bound_failure_accounting"]
    assert boundary["authority_boundary"] == "trigger_received_at>=prospective_start_at"
    assert boundary["inherited_pre_release_queue_rows_excluded"] is True
    assert boundary["legacy_failed_at_rows_authoritative"] is False
    store.close()


def test_terminal_non_candidate_does_not_become_anonymous_expiry(tmp_path):
    store = ObservationEventStore(tmp_path / "terminal.sqlite3")
    trigger = _now() - timedelta(seconds=25)
    repair._record_terminal_non_candidate(
        store,
        signature="non-swap-sig",
        trigger_received_at=trigger,
        reason="supported_swap_source_missing",
    )

    old_account = repair._ORIGINAL_ACCOUNT_SCOUT_EXPIRY
    repair._ORIGINAL_ACCOUNT_SCOUT_EXPIRY = lambda *_args, **_kwargs: "unexpected_delegate"
    try:
        outcome = repair._account_scout_expiry_release_bound(
            store,
            {
                "signature": "non-swap-sig",
                "trigger_received_at": trigger.isoformat(),
                "reason": "frozen_scout_processed_trigger",
            },
            outcome="expired_before_entry",
            failed_at=_now(),
        )
    finally:
        repair._ORIGINAL_ACCOUNT_SCOUT_EXPIRY = old_account

    assert outcome == "classified_non_candidate_terminal"
    assert repair._release_bound_failure_rows(store, since=None) == []
    store.close()


def _tracked_scout_transaction(*, supported_program: str | None = None) -> dict:
    account_keys = [
        {"pubkey": SCOUT, "signer": True, "writable": True},
        {"pubkey": "11111111111111111111111111111112", "signer": False, "writable": False},
    ]
    instructions = []
    if supported_program is not None:
        account_keys.append({"pubkey": supported_program, "signer": False, "writable": False})
        instructions.append({"programIdIndex": 2, "accounts": [], "data": ""})
    return {
        "slot": 444_000_000,
        "transaction": {"message": {"accountKeys": account_keys, "instructions": instructions}},
        "meta": {
            "err": None,
            "fee": 5000,
            "preBalances": [1_000_000_000, 0] + ([0] if supported_program else []),
            "postBalances": [999_995_000, 0] + ([0] if supported_program else []),
            "preTokenBalances": [],
            "postTokenBalances": [],
            "innerInstructions": [],
        },
    }


def test_tracked_scout_without_supported_swap_source_is_terminal_non_candidate(tmp_path):
    store = ObservationEventStore(tmp_path / "normalization.sqlite3")
    plane = SimpleNamespace(store=store, scout_wallets=(SCOUT,))
    trigger = _now()
    old_normalize = repair._ORIGINAL_SCOUT_NORMALIZE
    repair._ORIGINAL_SCOUT_NORMALIZE = lambda *_args, **_kwargs: None
    token = scout._SCOUT_HYDRATION_PLANE.set(plane)
    try:
        result = repair._normalize_with_terminal_non_candidate(
            _tracked_scout_transaction(),
            signature="tracked-non-swap",
            trigger_received_at=trigger,
        )
    finally:
        scout._SCOUT_HYDRATION_PLANE.reset(token)
        repair._ORIGINAL_SCOUT_NORMALIZE = old_normalize

    assert result is None
    row = repair._terminal_classification(store, "tracked-non-swap")
    assert row is not None
    assert row["classification"] == "non_candidate"
    assert row["reason"] == "supported_swap_source_missing"
    store.close()


def test_supported_program_normalization_failure_stays_fail_closed_not_terminal(tmp_path):
    store = ObservationEventStore(tmp_path / "supported-still-fail-closed.sqlite3")
    plane = SimpleNamespace(store=store, scout_wallets=(SCOUT,))
    trigger = _now()
    old_normalize = repair._ORIGINAL_SCOUT_NORMALIZE
    repair._ORIGINAL_SCOUT_NORMALIZE = lambda *_args, **_kwargs: None
    token = scout._SCOUT_HYDRATION_PLANE.set(plane)
    try:
        result = repair._normalize_with_terminal_non_candidate(
            _tracked_scout_transaction(supported_program=RAYDIUM),
            signature="supported-but-ambiguous",
            trigger_received_at=trigger,
        )
    finally:
        scout._SCOUT_HYDRATION_PLANE.reset(token)
        repair._ORIGINAL_SCOUT_NORMALIZE = old_normalize

    assert result is None
    assert repair._terminal_classification(store, "supported-but-ambiguous") is None
    store.close()


def test_boundaries_unchanged():
    assert repair.PAPER_ONLY is True
    assert repair.LIVE_MONEY_AUTHORITY is False
    assert repair.SIGNING_AVAILABLE is False
    assert repair.TRANSACTION_SUBMISSION_AVAILABLE is False
