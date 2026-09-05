from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from solana_roi import post178_e2e_residual_repair as repair
from solana_roi import release_bound_scout_classification_repair as release_bound
from solana_roi import scout_candidate_continuity_repair as scout


SCOUT_A = "ScoutA111111111111111111111111111111111111"
SCOUT_B = "ScoutB111111111111111111111111111111111111"
OTHER = "Other1111111111111111111111111111111111111"


def _string_key_result() -> dict:
    return {
        "transaction": {
            "message": {
                "header": {"numRequiredSignatures": 1},
                "accountKeys": [SCOUT_A, SCOUT_B, OTHER],
            }
        },
        "meta": {"err": None},
    }


def test_string_account_keys_use_message_header_signer_bits() -> None:
    entries = repair._header_aware_account_entries(_string_key_result())
    assert entries[0] == (SCOUT_A, True, 0)
    assert entries[1] == (SCOUT_B, False, 1)


def test_corrected_account_entries_resolve_unique_tracked_scout_signer(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(scout, "_account_entries", repair._header_aware_account_entries)
    wallet, error = scout._tracked_scout_wallet(_string_key_result(), (SCOUT_A, SCOUT_B))
    assert wallet == SCOUT_A
    assert error is None


def test_proven_unpriced_movement_is_terminal_noncopyable_not_synthetic_trade(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorded: dict[str, object] = {}
    plane = SimpleNamespace(store=object(), scout_wallets=(SCOUT_A,))
    trigger = datetime.now(timezone.utc)

    monkeypatch.setattr(repair, "_ORIGINAL_NORMALIZE", lambda *args, **kwargs: None)
    monkeypatch.setattr(scout, "_tracked_scout_wallet", lambda *args, **kwargs: (SCOUT_A, None))
    monkeypatch.setattr(release_bound, "_terminal_classification", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        repair.economic,
        "_economic_movement",
        lambda *args, **kwargs: (
            {
                "side": "buy",
                "token_mint": "Mint1111111111111111111111111111111111111",
                "token_amount": 100.0,
                "native_amount_sol": None,
                "movement_authority": "owner_token_delta",
            },
            None,
        ),
    )

    def record(store: object, **kwargs: object) -> None:
        recorded.update(kwargs)

    monkeypatch.setattr(release_bound, "_record_terminal_non_candidate", record)
    token = scout._SCOUT_HYDRATION_PLANE.set(plane)
    try:
        swap = repair._normalize_with_terminal_noncopyable(
            _string_key_result(),
            signature="sig-unpriced",
            trigger_received_at=trigger,
            source_hint=None,
        )
    finally:
        scout._SCOUT_HYDRATION_PLANE.reset(token)

    assert swap is None
    assert recorded["signature"] == "sig-unpriced"
    assert recorded["reason"] == "economic_movement_price_unresolved_noncopyable"
    assert getattr(plane, "_roi_post178_economic_movement_noncopyable_classifications") == 1


def test_unified_status_uses_forward_frontier_not_historical_catchup(monkeypatch: pytest.MonkeyPatch) -> None:
    def base(*args: object, **kwargs: object) -> dict:
        return {
            "solana": {"all_regimes_e2e_achievable": True},
            "fomo": {"all_regimes_e2e_achievable": True},
            "robinhood": {
                "blockers": [repair.LEGACY_ROBINHOOD_BLOCKER],
                "regimes": {
                    "v2": {
                        "paper_capable": True,
                        "e2e_achievable": False,
                        "blockers": [repair.LEGACY_ROBINHOOD_BLOCKER],
                    }
                },
                "all_regimes_e2e_achievable": False,
            },
            "overall": {
                "blocking_components": [repair.LEGACY_ROBINHOOD_BLOCKER],
                "all_paper_planes_e2e_achievable": False,
            },
        }

    monkeypatch.setattr(repair, "_ORIGINAL_UNIFIED_STATUS", base)
    payload = repair._unified_status_with_current_frontier(
        {},
        object(),
        {
            "runtime_ready": True,
            "paper_trading_authority": True,
            "failed_closed": False,
            "paper_decision_transport_ready": True,
        },
    )

    assert repair.LEGACY_ROBINHOOD_BLOCKER not in str(payload)
    assert repair.FORWARD_ROBINHOOD_BLOCKER not in payload["robinhood"]["blockers"]
    assert payload["robinhood"]["paper_decision_transport_ready"] is True
    assert payload["robinhood"]["regimes"]["v2"]["e2e_achievable"] is True
    assert payload["overall"]["all_paper_planes_e2e_achievable"] is True


def test_head_rpc_stall_does_not_advance_observer_generation(monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> SimpleNamespace:
        stop = asyncio.Event()

        class Rpc:
            async def block_number(self) -> int:
                stop.set()
                raise RuntimeError("temporary head read failure")

        plane = SimpleNamespace(rpc=Rpc(), _roi_post177_head_observer_generation=7)
        await repair._observe_robinhood_head_without_false_generation(plane, stop)
        return plane

    monkeypatch.setattr(repair.post177, "_inc", lambda *args, **kwargs: None)
    plane = asyncio.run(scenario())

    assert plane._roi_post177_head_observer_generation == 7
    assert plane._roi_post177_head_observer_continuity_ok is False
    assert plane._roi_post177_head_observer_last_gap_reason == "observer_rpc_stall_no_reanchor"


def test_robinhood_large_backlog_jumps_to_current_bounded_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: dict[str, object] = {}
    plane = SimpleNamespace(_roi_forward_only_chain_id_verified=True)

    async def current_head(self: object) -> int:
        return 1000

    async def sync_factory(self: object, *, from_block: int, to_block: int) -> int:
        calls["factory"] = (from_block, to_block)
        return 0

    async def fetch_logs(self: object, *, from_block: int, to_block: int) -> list:
        calls["logs"] = (from_block, to_block)
        return []

    async def fresh(self: object) -> bool:
        return True

    monkeypatch.setattr(repair.post177, "_current_observed_head", current_head)
    monkeypatch.setattr(repair.post177, "_schedule_rwa_refresh", lambda self: None)
    monkeypatch.setattr(repair.post177, "_observer_head_fresh", lambda self: True)
    monkeypatch.setattr(repair.post177, "_clear_pending_markets", lambda self: None)
    monkeypatch.setattr(repair.frontier, "_inc", lambda *args, **kwargs: None)
    monkeypatch.setattr(repair.frontier, "_live_epoch_active", lambda self: True)
    monkeypatch.setattr(repair.frontier, "_live_cursor", lambda self: 800)
    monkeypatch.setattr(repair.frontier, "_sync_factory_state", sync_factory)
    monkeypatch.setattr(repair.frontier, "_fetch_market_logs", fetch_logs)
    monkeypatch.setattr(repair.frontier, "_fresh_head_ready", fresh)

    asyncio.run(repair._advance_robinhood_current_window(plane))

    window = int(repair.frontier.MAX_LIVE_FRONTIER_GAP_BLOCKS)
    assert calls["factory"] == (1000 - window + 1, 1000)
    assert calls["logs"] == (1000 - window + 1, 1000)
    assert plane._roi_live_epoch_cursor == 1000
    assert plane._roi_live_epoch_ready is True
    assert plane._roi_post178_stale_backlog_blocks_skipped == 200 - window
    assert plane._roi_live_epoch_last_range["retrospective_entry_authority"] is False
