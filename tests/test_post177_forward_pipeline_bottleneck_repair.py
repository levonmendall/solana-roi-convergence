from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from solana_roi import ephemeral_candidate_retention as retention
from solana_roi import post177_forward_pipeline_bottleneck_repair as repair
from solana_roi import robinhood_live_frontier_verification_repair as frontier


def test_forward_transport_compat_shim_does_not_restore_historical_caught_up() -> None:
    seen: list[bool] = []

    async def legacy_inner(self: object) -> None:
        seen.append(bool(getattr(self, "_caught_up", False)))

    plane = SimpleNamespace(
        _caught_up=False,
        _roi_live_epoch_cursor=100,
        _roi_live_epoch_ready=True,
        _roi_live_epoch_suppress_entries=False,
    )
    wrapped = repair._forward_transport_compat_guard(legacy_inner)
    asyncio.run(wrapped(plane))

    assert seen == [True]
    assert plane._caught_up is False
    assert getattr(plane, "_roi_post177_legacy_caught_up_shims", 0) == 1


def test_large_robinhood_processing_backlog_is_drained_not_reanchored(monkeypatch) -> None:
    async def no_factory(*args, **kwargs) -> int:
        return 0

    async def no_market(*args, **kwargs) -> list[object]:
        return []

    monkeypatch.setattr(frontier, "_sync_factory_state", no_factory)
    monkeypatch.setattr(frontier, "_fetch_market_logs", no_market)

    head = 1100
    plane = SimpleNamespace(
        _roi_forward_only_chain_id_verified=True,
        _roi_live_epoch_cursor=1000,
        _roi_live_epoch_ready=False,
        _roi_live_epoch_suppress_entries=False,
        _roi_post177_head_observer_generation=0,
        _roi_post177_epoch_observer_generation=0,
        _roi_post177_head_observer_continuity_ok=True,
        _roi_post177_head_observer_last_success_monotonic=time.monotonic(),
        _roi_post177_head_observed_block=head,
        _latest_block=head,
        rpc=SimpleNamespace(),
    )

    asyncio.run(repair._advance_observed_forward_epoch(plane))

    assert plane._roi_live_epoch_cursor == 1000 + frontier.MAX_LIVE_FRONTIER_GAP_BLOCKS
    assert getattr(plane, "_roi_live_frontier_epoch_resets", 0) == 0
    assert getattr(plane, "_roi_post177_processing_backlog_chunks", 0) == 1
    assert plane._roi_live_epoch_ready is False
    assert plane._roi_live_epoch_last_range["processing_backlog_remaining_blocks"] == 36


def test_fomo_strategy_window_uses_event_time_not_delivery_timestamp() -> None:
    now = datetime.now(timezone.utc)
    rows = []
    for index in range(3):
        event_at = now - timedelta(seconds=6 - index)
        rows.append(
            {
                "signature": f"sig-{index}",
                "wallet": f"wallet-{index}",
                "token_mint": "mint-a",
                "side": "buy",
                "native_amount_sol": 1.0,
                "observed_at": event_at.isoformat(),
                # Deliberately stale delivery metadata proves the scanner no longer
                # shrinks the strategy window according to receipt bookkeeping.
                "received_at": (now - timedelta(seconds=120)).isoformat(),
                "source": "solana-direct:PUMP_AMM:buy",
            }
        )

    candidates, diagnostics = repair._fomo_scan_rows_event_time(rows, now=now)

    assert len(candidates) == 1
    assert candidates[0]["token"] == "mint-a"
    assert candidates[0]["state"] == "active_fomo"
    assert diagnostics["event_time_authoritative"] is True
    assert diagnostics["delivery_latency_changes_strategy_window"] is False


def test_scout_hydration_retention_extends_without_extending_candidate_state() -> None:
    assert retention.ENTRY_WINDOW_SECONDS == 20.0
    assert retention.SCOUT_HYDRATION_RETENTION_SECONDS == 60.0
    assert retention._hydration_retention_seconds("frozen_scout_processed_trigger") == 60.0
    assert retention._hydration_retention_seconds("frozen_scout_live_poll_trigger") == 60.0
    assert retention._hydration_retention_seconds("prospective_launch") == 20.0
    assert retention._hydration_retention_seconds("deterministic_market_sample") == 20.0


def test_candidate_retention_distinguishes_immediate_copy_from_operational_timeout() -> None:
    assert repair.IMMEDIATE_COPY_WINDOW_SECONDS == 20.0
    assert repair.CANDIDATE_OPERATIONAL_RETENTION_SECONDS == 60.0
    assert repair.CANDIDATE_OPERATIONAL_RETENTION_SECONDS > repair.IMMEDIATE_COPY_WINDOW_SECONDS


def test_unified_robinhood_blocker_uses_forward_frontier_semantics() -> None:
    payload = {
        "overall": {
            "blocking_components": [
                "robinhood_not_caught_up_for_paper_decisions",
                "some_other_blocker",
            ]
        },
        "robinhood": {
            "blockers": ["robinhood_not_caught_up_for_paper_decisions"],
            "regimes": {
                "neutral": {
                    "blockers": ["robinhood_not_caught_up_for_paper_decisions"]
                }
            },
        },
    }

    repaired = repair._replace_legacy_robinhood_blocker(payload)

    assert "robinhood_not_caught_up_for_paper_decisions" not in str(repaired)
    assert repaired["robinhood"]["blockers"] == ["robinhood_forward_frontier_not_ready"]
