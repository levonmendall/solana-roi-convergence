from __future__ import annotations

import asyncio

from solana_roi import strategy_relevant_continuity as repair
from solana_roi import target_stream_fanout as fanout
from solana_roi.direct_solana import DirectSolanaJournal, RpcEndpoint, WatchTarget
from solana_roi.live_poll_redundancy import POLL_PROVIDER_NAME
from solana_roi.observation_store import ObservationEventStore


def _plane(tmp_path):
    store = ObservationEventStore(tmp_path / "strategy-continuity.sqlite3")

    class Plane:
        watch_targets = (
            WatchTarget("scout", "scout-a", None),
            WatchTarget("program", "program-a", "PUMP_AMM"),
        )

    plane = Plane()
    plane.store = store
    plane.journal = DirectSolanaJournal(store)
    return plane


def _set_coverage(
    plane,
    *,
    scout_ws: bool,
    scout_poll: bool,
    program_ws: bool = False,
    program_poll: bool = False,
):
    _lock, provider_targets, _events, _states = fanout._state_maps(plane)
    scout, program = plane.watch_targets
    ws = set()
    poll = set()
    if scout_ws:
        ws.add(fanout._target_key(scout))
    if program_ws:
        ws.add(fanout._target_key(program))
    if scout_poll:
        poll.add(fanout._target_key(scout))
    if program_poll:
        poll.add(fanout._target_key(program))
    provider_targets["provider-a"] = ws
    provider_targets[POLL_PROVIDER_NAME] = poll


def test_strategy_epoch_arms_without_full_program_firehose_and_archives_prior_gap(tmp_path, monkeypatch):
    plane = _plane(tmp_path)
    monkeypatch.setenv("SOLANA_ROI_RELEASE_COMMIT", "strategy-continuity-test-release")
    _set_coverage(plane, scout_ws=True, scout_poll=True, program_ws=False, program_poll=False)

    now = repair.direct_module.utcnow()
    plane.journal.mark_outage(now)
    plane.journal.close_outage(complete=False, error="legacy full-program gap")
    assert plane.journal.status()["unresolved_gap"] is True

    original = repair._ORIGINAL_STATUS
    repair._ORIGINAL_STATUS = lambda self: self.journal.status()
    try:
        payload = repair._status_with_strategy_continuity(plane)
    finally:
        repair._ORIGINAL_STATUS = original

    assert payload["continuity_ok"] is True
    assert payload["unresolved_gap"] is False
    assert payload["strategy_relevant_continuity"]["epoch_started"] is True
    assert payload["strategy_relevant_continuity"]["websocket_coverage_ok"] is True
    assert payload["strategy_relevant_continuity"]["transport_coverage_ok"] is True
    assert payload["discovery_continuity"]["websocket_coverage_ok"] is False
    assert payload["discovery_continuity"]["blocks_strategy_execution_continuity"] is False

    row = plane.store.db.execute(
        "SELECT archived_unresolved_gap,archived_backfill_error "
        "FROM direct_solana_strategy_continuity_epoch WHERE release_commit=?",
        ("strategy-continuity-test-release",),
    ).fetchone()
    assert row is not None
    assert bool(row["archived_unresolved_gap"]) is True
    assert row["archived_backfill_error"] == "legacy full-program gap"


def test_program_irrecoverable_gap_is_discovery_degradation_not_strategy_gap(tmp_path, monkeypatch):
    plane = _plane(tmp_path)
    monkeypatch.setenv("SOLANA_ROI_RELEASE_COMMIT", "program-gap-test-release")
    _set_coverage(plane, scout_ws=True, scout_poll=True)
    assert repair._start_strategy_epoch_if_ready(plane) is True

    program = plane.watch_targets[1]
    assert repair._latch_generation_scoped(
        plane,
        program,
        7,
        repair.direct_module.utcnow().isoformat(),
    ) is True

    status = plane.journal.status()
    assert status["unresolved_gap"] is False
    summary = repair._discovery_gap_summary(plane)
    assert summary["gap_event_count"] == 1
    assert summary["targets"][0]["target"].startswith("program:")


def test_scout_irrecoverable_gap_keeps_existing_fail_closed_semantics(tmp_path):
    plane = _plane(tmp_path)
    scout = plane.watch_targets[0]
    assert repair._ORIGINAL_LATCH_GENERATION is not None
    assert repair._latch_generation_scoped(
        plane,
        scout,
        3,
        repair.direct_module.utcnow().isoformat(),
    ) is True

    status = plane.journal.status()
    assert status["unresolved_gap"] is True
    assert status["last_backfill_error"] == repair.lease.IRRECOVERABLE_POLL_GAP_ERROR


def test_program_poll_fallback_is_raw_only_and_never_enters_hydration_queue(tmp_path):
    async def scenario():
        plane = _plane(tmp_path)
        program = plane.watch_targets[1]
        inserted = await repair._record_poll_rows_scoped(
            plane,
            program,
            [
                {"signature": "program-gap-1", "slot": 100, "err": None},
                {"signature": "program-gap-2", "slot": 101, "err": None},
            ],
        )
        assert inserted == 2
        with plane.store._lock:
            queued = plane.store.db.execute(
                "SELECT COUNT(*) FROM direct_solana_hydration_queue "
                "WHERE signature IN ('program-gap-1','program-gap-2')"
            ).fetchone()[0]
            receipts = plane.store.db.execute(
                "SELECT COUNT(*) FROM direct_solana_recent_receipts "
                "WHERE signature IN ('program-gap-1','program-gap-2')"
            ).fetchone()[0]
        assert queued == 0
        assert receipts == 2
        assert plane._roi_program_poll_rows_raw_only_total == 2

    asyncio.run(scenario())


def test_scout_poll_bridge_preserved_after_strategy_epoch_arms(tmp_path, monkeypatch):
    async def scenario():
        plane = _plane(tmp_path)
        monkeypatch.setenv("SOLANA_ROI_RELEASE_COMMIT", "poll-bridge-test-release")
        websocket = RpcEndpoint("provider-a", "https://a.invalid", "wss://a.invalid")
        poll = RpcEndpoint(POLL_PROVIDER_NAME, "https://poll.invalid", "wss://poll.invalid")
        scout, _program = plane.watch_targets

        _set_coverage(plane, scout_ws=True, scout_poll=True)
        assert repair._start_strategy_epoch_if_ready(plane) is True
        assert plane.journal.outage_started_at() is None

        # Losing the real WS copy alone is bridged by the confirmed live-poll lane.
        await repair._set_target_state_scoped(
            plane,
            websocket,
            scout,
            connected=False,
            error_type="ConnectionClosedError",
        )
        assert plane.journal.outage_started_at() is None

        # Losing the bridge too is a genuine strategy transport outage.
        await repair._set_target_state_scoped(
            plane,
            poll,
            scout,
            connected=False,
            error_type="LivePollUnavailable",
        )
        assert plane.journal.outage_started_at() is not None

    asyncio.run(scenario())


def test_strategy_status_preserves_paper_only_and_raw_discovery_separation(tmp_path, monkeypatch):
    plane = _plane(tmp_path)
    monkeypatch.setenv("SOLANA_ROI_RELEASE_COMMIT", "status-test-release")
    _set_coverage(plane, scout_ws=True, scout_poll=True, program_ws=False, program_poll=True)

    original = repair._ORIGINAL_STATUS
    repair._ORIGINAL_STATUS = lambda self: {
        **self.journal.status(),
        "provider_runtime_policy": {},
        "throughput_policy": {},
        "continuity_startup_barrier": {"required": True, "armed": False},
    }
    try:
        payload = repair._status_with_strategy_continuity(plane)
    finally:
        repair._ORIGINAL_STATUS = original

    assert payload["continuity_ok"] is True
    assert payload["strategy_connected_provider_count"] == 1
    assert payload["connected_provider_count"] == 1
    assert payload["strategy_relevant_continuity"]["paper_only"] is True
    assert payload["strategy_relevant_continuity"]["live_money_authority"] is False
    assert payload["strategy_relevant_continuity"]["signing_available"] is False
    assert payload["strategy_relevant_continuity"]["transaction_submission_available"] is False
    assert payload["strategy_relevant_continuity"]["post_start_poll_can_bridge_websocket_loss"] is True
    assert payload["throughput_policy"]["full_raw_market_scope_preserved"] is True
    assert payload["throughput_policy"]["program_gap_fallback_hydration"] is False
    assert payload["provider_runtime_policy"]["scout_gap_fail_closed_semantics_unchanged"] is True
    assert payload["provider_runtime_policy"]["scout_poll_bridge_semantics_unchanged"] is True
    assert payload["raw_discovery_startup_barrier"]["armed"] is False
