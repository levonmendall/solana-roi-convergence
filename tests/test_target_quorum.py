from __future__ import annotations

import asyncio

from solana_roi.direct_solana import DirectSolanaJournal, WatchTarget
from solana_roi.observation_store import ObservationEventStore
from solana_roi.solana_rpc import RpcEndpoint
from solana_roi.target_quorum import GAP_ERROR, _quorum_set_target_state, _reject_historical_gap_recovery


def _plane(tmp_path):
    store = ObservationEventStore(tmp_path / "quorum.sqlite3")

    class Plane:
        watch_targets = (
            WatchTarget("scout", "scout-a", None),
            WatchTarget("program", "program-a", "RAYDIUM"),
        )
        _recovering = False

    plane = Plane()
    plane.store = store
    plane.journal = DirectSolanaJournal(store)
    return plane


def test_two_partial_providers_can_preserve_full_scope_without_false_outage(tmp_path):
    async def scenario() -> None:
        plane = _plane(tmp_path)
        a = RpcEndpoint("provider-a", "https://a.invalid", "wss://a.invalid")
        b = RpcEndpoint("provider-b", "https://b.invalid", "wss://b.invalid")
        scout, program = plane.watch_targets

        # Neither provider is individually 2/2, but together they cover the full
        # frozen target set. This must count as continuous full-scope observation.
        await _quorum_set_target_state(plane, a, scout, connected=True)
        await _quorum_set_target_state(plane, b, program, connected=True)

        assert plane._roi_full_scope_target_coverage_ok is True
        assert plane._roi_full_scope_target_coverage_count == 2
        assert plane.journal.outage_started_at() is None
        status = plane.journal.status()
        assert status["unresolved_gap"] is False
        # Provider-level telemetry remains strict because neither provider is 2/2.
        assert status["connected_provider_count"] == 0

        # Moving the scout from provider-a to provider-b with a real uncovered
        # interval creates a true prospective continuity gap.
        await _quorum_set_target_state(plane, a, scout, connected=False, error_type="ConnectionClosedError")
        assert plane.journal.outage_started_at() is not None
        await _quorum_set_target_state(plane, b, scout, connected=True)

        recovered = plane.journal.status()
        assert recovered["unresolved_gap"] is True
        assert recovered["last_backfill_error"] == GAP_ERROR
        assert recovered["outage_started_at"] is None

    asyncio.run(scenario())


def test_historical_gap_recovery_is_quarantined_not_enqueued_into_candidate_lane(tmp_path):
    async def scenario() -> None:
        plane = _plane(tmp_path)
        now = __import__("datetime").datetime.now(__import__("datetime").timezone.utc)
        plane.journal.mark_outage(now)
        plane.journal.enqueue(
            signature="old-gap-scout",
            slot=1,
            trigger_received_at=now,
            source_hint=None,
            priority=2,
            reason="gap_backfill",
        )
        plane.journal.enqueue(
            signature="live-scout",
            slot=2,
            trigger_received_at=now,
            source_hint=None,
            priority=0,
            reason="frozen_scout_processed_trigger",
        )

        await _reject_historical_gap_recovery(plane, now)

        with plane.store._lock:
            old_gap = plane.store.db.execute(
                "SELECT status, last_error FROM direct_solana_hydration_queue WHERE signature='old-gap-scout'"
            ).fetchone()
            live = plane.store.db.execute(
                "SELECT status, reason FROM direct_solana_hydration_queue WHERE signature='live-scout'"
            ).fetchone()

        assert old_gap["status"] == "failed"
        assert old_gap["last_error"] == GAP_ERROR
        assert live["status"] == "pending"
        assert live["reason"] == "frozen_scout_processed_trigger"
        status = plane.journal.status()
        assert status["unresolved_gap"] is True
        assert status["last_backfill_error"] == GAP_ERROR

    asyncio.run(scenario())
