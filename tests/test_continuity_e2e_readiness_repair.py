from __future__ import annotations

import os
import sqlite3
import threading
from types import SimpleNamespace

from solana_roi import continuity_e2e_readiness_repair as repair
from solana_roi.direct_solana import DirectSolanaJournal


class Store:
    def __init__(self) -> None:
        self.db = sqlite3.connect(":memory:")
        self.db.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        self.events: list[tuple[object, ...]] = []

    def append(self, *args: object) -> None:
        self.events.append(args)


def test_failed_gap_recovery_preserves_exact_outage_boundary() -> None:
    store = Store()
    journal = DirectSolanaJournal(store)
    repair.install_continuity_e2e_readiness_repair()
    boundary = "2026-09-04T19:00:00+00:00"
    with store._lock, store.db:
        store.db.execute(
            "UPDATE direct_solana_global_state SET outage_started_at=?,unresolved_gap=1 WHERE id=1",
            (boundary,),
        )

    journal.close_outage(complete=False, error="bounded recovery incomplete")

    with store._lock:
        row = store.db.execute(
            "SELECT outage_started_at,unresolved_gap,last_backfill_error FROM direct_solana_global_state WHERE id=1"
        ).fetchone()
    assert row["outage_started_at"] == boundary
    assert row["unresolved_gap"] == 1
    assert row["last_backfill_error"] == "bounded recovery incomplete"


def test_new_release_can_archive_only_inherited_orphaned_gap_once(monkeypatch) -> None:
    store = Store()
    DirectSolanaJournal(store)
    plane = SimpleNamespace(store=store)
    monkeypatch.setenv("RENDER_GIT_COMMIT", "release-a")
    with store._lock, store.db:
        store.db.execute(
            "UPDATE direct_solana_global_state SET outage_started_at=NULL,unresolved_gap=1,last_backfill_error='old failure' WHERE id=1"
        )

    repair._start_release_epoch_if_safe(plane)

    with store._lock:
        state = store.db.execute(
            "SELECT unresolved_gap,last_backfill_error FROM direct_solana_global_state WHERE id=1"
        ).fetchone()
        epoch = store.db.execute(
            "SELECT inherited_orphaned_gap,inherited_backfill_error FROM direct_solana_release_continuity_epoch "
            "WHERE release_commit='release-a'"
        ).fetchone()
    assert state["unresolved_gap"] == 0
    assert state["last_backfill_error"] is None
    assert epoch["inherited_orphaned_gap"] == 1
    assert epoch["inherited_backfill_error"] == "old failure"

    # A restart of the same release must not be able to clear a new unresolved gap.
    with store._lock, store.db:
        store.db.execute(
            "UPDATE direct_solana_global_state SET outage_started_at=NULL,unresolved_gap=1,last_backfill_error='same release failure' WHERE id=1"
        )
    repair._start_release_epoch_if_safe(plane)
    with store._lock:
        state = store.db.execute(
            "SELECT unresolved_gap,last_backfill_error FROM direct_solana_global_state WHERE id=1"
        ).fetchone()
    assert state["unresolved_gap"] == 1
    assert state["last_backfill_error"] == "same release failure"


def test_robinhood_not_caught_up_is_not_reported_e2e_achievable(monkeypatch) -> None:
    original = repair._ORIGINAL_UNIFIED_STATUS
    try:
        repair._ORIGINAL_UNIFIED_STATUS = lambda base, runtime, robinhood: {
            "solana": {"all_regimes_e2e_achievable": True, "blockers": []},
            "fomo": {"all_regimes_e2e_achievable": True, "blockers": []},
            "robinhood": {
                "all_regimes_e2e_achievable": True,
                "blockers": [],
                "regimes": {
                    "neutral": {
                        "e2e_achievable": True,
                        "blockers": ["awaiting_executable_candidate_in_regime"],
                    }
                },
            },
            "overall": {"all_paper_planes_e2e_achievable": True, "blocking_components": []},
        }
        base = {
            "direct_solana": {
                "enabled": True,
                "continuity_ok": True,
                "connected_provider_count": 1,
                "unresolved_gap": False,
            }
        }
        robinhood = {
            "runtime_ready": True,
            "paper_trading_authority": True,
            "caught_up_for_paper_decisions": False,
            "failed_closed": False,
        }
        payload = repair._unified_status_with_strict_transport(base, SimpleNamespace(), robinhood)
        assert payload["robinhood"]["paper_decision_transport_ready"] is False
        assert payload["robinhood"]["all_regimes_e2e_achievable"] is False
        assert payload["robinhood"]["regimes"]["neutral"]["e2e_achievable"] is False
        assert "robinhood_not_caught_up_for_paper_decisions" in payload["robinhood"]["blockers"]
        assert payload["overall"]["all_paper_planes_e2e_achievable"] is False
    finally:
        repair._ORIGINAL_UNIFIED_STATUS = original


def test_direct_transport_diagnostics_distinguish_provider_and_gap_blockers() -> None:
    original = repair._ORIGINAL_UNIFIED_STATUS
    try:
        repair._ORIGINAL_UNIFIED_STATUS = lambda base, runtime, robinhood: {
            "solana": {"all_regimes_e2e_achievable": False, "blockers": ["direct_solana_continuity_not_ok"]},
            "fomo": {"all_regimes_e2e_achievable": False, "blockers": ["direct_solana_continuity_not_ok"]},
            "robinhood": {"all_regimes_e2e_achievable": False, "blockers": [], "regimes": {}},
            "overall": {"all_paper_planes_e2e_achievable": False, "blocking_components": []},
        }
        base = {
            "direct_solana": {
                "enabled": True,
                "continuity_ok": False,
                "connected_provider_count": 0,
                "unresolved_gap": True,
                "outage_started_at": "2026-09-04T19:00:00+00:00",
                "last_backfill_error": "bounded recovery incomplete",
            }
        }
        payload = repair._unified_status_with_strict_transport(
            base,
            SimpleNamespace(),
            {"runtime_ready": False, "caught_up_for_paper_decisions": False},
        )
        diagnostics = payload["solana"]["transport_diagnostics"]
        assert diagnostics["connected_provider_count"] == 0
        assert diagnostics["unresolved_gap"] is True
        assert "direct_solana_no_connected_provider" in payload["solana"]["blockers"]
        assert "direct_solana_unresolved_gap" in payload["solana"]["blockers"]
    finally:
        repair._ORIGINAL_UNIFIED_STATUS = original
