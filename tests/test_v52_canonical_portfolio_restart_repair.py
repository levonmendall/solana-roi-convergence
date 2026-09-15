from __future__ import annotations

import pytest

from solana_roi.observation_store import ObservationEventStore
from solana_roi import v52_canonical_portfolio_restart_repair as restart
from solana_roi.v51_atomic_paper_capital import capital_reconciliation, reserve_paper_capital


class Adapter:
    def __init__(self, store: ObservationEventStore, release: str) -> None:
        self.store = store
        self.release_commit = release


def _schema(store: ObservationEventStore) -> None:
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE risk_conditioned_alpha_v5_outcomes ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,release_commit TEXT NOT NULL,source_signature TEXT NOT NULL,"
            "exit_signature TEXT NOT NULL,net_return REAL NOT NULL,settled_at TEXT NOT NULL)"
        )
        store.db.execute(
            "CREATE TABLE fomo_paper_outcomes ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,release_commit TEXT NOT NULL,source_signature TEXT NOT NULL,"
            "exit_signature TEXT NOT NULL,net_return REAL NOT NULL,settled_at TEXT NOT NULL)"
        )


def test_restart_settles_old_release_solana_reservation_from_durable_outcome(tmp_path) -> None:
    store = ObservationEventStore(tmp_path / "restart-solana.sqlite3")
    _schema(store)
    reserve_paper_capital(
        store,
        release_commit="release-a",
        reservation_id="solana:old-position",
        lane="SOLANA:test",
        candidate_id="old-position",
        requested_fraction=0.40,
        allow_downsize=False,
        minimum_fraction=0.40,
    )
    with store._lock, store.db:
        store.db.execute(
            "INSERT INTO risk_conditioned_alpha_v5_outcomes("
            "release_commit,source_signature,exit_signature,net_return,settled_at) VALUES (?,?,?,?,?)",
            ("release-a", "old-position", "exit-a", 0.25, "2026-09-15T12:00:00+00:00"),
        )

    adapter = Adapter(store, "release-b")
    assert restart.sync_settlements_across_releases(adapter) == 1
    assert restart.sync_settlements_across_releases(adapter) == 0

    state = capital_reconciliation(store, release_commit="release-b")
    assert state["active_reserved_fraction"] == 0.0
    assert state["settlement_count"] == 1
    assert state["realized_return_contribution"] == pytest.approx(0.10)
    with store._lock:
        settlement = store.db.execute(
            "SELECT release_commit,reservation_id FROM v51_paper_capital_settlements"
        ).fetchone()
    assert settlement["release_commit"] == "release-a"
    assert settlement["reservation_id"] == "solana:old-position"
    store.close()


def test_restart_keeps_old_open_position_reserved_when_no_exit_exists(tmp_path) -> None:
    store = ObservationEventStore(tmp_path / "restart-open.sqlite3")
    _schema(store)
    reserve_paper_capital(
        store,
        release_commit="release-a",
        reservation_id="fomo:still-open",
        lane="FOMO",
        candidate_id="still-open",
        requested_fraction=0.70,
        allow_downsize=False,
        minimum_fraction=0.70,
    )
    adapter = Adapter(store, "release-b")

    assert restart.sync_settlements_across_releases(adapter) == 0
    new_candidate = reserve_paper_capital(
        store,
        release_commit="release-b",
        reservation_id="solana:new-candidate",
        lane="SOLANA:test",
        candidate_id="new-candidate",
        requested_fraction=0.40,
        allow_downsize=False,
        minimum_fraction=0.40,
    )

    assert new_candidate["status"] == "rejected"
    state = capital_reconciliation(store, release_commit="release-b")
    assert state["active_reserved_fraction"] == pytest.approx(0.70)
    assert state["available_fraction"] == pytest.approx(0.30)
    store.close()


def test_only_active_reservation_can_be_closed_after_restart(tmp_path) -> None:
    store = ObservationEventStore(tmp_path / "restart-idempotent.sqlite3")
    _schema(store)
    reserve_paper_capital(
        store,
        release_commit="release-a",
        reservation_id="fomo:one",
        lane="FOMO",
        candidate_id="one",
        requested_fraction=0.20,
        allow_downsize=False,
        minimum_fraction=0.20,
    )
    with store._lock, store.db:
        store.db.execute(
            "INSERT INTO fomo_paper_outcomes("
            "release_commit,source_signature,exit_signature,net_return,settled_at) VALUES (?,?,?,?,?)",
            ("release-a", "one", "exit-one", -0.50, "2026-09-15T12:00:00+00:00"),
        )
    adapter = Adapter(store, "release-c")

    assert restart.sync_settlements_across_releases(adapter) == 1
    assert restart.sync_settlements_across_releases(adapter) == 0
    state = capital_reconciliation(store, release_commit="release-c")
    assert state["settlement_count"] == 1
    assert state["realized_return_contribution"] == pytest.approx(-0.10)
    assert state["paper_nav_multiplier"] == pytest.approx(0.90)
    store.close()
