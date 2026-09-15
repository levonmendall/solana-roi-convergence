from __future__ import annotations

import pytest

from solana_roi.observation_store import ObservationEventStore
from solana_roi import v52_robinhood_canonical_capital_bridge as bridge
from solana_roi import v52_robinhood_shared_capital_repair as shared
from solana_roi.v51_atomic_paper_capital import capital_reconciliation, reserve_paper_capital


TOKEN = "0x" + "1" * 40
MARKET = "0x" + "2" * 40
EVIDENCE = "e" * 64


class Owner:
    def __init__(self, store: ObservationEventStore, release: str) -> None:
        self.store = store
        self.release_commit = release
        self.starting_nav_usd = 500.0


def _schema(store: ObservationEventStore) -> None:
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE robinhood_paper_trials ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,release_commit TEXT NOT NULL,token TEXT NOT NULL,market TEXT NOT NULL,"
            "position_fraction REAL NOT NULL,capital_reservation_id TEXT)"
        )
        store.db.execute(
            "CREATE TABLE robinhood_paper_outcomes ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,trial_id INTEGER NOT NULL,paper_nav_multiplier REAL NOT NULL,"
            "paper_only INTEGER NOT NULL DEFAULT 1)"
        )
        store.db.execute(
            "CREATE TABLE v52_robinhood_position_lots ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,position_id INTEGER NOT NULL,trial_id INTEGER NOT NULL UNIQUE,"
            "entry_fraction REAL NOT NULL,remaining_fraction REAL NOT NULL,entry_total_cost_wei TEXT NOT NULL,"
            "remaining_entry_cost_wei TEXT NOT NULL,realized_exit_net_wei TEXT NOT NULL,"
            "capital_reservation_id TEXT,evidence_fingerprint TEXT NOT NULL)"
        )
        store.db.execute(
            "CREATE TABLE v52_robinhood_position_events ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,position_id INTEGER NOT NULL,paper_nav_multiplier REAL)"
        )


def _old_release_lot(store: ObservationEventStore, reservation_id: str) -> int:
    with store._lock, store.db:
        trial = store.db.execute(
            "INSERT INTO robinhood_paper_trials(release_commit,token,market,position_fraction,capital_reservation_id) "
            "VALUES (?,?,?,?,?)",
            ("release-a", TOKEN, MARKET, 0.20, reservation_id),
        )
        trial_id = int(trial.lastrowid)
        store.db.execute(
            "INSERT INTO v52_robinhood_position_lots("
            "position_id,trial_id,entry_fraction,remaining_fraction,entry_total_cost_wei,remaining_entry_cost_wei,"
            "realized_exit_net_wei,capital_reservation_id,evidence_fingerprint) VALUES (?,?,?,?,?,?,?,?,?)",
            (1, trial_id, 0.20, 0.20, "1000", "1000", "0", reservation_id, EVIDENCE),
        )
    return trial_id


def test_restart_recovers_prior_release_robinhood_lot_by_economic_identity(tmp_path) -> None:
    store = ObservationEventStore(tmp_path / "rh-cross-release.sqlite3")
    _schema(store)
    owner = Owner(store, "release-b")
    shared._ensure_schema(owner)
    reservation_id = f"robinhood-v52:{TOKEN}:{MARKET}:{EVIDENCE}"
    reserve_paper_capital(
        store,
        release_commit="release-a",
        reservation_id=reservation_id,
        lane="robinhood",
        candidate_id=EVIDENCE,
        requested_fraction=0.20,
        allow_downsize=False,
        minimum_fraction=0.20,
    )
    trial_id = _old_release_lot(store, reservation_id)

    lot = bridge._matching_lot_canonical(
        owner,
        payload={"token": TOKEN, "market": MARKET},
        evidence=EVIDENCE,
    )
    assert lot is not None
    assert int(lot["trial_id"]) == trial_id
    assert lot["release_commit"] == "release-a"

    bridge._link_reservation_canonical(owner, lot=lot, reservation_id=reservation_id)
    line = shared._lineage(owner, trial_id)
    assert line is not None
    assert line["release_commit"] == "release-a"
    state = capital_reconciliation(store, release_commit="release-b")
    assert state["active_reserved_fraction"] == pytest.approx(0.20)
    assert state["active_release_count"] == 1
    store.close()


def test_cross_release_staged_exit_settles_origin_reservation_and_keeps_residual(tmp_path) -> None:
    store = ObservationEventStore(tmp_path / "rh-exit-cross-release.sqlite3")
    _schema(store)
    owner = Owner(store, "release-b")
    shared._ensure_schema(owner)
    reservation_id = f"robinhood-v52:{TOKEN}:{MARKET}:{EVIDENCE}"
    reserve_paper_capital(
        store,
        release_commit="release-a",
        reservation_id=reservation_id,
        lane="robinhood",
        candidate_id=EVIDENCE,
        requested_fraction=0.20,
        allow_downsize=False,
        minimum_fraction=0.20,
    )
    trial_id = _old_release_lot(store, reservation_id)
    lot = bridge._matching_lot_canonical(owner, payload={"token": TOKEN, "market": MARKET}, evidence=EVIDENCE)
    assert lot is not None
    bridge._link_reservation_canonical(owner, lot=lot, reservation_id=reservation_id)

    with store._lock, store.db:
        store.db.execute(
            "UPDATE v52_robinhood_position_lots SET remaining_fraction=0.10,remaining_entry_cost_wei='500',"
            "realized_exit_net_wei='600' WHERE trial_id=?",
            (trial_id,),
        )
        changed_lot = dict(store.db.execute(
            "SELECT * FROM v52_robinhood_position_lots WHERE trial_id=?", (trial_id,)
        ).fetchone())
        line = dict(store.db.execute(
            "SELECT * FROM v52_robinhood_capital_lineage WHERE trial_id=?", (trial_id,)
        ).fetchone())

    bridge._settle_lot_delta_canonical(owner, changed_lot, line)
    state = capital_reconciliation(store, release_commit="release-b")
    assert state["active_reserved_fraction"] == pytest.approx(0.10)
    assert state["realized_return_contribution"] == pytest.approx(0.02)
    with store._lock:
        settlement = store.db.execute(
            "SELECT release_commit,portfolio_id FROM v51_paper_capital_settlements LIMIT 1"
        ).fetchone()
        residual = store.db.execute(
            "SELECT release_commit,status FROM v51_paper_capital_reservations "
            "WHERE reservation_id=?",
            (f"robinhood-v52:trial:{trial_id}:residual:1",),
        ).fetchone()
    assert settlement["release_commit"] == "release-a"
    assert residual["release_commit"] == "release-a"
    assert residual["status"] == "active"
    store.close()


def test_bridge_install_replaces_release_scoped_shared_helpers() -> None:
    bridge.install_v52_robinhood_canonical_capital_bridge()
    assert shared._matching_lot is bridge._matching_lot_canonical
    assert shared._link_reservation is bridge._link_reservation_canonical
    assert shared._settle_lot_delta is bridge._settle_lot_delta_canonical
