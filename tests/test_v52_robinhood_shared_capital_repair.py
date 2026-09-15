from __future__ import annotations

import asyncio

import pytest

from solana_roi.observation_store import ObservationEventStore
from solana_roi import v52_robinhood_position_lifecycle as lifecycle
from solana_roi import v52_robinhood_shared_capital_repair as repair
from solana_roi.v51_atomic_paper_capital import capital_reconciliation, reserve_paper_capital


RELEASE = "shared-capital-test"
TOKEN = "0x" + "1" * 40
MARKET = "0x" + "2" * 40
EVIDENCE = "e" * 64


class Owner:
    def __init__(self, store: ObservationEventStore) -> None:
        self.store = store
        self.release_commit = RELEASE
        self.starting_nav_usd = 500.0


def _minimal_lifecycle_schema(store: ObservationEventStore) -> None:
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE robinhood_paper_trials ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,release_commit TEXT NOT NULL,token TEXT NOT NULL,market TEXT NOT NULL,"
            "position_fraction REAL NOT NULL,capital_reservation_id TEXT)"
        )
        store.db.execute(
            "CREATE TABLE robinhood_paper_outcomes ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,trial_id INTEGER NOT NULL,net_return REAL NOT NULL)"
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
            "id INTEGER PRIMARY KEY AUTOINCREMENT,position_id INTEGER NOT NULL,position_fraction REAL NOT NULL,"
            "net_return REAL)"
        )


def _insert_lot(store: ObservationEventStore, *, reservation_id: str) -> int:
    with store._lock, store.db:
        cursor = store.db.execute(
            "INSERT INTO robinhood_paper_trials(release_commit,token,market,position_fraction,capital_reservation_id) "
            "VALUES (?,?,?,?,?)",
            (RELEASE, TOKEN, MARKET, 0.20, reservation_id),
        )
        trial_id = int(cursor.lastrowid)
        store.db.execute(
            "INSERT INTO v52_robinhood_position_lots("
            "position_id,trial_id,entry_fraction,remaining_fraction,entry_total_cost_wei,remaining_entry_cost_wei,"
            "realized_exit_net_wei,capital_reservation_id,evidence_fingerprint) VALUES (?,?,?,?,?,?,?,?,?)",
            (1, trial_id, 0.20, 0.20, "1000", "1000", "0", reservation_id, EVIDENCE),
        )
    return trial_id


def test_failed_v52_entry_releases_precommit_reservation(tmp_path, monkeypatch) -> None:
    store = ObservationEventStore(tmp_path / "failed-entry.sqlite3")
    _minimal_lifecycle_schema(store)
    owner = Owner(store)
    repair._ensure_schema(owner)
    lifecycle._pending_map(owner)[TOKEN] = {
        "evidence_fingerprint": EVIDENCE,
        "stage": "starter",
    }

    async def reject(_owner, _payload, *, venue_object):
        _ = venue_object
        return False

    monkeypatch.setattr(repair, "_BASE_VALIDATE", reject)
    payload = {"token": TOKEN, "market": MARKET, "fraction": 0.25}
    committed = asyncio.run(repair._validate_with_shared_capital(owner, payload, venue_object=object()))

    assert committed is False
    with store._lock:
        row = store.db.execute(
            "SELECT status,reserved_fraction FROM v51_paper_capital_reservations WHERE release_commit=?",
            (RELEASE,),
        ).fetchone()
    assert row is not None
    assert row["status"] == "cancelled"
    assert float(row["reserved_fraction"]) == pytest.approx(0.25)
    state = capital_reconciliation(store, release_commit=RELEASE)
    assert state["active_reserved_fraction"] == 0.0
    store.close()


def test_staged_exit_releases_only_sold_capital_and_nav_is_linear(tmp_path) -> None:
    store = ObservationEventStore(tmp_path / "staged-exit.sqlite3")
    _minimal_lifecycle_schema(store)
    owner = Owner(store)
    repair._ensure_schema(owner)

    initial = reserve_paper_capital(
        store,
        release_commit=RELEASE,
        reservation_id="robinhood-v52:initial",
        lane="robinhood",
        candidate_id=EVIDENCE,
        requested_fraction=0.20,
        allow_downsize=False,
        minimum_fraction=0.20,
    )
    assert initial["status"] == "active"
    trial_id = _insert_lot(store, reservation_id="robinhood-v52:initial")
    with store._lock, store.db:
        store.db.execute(
            "INSERT INTO v52_robinhood_capital_lineage("
            "trial_id,release_commit,original_reservation_id,current_reservation_id,original_fraction,"
            "accounted_remaining_fraction,accounted_realized_exit_net_wei,accounted_sold_cost_wei,"
            "settlement_sequence,updated_at,paper_only,live_money_authority) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,1,0)",
            (
                trial_id,
                RELEASE,
                "robinhood-v52:initial",
                "robinhood-v52:initial",
                0.20,
                0.20,
                "0",
                "0",
                0,
                "2026-09-15T00:00:00+00:00",
            ),
        )
        store.db.execute(
            "UPDATE v52_robinhood_position_lots SET remaining_fraction=0.10,remaining_entry_cost_wei='500',"
            "realized_exit_net_wei='600' WHERE trial_id=?",
            (trial_id,),
        )
        first_lot = dict(store.db.execute(
            "SELECT * FROM v52_robinhood_position_lots WHERE trial_id=?", (trial_id,)
        ).fetchone())
        first_line = dict(store.db.execute(
            "SELECT * FROM v52_robinhood_capital_lineage WHERE trial_id=?", (trial_id,)
        ).fetchone())

    repair._settle_lot_delta(owner, first_lot, first_line)
    first = capital_reconciliation(store, release_commit=RELEASE)
    assert first["active_reserved_fraction"] == pytest.approx(0.10)
    assert first["available_fraction"] == pytest.approx(0.90)
    assert first["realized_return_contribution"] == pytest.approx(0.02)
    assert first["paper_nav_multiplier"] == pytest.approx(1.02)

    with store._lock, store.db:
        store.db.execute(
            "UPDATE v52_robinhood_position_lots SET remaining_fraction=0.0,remaining_entry_cost_wei='0',"
            "realized_exit_net_wei='1200' WHERE trial_id=?",
            (trial_id,),
        )
        second_lot = dict(store.db.execute(
            "SELECT * FROM v52_robinhood_position_lots WHERE trial_id=?", (trial_id,)
        ).fetchone())
        second_line = dict(store.db.execute(
            "SELECT * FROM v52_robinhood_capital_lineage WHERE trial_id=?", (trial_id,)
        ).fetchone())

    repair._settle_lot_delta(owner, second_lot, second_line)
    final = capital_reconciliation(store, release_commit=RELEASE)
    assert final["active_reserved_fraction"] == 0.0
    assert final["available_fraction"] == pytest.approx(1.0)
    assert final["settlement_count"] == 2
    assert final["realized_return_contribution"] == pytest.approx(0.04)
    assert final["paper_nav_multiplier"] == pytest.approx(1.04)
    assert repair._paper_nav_with_shared_capital(owner) == pytest.approx(520.0)

    # The former slice-compounding defect would have produced 1.02 * 1.02 = 1.0404.
    assert final["paper_nav_multiplier"] != pytest.approx(1.0404)
    store.close()


def test_replaying_staged_exit_delta_does_not_release_capital_twice(tmp_path) -> None:
    store = ObservationEventStore(tmp_path / "replay-exit.sqlite3")
    _minimal_lifecycle_schema(store)
    owner = Owner(store)
    repair._ensure_schema(owner)
    reserve_paper_capital(
        store,
        release_commit=RELEASE,
        reservation_id="robinhood-v52:replay",
        lane="robinhood",
        candidate_id=EVIDENCE,
        requested_fraction=0.20,
        allow_downsize=False,
        minimum_fraction=0.20,
    )
    trial_id = _insert_lot(store, reservation_id="robinhood-v52:replay")
    with store._lock, store.db:
        store.db.execute(
            "INSERT INTO v52_robinhood_capital_lineage("
            "trial_id,release_commit,original_reservation_id,current_reservation_id,original_fraction,"
            "accounted_remaining_fraction,accounted_realized_exit_net_wei,accounted_sold_cost_wei,"
            "settlement_sequence,updated_at,paper_only,live_money_authority) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,1,0)",
            (trial_id, RELEASE, "robinhood-v52:replay", "robinhood-v52:replay", 0.20, 0.20, "0", "0", 0,
             "2026-09-15T00:00:00+00:00"),
        )
        store.db.execute(
            "UPDATE v52_robinhood_position_lots SET remaining_fraction=0.10,remaining_entry_cost_wei='500',"
            "realized_exit_net_wei='600' WHERE trial_id=?",
            (trial_id,),
        )
        lot = dict(store.db.execute(
            "SELECT * FROM v52_robinhood_position_lots WHERE trial_id=?", (trial_id,)
        ).fetchone())
        line = dict(store.db.execute(
            "SELECT * FROM v52_robinhood_capital_lineage WHERE trial_id=?", (trial_id,)
        ).fetchone())

    repair._settle_lot_delta(owner, lot, line)
    refreshed = repair._lineage(owner, trial_id)
    assert refreshed is not None
    repair._settle_lot_delta(owner, lot, refreshed)

    state = capital_reconciliation(store, release_commit=RELEASE)
    assert state["active_reserved_fraction"] == pytest.approx(0.10)
    assert state["settlement_count"] == 1
    assert state["realized_return_contribution"] == pytest.approx(0.02)
    store.close()
