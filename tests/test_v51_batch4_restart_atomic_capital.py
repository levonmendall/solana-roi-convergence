from __future__ import annotations

import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from solana_roi.observation_store import ObservationEventStore
from solana_roi.robinhood_chain_paper import RobinhoodChainPaperPlane
from solana_roi.strategy_v51_authority import authority
from solana_roi.v51_atomic_paper_capital import (
    ATOMIC_CAPITAL_VERSION,
    capital_reconciliation,
    ensure_atomic_capital_schema,
    lifecycle_events,
    record_lifecycle_event,
    reserve_paper_capital,
    settle_paper_capital,
)


RELEASE = "4" * 40
LIFECYCLE = (
    "observation",
    "evaluation",
    "authorization",
    "entry",
    "open",
    "exit_request",
    "settlement",
)


def _drive_candidate(store: ObservationEventStore, *, candidate: str, stop_after: str | None = None) -> None:
    for stage in LIFECYCLE:
        if stage == "entry":
            reserve_paper_capital(
                store,
                release_commit=RELEASE,
                reservation_id=f"reservation:{candidate}",
                lane="pump_fun",
                candidate_id=candidate,
                requested_fraction=0.10,
                capacity_fraction=1.0,
                allow_downsize=False,
                minimum_fraction=0.10,
            )
        elif stage == "settlement":
            settle_paper_capital(
                store,
                release_commit=RELEASE,
                reservation_id=f"reservation:{candidate}",
                settlement_id=f"settlement:{candidate}",
                net_return=0.25,
            )
        record_lifecycle_event(
            store,
            release_commit=RELEASE,
            candidate_id=candidate,
            event_key=stage,
            stage=stage,
            payload={"candidate_id": candidate, "stage": stage},
        )
        if stage == stop_after:
            return


def _counts(store: ObservationEventStore, candidate: str) -> tuple[int, int, int]:
    with store._lock:
        events = store.db.execute(
            "SELECT COUNT(*) AS n FROM v51_paper_lifecycle_events WHERE release_commit=? AND candidate_id=?",
            (RELEASE, candidate),
        ).fetchone()["n"]
        reservations = store.db.execute(
            "SELECT COUNT(*) AS n FROM v51_paper_capital_reservations WHERE release_commit=? AND candidate_id=?",
            (RELEASE, candidate),
        ).fetchone()["n"]
        settlements = store.db.execute(
            "SELECT COUNT(*) AS n FROM v51_paper_capital_settlements WHERE release_commit=? AND reservation_id=?",
            (RELEASE, f"reservation:{candidate}"),
        ).fetchone()["n"]
    return int(events), int(reservations), int(settlements)


@pytest.mark.parametrize("checkpoint", LIFECYCLE)
def test_restart_matrix_is_lossless_idempotent_and_equivalent(tmp_path, checkpoint: str) -> None:
    path = tmp_path / f"restart-{checkpoint}.sqlite3"
    candidate = f"candidate-{checkpoint}"

    first = ObservationEventStore(path)
    _drive_candidate(first, candidate=candidate, stop_after=checkpoint)
    before = capital_reconciliation(first, release_commit=RELEASE)
    first.close()

    if checkpoint in {"entry", "open", "exit_request"}:
        assert before["active_reserved_fraction"] == pytest.approx(0.10)
    if checkpoint == "settlement":
        assert before["active_reserved_fraction"] == pytest.approx(0.0)
        assert before["paper_nav_multiplier"] == pytest.approx(1.025)

    recovered = ObservationEventStore(path)
    _drive_candidate(recovered, candidate=candidate)
    final = capital_reconciliation(recovered, release_commit=RELEASE)
    rows = lifecycle_events(recovered, release_commit=RELEASE, candidate_id=candidate)

    assert [row["stage"] for row in rows] == list(LIFECYCLE)
    assert _counts(recovered, candidate) == (7, 1, 1)
    assert final["active_reserved_fraction"] == pytest.approx(0.0)
    assert final["paper_nav_multiplier"] == pytest.approx(1.025)
    assert final["capital_conserved"] is True
    recovered.close()

    control_path = tmp_path / f"control-{checkpoint}.sqlite3"
    control = ObservationEventStore(control_path)
    _drive_candidate(control, candidate=candidate)
    uninterrupted = capital_reconciliation(control, release_commit=RELEASE)
    assert final["paper_nav_multiplier"] == pytest.approx(uninterrupted["paper_nav_multiplier"])
    assert final["realized_return_contribution"] == pytest.approx(uninterrupted["realized_return_contribution"])
    control.close()


def test_five_lane_concurrency_cannot_double_spend_and_settles_once(tmp_path) -> None:
    path = tmp_path / "five-lane.sqlite3"
    bootstrap = ObservationEventStore(path)
    ensure_atomic_capital_schema(bootstrap)
    bootstrap.close()

    lanes = ("pump_fun", "pump_amm", "raydium", "fomo", "robinhood")
    barrier = threading.Barrier(len(lanes))

    def reserve(lane: str) -> dict:
        store = ObservationEventStore(path)
        try:
            barrier.wait(timeout=5)
            return reserve_paper_capital(
                store,
                release_commit=RELEASE,
                reservation_id=f"reservation:{lane}",
                lane=lane,
                candidate_id=f"candidate:{lane}",
                requested_fraction=0.30,
                capacity_fraction=0.75,
                allow_downsize=True,
                minimum_fraction=0.10,
            )
        finally:
            store.close()

    with ThreadPoolExecutor(max_workers=len(lanes)) as pool:
        results = list(pool.map(reserve, lanes))

    store = ObservationEventStore(path)
    reconciliation = capital_reconciliation(store, release_commit=RELEASE, capacity_fraction=0.75)
    active = [row for row in results if row["status"] == "active"]
    rejected = [row for row in results if row["status"] == "rejected"]

    assert reconciliation["capital_conserved"] is True
    assert reconciliation["active_reserved_fraction"] == pytest.approx(0.75)
    assert sum(float(row["reserved_fraction"]) for row in active) == pytest.approx(0.75)
    assert len(active) == 3
    assert len(rejected) == 2
    assert any(float(row["reserved_fraction"]) == pytest.approx(0.15) for row in active)
    assert len({row["reservation_id"] for row in results}) == 5

    returns = {
        "pump_fun": 0.10,
        "pump_amm": -0.05,
        "raydium": 0.20,
        "fomo": 0.15,
        "robinhood": -0.10,
    }
    expected = 0.0
    for row in active:
        lane = str(row["lane"])
        expected += float(row["reserved_fraction"]) * returns[lane]
        first = settle_paper_capital(
            store,
            release_commit=RELEASE,
            reservation_id=str(row["reservation_id"]),
            settlement_id=f"settlement:{lane}",
            net_return=returns[lane],
        )
        replay = settle_paper_capital(
            store,
            release_commit=RELEASE,
            reservation_id=str(row["reservation_id"]),
            settlement_id=f"settlement:{lane}",
            net_return=returns[lane],
        )
        assert first["idempotent_replay"] is False
        assert replay["idempotent_replay"] is True

    final = capital_reconciliation(store, release_commit=RELEASE, capacity_fraction=0.75)
    assert final["active_reserved_fraction"] == pytest.approx(0.0)
    assert final["settlement_count"] == len(active)
    assert final["realized_return_contribution"] == pytest.approx(expected)
    assert final["paper_nav_multiplier"] == pytest.approx(1.0 + expected)
    assert final["capital_conserved"] is True
    assert final["sqlite_busy_retries"] >= 0
    assert final["max_write_latency_ms"] >= 0.0
    assert final["avg_write_latency_ms"] >= 0.0
    store.close()


def test_same_reservation_concurrent_replay_creates_one_row(tmp_path) -> None:
    path = tmp_path / "same-reservation.sqlite3"
    bootstrap = ObservationEventStore(path)
    ensure_atomic_capital_schema(bootstrap)
    bootstrap.close()
    barrier = threading.Barrier(8)

    def attempt(index: int) -> dict:
        store = ObservationEventStore(path)
        try:
            barrier.wait(timeout=5)
            return reserve_paper_capital(
                store,
                release_commit=RELEASE,
                reservation_id="same-reservation",
                lane="robinhood",
                candidate_id="same-candidate",
                requested_fraction=0.05,
                capacity_fraction=0.15,
                allow_downsize=False,
                minimum_fraction=0.05,
            )
        finally:
            store.close()

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(attempt, range(8)))

    store = ObservationEventStore(path)
    with store._lock:
        count = store.db.execute(
            "SELECT COUNT(*) AS n FROM v51_paper_capital_reservations WHERE release_commit=? AND reservation_id=?",
            (RELEASE, "same-reservation"),
        ).fetchone()["n"]
    assert int(count) == 1
    assert all(row["status"] == "active" for row in results)
    assert sum(not bool(row["idempotent_replay"]) for row in results) == 1
    assert capital_reconciliation(store, release_commit=RELEASE, capacity_fraction=0.15)["active_reserved_fraction"] == pytest.approx(0.05)
    store.close()


def test_robinhood_trial_entry_is_bound_to_one_reservation_and_restore_repairs_settlement(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("ROBINHOOD_ENTITY_RESOLUTION_REQUIRED", "true")
    monkeypatch.setenv("ROBINHOOD_RWA_FILTER_REQUIRED", "true")
    path = tmp_path / "robinhood-batch4.sqlite3"
    store = ObservationEventStore(path)
    plane = RobinhoodChainPaperPlane(store, release_commit=RELEASE)
    token = "0x" + "1" * 40
    market = "0x" + "2" * 40
    quote = {
        "amount_in_wei": 1000,
        "token_out": 100,
        "entry_gas_wei": 10,
        "entry_total_cost_wei": 1010,
        "entry_price_eth": 1.0,
        "round_trip_cost_fraction": 0.01,
    }

    assert plane._insert_trial(
        token=token,
        market=market,
        venue="UNISWAP_V3_DIRECT",
        lifecycle="new_weth_pool",
        trigger_actor="0x" + "3" * 40,
        trigger_entity="0x" + "4" * 40,
        fomo_state="active_fomo",
        context_state="bootstrap_paper_evidence",
        fraction=0.01,
        quote=quote,
        decision_reason="batch4-test",
    ) is True
    assert plane._insert_trial(
        token=token,
        market=market,
        venue="UNISWAP_V3_DIRECT",
        lifecycle="new_weth_pool",
        trigger_actor="0x" + "3" * 40,
        trigger_entity="0x" + "4" * 40,
        fomo_state="active_fomo",
        context_state="bootstrap_paper_evidence",
        fraction=0.01,
        quote=quote,
        decision_reason="batch4-test",
    ) is False

    with store._lock, store.db:
        trial = dict(store.db.execute("SELECT * FROM robinhood_paper_trials").fetchone())
        count = store.db.execute("SELECT COUNT(*) AS n FROM robinhood_paper_trials").fetchone()["n"]
        store.db.execute(
            "INSERT INTO robinhood_paper_outcomes("
            "release_commit,trial_id,token,market,venue,lifecycle,trigger_actor,trigger_entity,fomo_state,position_fraction,"
            "net_return,paper_nav_multiplier,exit_quote_out_wei,exit_gas_wei,exit_reason,settled_at,paper_only,live_money_authority"
            ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0)",
            (
                RELEASE,
                int(trial["id"]),
                token,
                market,
                "UNISWAP_V3_DIRECT",
                "new_weth_pool",
                "0x" + "3" * 40,
                "0x" + "4" * 40,
                "active_fomo",
                0.01,
                0.20,
                1.002,
                "1200",
                "10",
                "batch4-test",
                "2026-09-06T00:00:00+00:00",
            ),
        )
    assert int(count) == 1
    reservation_id = str(trial["capital_reservation_id"])
    assert reservation_id
    before = capital_reconciliation(store, release_commit=RELEASE, capacity_fraction=0.15)
    assert before["active_reserved_fraction"] == pytest.approx(0.01)
    asyncio.run(plane.close())
    store.close()

    reopened_store = ObservationEventStore(path)
    reopened = RobinhoodChainPaperPlane(reopened_store, release_commit=RELEASE)
    after = capital_reconciliation(reopened_store, release_commit=RELEASE, capacity_fraction=0.15)
    assert after["active_reserved_fraction"] == pytest.approx(0.0)
    assert after["settlement_count"] == 1
    assert after["paper_nav_multiplier"] == pytest.approx(1.002)
    asyncio.run(reopened.close())
    reopened_store.close()


def test_batch4_preserves_v51_economics_and_paper_only_authority() -> None:
    current = authority()
    assert current["paper_only"] is True
    assert current["live_money_authority"] is False
    assert current["signing_available"] is False
    assert current["transaction_submission_available"] is False
    assert current["execution"]["latency_hard_max_seconds"] == 20.0
    assert current["execution"]["chase_baseline_fraction"] == 0.15
    assert current["execution"]["chase_observe_only_above_fraction"] == 0.40
    assert ATOMIC_CAPITAL_VERSION == "v51-atomic-paper-capital-v1"
