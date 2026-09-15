from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest

from solana_roi.observation_store import ObservationEventStore
from solana_roi import v51_atomic_paper_capital as capital
from solana_roi import v51_paper_lifecycle_runtime as paper_lifecycle
from solana_roi import v52_shared_paper_capital_bridge as bridge


RELEASE = "b" * 40
TOKEN = "0x00000000000000000000000000000000000000b2"


@pytest.fixture(autouse=True)
def _reset_active_adapter():
    previous = paper_lifecycle._ACTIVE_ADAPTER
    paper_lifecycle._ACTIVE_ADAPTER = None
    yield
    paper_lifecycle._ACTIVE_ADAPTER = previous


def _bind(store: ObservationEventStore):
    adapter = SimpleNamespace(store=store, release_commit=RELEASE)
    owner = SimpleNamespace(release_commit=RELEASE)
    paper_lifecycle._ACTIVE_ADAPTER = adapter
    capital.ensure_atomic_capital_schema(store)
    return owner


def test_499_50_reserved_plus_6_25_robinhood_request_cannot_open(tmp_path) -> None:
    store = ObservationEventStore(tmp_path / "shared.sqlite3")
    owner = _bind(store)
    existing = capital.reserve_paper_capital(
        store,
        release_commit=RELEASE,
        reservation_id="solana:already-open",
        lane="SOLANA:pumpfun",
        candidate_id="already-open",
        requested_fraction=499.50 / 500.0,
        allow_downsize=False,
        minimum_fraction=499.50 / 500.0,
    )
    assert existing["status"] == "active"

    robinhood = bridge.reserve_robinhood_capital(
        owner,
        token=TOKEN,
        evidence_fingerprint="evidence-a",
        lane="entity_flow_accumulation",
        requested_fraction=6.25 / 500.0,
    )
    assert robinhood["status"] == "rejected"
    assert float(robinhood["reserved_fraction"]) == 0.0
    reconciliation = capital.capital_reconciliation(store, release_commit=RELEASE)
    assert reconciliation["active_reserved_fraction"] == pytest.approx(0.999)
    assert reconciliation["available_fraction"] == pytest.approx(0.001)
    assert reconciliation["capital_conserved"] is True
    store.close()


def test_sufficient_shared_capital_allows_exact_robinhood_reservation_and_replay_is_idempotent(tmp_path) -> None:
    store = ObservationEventStore(tmp_path / "available.sqlite3")
    owner = _bind(store)
    first = bridge.reserve_robinhood_capital(
        owner,
        token=TOKEN,
        evidence_fingerprint="evidence-b",
        lane="fomo_continuation",
        requested_fraction=0.125,
    )
    second = bridge.reserve_robinhood_capital(
        owner,
        token=TOKEN,
        evidence_fingerprint="evidence-b",
        lane="fomo_continuation",
        requested_fraction=0.125,
    )
    assert first["status"] == "active"
    assert float(first["reserved_fraction"]) == pytest.approx(0.125)
    assert first["idempotent_replay"] is False
    assert second["status"] == "active"
    assert second["idempotent_replay"] is True
    with store._lock:
        count = store.db.execute(
            "SELECT COUNT(*) FROM v51_paper_capital_reservations WHERE release_commit=?",
            (RELEASE,),
        ).fetchone()[0]
    assert int(count) == 1
    store.close()


def test_near_concurrent_cross_lane_candidates_cannot_double_spend_shared_capacity(tmp_path) -> None:
    store = ObservationEventStore(tmp_path / "race.sqlite3")
    owner = _bind(store)
    barrier = threading.Barrier(2)

    def reserve_solana():
        barrier.wait()
        return capital.reserve_paper_capital(
            store,
            release_commit=RELEASE,
            reservation_id="solana:race",
            lane="SOLANA:pumpswap",
            candidate_id="solana-race",
            requested_fraction=0.60,
            allow_downsize=False,
            minimum_fraction=0.60,
        )

    def reserve_robinhood():
        barrier.wait()
        return bridge.reserve_robinhood_capital(
            owner,
            token=TOKEN,
            evidence_fingerprint="race",
            lane="elite_entity_continuation",
            requested_fraction=0.60,
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = [pool.submit(reserve_solana), pool.submit(reserve_robinhood)]
        rows = [future.result() for future in results]
    assert sorted(str(row["status"]) for row in rows) == ["active", "rejected"]
    reconciliation = capital.capital_reconciliation(store, release_commit=RELEASE)
    assert reconciliation["active_reserved_fraction"] == pytest.approx(0.60)
    assert reconciliation["available_fraction"] == pytest.approx(0.40)
    assert reconciliation["capital_conserved"] is True
    store.close()


def test_robinhood_realization_contributes_once_to_canonical_nav(tmp_path) -> None:
    store = ObservationEventStore(tmp_path / "nav.sqlite3")
    _bind(store)
    bridge.install_v52_shared_paper_capital_bridge()
    capital.record_lifecycle_event(
        store,
        release_commit=RELEASE,
        candidate_id="robinhood-position:7",
        event_key="ROBINHOOD:REALIZED:41",
        stage="ROBINHOOD_REALIZED",
        payload={
            "surface": "ROBINHOOD",
            "position_id": 7,
            "position_event_id": 41,
            "released_fraction": 0.01,
            "net_return": 0.20,
            "realized_contribution": 0.002,
            "paper_only": True,
            "live_money_authority": False,
        },
    )
    # Duplicate event replay is ignored by the canonical unique lifecycle key.
    capital.record_lifecycle_event(
        store,
        release_commit=RELEASE,
        candidate_id="robinhood-position:7",
        event_key="ROBINHOOD:REALIZED:41",
        stage="ROBINHOOD_REALIZED",
        payload={"realized_contribution": 999.0},
    )
    reconciliation = capital.capital_reconciliation(store, release_commit=RELEASE)
    assert reconciliation["robinhood_realized_return_contribution"] == pytest.approx(0.002)
    assert reconciliation["realized_return_contribution"] == pytest.approx(0.002)
    assert reconciliation["paper_nav_multiplier"] == pytest.approx(1.002)
    store.close()


def test_bridge_preserves_paper_only_authority() -> None:
    status = bridge.status()
    assert status["single_buying_power_authority"] == "v51_paper_capital_reservations"
    assert status["cross_lane_capacity_shared"] is True
    assert status["paper_only"] is True
    assert status["live_money_authority"] is False
    assert status["signing_available"] is False
    assert status["transaction_submission_available"] is False
