from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest

from solana_roi.observation_store import ObservationEventStore
from solana_roi.v51_atomic_paper_capital import (
    CANONICAL_PORTFOLIO_ID,
    capital_reconciliation,
    reserve_paper_capital,
    settle_paper_capital,
)


RELEASE = "paper-capital-regression"


def _reserve(
    store: ObservationEventStore,
    reservation_id: str,
    lane: str,
    fraction: float,
    *,
    release: str = RELEASE,
) -> dict[str, object]:
    return reserve_paper_capital(
        store,
        release_commit=release,
        reservation_id=reservation_id,
        lane=lane,
        candidate_id=reservation_id,
        requested_fraction=fraction,
        allow_downsize=False,
        minimum_fraction=fraction,
    )


def test_499_50_reserved_blocks_additional_6_25_position(tmp_path) -> None:
    store = ObservationEventStore(tmp_path / "exact-boundary.sqlite3")

    first = _reserve(store, "solana:first", "solana", 499.50 / 500.0)
    second = _reserve(store, "robinhood:second", "robinhood", 6.25 / 500.0)
    state = capital_reconciliation(store, release_commit=RELEASE)

    assert first["status"] == "active"
    assert float(first["reserved_fraction"]) == pytest.approx(0.999)
    assert second["status"] == "rejected"
    assert float(second["reserved_fraction"]) == 0.0
    assert state["portfolio_id"] == CANONICAL_PORTFOLIO_ID
    assert state["active_reserved_fraction"] == pytest.approx(0.999)
    assert state["available_fraction"] == pytest.approx(0.001)
    assert state["capital_conserved"] is True
    store.close()


def test_near_concurrent_cross_lane_candidates_cannot_overspend(tmp_path) -> None:
    path = tmp_path / "concurrent-boundary.sqlite3"
    bootstrap = ObservationEventStore(path)
    bootstrap.close()

    def attempt(reservation_id: str, lane: str) -> dict[str, object]:
        store = ObservationEventStore(path)
        try:
            return _reserve(store, reservation_id, lane, 0.60)
        finally:
            store.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(
            pool.map(
                lambda args: attempt(*args),
                [("solana:race", "solana"), ("robinhood:race", "robinhood")],
            )
        )

    statuses = sorted(str(result["status"]) for result in results)
    assert statuses == ["active", "rejected"]

    check = ObservationEventStore(path)
    state = capital_reconciliation(check, release_commit=RELEASE)
    assert state["active_reserved_fraction"] == pytest.approx(0.60)
    assert state["capital_conserved"] is True
    check.close()


def test_release_change_cannot_recreate_buying_power(tmp_path) -> None:
    store = ObservationEventStore(tmp_path / "cross-release.sqlite3")

    first = _reserve(
        store,
        "solana:old-release-position",
        "solana",
        499.50 / 500.0,
        release="release-a",
    )
    second = _reserve(
        store,
        "robinhood:new-release-candidate",
        "robinhood",
        6.25 / 500.0,
        release="release-b",
    )
    state = capital_reconciliation(store, release_commit="release-b")

    assert first["status"] == "active"
    assert second["status"] == "rejected"
    assert state["active_reserved_fraction"] == pytest.approx(0.999)
    assert state["available_fraction"] == pytest.approx(0.001)
    assert state["active_release_count"] == 1
    assert state["capital_conserved"] is True
    store.close()


def test_restart_replay_finds_same_reservation_across_release_boundary(tmp_path) -> None:
    store = ObservationEventStore(tmp_path / "restart-replay.sqlite3")
    opened = _reserve(
        store,
        "fomo:stable-candidate",
        "fomo",
        0.25,
        release="release-a",
    )
    replay = _reserve(
        store,
        "fomo:stable-candidate",
        "fomo",
        0.25,
        release="release-b",
    )

    assert opened["status"] == "active"
    assert replay["idempotent_replay"] is True
    assert int(replay["id"]) == int(opened["id"])
    assert replay["release_commit"] == "release-a"
    state = capital_reconciliation(store, release_commit="release-b")
    assert state["active_reserved_fraction"] == pytest.approx(0.25)
    assert state["reservation_status"]["active"]["count"] == 1
    store.close()


def test_restart_can_settle_reservation_created_by_prior_release_exactly_once(tmp_path) -> None:
    store = ObservationEventStore(tmp_path / "restart-settlement.sqlite3")
    _reserve(
        store,
        "solana:position-survives-release",
        "solana",
        0.40,
        release="release-a",
    )

    settled = settle_paper_capital(
        store,
        release_commit="release-b",
        reservation_id="solana:position-survives-release",
        settlement_id="solana:position-survives-release:exit",
        net_return=0.10,
    )
    replay = settle_paper_capital(
        store,
        release_commit="release-c",
        reservation_id="solana:position-survives-release",
        settlement_id="solana:position-survives-release:exit",
        net_return=0.10,
    )
    state = capital_reconciliation(store, release_commit="release-c")

    assert settled["idempotent_replay"] is False
    assert settled["release_commit"] == "release-a"
    assert replay["idempotent_replay"] is True
    assert state["active_reserved_fraction"] == 0.0
    assert state["settlement_count"] == 1
    assert state["realized_return_contribution"] == pytest.approx(0.04)
    assert state["paper_nav_multiplier"] == pytest.approx(1.04)
    store.close()
