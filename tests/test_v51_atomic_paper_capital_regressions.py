from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest

from solana_roi.observation_store import ObservationEventStore
from solana_roi.v51_atomic_paper_capital import capital_reconciliation, reserve_paper_capital


RELEASE = "paper-capital-regression"


def _reserve(store: ObservationEventStore, reservation_id: str, lane: str, fraction: float) -> dict[str, object]:
    return reserve_paper_capital(
        store,
        release_commit=RELEASE,
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
