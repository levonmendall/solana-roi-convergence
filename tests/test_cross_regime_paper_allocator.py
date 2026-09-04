from __future__ import annotations

import sqlite3
import threading

from solana_roi.cross_regime_paper_allocator import build_cross_regime_allocation


class _Store:
    def __init__(self) -> None:
        self.db = sqlite3.connect(":memory:")
        self.db.row_factory = sqlite3.Row
        self._lock = threading.RLock()


def test_allocator_keeps_immature_or_unknown_correlation_capacity_in_cash() -> None:
    store = _Store()
    with store.db:
        store.db.execute(
            "CREATE TABLE risk_conditioned_alpha_v5_outcomes ("
            "id INTEGER PRIMARY KEY, release_commit TEXT, venue TEXT, net_return REAL)"
        )
        store.db.execute(
            "CREATE TABLE fomo_paper_outcomes ("
            "id INTEGER PRIMARY KEY, release_commit TEXT, net_return REAL)"
        )
        for index, value in enumerate([1.0] * 16 + [-0.20] * 24, start=1):
            store.db.execute(
                "INSERT INTO risk_conditioned_alpha_v5_outcomes VALUES (?,?,?,?)",
                (index, "release", "PUMP_AMM", value),
            )
        for index, value in enumerate([0.8] * 16 + [-0.15] * 24, start=1):
            store.db.execute(
                "INSERT INTO fomo_paper_outcomes VALUES (?,?,?)",
                (index, "release", value),
            )
    result = build_cross_regime_allocation(store, "release")
    assert result["mature_promoted_segments"] == 2
    assert result["paper_allocation_weights"]["PUMP_AMM"] <= 0.25
    assert result["paper_allocation_weights"]["FOMO"] <= 0.25
    assert result["paper_cash_weight"] >= 0.50
    assert result["live_money_authority"] is False
