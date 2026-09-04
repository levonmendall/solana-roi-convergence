from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timedelta, timezone

from solana_roi.continuation_market_context import (
    _flow_context,
    market_cap_band,
    price_sensitivity_band,
)


class Store:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.db = sqlite3.connect(":memory:")
        self.db.row_factory = sqlite3.Row
        self.db.execute(
            "CREATE TABLE normalized_swaps ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, token_mint TEXT, side TEXT, native_amount_sol REAL, "
            "reference_price_sol REAL, received_at TEXT)"
        )


def test_market_cap_is_descriptive_across_microcap_bands() -> None:
    assert market_cap_band(None) == "unknown"
    assert market_cap_band(50_000) == "ultra_micro_lt_100k"
    assert market_cap_band(250_000) == "micro_100k_500k"
    assert market_cap_band(1_000_000) == "small_500k_2m"
    assert market_cap_band(5_000_000) == "developing_2m_10m"
    assert market_cap_band(25_000_000) == "developed_ge_10m"


def test_price_sensitivity_records_how_easily_price_moves_per_sol() -> None:
    assert price_sensitivity_band(0.005) == "low_lt_1pct_per_sol"
    assert price_sensitivity_band(0.02) == "moderate_1_3pct_per_sol"
    assert price_sensitivity_band(0.05) == "high_3_10pct_per_sol"
    assert price_sensitivity_band(0.15) == "extreme_ge_10pct_per_sol"


def test_flow_context_measures_price_change_relative_to_gross_flow() -> None:
    store = Store()
    now = datetime.now(timezone.utc)
    rows = [
        ("buy", 1.0, 1.00, 50),
        ("buy", 1.0, 1.20, 30),
        ("sell", 0.5, 1.50, 10),
    ]
    with store._lock, store.db:
        for side, native, price, age in rows:
            store.db.execute(
                "INSERT INTO normalized_swaps(token_mint,side,native_amount_sol,reference_price_sol,received_at) VALUES (?,?,?,?,?)",
                ("mint", side, native, price, (now - timedelta(seconds=age)).isoformat()),
            )
    result = _flow_context(store, "mint", now=now)
    assert result["swap_count"] == 3
    assert result["gross_flow_sol"] == 2.5
    assert abs(result["reference_price_change_fraction"] - 0.50) < 1e-9
    assert abs(result["absolute_price_change_per_gross_sol"] - 0.20) < 1e-9
    assert result["price_sensitivity_band"] == "extreme_ge_10pct_per_sol"
