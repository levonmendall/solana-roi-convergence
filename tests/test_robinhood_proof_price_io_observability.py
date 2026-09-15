from __future__ import annotations

import sqlite3
import threading
from pathlib import Path
from typing import Any

from solana_roi import robinhood_proof_price_io_observability as observed
from solana_roi import v51_counterfactual_extension as counterfactual


class _Store:
    def __init__(self, path: Path) -> None:
        self.path = str(path)
        self._lock = threading.RLock()
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        with self.db:
            self.db.execute(
                "CREATE TABLE robinhood_swaps ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, release_commit TEXT NOT NULL, venue TEXT NOT NULL, "
                "lifecycle TEXT NOT NULL, token TEXT NOT NULL, market TEXT NOT NULL, tx_hash TEXT NOT NULL, "
                "log_index INTEGER NOT NULL, block_number INTEGER NOT NULL, actor TEXT, actor_source TEXT NOT NULL, "
                "side TEXT NOT NULL, quote_amount_wei TEXT NOT NULL, token_amount_raw TEXT NOT NULL, "
                "price_eth REAL, fee_or_tax_wei TEXT, observed_at TEXT NOT NULL, "
                "UNIQUE(release_commit, tx_hash, log_index))"
            )
            self.db.execute(
                "CREATE INDEX ix_robinhood_swaps_market_time "
                "ON robinhood_swaps(release_commit, market, id)"
            )

    def close(self) -> None:
        self.db.close()


def _insert(store: _Store, *, observed_at: str, tx_hash: str, price: float) -> None:
    with store.db:
        store.db.execute(
            "INSERT INTO robinhood_swaps("
            "release_commit,venue,lifecycle,token,market,tx_hash,log_index,block_number,actor,actor_source,side,"
            "quote_amount_wei,token_amount_raw,price_eth,fee_or_tax_wei,observed_at) "
            "VALUES ('r','V','L','t','m',?,0,1,NULL,'x','buy','1','1',?,NULL,?)",
            (tx_hash, price, observed_at),
        )


def test_current_price_lookup_plan_exposes_timestamp_temp_sort(tmp_path: Path) -> None:
    store = _Store(tmp_path / "proof.sqlite3")
    try:
        entry_plan = observed._query_plan(store, bounded_after=False)
        exit_plan = observed._query_plan(store, bounded_after=True)
        joined = " | ".join(entry_plan + exit_plan).upper()
        assert "IX_ROBINHOOD_SWAPS_MARKET_TIME" in joined
        assert "TEMP B-TREE" in joined
    finally:
        store.close()


def test_observed_price_lookup_preserves_exact_row_selection(tmp_path: Path) -> None:
    store = _Store(tmp_path / "proof.sqlite3")
    try:
        _insert(store, observed_at="2026-09-15T00:00:00+00:00", tx_hash="a", price=1.0)
        _insert(store, observed_at="2026-09-15T00:01:00+00:00", tx_hash="b", price=2.0)
        _insert(store, observed_at="2026-09-15T00:02:00+00:00", tx_hash="c", price=3.0)

        observed.install_robinhood_proof_price_io_observability()
        original = observed._ORIGINAL_PRICE_ROW
        assert original is not None

        kwargs: dict[str, Any] = {
            "release_commit": "r",
            "market": "m",
            "before_or_at": "2026-09-15T00:01:30+00:00",
            "after": "2026-09-15T00:00:00+00:00",
        }
        expected = original(store, **kwargs)
        actual = counterfactual._price_row(store, **kwargs)
        assert actual == expected
        assert actual is not None
        assert actual["tx_hash"] == "b"
        assert float(actual["price_eth"]) == 2.0
    finally:
        store.close()
