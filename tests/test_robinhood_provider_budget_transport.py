from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path
from types import SimpleNamespace

from solana_roi import robinhood_provider_budget_transport as budget
from solana_roi import robinhood_production_provider_finalizer as finalizer


class _Store:
    def __init__(self) -> None:
        self.db = sqlite3.connect(":memory:")
        self.db.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        self.db.execute(
            "CREATE TABLE robinhood_launches ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, release_commit TEXT NOT NULL, protocol TEXT NOT NULL, "
            "venue TEXT NOT NULL, lifecycle TEXT NOT NULL, token TEXT NOT NULL, pool TEXT, curve TEXT, "
            "deployer TEXT, pair_token TEXT, fee INTEGER, launch_block INTEGER NOT NULL, "
            "restrictions_end_block INTEGER NOT NULL DEFAULT 0, graduation_threshold TEXT, paper_eligible INTEGER NOT NULL)"
        )
        self.db.execute(
            "CREATE TABLE robinhood_paper_trials (id INTEGER PRIMARY KEY, release_commit TEXT NOT NULL, market TEXT NOT NULL)"
        )
        self.db.execute(
            "CREATE TABLE robinhood_paper_outcomes (id INTEGER PRIMARY KEY, trial_id INTEGER NOT NULL)"
        )


def _plane() -> SimpleNamespace:
    return SimpleNamespace(
        store=_Store(),
        release_commit="test-release",
        v3_pools={},
        v2_curves={},
    )


def _insert_v3(plane: SimpleNamespace, *, index: int, launch_block: int) -> str:
    token = f"0x{index + 1000:040x}"
    pool = f"0x{index + 2000:040x}"
    plane.store.db.execute(
        "INSERT INTO robinhood_launches("
        "release_commit,protocol,venue,lifecycle,token,pool,curve,deployer,pair_token,fee,launch_block,"
        "restrictions_end_block,graduation_threshold,paper_eligible) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,1)",
        (
            plane.release_commit,
            "uniswap_v3",
            "UNISWAP_V3_DIRECT",
            "new_weth_pool",
            token,
            pool,
            None,
            f"0x{index + 3000:040x}",
            "0x0bd7d308f8e1639fab988df18a8011f41eacad73",
            10000,
            launch_block,
            0,
            None,
        ),
    )
    plane.store.db.commit()
    return pool


def test_candidate_universe_comes_from_durable_launches_not_live_cap() -> None:
    plane = _plane()
    addresses = {_insert_v3(plane, index=i, launch_block=100 + i) for i in range(20)}
    universe = budget._candidate_universe(plane)
    assert set(universe) == addresses
    assert len(universe) > budget.DEFAULT_LIVE_MARKET_CAP


def test_live_shortlist_is_bounded_but_open_position_is_forced_live() -> None:
    plane = _plane()
    markets = [_insert_v3(plane, index=i, launch_block=100 + i) for i in range(20)]
    now = time.monotonic()
    plane._roi_research_screen_events = {
        address: __import__("collections").deque(
            [{"at": now, "side": "buy", "quote_amount": 10**18 + i, "actor": f"0x{i + 5000:040x}"}],
            maxlen=256,
        )
        for i, address in enumerate(markets)
    }
    forced = markets[-1]
    plane.store.db.execute(
        "INSERT INTO robinhood_paper_trials(id,release_commit,market) VALUES (1,?,?)",
        (plane.release_commit, forced),
    )
    plane.store.db.commit()

    selected, reasons = budget._selected_market_targets(plane)
    assert forced in selected
    assert reasons[forced] == "open_position_forced_live"
    assert len(selected) <= budget.DEFAULT_LIVE_MARKET_CAP
    assert len(budget._candidate_universe(plane)) == 20


def test_public_research_promotion_has_no_paper_authority() -> None:
    status = budget.status()
    assert status["research_transport_authority"] == "promotion_only_no_paper_entry"
    assert status["promotion_to_alchemy_authority"] == "prospective_next_live_event_only"
    assert status["alchemy_gap_backfill_enabled"] is False
    assert status["candidate_discovery_constrained_by_live_cap"] is False


def test_provider_budget_keeps_v51_and_paper_only_boundaries() -> None:
    status = budget.status()
    assert status["canonical_latency_hard_max_seconds"] == 20.0
    assert status["legacy_two_block_gate_has_production_authority"] is False
    assert status["paper_only"] is True
    assert status["live_money_authority"] is False
    assert status["signing_available"] is False
    assert status["transaction_submission_available"] is False
    assert status["provider_budget_target_cu_per_minute"] == 600


def test_budget_source_does_not_send_transactions_or_subscribe_global_heads() -> None:
    source = Path(budget.__file__).read_text(encoding="utf-8")
    assert '"newHeads"' not in source
    assert "eth_sendRawTransaction" not in source
    assert "eth_sendTransaction" not in source
    assert "runtime.ROBINHOOD_PUBLIC_RPC" in source
    assert "promotion_only_no_paper_entry" in source


def test_finalizer_installs_budget_plane_before_bounded_and_production_transports() -> None:
    source = Path(finalizer.__file__).read_text(encoding="utf-8")
    budget_install = source.index("install_robinhood_provider_budget_transport()")
    bounded_install = source.index("install_robinhood_usage_bounded_transport()")
    production_install = source.index("production_transport.install_robinhood_production_ws_transport(plane_cls)")
    assert budget_install < bounded_install < production_install
