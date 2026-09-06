from __future__ import annotations

import asyncio
import sqlite3
import threading
from datetime import datetime, timedelta, timezone

from solana_roi import robinhood_decision_tail_repair as tail
from solana_roi import v51_evidence_analytics as analytics
from solana_roi.robinhood_chain_core import LIVE_LAG_BLOCKS, MAX_HOLD_SECONDS
from solana_roi.v51_counterfactual_extension import refresh_all_rejected_counterfactuals
from solana_roi.v51_empty_epoch_slo_repair import install_empty_epoch_slo_repair


class _Store:
    def __init__(self) -> None:
        self.db = sqlite3.connect(":memory:", check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self._lock = threading.RLock()


class _Plane:
    def __init__(self, store: _Store) -> None:
        self.store = store
        self._roi_forward_only_chain_id_verified = True
        self._roi_live_epoch_cursor = 100
        self._roi_live_epoch_ready = False
        self._roi_live_epoch_factory_verified_through = 100
        self._roi_post177_head_observed_block = 166
        self._latest_block = 166
        self.v3_pools = {}
        self.v2_curves = {}



def _iso(seconds_ago: float) -> str:
    return (datetime.now(timezone.utc) - timedelta(seconds=seconds_ago)).isoformat()


def _robinhood_schema(store: _Store) -> None:
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE v51_robinhood_candidate_ledger("
            "candidate_id TEXT PRIMARY KEY,release_commit TEXT,token TEXT,market TEXT,venue TEXT,lifecycle TEXT,"
            "selected_lane TEXT,position_fraction REAL,decision TEXT,decision_reason TEXT,observed_at TEXT,updated_at TEXT)"
        )
        store.db.execute(
            "CREATE TABLE robinhood_swaps("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,release_commit TEXT NOT NULL,market TEXT NOT NULL,tx_hash TEXT,"
            "log_index INTEGER,price_eth REAL,observed_at TEXT NOT NULL)"
        )
        store.db.execute(
            "CREATE INDEX ix_test_rh_swaps ON robinhood_swaps(release_commit,market,id)"
        )


def test_empty_canonical_epoch_is_confirmed_zero_flow_not_measurement_unavailable() -> None:
    store = _Store()
    install_empty_epoch_slo_repair()
    proof = analytics.build_forward_proof_slo(store)
    assert proof["proof_state"] == "confirmed"
    assert proof["stage_events_last_60m"] == 0
    assert proof["coverage_debt_count"] == 0
    assert proof["paper_only"] is True
    assert proof["live_money_authority"] is False


def test_robinhood_tail_keeps_metadata_64_but_trade_decisions_at_existing_2_block_frontier(monkeypatch) -> None:
    store = _Store()
    plane = _Plane(store)
    calls: dict[str, tuple[int, int]] = {}

    async def current_head(_self):
        return 166

    async def sync_factory(_self, *, from_block: int, to_block: int):
        calls["metadata"] = (from_block, to_block)
        return 0

    async def market_logs(_self, *, from_block: int, to_block: int):
        calls["trades"] = (from_block, to_block)
        return []

    async def fresh(_self):
        # Regression for the prior cold-start self-lock: the just-completed cursor
        # must be provisionally ready before the unchanged fresh-head guard runs.
        assert _self._roi_live_epoch_ready is True
        return True

    monkeypatch.setattr(tail.post177, "_current_observed_head", current_head)
    monkeypatch.setattr(tail.post177, "_observer_head_fresh", lambda _self: True)
    monkeypatch.setattr(tail.post177, "_schedule_rwa_refresh", lambda _self: None)
    monkeypatch.setattr(tail.post177, "_clear_pending_markets", lambda _self: None)
    monkeypatch.setattr(tail.frontier, "_sync_factory_state", sync_factory)
    monkeypatch.setattr(tail.frontier, "_fetch_market_logs", market_logs)
    monkeypatch.setattr(tail.frontier, "_fresh_head_ready", fresh)
    monkeypatch.setattr(tail, "refresh_all_rejected_counterfactuals", lambda _store: {})

    asyncio.run(tail._advance_current_decision_tail(plane))

    assert tail.DECISION_WINDOW_BLOCKS == LIVE_LAG_BLOCKS == 2
    assert calls["metadata"] == (103, 166)
    assert calls["trades"] == (165, 166)
    assert plane._roi_live_epoch_cursor == 166
    assert plane._roi_live_epoch_ready is True
    assert plane._roi_live_epoch_last_range["stale_trade_blocks_have_retrospective_entry_authority"] is False


def test_mature_rejected_robinhood_candidate_resolves_from_forward_observed_market_price() -> None:
    store = _Store()
    _robinhood_schema(store)
    decision = datetime.now(timezone.utc) - timedelta(seconds=MAX_HOLD_SECONDS + 300)
    target = decision + timedelta(seconds=MAX_HOLD_SECONDS)
    with store._lock, store.db:
        store.db.execute(
            "INSERT INTO v51_robinhood_candidate_ledger VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "rh:test:1", "release-a", "0xtoken", "0xmarket", "UNISWAP_V3_DIRECT", "continuation",
                None, 0.0, "paper_reject", "evidence_failed_closed", decision.isoformat(), decision.isoformat(),
            ),
        )
        store.db.execute(
            "INSERT INTO robinhood_swaps(release_commit,market,tx_hash,log_index,price_eth,observed_at) VALUES (?,?,?,?,?,?)",
            ("release-a", "0xmarket", "0xentry", 1, 1.0, decision.isoformat()),
        )
        store.db.execute(
            "INSERT INTO robinhood_swaps(release_commit,market,tx_hash,log_index,price_eth,observed_at) VALUES (?,?,?,?,?,?)",
            ("release-a", "0xmarket", "0xexit", 2, 1.5, (target - timedelta(seconds=30)).isoformat()),
        )

    result = refresh_all_rejected_counterfactuals(store)
    assert result["rejected_candidate_count"] == 1
    assert result["resolved_count"] == 1
    assert result["pending_count"] == 0
    assert result["resolved_positive_count"] == 1
    assert result["resolved_gross_market_return_count"] == 1
    assert result["gross_market_return_has_promotion_authority"] is False
    assert result["retrospective_entry_authority"] is False
    with store._lock:
        row = store.db.execute(
            "SELECT forward_net_return,forward_gross_return,counterfactual_state,resolution_semantics,"
            "retrospective_entry_authority FROM v51_rejected_counterfactuals"
        ).fetchone()
    assert row["forward_net_return"] is None
    assert abs(float(row["forward_gross_return"]) - 0.5) < 1e-12
    assert row["counterfactual_state"] == "resolved_forward_market_return"
    assert "not_executable_trade_pnl" in str(row["resolution_semantics"])
    assert row["retrospective_entry_authority"] == 0


def test_no_fresh_max_hold_price_is_resolved_as_unobservable_not_fake_loss() -> None:
    store = _Store()
    _robinhood_schema(store)
    decision = datetime.now(timezone.utc) - timedelta(seconds=MAX_HOLD_SECONDS + 300)
    with store._lock, store.db:
        store.db.execute(
            "INSERT INTO v51_robinhood_candidate_ledger VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "rh:test:2", "release-b", "0xtoken", "0xmarket2", "PONS_V2_CURVE", "continuation",
                None, 0.0, "paper_reject", "risk_failed_closed", decision.isoformat(), decision.isoformat(),
            ),
        )
        store.db.execute(
            "INSERT INTO robinhood_swaps(release_commit,market,tx_hash,log_index,price_eth,observed_at) VALUES (?,?,?,?,?,?)",
            ("release-b", "0xmarket2", "0xentry", 1, 2.0, decision.isoformat()),
        )

    result = refresh_all_rejected_counterfactuals(store)
    assert result["resolved_count"] == 1
    assert result["resolved_positive_count"] == 0
    assert result["resolved_no_forward_market_observation_count"] == 1
    with store._lock:
        row = store.db.execute(
            "SELECT forward_net_return,forward_gross_return,counterfactual_state FROM v51_rejected_counterfactuals"
        ).fetchone()
    assert row["forward_net_return"] is None
    assert row["forward_gross_return"] is None
    assert row["counterfactual_state"] == "resolved_no_forward_market_observation"
