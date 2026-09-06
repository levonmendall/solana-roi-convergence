from __future__ import annotations

import asyncio
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

from solana_roi import robinhood_chain_runtime as runtime
from solana_roi import robinhood_event_driven_settlement as settlement
from solana_roi import robinhood_production_provider_finalizer as finalizer


MARKET = "0x1111111111111111111111111111111111111111"


class _Store:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.db = sqlite3.connect(":memory:")
        self.db.row_factory = sqlite3.Row
        self.db.execute(
            "CREATE TABLE robinhood_paper_trials ("
            "id INTEGER PRIMARY KEY, release_commit TEXT NOT NULL, market TEXT NOT NULL, opened_at TEXT NOT NULL)"
        )
        self.db.execute(
            "CREATE TABLE robinhood_paper_outcomes (id INTEGER PRIMARY KEY, trial_id INTEGER NOT NULL)"
        )


def _plane(*, opened_seconds_ago: float) -> SimpleNamespace:
    store = _Store()
    opened = datetime.now(timezone.utc) - timedelta(seconds=opened_seconds_ago)
    store.db.execute(
        "INSERT INTO robinhood_paper_trials(id,release_commit,market,opened_at) VALUES (1,'release',?,?)",
        (MARKET, opened.isoformat()),
    )
    store.db.commit()
    calls: list[int] = []

    async def settle_one(trial: dict[str, object]) -> None:
        calls.append(int(trial["id"]))

    plane = SimpleNamespace(store=store, release_commit="release", _settle_one=settle_one)
    plane.calls = calls
    return plane


def test_idle_open_position_does_not_buy_repeated_exact_quotes() -> None:
    plane = _plane(opened_seconds_ago=5.0)
    asyncio.run(settlement._settle_due_positions(plane))
    asyncio.run(settlement._settle_due_positions(plane))
    assert plane.calls == []


def test_authoritative_market_event_triggers_one_exact_settlement_attempt() -> None:
    plane = _plane(opened_seconds_ago=5.0)
    settlement._dirty_markets(plane).add(MARKET)
    asyncio.run(settlement._settle_due_positions(plane))
    asyncio.run(settlement._settle_due_positions(plane))
    assert plane.calls == [1]


def test_max_hold_deadline_still_triggers_exact_settlement_without_swap() -> None:
    plane = _plane(opened_seconds_ago=runtime.MAX_HOLD_SECONDS + 1.0)
    asyncio.run(settlement._settle_due_positions(plane))
    asyncio.run(settlement._settle_due_positions(plane))
    assert plane.calls == [1]


def test_only_current_authoritative_market_events_mark_settlement_dirty() -> None:
    plane = SimpleNamespace()
    live_item = {
        "generation": 7,
        "received_monotonic": settlement.time.monotonic(),
        "live_authority": True,
        "log": {"address": MARKET, "topics": [runtime.V3_SWAP_TOPIC]},
    }
    research_item = {
        "generation": 7,
        "received_monotonic": settlement.time.monotonic(),
        "live_authority": False,
        "log": {"address": "0x2222222222222222222222222222222222222222", "topics": [runtime.V3_SWAP_TOPIC]},
    }
    settlement._mark_authoritative_market_events(plane, [live_item, research_item], generation=7)
    assert settlement._dirty_markets(plane) == {MARKET}


def test_settlement_patch_preserves_v51_and_paper_only_authority() -> None:
    status = settlement.status()
    assert status["settlement_trigger"] == "authoritative_live_market_event_or_max_hold_deadline"
    assert status["idle_exact_quote_polling"] is False
    assert status["entry_authority_changed"] is False
    assert status["exit_economics_changed"] is False
    assert status["paper_only"] is True
    assert status["live_money_authority"] is False
    assert status["signing_available"] is False
    assert status["transaction_submission_available"] is False
    assert runtime.MAX_HOLD_SECONDS == 20 * 60

    source = Path(settlement.__file__).read_text(encoding="utf-8")
    assert "eth_sendRawTransaction" not in source
    assert "eth_sendTransaction" not in source


def test_finalizer_installs_event_driven_settlement_after_provider_transport() -> None:
    source = Path(finalizer.__file__).read_text(encoding="utf-8")
    provider_install = source.index("production_transport.install_robinhood_production_ws_transport(plane_cls)")
    settlement_install = source.index("install_robinhood_event_driven_settlement(plane_cls)")
    enforcing_install = source.index("plane_cls.run = _enforcing_run(current_run)")
    assert provider_install < settlement_install < enforcing_install
