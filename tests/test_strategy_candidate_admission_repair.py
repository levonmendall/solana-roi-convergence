from __future__ import annotations

import asyncio
import sqlite3
import threading
from datetime import datetime, timedelta, timezone

from solana_roi.profit_first_entity_final import MarketRegime
from solana_roi import strategy_candidate_admission_repair as repair


def _row(**overrides):
    base = {
        "signature": "sig-1",
        "token_mint": "mint-1",
        "wallet": "wallet-1",
        "side": "buy",
        "wallet_price_sol": 0.001,
        "observation_lag_ms": 3_000.0,
        "copyable": 0,
    }
    base.update(overrides)
    return base


def test_strategy_evaluation_eligible_ignores_legacy_copyable_bit() -> None:
    assert repair.strategy_evaluation_eligible(_row(copyable=0)) is True
    assert repair.strategy_evaluation_eligible(_row(copyable=1)) is True
    assert repair.strategy_evaluation_eligible(_row(observation_lag_ms=20_001.0)) is False
    assert repair.strategy_evaluation_eligible(_row(wallet_price_sol=0.0)) is False
    assert repair.strategy_evaluation_eligible(_row(side="sell")) is False


def test_noncopyable_timely_buy_reaches_active_v51_path(monkeypatch) -> None:
    seen = []

    async def original(_self, row):
        seen.append(dict(row))

    monkeypatch.setattr(repair, "_ORIGINAL_BUY", original)
    fake = type("Fake", (), {})()
    asyncio.run(repair._buy_with_modern_admission(fake, _row(copyable=0)))

    assert len(seen) == 1
    assert seen[0]["copyable"] == 1
    assert fake._roi_strategy_admission_bypasses == 1


def test_stale_noncopyable_buy_does_not_bypass_entry_window(monkeypatch) -> None:
    seen = []

    async def original(_self, row):
        seen.append(dict(row))

    monkeypatch.setattr(repair, "_ORIGINAL_BUY", original)
    fake = type("Fake", (), {})()
    asyncio.run(
        repair._buy_with_modern_admission(
            fake,
            _row(copyable=0, observation_lag_ms=repair.ENTRY_WINDOW_SECONDS * 1000.0 + 1.0),
        )
    )

    assert len(seen) == 1
    assert seen[0]["copyable"] == 0
    assert fake._roi_strategy_admission_legacy_rejections == 1


class _Store:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.db = sqlite3.connect(":memory:")
        self.db.row_factory = sqlite3.Row
        self.db.execute(
            "CREATE TABLE wallet_discovery_forward_observations ("
            "side TEXT NOT NULL, observation_lag_ms REAL NOT NULL, wallet_price_sol REAL NOT NULL, "
            "received_at TEXT NOT NULL)"
        )


class _Adapter:
    def __init__(self) -> None:
        self.store = _Store()


def test_neutral_regime_uses_timely_strategy_evaluable_buys_not_legacy_copyable(monkeypatch) -> None:
    adapter = _Adapter()
    at = datetime.now(timezone.utc)
    rows = [
        ("buy", 2_000.0, 0.001, (at - timedelta(seconds=2)).isoformat()),
        ("buy", 3_000.0, 0.0012, (at - timedelta(seconds=3)).isoformat()),
        ("sell", 2_500.0, 0.0011, (at - timedelta(seconds=4)).isoformat()),
    ]
    adapter.store.db.executemany(
        "INSERT INTO wallet_discovery_forward_observations(side,observation_lag_ms,wallet_price_sol,received_at) "
        "VALUES (?,?,?,?)",
        rows,
    )
    monkeypatch.setattr(repair, "_ORIGINAL_MARKET_REGIME", lambda _self, _at: MarketRegime.NEUTRAL)

    assert repair._market_regime_with_modern_admission(adapter, at) is MarketRegime.HIGH_SPECULATION
    assert adapter._roi_strategy_admission_high_speculation_repairs == 1


def test_existing_weak_or_mania_regime_precedence_is_unchanged(monkeypatch) -> None:
    adapter = _Adapter()
    at = datetime.now(timezone.utc)
    monkeypatch.setattr(repair, "_ORIGINAL_MARKET_REGIME", lambda _self, _at: MarketRegime.WEAK)
    assert repair._market_regime_with_modern_admission(adapter, at) is MarketRegime.WEAK
    monkeypatch.setattr(repair, "_ORIGINAL_MARKET_REGIME", lambda _self, _at: MarketRegime.BROAD_MANIA)
    assert repair._market_regime_with_modern_admission(adapter, at) is MarketRegime.BROAD_MANIA
