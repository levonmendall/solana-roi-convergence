from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from solana_roi.observation_store import ObservationEventStore
from solana_roi.profit_first_entity_research import ProfitFirstResearchAdapter
from solana_roi.profit_first_entity_strategy import STRATEGY_VERSION


class _Rpc:
    _roi_wallet_research_pool = True


class _EntityResolver:
    def component(self, wallet: str, *, as_of: datetime) -> set[str]:
        return {wallet}

    def entity_id_for(self, wallet: str, *, fallback_entity_id: str | None, as_of: datetime) -> str:
        return "graph:" + wallet


class _Risk:
    async def snapshot(self, token_mint: str, observed_at: datetime, **_: object):
        return SimpleNamespace(
            unacceptable_liquidity=False,
            bundled_launch=False,
            sniper_heavy=False,
            abnormal_sell_pressure=False,
            common_funded_early_wallet_cluster=False,
            scout_deployer_connection=False,
            early_buyers_exiting=False,
        )


class _Discovery:
    def __init__(self, store: ObservationEventStore):
        self.store = store
        self.rpc = _Rpc()
        self.entity_resolver = _EntityResolver()
        self.risk = _Risk()

    async def _risk_flags(self, swap):
        return True, False, False


def _create_forward_table(store: ObservationEventStore) -> None:
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE wallet_discovery_forward_observations ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, signature TEXT NOT NULL UNIQUE, wallet TEXT NOT NULL, "
            "token_mint TEXT NOT NULL, side TEXT NOT NULL, token_amount REAL NOT NULL, observed_at TEXT NOT NULL, "
            "received_at TEXT NOT NULL, wallet_price_sol REAL NOT NULL, copyable_price_sol REAL, "
            "chase_fraction REAL, copyable INTEGER NOT NULL, observation_lag_ms REAL NOT NULL, "
            "risk_complete INTEGER NOT NULL, manipulation_flag INTEGER NOT NULL, side_wallet_flag INTEGER NOT NULL, "
            "source TEXT NOT NULL)"
        )


def _insert_forward(
    store: ObservationEventStore,
    *,
    signature: str,
    side: str,
    observed_at: datetime,
    received_at: datetime,
) -> None:
    with store._lock, store.db:
        store.db.execute(
            "INSERT INTO wallet_discovery_forward_observations("
            "signature, wallet, token_mint, side, token_amount, observed_at, received_at, wallet_price_sol, "
            "copyable_price_sol, chase_fraction, copyable, observation_lag_ms, risk_complete, manipulation_flag, "
            "side_wallet_flag, source) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                signature,
                "wallet-a",
                "mint-a",
                side,
                100.0,
                observed_at.isoformat(),
                received_at.isoformat(),
                0.01,
                0.0105,
                0.05,
                1,
                250.0,
                1,
                0,
                0,
                "test",
            ),
        )


@pytest.mark.asyncio
async def test_adapter_persists_release_bound_shadow_trials_and_exact_exit_outcomes(monkeypatch, tmp_path):
    monkeypatch.setenv("RENDER_GIT_COMMIT", "release-abc")
    store = ObservationEventStore(tmp_path / "research.sqlite3")
    _create_forward_table(store)
    discovery = _Discovery(store)
    adapter = ProfitFirstResearchAdapter(discovery)  # type: ignore[arg-type]

    now = datetime.now(timezone.utc)
    _insert_forward(store, signature="buy-sig", side="buy", observed_at=now, received_at=now)

    async def entry_execution(_row):
        return {
            "input_lamports": 1_000_000_000,
            "entry_fee_lamports": 10_000,
            "entry_cost_sol": 1.00001,
            "token_raw": 100_000_000,
            "decimals": 6,
            "entry_price": 0.0100001,
            "chase": 0.00001,
            "exit_net_sol": 0.99,
            "round_trip": 0.0100099,
        }

    adapter._entry = entry_execution  # type: ignore[method-assign]
    await adapter.observe("buy-sig")

    status = adapter.status()
    assert status["strategy_version"] == STRATEGY_VERSION
    assert status["release_commit"] == "release-abc"
    assert status["release_bound"] is True
    assert status["paper_only"] is True
    assert status["live_money_authority"] is False
    assert status["signing_available"] is False
    assert status["transaction_submission_available"] is False
    assert status["active_v3_1_cohort_mutation_allowed"] is False
    assert status["shadow_trials"] == 2
    assert status["fully_executable_shadow_trials"] == 2

    sell_at = now + timedelta(seconds=10)
    _insert_forward(store, signature="sell-sig", side="sell", observed_at=sell_at, received_at=sell_at)

    async def sell_route(*_args):
        return {
            "out_amount": 1_100_000_000,
            "fee_lamports": 10_000,
        }

    adapter._route = sell_route  # type: ignore[method-assign]
    await adapter.observe("sell-sig")

    status = adapter.status()
    assert status["forward_outcomes"] == 2
    assert status["current_release_forward_outcomes"] == 2
    with store._lock:
        rows = store.db.execute(
            "SELECT release_commit, entry_signature, exit_signature, net_return, exit_reason "
            "FROM profit_first_entity_forward_outcomes ORDER BY lane"
        ).fetchall()
    assert {row["release_commit"] for row in rows} == {"release-abc"}
    assert {row["entry_signature"] for row in rows} == {"buy-sig"}
    assert {row["exit_signature"] for row in rows} == {"sell-sig"}
    assert all(float(row["net_return"]) > 0.09 for row in rows)
    assert all("exact_shadow_exit" in str(row["exit_reason"]) for row in rows)


def test_adapter_has_no_signing_or_submission_surface(tmp_path, monkeypatch):
    monkeypatch.setenv("RENDER_GIT_COMMIT", "release-safe")
    store = ObservationEventStore(tmp_path / "research.sqlite3")
    _create_forward_table(store)
    adapter = ProfitFirstResearchAdapter(_Discovery(store))  # type: ignore[arg-type]
    names = {name.lower() for name in dir(adapter)}
    assert "sign" not in names
    assert "submit" not in names
    assert "send_transaction" not in names
    status = adapter.status()
    assert status["critical_continuity_path_modified"] is False
    assert status["continuity_lease_seconds_unchanged"] == 12.0
    assert status["recovery_bound_unchanged"] == "3x1000"
    assert status["rpc_workload_class"] == "research"
