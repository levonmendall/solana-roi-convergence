from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from solana_roi.observation import WSOL_MINT
from solana_roi.observation_store import ObservationEventStore
from solana_roi.profit_first_entity_final import MarketRegime
from solana_roi.profit_first_entity_final_research import FinalProfitFirstResearchAdapter, USDC_MINT
from solana_roi.risk_conditioned_alpha_v5 import install_risk_conditioned_alpha_v5
from solana_roi.v52_authoritative_strategy import install_v52_authoritative_strategy
from solana_roi import risk_conditioned_alpha_v5 as v5
from solana_roi import v51_paper_lifecycle_runtime as lifecycle
from solana_roi.v51_atomic_paper_capital import capital_reconciliation
from solana_roi.strategy_v52_authority import STRATEGY_VERSION
from solana_roi.v52_cross_lane_paper_certification_repair import (
    configure_v52_cross_lane_paper_certification_repair,
)


class _Rpc:
    _roi_wallet_research_pool = True


class _Resolver:
    def component(self, wallet: str, *, as_of: datetime):
        _ = as_of
        return {wallet}


class _Discovery:
    def __init__(self, store: ObservationEventStore) -> None:
        self.store = store
        self.rpc = _Rpc()
        self.entity_resolver = _Resolver()


def _create_forward_table(store: ObservationEventStore) -> None:
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE wallet_discovery_forward_observations ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, signature TEXT NOT NULL UNIQUE, wallet TEXT NOT NULL, "
            "token_mint TEXT NOT NULL, side TEXT NOT NULL, token_amount REAL NOT NULL, observed_at TEXT NOT NULL, "
            "received_at TEXT NOT NULL, wallet_price_sol REAL NOT NULL, copyable_price_sol REAL, chase_fraction REAL, "
            "copyable INTEGER NOT NULL, observation_lag_ms REAL NOT NULL, risk_complete INTEGER NOT NULL, "
            "manipulation_flag INTEGER NOT NULL, side_wallet_flag INTEGER NOT NULL, source TEXT NOT NULL)"
        )


def _record_forward(store: ObservationEventStore, row: dict[str, object]) -> None:
    with store._lock, store.db:
        store.db.execute(
            "INSERT INTO wallet_discovery_forward_observations("
            "signature,wallet,token_mint,side,token_amount,observed_at,received_at,wallet_price_sol,"
            "copyable_price_sol,chase_fraction,copyable,observation_lag_ms,risk_complete,manipulation_flag,side_wallet_flag,source) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                row["signature"], row["wallet"], row["token_mint"], row["side"], row["token_amount"],
                row["observed_at"], row["received_at"], row["wallet_price_sol"], row["copyable_price_sol"],
                row["chase_fraction"], row["copyable"], row["observation_lag_ms"], row["risk_complete"],
                row["manipulation_flag"], row["side_wallet_flag"], row["source"],
            ),
        )


def _row(*, signature: str, token: str, venue: str, side: str, at: datetime) -> dict[str, object]:
    return {
        "signature": signature,
        "wallet": "wallet-alpha",
        "token_mint": token,
        "side": side,
        "token_amount": 25.0,
        "observed_at": at.isoformat(),
        "received_at": (at + timedelta(milliseconds=10)).isoformat(),
        "wallet_price_sol": 0.001,
        "copyable_price_sol": 0.001,
        "chase_fraction": 0.0,
        "copyable": 1,
        "observation_lag_ms": 10.0,
        "risk_complete": 1,
        "manipulation_flag": 0,
        "side_wallet_flag": 0,
        "source": f"solana-direct:{venue}:{side}",
    }


def _wrapper_lineage(fn) -> list[str]:
    names: list[str] = []
    seen: set[int] = set()
    current = fn
    while callable(current) and id(current) not in seen:
        seen.add(id(current))
        names.append(f"{getattr(current, '__module__', '?')}:{getattr(current, '__name__', '?')}")
        current = getattr(current, "__wrapped__", None)
    return names


def _build_adapter(monkeypatch, tmp_path, *, hard_flags=()):
    monkeypatch.setenv("RENDER_GIT_COMMIT", "v52-solana-connected-certification")
    install_risk_conditioned_alpha_v5()
    install_v52_authoritative_strategy()
    configure_v52_cross_lane_paper_certification_repair()
    assert v5._choose_lane_and_fraction.__module__.endswith("v52_authoritative_strategy")
    assert bool(getattr(v5._choose_lane_and_fraction, "_roi_v52_final_authority", False)) is True

    store = ObservationEventStore(tmp_path / "solana-connected.sqlite3")
    _create_forward_table(store)
    adapter = FinalProfitFirstResearchAdapter(_Discovery(store))  # type: ignore[arg-type]

    async def risk(_row, _at):
        return set(hard_flags), set(), 0.0

    async def decimals(_mint):
        return 6

    route_calls: list[tuple[str, str, int]] = []

    async def route(input_mint: str, output_mint: str, amount: int):
        route_calls.append((input_mint, output_mint, int(amount)))
        if input_mint == WSOL_MINT and output_mint == USDC_MINT:
            return {"out_amount": 100_000_000, "fee_lamports": 0}
        if input_mint == WSOL_MINT:
            return {"out_amount": int(amount), "fee_lamports": 5_000}
        if output_mint == WSOL_MINT:
            return {"out_amount": int(round(int(amount) * 1.05)) + 5_000, "fee_lamports": 5_000}
        return None

    adapter.execution._risk = risk  # type: ignore[method-assign]
    adapter.execution._deployer = lambda *_args: "creator-alpha"  # type: ignore[method-assign]
    adapter.execution._token_decimals = decimals  # type: ignore[method-assign]
    adapter.execution._route = route  # type: ignore[method-assign]
    adapter._confirmation_context = lambda *_args: ("entity:wallet-alpha", "entity:creator-alpha", 2)  # type: ignore[method-assign]
    adapter._creator_flow_state = lambda *_args: "neutral"  # type: ignore[method-assign]
    adapter._market_regime = lambda *_args: MarketRegime.NEUTRAL  # type: ignore[method-assign]
    return store, adapter, route_calls


@pytest.mark.parametrize(
    ("venue", "expected_lifecycle"),
    (
        ("PUMP_FUN", "pump_bonding_curve"),
        ("PUMP_AMM", "pump_amm_immediate_graduation_0_30s"),
    ),
)
def test_pump_surface_reaches_v52_shared_capital_exit_and_canonical_settlement(
    monkeypatch, tmp_path, venue: str, expected_lifecycle: str
) -> None:
    store, adapter, route_calls = _build_adapter(monkeypatch, tmp_path)
    token = f"token-{venue.lower()}"
    now = datetime.now(timezone.utc)
    buy = _row(signature=f"buy-{venue}", token=token, venue=venue, side="buy", at=now - timedelta(seconds=2))
    _record_forward(store, buy)

    asyncio.run(adapter.observe(str(buy["signature"])))

    with store._lock:
        trial = store.db.execute(
            "SELECT * FROM risk_conditioned_alpha_v5_trials WHERE release_commit=? AND source_signature=? "
            "AND selected=1 ORDER BY id DESC LIMIT 1",
            (adapter.release_commit, buy["signature"]),
        ).fetchone()
        fomo = store.db.execute(
            "SELECT decision,position_fraction FROM fomo_paper_trials "
            "WHERE release_commit=? AND source_signature=? LIMIT 1",
            (adapter.release_commit, buy["signature"]),
        ).fetchone()
    assert trial is not None
    trial = dict(trial)
    assert trial["strategy_version"] == STRATEGY_VERSION
    assert trial["venue"] == venue
    assert trial["lifecycle"] == expected_lifecycle
    assert str(trial["decision"]).startswith("paper_enter")
    assert trial["entry_executable"] == 1
    assert trial["exit_executable"] == 1
    assert trial["paper_only"] == 1
    assert trial["live_money_authority"] == 0
    fraction = float(trial["position_fraction"])
    assert fraction > 0.0

    if fomo is not None:
        assert fomo["decision"] == "no_entry_duplicate_authoritative_solana_opportunity"
        assert float(fomo["position_fraction"] or 0.0) == 0.0

    assert lifecycle.sync_entry_reservations(adapter, str(buy["signature"])) == 1
    reserved = capital_reconciliation(store, release_commit=adapter.release_commit)
    assert reserved["active_reserved_fraction"] == pytest.approx(fraction)
    assert reserved["available_fraction"] == pytest.approx(1.0 - fraction)

    sell = _row(signature=f"sell-{venue}", token=token, venue=venue, side="sell", at=now - timedelta(seconds=1))
    _record_forward(store, sell)
    asyncio.run(adapter.observe(str(sell["signature"])))

    with store._lock:
        outcome = store.db.execute(
            "SELECT * FROM risk_conditioned_alpha_v5_outcomes WHERE release_commit=? AND source_signature=? "
            "ORDER BY id DESC LIMIT 1",
            (adapter.release_commit, buy["signature"]),
        ).fetchone()
    assert outcome is not None
    outcome = dict(outcome)
    assert outcome["strategy_version"] == STRATEGY_VERSION
    assert outcome["venue"] == venue
    assert float(outcome["net_return"]) > 0.0
    assert outcome["paper_only"] == 1
    assert outcome["live_money_authority"] == 0

    # The lifecycle wrapper automatically reconciles settlement after observe().
    # A second explicit sync is therefore the idempotent replay proof, not the first settlement.
    settled = capital_reconciliation(store, release_commit=adapter.release_commit)
    assert settled["active_reserved_fraction"] == pytest.approx(0.0)
    assert settled["available_fraction"] == pytest.approx(1.0)
    assert settled["settlement_count"] == 1
    assert settled["realized_return_contribution"] == pytest.approx(fraction * float(outcome["net_return"]))
    assert settled["paper_nav_multiplier"] == pytest.approx(1.0 + fraction * float(outcome["net_return"]))
    assert lifecycle.sync_settlements(adapter) == 0

    assert sum(1 for input_mint, output_mint, _ in route_calls if input_mint == token and output_mint == WSOL_MINT) >= 3
    assert lifecycle.sync_entry_reservations(adapter, str(buy["signature"])) == 0
    assert lifecycle.sync_settlements(adapter) == 0
    assert capital_reconciliation(store, release_commit=adapter.release_commit)["settlement_count"] == 1
    store.close()


@pytest.mark.parametrize("venue", ("PUMP_FUN", "PUMP_AMM"))
def test_pump_surface_mechanical_hard_stop_rejects_before_capital(monkeypatch, tmp_path, venue: str) -> None:
    store, adapter, route_calls = _build_adapter(monkeypatch, tmp_path, hard_flags=("liquidity_unexitable",))
    token = f"blocked-{venue.lower()}"
    now = datetime.now(timezone.utc)
    buy = _row(signature=f"blocked-buy-{venue}", token=token, venue=venue, side="buy", at=now - timedelta(seconds=1))
    _record_forward(store, buy)

    asyncio.run(adapter.observe(str(buy["signature"])))

    with store._lock:
        v5_rows = store.db.execute(
            "SELECT lane,selected,decision,decision_reason,venue,lifecycle,risk_json FROM risk_conditioned_alpha_v5_trials "
            "WHERE release_commit=? AND source_signature=? ORDER BY id",
            (adapter.release_commit, buy["signature"]),
        ).fetchall()
        rejections = [row for row in v5_rows if str(row["decision"]) == "reject_mechanical_hard_stop"]
        entries = sum(1 for row in v5_rows if str(row["decision"]).startswith("paper_enter"))
        final_rows = store.db.execute(
            "SELECT lane,decision_json,entry_executable,exit_executable FROM profit_first_final_trials "
            "WHERE epoch_id=? AND source_signature=? ORDER BY id",
            (adapter.epoch_id, buy["signature"]),
        ).fetchall()
    diagnostic = {
        "v5_rows": [dict(row) for row in v5_rows],
        "profit_first_rows": [dict(row) for row in final_rows],
        "route_calls": route_calls,
        "buy_wrapper_lineage": _wrapper_lineage(FinalProfitFirstResearchAdapter._buy),
        "v5_choose_lineage": _wrapper_lineage(v5._choose_lane_and_fraction),
    }
    assert rejections, diagnostic
    assert all(str(row["venue"]) == venue for row in rejections), diagnostic
    assert int(entries) == 0, diagnostic
    assert lifecycle.sync_entry_reservations(adapter, str(buy["signature"])) == 0
    state = capital_reconciliation(store, release_commit=adapter.release_commit)
    assert state["active_reserved_fraction"] == 0.0
    assert state["settlement_count"] == 0
    assert route_calls == [], diagnostic
    store.close()
