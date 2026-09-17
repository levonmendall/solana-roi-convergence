from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from solana_roi import risk_conditioned_alpha_v5 as v5
from solana_roi.observation import WSOL_MINT
from solana_roi.observation_store import ObservationEventStore
from solana_roi.profit_first_entity_final import MarketRegime
from solana_roi.profit_first_entity_final_research import FinalProfitFirstResearchAdapter, USDC_MINT
from solana_roi.risk_conditioned_alpha_v5 import install_risk_conditioned_alpha_v5
from solana_roi.strategy_v52_authority import STRATEGY_VERSION
from solana_roi.v51_atomic_paper_capital import capital_reconciliation
from solana_roi import v51_paper_lifecycle_runtime as lifecycle
from solana_roi.v52_authoritative_strategy import install_v52_authoritative_strategy
from solana_roi.v52_cross_lane_paper_certification_repair import (
    configure_v52_cross_lane_paper_certification_repair,
)
from solana_roi.v52_lane_contract import canonical_lane_for_surface, descriptor


ACCEPTANCE_CONTRACT = "continuation-v1"


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


def _row(
    *,
    signature: str,
    token: str,
    venue: str,
    side: str,
    at: datetime,
    wallet: str = "wallet-alpha",
) -> dict[str, object]:
    return {
        "signature": signature,
        "wallet": wallet,
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


def _build_adapter(monkeypatch, tmp_path):
    monkeypatch.setenv("RENDER_GIT_COMMIT", "continuation-v1-natural-pump-pumpswap")
    install_risk_conditioned_alpha_v5()
    install_v52_authoritative_strategy()
    configure_v52_cross_lane_paper_certification_repair()

    store = ObservationEventStore(tmp_path / "continuation-v1-natural-pump-pumpswap.sqlite3")
    _create_forward_table(store)
    adapter = FinalProfitFirstResearchAdapter(_Discovery(store))  # type: ignore[arg-type]

    async def risk(_row, _at):
        return set(), set(), 0.0

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


def test_continuation_v1_pump_position_naturally_graduates_to_pumpswap_and_settles(
    monkeypatch, tmp_path
) -> None:
    """No graduated flag is injected: the first real-shaped PUMP_AMM event is the graduation evidence."""
    assert ACCEPTANCE_CONTRACT == "continuation-v1"
    assert STRATEGY_VERSION == "roi-convergence-v5.2-continuation-capture-1"
    assert canonical_lane_for_surface("PUMPSWAP") == "pump_amm"
    pump_swap = descriptor("pump_amm")
    assert pump_swap.venue == "PUMP_AMM"
    assert pump_swap.lifecycle == "early_post_graduation"
    assert "PUMPSWAP" in pump_swap.surface_aliases

    store, adapter, route_calls = _build_adapter(monkeypatch, tmp_path)
    token = "continuation-v1-natural-graduation-token"
    now = datetime.now(timezone.utc)

    entry = _row(
        signature="continuation-v1-entry",
        token=token,
        venue="PUMP_FUN",
        side="buy",
        at=now - timedelta(seconds=3),
    )
    _record_forward(store, entry)
    asyncio.run(adapter.observe(str(entry["signature"])))

    with store._lock:
        trial_row = store.db.execute(
            "SELECT * FROM risk_conditioned_alpha_v5_trials "
            "WHERE release_commit=? AND source_signature=? AND selected=1 "
            "ORDER BY id DESC LIMIT 1",
            (adapter.release_commit, entry["signature"]),
        ).fetchone()
    assert trial_row is not None
    trial = dict(trial_row)
    assert trial["strategy_version"] == STRATEGY_VERSION
    assert trial["venue"] == "PUMP_FUN"
    assert trial["lifecycle"] == "pump_bonding_curve"
    assert str(trial["decision"]).startswith("paper_enter")
    assert trial["paper_only"] == 1 and trial["live_money_authority"] == 0
    entry_lane = str(trial["lane"])
    entry_fraction = float(trial["position_fraction"])
    assert entry_fraction > 0.0

    assert lifecycle.sync_entry_reservations(adapter, str(entry["signature"])) == 1
    opened = capital_reconciliation(store, release_commit=adapter.release_commit)
    assert opened["active_reserved_fraction"] == pytest.approx(entry_fraction)
    assert opened["settlement_count"] == 0

    # The position begins on the Pump bonding curve. Its first PumpSwap/PUMP_AMM
    # market observation is a normal source row, not a synthetic `graduated=True`
    # control. The canonical lifecycle classifier must infer immediate graduation.
    graduation_monitor = _row(
        signature="continuation-v1-pumpswap-monitor",
        token=token,
        venue="PUMP_AMM",
        side="sell",
        wallet="unrelated-holder",
        at=now - timedelta(seconds=2),
    )
    assert not any("graduat" in str(key).lower() for key in graduation_monitor)
    _record_forward(store, graduation_monitor)
    market_state = v5._v5_pre_context(
        adapter,
        graduation_monitor,
        hard=(),
        soft=(),
        early_exit=0.0,
    )
    assert market_state["venue"] == "PUMP_AMM"
    assert market_state["lifecycle"] == "pump_amm_immediate_graduation_0_30s"

    asyncio.run(adapter.observe(str(graduation_monitor["signature"])))
    with store._lock:
        premature = store.db.execute(
            "SELECT 1 FROM risk_conditioned_alpha_v5_outcomes "
            "WHERE release_commit=? AND source_signature=? LIMIT 1",
            (adapter.release_commit, entry["signature"]),
        ).fetchone()
    assert premature is None
    monitoring = capital_reconciliation(store, release_commit=adapter.release_commit)
    assert monitoring["active_reserved_fraction"] == pytest.approx(entry_fraction)
    assert monitoring["settlement_count"] == 0

    pumpswap_exit = _row(
        signature="continuation-v1-pumpswap-exit",
        token=token,
        venue="PUMP_AMM",
        side="sell",
        wallet="wallet-alpha",
        at=now - timedelta(seconds=1),
    )
    _record_forward(store, pumpswap_exit)
    asyncio.run(adapter.observe(str(pumpswap_exit["signature"])))

    with store._lock:
        outcome_row = store.db.execute(
            "SELECT * FROM risk_conditioned_alpha_v5_outcomes "
            "WHERE release_commit=? AND source_signature=? ORDER BY id DESC LIMIT 1",
            (adapter.release_commit, entry["signature"]),
        ).fetchone()
        exit_source = store.db.execute(
            "SELECT source FROM wallet_discovery_forward_observations WHERE signature=?",
            (pumpswap_exit["signature"],),
        ).fetchone()
        selected_entries = int(store.db.execute(
            "SELECT COUNT(*) FROM risk_conditioned_alpha_v5_trials "
            "WHERE release_commit=? AND token_mint=? AND selected=1 AND decision LIKE 'paper_enter%'",
            (adapter.release_commit, token),
        ).fetchone()[0])
        reservations = store.db.execute(
            "SELECT reservation_id,candidate_id,status,reserved_fraction FROM v51_paper_capital_reservations "
            "WHERE candidate_id=? ORDER BY id",
            (entry["signature"],),
        ).fetchall()
        settlements = store.db.execute(
            "SELECT settlement_id,reservation_id,net_return,realized_contribution FROM v51_paper_capital_settlements "
            "ORDER BY id",
        ).fetchall()

    assert outcome_row is not None
    outcome = dict(outcome_row)
    assert outcome["strategy_version"] == STRATEGY_VERSION
    assert outcome["source_signature"] == entry["signature"]
    assert outcome["exit_signature"] == pumpswap_exit["signature"]
    assert outcome["lane"] == entry_lane
    assert float(outcome["net_return"]) > 0.0
    assert exit_source is not None and str(exit_source["source"]) == "solana-direct:PUMP_AMM:sell"

    # Graduation did not manufacture a second entry or reservation. The one Pump
    # position identity is the one settled after the PumpSwap exit.
    assert selected_entries == 1
    assert len(reservations) == 1
    assert len(settlements) == 1
    assert reservations[0]["reservation_id"] == settlements[0]["reservation_id"]
    assert reservations[0]["candidate_id"] == entry["signature"]
    assert reservations[0]["status"] == "settled"

    settled = capital_reconciliation(store, release_commit=adapter.release_commit)
    assert settled["active_reserved_fraction"] == pytest.approx(0.0)
    assert settled["available_fraction"] == pytest.approx(1.0)
    assert settled["settlement_count"] == 1
    assert settled["realized_return_contribution"] == pytest.approx(entry_fraction * float(outcome["net_return"]))
    assert lifecycle.sync_entry_reservations(adapter, str(entry["signature"])) == 0
    assert lifecycle.sync_settlements(adapter) == 0
    assert capital_reconciliation(store, release_commit=adapter.release_commit)["settlement_count"] == 1

    assert sum(
        1 for input_mint, output_mint, _ in route_calls
        if input_mint == token and output_mint == WSOL_MINT
    ) >= 3
    store.close()
