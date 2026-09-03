from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from solana_roi.activation import CandidateActivationGate
from solana_roi.all_in_execution_cost_gate import install_all_in_execution_cost_gate
from solana_roi.config import BASELINE
from solana_roi.ingestion import WalletProfile
from solana_roi.models import RiskSnapshot, WalletTier
from solana_roi.observation_store import ObservationEventStore
from solana_roi.quote import ExecutableQuote
from solana_roi.risk import RiskDimension
from solana_roi.shadow_execution import ShadowExecutionObservation, ShadowWalletExecutableQuoteHandoff


T0 = datetime(2026, 9, 3, 1, 0, tzinfo=timezone.utc)
SCOUT_PRICE = 0.001
REQUESTED_NOTIONAL = 3.75
INPUT_SOL = REQUESTED_NOTIONAL / 200.0
ROUTE_PRICE = 0.0011
OUTPUT_UNITS = INPUT_SOL / ROUTE_PRICE


install_all_in_execution_cost_gate()


class FakeQuoteClient:
    async def quote_buy(self, **kwargs):
        return ExecutableQuote(
            token_mint="mint",
            stage="starter",
            requested_notional_usd=REQUESTED_NOTIONAL,
            input_sol=INPUT_SOL,
            sol_usd=200.0,
            output_token_units=INPUT_SOL / SCOUT_PRICE,
            effective_price_sol=SCOUT_PRICE,
            scout_reference_price_sol=SCOUT_PRICE,
            drift_fraction=0.0,
            router="quote",
            fee_bps=0,
            token_decimals=6,
            quoted_at=T0 + timedelta(milliseconds=10),
            received_at=T0 + timedelta(milliseconds=20),
            quote_latency_ms=10.0,
            chain_to_quote_ms=20.0,
            usable=True,
            reason="within chase ceiling",
        )


class FakeSimulator:
    def __init__(self, *, network_fee_lamports: int | None):
        self.network_fee_lamports = network_fee_lamports

    async def observe(self, quote: ExecutableQuote) -> ShadowExecutionObservation:
        if self.network_fee_lamports is None:
            signature_fee = None
            priority_fee = None
            rent_fee = None
        else:
            signature_fee = 5_000
            priority_fee = 0
            rent_fee = max(0, self.network_fee_lamports - signature_fee)
        return ShadowExecutionObservation(
            token_mint=quote.token_mint,
            stage=quote.stage,
            shadow_wallet="11111111111111111111111111111111",
            observed_at=T0 + timedelta(milliseconds=30),
            completed_at=T0 + timedelta(milliseconds=40),
            input_lamports=int(round(INPUT_SOL * 1_000_000_000)),
            transaction_built=True,
            transaction_sha256="abc",
            transaction_size_bytes=800,
            last_valid_block_height=123,
            router="iris-taker",
            order_out_token_units=OUTPUT_UNITS,
            order_effective_price_sol=ROUTE_PRICE,
            order_drift_fraction=0.10,
            signature_fee_lamports=signature_fee,
            prioritization_fee_lamports=priority_fee,
            rent_fee_lamports=rent_fee,
            simulation_ok=True,
            units_consumed=100_000,
            simulation_slot=999,
            logs_count=1,
            total_latency_ms=20.0,
            error=None,
        )


class FakePortfolio:
    def __init__(self, cash_usd: float = 500.0):
        self.cash_usd = cash_usd

    def full_position_notional(self, marks=None):
        return 12.5


class FakeEngine:
    def __init__(self, cash_usd: float = 500.0):
        self.config = BASELINE
        self.portfolio = FakePortfolio(cash_usd)
        self.marks = {}


class FakeQuoteGate:
    policy = SimpleNamespace(
        max_p95_quote_latency_ms=2_000.0,
        max_p99_chain_to_quote_ms=10_000.0,
    )


class FakeController:
    quote_gate = FakeQuoteGate()

    def is_armed(self):
        return True

    def runtime_continuity_ok(self):
        return True

    def status(self):
        return {
            "coverage": {"certified": True},
            "latency": {"certified": True},
            "execution_quotes": {"certified": True},
        }


def _readiness():
    return {
        "complete": True,
        "fresh": True,
        "fresh_dimensions": {dimension.value: True for dimension in RiskDimension},
    }


def _setup(tmp_path, *, network_fee_lamports: int | None, cash_usd: float = 500.0):
    store = ObservationEventStore(tmp_path / "all-in.sqlite3")
    store.claim_first_touch(
        token_mint="mint",
        signature="first",
        wallet="scout",
        entity_id="entity-scout",
        tier="S",
        observed_at=T0.isoformat(),
        reference_price_sol=SCOUT_PRICE,
    )
    profile = WalletProfile("scout", "entity-scout", WalletTier.S, 100, True, T0)
    handoff = ShadowWalletExecutableQuoteHandoff(
        store=store,
        client=FakeQuoteClient(),
        simulator=FakeSimulator(network_fee_lamports=network_fee_lamports),
        full_position_notional_fn=lambda: 12.5,
        max_chase_fraction=BASELINE.max_chase_fraction,
    )
    gate = CandidateActivationGate(
        controller=FakeController(),
        engine=FakeEngine(cash_usd),
        store=store,
    )
    return store, profile, handoff, gate


async def _observe_then_decide(handoff, gate, profile):
    quote = await handoff.observe(
        token_mint="mint",
        stage="starter",
        fraction_of_full_position=BASELINE.starter_fraction_of_full_position,
        scout_reference_price_sol=SCOUT_PRICE,
        trigger_observed_at=T0,
    )
    assert quote is not None
    decision_at = quote.received_at + timedelta(milliseconds=10)
    decision = gate.evaluate(
        token_mint="mint",
        stage="starter",
        fraction_of_full_position=BASELINE.starter_fraction_of_full_position,
        scout_profile=profile,
        first_touch=gate.store.first_touch("mint"),
        risk=RiskSnapshot(observed_at=decision_at),
        risk_readiness=_readiness(),
        quote=quote,
        risk_completed_at=T0 + timedelta(milliseconds=5),
        decision_at=decision_at,
    )
    return quote, decision


def test_route_inside_chase_but_all_in_cost_outside_chase_is_rejected(tmp_path):
    store, profile, handoff, gate = _setup(tmp_path, network_fee_lamports=1_000_000)
    quote, decision = asyncio.run(_observe_then_decide(handoff, gate, profile))

    assert quote.usable is True
    assert quote.drift_fraction == pytest.approx(0.10)
    assert decision.authorized is False
    assert "all_in_execution_cost_exceeds_chase_ceiling" in decision.blockers

    row = store.db.execute(
        "SELECT payload_json FROM events WHERE event_type='all_in_execution_cost_decision' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert row is not None
    raw = str(row["payload_json"])
    assert '"all_in_gate_passed":false' in raw
    assert '"network_fee_lamports":1000000' in raw


def test_all_in_cost_inside_existing_chase_ceiling_can_authorize(tmp_path):
    _store, profile, handoff, gate = _setup(tmp_path, network_fee_lamports=100_000)
    quote, decision = asyncio.run(_observe_then_decide(handoff, gate, profile))

    assert quote.usable is True
    assert quote.drift_fraction == pytest.approx(0.10)
    assert decision.authorized is True
    assert decision.code == "PAPER_ENTRY_AUTHORIZED"
    assert "all_in_execution_cost_exceeds_chase_ceiling" not in decision.blockers


def test_network_cost_is_included_in_buying_power(tmp_path):
    _store, profile, handoff, gate = _setup(
        tmp_path,
        network_fee_lamports=1_000_000,
        cash_usd=3.80,
    )
    _quote, decision = asyncio.run(_observe_then_decide(handoff, gate, profile))

    assert decision.authorized is False
    assert "insufficient_paper_buying_power_including_network_cost" in decision.blockers


def test_missing_fee_evidence_fails_closed_before_activation(tmp_path):
    store, _profile, handoff, _gate = _setup(tmp_path, network_fee_lamports=None)
    quote = asyncio.run(
        handoff.observe(
            token_mint="mint",
            stage="starter",
            fraction_of_full_position=BASELINE.starter_fraction_of_full_position,
            scout_reference_price_sol=SCOUT_PRICE,
            trigger_observed_at=T0,
        )
    )

    assert quote is not None
    assert quote.usable is False
    assert quote.reason.startswith("all-in execution cost unavailable:")
    row = store.db.execute(
        "SELECT payload_json FROM events WHERE event_type='all_in_execution_cost_observation' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert row is not None
    raw = str(row["payload_json"])
    assert '"evidence_complete":false' in raw
    assert "signature_fee_lamports" in raw
