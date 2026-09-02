from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from solana_roi.activation import (
    ARM_CONFIRMATION,
    CandidateActivationGate,
    CoverageCertificationPolicy,
    ForwardCohortController,
    ProgramCoverageCertificationGate,
)
from solana_roi.engine import PaperTradingEngine
from solana_roi.ingestion import WalletProfile, WalletProfileRegistry
from solana_roi.models import Confirmation, IntentKind, RiskSnapshot, WalletTier, WalletTouch
from solana_roi.observation import LatencyCertificationPolicy
from solana_roi.observation_store import ObservationEventStore
from solana_roi.quote import ExecutableQuote, QuoteCertificationPolicy
from solana_roi.risk import RiskDimension, RiskPolicy


class FixedGate:
    def __init__(self, policy, certified=True):
        self.policy = policy
        self.certified = certified

    def status(self):
        return {"certified": self.certified}


def _controller(tmp_path, *, suffix="a"):
    store = ObservationEventStore(tmp_path / f"forward-{suffix}.sqlite3")
    engine = PaperTradingEngine(store=store)
    registry = WalletProfileRegistry(store)
    now = datetime(2026, 9, 1, tzinfo=timezone.utc)
    profile = WalletProfile("scout", "entity-scout", WalletTier.S, 100, True, now)
    registry.register(profile)
    controller = ForwardCohortController(
        store=store,
        engine=engine,
        config=engine.config,
        risk_policy=RiskPolicy(),
        latency_gate=FixedGate(LatencyCertificationPolicy()),
        quote_gate=FixedGate(QuoteCertificationPolicy()),
        coverage_gate=FixedGate(CoverageCertificationPolicy()),
        release_commit_fn=lambda: "a" * 40,
        now_fn=lambda: now,
    )
    return store, engine, registry, profile, controller, now


def _clean_readiness():
    return {
        "complete": True,
        "fresh": True,
        "fresh_dimensions": {dimension.value: True for dimension in RiskDimension},
    }


def _starter_quote(engine, now, *, notional=None):
    requested = engine.portfolio.full_position_notional(engine.marks) * engine.config.starter_fraction_of_full_position
    return ExecutableQuote(
        token_mint="mint",
        stage="starter",
        requested_notional_usd=requested if notional is None else notional,
        input_sol=.02,
        sol_usd=187.5,
        output_token_units=20.0,
        effective_price_sol=.00105,
        scout_reference_price_sol=.001,
        drift_fraction=.05,
        router="metis",
        fee_bps=50,
        token_decimals=6,
        quoted_at=now + timedelta(seconds=1),
        received_at=now + timedelta(seconds=2),
        quote_latency_ms=500,
        chain_to_quote_ms=2000,
        usable=True,
        reason="within chase ceiling",
    )


def test_manifest_freeze_and_arm_are_one_way_and_bind_exact_release(tmp_path):
    store, engine, registry, profile, controller, now = _controller(tmp_path)
    assert controller.status()["forward_cohort_ready"] is False
    manifest = controller.freeze_manifest()
    assert manifest["release_commit"] == "a" * 40
    assert manifest["manifest"]["strategy_name"] == "ROI Convergence v3.1"
    assert manifest["manifest"]["genesis_nav_usd"] == 500.0
    assert controller.status()["forward_cohort_ready"] is True
    with pytest.raises(ValueError):
        controller.arm("arm it")
    arm = controller.arm(ARM_CONFIRMATION)
    assert arm["manifest_sha256"] == manifest["manifest_sha256"]
    assert controller.is_armed() is True
    assert controller.status()["forward_cohort_ready"] is False
    assert store.verify()


def test_only_final_candidate_gate_authorizes_clean_amount_specific_starter(tmp_path):
    store, engine, registry, profile, controller, now = _controller(tmp_path, suffix="gate")
    store.claim_first_touch(
        token_mint="mint",
        signature="first",
        wallet=profile.wallet,
        entity_id=profile.entity_id,
        tier=profile.tier.value,
        observed_at=now.isoformat(),
        reference_price_sol=.001,
    )
    controller.freeze_manifest()
    controller.arm(ARM_CONFIRMATION)
    gate = CandidateActivationGate(controller=controller, engine=engine, store=store)
    decision = gate.evaluate(
        token_mint="mint",
        stage="starter",
        fraction_of_full_position=engine.config.starter_fraction_of_full_position,
        scout_profile=profile,
        first_touch=store.first_touch("mint"),
        risk=RiskSnapshot(observed_at=now + timedelta(seconds=2)),
        risk_readiness=_clean_readiness(),
        quote=_starter_quote(engine, now),
        risk_completed_at=now + timedelta(seconds=1),
        decision_at=now + timedelta(seconds=2, milliseconds=100),
    )
    assert decision.authorized is True
    assert decision.code == "PAPER_ENTRY_AUTHORIZED"
    assert decision.blockers == ()
    assert store.paper_entry_authorization_count() == 1


def test_candidate_gate_rejects_quote_not_sized_to_current_nav(tmp_path):
    store, engine, registry, profile, controller, now = _controller(tmp_path, suffix="sizing")
    store.claim_first_touch(
        token_mint="mint",
        signature="first",
        wallet=profile.wallet,
        entity_id=profile.entity_id,
        tier=profile.tier.value,
        observed_at=now.isoformat(),
        reference_price_sol=.001,
    )
    controller.freeze_manifest()
    controller.arm(ARM_CONFIRMATION)
    gate = CandidateActivationGate(controller=controller, engine=engine, store=store)
    decision = gate.evaluate(
        token_mint="mint",
        stage="starter",
        fraction_of_full_position=engine.config.starter_fraction_of_full_position,
        scout_profile=profile,
        first_touch=store.first_touch("mint"),
        risk=RiskSnapshot(observed_at=now + timedelta(seconds=2)),
        risk_readiness=_clean_readiness(),
        quote=_starter_quote(engine, now, notional=9.99),
        risk_completed_at=now + timedelta(seconds=1),
        decision_at=now + timedelta(seconds=2, milliseconds=100),
    )
    assert decision.authorized is False
    assert "quote_not_sized_to_current_nav" in decision.blockers
    assert store.paper_entry_authorization_count() == 0


def test_program_coverage_needs_observed_proof_and_detects_out_of_order_first_touch(tmp_path):
    store = ObservationEventStore(tmp_path / "coverage.sqlite3")
    now = datetime(2026, 9, 1, tzinfo=timezone.utc)
    gate = ProgramCoverageCertificationGate(store, configured_fn=lambda: True)
    for i in range(100):
        at = now + timedelta(seconds=i)
        store.record_program_coverage(
            token_mint=f"mint-{i}",
            pair_created_at=at.isoformat(),
            assessed_at=(at + timedelta(seconds=8)).isoformat(),
            launch_lag_ms=1000,
            launch_near_creation=True,
            early_buy_count=5,
            early_buyer_count=5,
            early_buyers_complete=True,
        )
        store.mark_program_coverage_funding_complete(f"mint-{i}", assessed_at=(at + timedelta(seconds=9)).isoformat())
    assert gate.status()["certified"] is True

    registry = WalletProfileRegistry(store)
    registry.register(WalletProfile("late", "entity-late", WalletTier.S, 100, True, now))
    store.claim_first_touch(
        token_mint="conflict",
        signature="later",
        wallet="late",
        entity_id="entity-late",
        tier="S",
        observed_at=(now + timedelta(seconds=5)).isoformat(),
        reference_price_sol=.001,
    )
    inserted = store.record_swap(
        signature="earlier",
        slot=1,
        observed_at=(now + timedelta(seconds=1)).isoformat(),
        received_at=(now + timedelta(seconds=6)).isoformat(),
        wallet="late",
        token_mint="conflict",
        side="buy",
        token_amount=1000,
        native_amount_sol=1,
        reference_price_sol=.001,
        ingestion_latency_ms=5000,
        source="test",
    )
    duplicate = store.record_swap(
        signature="earlier",
        slot=1,
        observed_at=(now + timedelta(seconds=1)).isoformat(),
        received_at=(now + timedelta(seconds=7)).isoformat(),
        wallet="late",
        token_mint="conflict",
        side="buy",
        token_amount=1000,
        native_amount_sol=1,
        reference_price_sol=.001,
        ingestion_latency_ms=6000,
        source="test",
    )
    assert inserted is True
    assert duplicate is False
    assert store.first_touch_chronology_conflicts() == 1
    assert gate.status()["certified"] is False


def test_frozen_v31_paper_state_machine_s_a_and_exit_semantics(tmp_path):
    clean = RiskSnapshot(observed_at=datetime(2026, 9, 1, tzinfo=timezone.utc))
    t0 = clean.observed_at

    s = PaperTradingEngine(store=ObservationEventStore(tmp_path / "s.sqlite3"))
    s_touch = WalletTouch("s", "scout-s", "entity-s", t0, 1.0, None, WalletTier.S, True)
    s.on_first_touch(s_touch, clean, execution_price=1.0)
    spos = s.portfolio.positions["s"]
    assert round(spos.entry_capital_usd, 2) == 3.75
    assert spos.fills[0].intent is IntentKind.OPEN_STARTER
    assert spos.fills[0].execution_drag_usd > 0
    s.on_confirmation(Confirmation("s", "confirm", "entity-c", t0 + timedelta(seconds=10), 1.10, True), clean, execution_price=1.10)
    assert len(spos.fills) == 2
    assert spos.fills[1].intent is IntentKind.ADD_CONFIRMATION
    units_before_harvest = spos.units
    s.on_price("s", t0 + timedelta(seconds=20), 1.66)
    assert spos.harvest_hit is True
    assert pytest.approx(spos.units, rel=1e-9) == units_before_harvest * .30
    assert spos.runner_units == spos.units
    s.on_price("s", t0 + timedelta(seconds=25), 2.0)
    assert spos.high_water_price == 2.0
    s.on_price("s", t0 + timedelta(seconds=30), 1.19)
    assert spos.is_open is False
    assert spos.closed_reason == "40% runner trailing drawdown"
    assert all(fill.execution_drag_usd > 0 for fill in spos.fills)

    timeout = PaperTradingEngine(store=ObservationEventStore(tmp_path / "timeout.sqlite3"))
    timeout.on_first_touch(WalletTouch("timeout", "s", "es", t0, 1.0, None, WalletTier.S, True), clean, execution_price=1.0)
    timeout.on_price("timeout", t0 + timedelta(seconds=21), 1.0)
    assert timeout.portfolio.positions["timeout"].is_open is False
    assert "20 seconds" in timeout.portfolio.positions["timeout"].closed_reason

    a = PaperTradingEngine(store=ObservationEventStore(tmp_path / "a.sqlite3"))
    a.on_first_touch(WalletTouch("a", "scout-a", "entity-a", t0, 1.0, None, WalletTier.A, True), clean)
    assert "a" not in a.portfolio.positions
    a.on_confirmation(Confirmation("a", "confirm-a", "entity-b", t0 + timedelta(seconds=10), 1.1, True), clean, execution_price=1.1)
    assert a.portfolio.positions["a"].fills[0].intent is IntentKind.OPEN_FULL

    stop = PaperTradingEngine(store=ObservationEventStore(tmp_path / "stop.sqlite3"))
    stop.on_first_touch(WalletTouch("stop", "s", "es", t0, 1.0, None, WalletTier.S, True), clean, execution_price=1.0)
    stop.on_confirmation(Confirmation("stop", "c", "ec", t0 + timedelta(seconds=10), 1.1, True), clean, execution_price=1.1)
    stop.on_price("stop", t0 + timedelta(seconds=20), .76)
    assert stop.portfolio.positions["stop"].closed_reason == "-30% catastrophic stop"

    stagnation = PaperTradingEngine(store=ObservationEventStore(tmp_path / "stagnation.sqlite3"))
    stagnation.on_first_touch(WalletTouch("stagnation", "s", "es", t0, 1.0, None, WalletTier.S, True), clean, execution_price=1.0)
    stagnation.on_confirmation(Confirmation("stagnation", "c", "ec", t0 + timedelta(seconds=10), 1.1, True), clean, execution_price=1.1)
    stagnation.on_price("stagnation", t0 + timedelta(seconds=101), 1.09)
    assert stagnation.portfolio.positions["stagnation"].closed_reason == "non-positive after 90 seconds"

    thesis = PaperTradingEngine(store=ObservationEventStore(tmp_path / "thesis.sqlite3"))
    thesis.on_first_touch(WalletTouch("thesis", "s", "es", t0, 1.0, None, WalletTier.S, True), clean, execution_price=1.0)
    thesis.on_confirmation(Confirmation("thesis", "c", "ec", t0 + timedelta(seconds=10), 1.1, True), clean, execution_price=1.1)
    thesis.on_price("thesis", t0 + timedelta(seconds=191), 1.2)
    assert thesis.portfolio.positions["thesis"].closed_reason == "+50% thesis not reached within 180 seconds"
