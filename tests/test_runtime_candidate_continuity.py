from __future__ import annotations

from datetime import datetime, timezone

from solana_roi.activation import (
    ARM_CONFIRMATION,
    CandidateActivationGate,
    CoverageCertificationPolicy,
)
from solana_roi.engine import PaperTradingEngine
from solana_roi.observation import LatencyCertificationPolicy
from solana_roi.observation_store import ObservationEventStore
from solana_roi.quote import QuoteCertificationPolicy
from solana_roi.risk import RiskPolicy
from solana_roi.runtime import RuntimeForwardCohortController


class FixedGate:
    def __init__(self, policy):
        self.policy = policy

    def status(self):
        return {"certified": True}


class DrainedQueue:
    def status(self):
        return {"pending": 0, "complete": 0, "failed": 0}


class MutableDirectPlane:
    def __init__(self):
        self.enabled = True
        self.continuity_ok = True
        self.strategy_scope_reduced = False

    def status(self):
        return {
            "enabled": self.enabled,
            "continuity_ok": self.continuity_ok,
            "strategy_scope_reduced": self.strategy_scope_reduced,
            "full_program_scope": [f"program-{index}" for index in range(7)],
        }


def _armed_runtime_controller(tmp_path, monkeypatch):
    monkeypatch.setenv("SOLANA_ROI_SHADOW_CLOCK_ENABLED", "true")
    store = ObservationEventStore(tmp_path / "candidate-continuity.sqlite3")
    engine = PaperTradingEngine(store=store)
    now = datetime(2026, 9, 2, tzinfo=timezone.utc)
    controller = RuntimeForwardCohortController(
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
    direct = MutableDirectPlane()
    controller.webhook_queue = DrainedQueue()
    controller.direct_ingestion = direct
    controller.freeze_manifest()
    controller.arm(ARM_CONFIRMATION)
    return store, engine, controller, direct, now


def _record_only_probe(gate, engine, now):
    return gate.evaluate(
        token_mint="mint",
        stage="starter",
        fraction_of_full_position=engine.config.starter_fraction_of_full_position,
        scout_profile=None,
        first_touch={"wallet": "", "observed_at": now.isoformat()},
        risk=None,
        risk_readiness={},
        quote=None,
        risk_completed_at=now,
        decision_at=now,
    )


def test_armed_candidate_rechecks_live_direct_stream_and_fails_closed(tmp_path, monkeypatch):
    store, engine, controller, direct, now = _armed_runtime_controller(tmp_path, monkeypatch)
    gate = CandidateActivationGate(controller=controller, engine=engine, store=store)

    assert controller.runtime_continuity_ok() is True
    healthy = _record_only_probe(gate, engine, now)
    assert "runtime_portfolio_continuity_unproven" not in healthy.blockers

    direct.continuity_ok = False
    assert controller.runtime_continuity_ok() is False
    stale = _record_only_probe(gate, engine, now)
    assert stale.authorized is False
    assert stale.code == "record_only"
    assert "runtime_portfolio_continuity_unproven" in stale.blockers
    assert store.paper_entry_authorization_count() == 0


def test_candidate_continuity_requires_exact_full_scope_not_merely_connected(tmp_path, monkeypatch):
    store, engine, controller, direct, now = _armed_runtime_controller(tmp_path, monkeypatch)
    gate = CandidateActivationGate(controller=controller, engine=engine, store=store)

    direct.strategy_scope_reduced = True
    assert controller.runtime_continuity_ok() is False
    decision = _record_only_probe(gate, engine, now)
    assert "runtime_portfolio_continuity_unproven" in decision.blockers

    direct.strategy_scope_reduced = False
    direct.enabled = False
    assert controller.runtime_continuity_ok() is False
    decision = _record_only_probe(gate, engine, now)
    assert "runtime_portfolio_continuity_unproven" in decision.blockers
