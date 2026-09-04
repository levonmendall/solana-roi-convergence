from __future__ import annotations

from types import SimpleNamespace

from solana_roi.direct_solana import DirectSolanaJournal
from solana_roi.observation_store import ObservationEventStore
from solana_roi.wallet_discovery import ContinuousWalletDiscovery, WalletDiscoveryPolicy
from solana_roi import profit_first_entity_final_research as final_research
from solana_roi import wallet_entity_universe_v4 as v4
from solana_roi import wallet_forward_pipeline_architecture as pipeline
from solana_roi.forward_evidence_runtime_repair import (
    CANDIDATE_MAX_INFLIGHT,
    _claim_forward_work,
    install_forward_evidence_runtime_repair,
)


class _Intelligence:
    def status(self):
        return {"startup_state": "ready"}


class _Overlay:
    def __init__(self, payload):
        self.payload = payload

    def status(self):
        return dict(self.payload)


class _UniverseOverlay(_Overlay):
    def ensure_seed_candidates(self):
        return None


def _discovery(tmp_path):
    store = ObservationEventStore(tmp_path / "forward-evidence.sqlite3")
    return ContinuousWalletDiscovery(
        store=store,
        rpc=SimpleNamespace(),
        entity_resolver=SimpleNamespace(),
        risk=SimpleNamespace(),
        risk_collectors=SimpleNamespace(),
        intelligence=_Intelligence(),
        mark_provider=SimpleNamespace(),
        policy=WalletDiscoveryPolicy(),
        enabled=False,
    )


def test_final_wallet_status_is_acyclic_even_if_legacy_delegate_globals_self_reference(monkeypatch, tmp_path):
    install_forward_evidence_runtime_repair()
    discovery = _discovery(tmp_path)

    # Reproduce the exact class of production defect: mutable module globals in old
    # wrapper layers point back at their own wrapper. The final composition must not
    # traverse any of them. Use monkeypatch so the poisoned legacy globals cannot
    # leak into unrelated regressions.
    monkeypatch.setattr(final_research, "_ORIGINAL_STATUS", final_research._status_with_final)
    monkeypatch.setattr(v4, "_ORIGINAL_STATUS", v4._status_with_v4_universe)
    monkeypatch.setattr(pipeline, "_ORIGINAL_DISCOVERY_STATUS", pipeline._status_with_forward_pipeline)

    monkeypatch.setattr(final_research, "_adapter", lambda _discovery: _Overlay({"ok": True}))
    monkeypatch.setattr(v4, "_universe", lambda _discovery: _UniverseOverlay({"ok": True}))

    status = discovery.status()
    assert status["operational"] is True
    assert status["wallet_discovery_final_composition"]["acyclic_status_path"] is True
    assert status["historical_screen_has_promotion_authority"] is False
    assert status["paper_only"] is True
    assert status["live_money_authority"] is False


def test_forward_evidence_installer_is_idempotent():
    install_forward_evidence_runtime_repair()
    first_status = ContinuousWalletDiscovery.status
    first_run_once = ContinuousWalletDiscovery.run_once

    install_forward_evidence_runtime_repair()

    assert ContinuousWalletDiscovery.status is first_status
    assert ContinuousWalletDiscovery.run_once is first_run_once
    assert getattr(first_status, "_roi_forward_evidence_final_wallet_status", False) is True
    assert getattr(first_run_once, "_roi_forward_evidence_final_wallet_run_once", False) is True


def test_candidate_claims_are_not_moved_to_processing_when_real_rpc_capacity_is_full(tmp_path):
    store = ObservationEventStore(tmp_path / "candidate-admission.sqlite3")
    journal = DirectSolanaJournal(store)
    plane = SimpleNamespace(journal=journal, worker_count=12)
    setattr(plane, "_roi_forward_evidence_active_candidates", CANDIDATE_MAX_INFLIGHT)

    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    journal.enqueue(
        signature="scout-a",
        slot=1,
        trigger_received_at=now,
        source_hint=None,
        priority=0,
        reason="frozen_scout_processed_trigger",
    )

    row, lane = _claim_forward_work(plane, fast_only=True)
    assert row is None
    assert lane == "none"
    with store._lock:
        state = store.db.execute(
            "SELECT status FROM direct_solana_hydration_queue WHERE signature='scout-a'"
        ).fetchone()
    assert state["status"] == "pending"

    setattr(plane, "_roi_forward_evidence_active_candidates", 0)
    row, lane = _claim_forward_work(plane, fast_only=True)
    assert row is not None
    assert row["signature"] == "scout-a"
    assert lane == "candidate_reserved"


def test_candidate_admission_never_blocks_non_scout_priority_work(tmp_path):
    store = ObservationEventStore(tmp_path / "candidate-continuity.sqlite3")
    journal = DirectSolanaJournal(store)
    plane = SimpleNamespace(journal=journal, worker_count=12)
    setattr(plane, "_roi_forward_evidence_active_candidates", CANDIDATE_MAX_INFLIGHT)

    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    journal.enqueue(
        signature="gap-a",
        slot=2,
        trigger_received_at=now,
        source_hint="PUMP_FUN",
        priority=2,
        reason="gap_backfill",
    )

    row, lane = _claim_forward_work(plane, fast_only=True)
    assert row is not None
    assert row["signature"] == "gap-a"
    assert lane == "candidate_reserved"
