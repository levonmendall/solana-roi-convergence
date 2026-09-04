from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from solana_roi import candidate_compute_admission as admission
from solana_roi import context_research_bandwidth_governor as bandwidth
from solana_roi.observation_store import ObservationEventStore


def _candidate(
    *,
    now: datetime,
    signature: str = "candidate-admission-test",
    source: str = "solana-direct:PUMP_FUN:buy",
    slot: int = 101,
    age_seconds: float = 1.0,
):
    observed = now - timedelta(seconds=age_seconds)
    received = observed + timedelta(milliseconds=100)
    return SimpleNamespace(
        signature=signature,
        slot=slot,
        observed_at=observed,
        received_at=received,
        ingestion_latency_ms=100.0,
        wallet="ScoutWallet111111111111111111111111111111",
        token_mint="Mint1111111111111111111111111111111111111",
        side="buy",
        source=source,
    )


def _obj(tmp_path):
    store = ObservationEventStore(tmp_path / "candidate-compute-admission.sqlite3")
    return SimpleNamespace(store=store)


def _seed_launch_slot(store: ObservationEventStore, *, mint: str, slot: int) -> None:
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS launch_near_creation_diagnostics ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,token_mint TEXT NOT NULL,launch_slot INTEGER)"
        )
        store.db.execute(
            "INSERT INTO launch_near_creation_diagnostics(token_mint,launch_slot) VALUES (?,?)",
            (mint, slot),
        )


def test_candidate_already_outside_entry_window_skips_expensive_compute(tmp_path):
    now = datetime.now(timezone.utc)
    obj = _obj(tmp_path)
    candidate = _candidate(now=now, age_seconds=21.0, source="solana-direct:RAYDIUM:buy")

    policy = admission.candidate_compute_policy(obj, candidate, now=now)

    assert policy["tier"] == "outside_entry_window_research_only"
    assert policy["fraction"] == 0.0
    assert policy["selected"] is False
    assert policy["observed_age_ms"] > admission.ENTRY_WINDOW_SECONDS * 1000.0


def test_exact_first_slot_pump_fun_candidate_is_research_only(tmp_path):
    now = datetime.now(timezone.utc)
    obj = _obj(tmp_path)
    candidate = _candidate(now=now, slot=100)
    _seed_launch_slot(obj.store, mint=candidate.token_mint, slot=100)

    policy = admission.candidate_compute_policy(obj, candidate, now=now)

    assert policy["tier"] == "pump_first_slot_research_only"
    assert policy["fraction"] == 0.0
    assert policy["selected"] is False
    assert policy["candidate_slot"] == policy["launch_slot"] == 100


def test_pump_fun_residual_continuation_is_not_rejected_by_first_slot_gate(tmp_path):
    now = datetime.now(timezone.utc)
    obj = _obj(tmp_path)
    candidate = _candidate(now=now, slot=101)
    _seed_launch_slot(obj.store, mint=candidate.token_mint, slot=100)

    policy = admission.candidate_compute_policy(obj, candidate, now=now)

    assert policy["tier"] == "actionable_or_unresolved_full_rate"
    assert policy["fraction"] == 1.0
    assert policy["selected"] is True


def test_mature_negative_context_reuses_pr111_exploration_floor(tmp_path, monkeypatch):
    now = datetime.now(timezone.utc)
    obj = _obj(tmp_path)
    candidate = _candidate(now=now, source="solana-direct:RAYDIUM:buy")
    monkeypatch.setattr(
        admission,
        "_context_actions",
        lambda *args, **kwargs: [
            "demote_for_future_context_influence",
            "withhold_from_future_context_influence",
        ],
    )
    monkeypatch.setattr(bandwidth, "_deterministic_selected", lambda signature, fraction: False)

    policy = admission.candidate_compute_policy(obj, candidate, now=now)

    assert policy["tier"] == "mature_negative_context_exploration"
    assert policy["fraction"] == bandwidth.MATURE_NEGATIVE_EXPLORATION_FRACTION
    assert policy["selected"] is False


def test_missing_or_mixed_context_does_not_manufacture_a_rejection(tmp_path, monkeypatch):
    now = datetime.now(timezone.utc)
    obj = _obj(tmp_path)
    candidate = _candidate(now=now, source="solana-direct:PUMP_AMM:buy")
    monkeypatch.setattr(admission, "_context_actions", lambda *args, **kwargs: [])

    policy = admission.candidate_compute_policy(obj, candidate, now=now)

    assert policy["tier"] == "actionable_or_unresolved_full_rate"
    assert policy["selected"] is True


def test_deferred_risk_refresh_records_fail_closed_certification_evidence_without_rpc():
    now = datetime.now(timezone.utc)
    candidate = _candidate(now=now, source="solana-direct:PUMP_FUN:buy")
    calls: list[dict] = []
    store = SimpleNamespace(record_risk_refresh=lambda **kwargs: calls.append(kwargs))
    collectors = SimpleNamespace(store=store, now_fn=lambda: now)
    decision = {
        "tier": "pump_first_slot_research_only",
        "reason": "first_slot_pump_fun_sniping_is_outside_target_execution_capability",
    }

    admission._record_skipped_risk_refresh(collectors, candidate, decision)

    assert len(calls) == 1
    assert calls[0]["complete"] is False
    assert calls[0]["fresh"] is False
    assert calls[0]["readiness"]["candidate_compute_admission_deferred"] is True
    assert calls[0]["readiness"]["collector_rpc_attempted"] is False
    assert calls[0]["readiness"]["certification_thresholds_unchanged"] is True


def test_admission_preserves_frozen_authority_and_threshold_boundaries():
    assert admission.ENTRY_WINDOW_SECONDS == 20.0
    assert admission.MAX_CHASE_FRACTION == 0.15
    assert admission.PAPER_ONLY is True
    assert admission.LIVE_MONEY_AUTHORITY is False
    assert admission.SIGNING_AVAILABLE is False
    assert admission.TRANSACTION_SUBMISSION_AVAILABLE is False
    assert admission.FULL_MARKET_OBSERVATION_REDUCED is False
    assert admission.CONTINUITY_SCOPE_REDUCED is False
    assert admission.CERTIFICATION_THRESHOLDS_CHANGED is False
