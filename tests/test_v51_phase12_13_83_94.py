from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pytest

from solana_roi import api
from solana_roi.observation_store import ObservationEventStore
from solana_roi.storage import AppendOnlyEventStore
from solana_roi.strategy_v51_authority import ECONOMIC_FREEZE_EPOCH, authority
from solana_roi.v51_candidate_ledger import record_solana_candidate, record_stage_event
from solana_roi.v51_phase12_13_operations import build_operations_proof, normalize_subsystem
from solana_roi.v51_seeded_e2e import run_seeded_equivalence_case


class MemoryStore:
    def __init__(self) -> None:
        self.db = sqlite3.connect(":memory:", check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self._lock = threading.RLock()


class StatusObject:
    def __init__(self, payload: dict):
        self.payload = payload

    def status(self) -> dict:
        return dict(self.payload)


class FakeRuntime:
    def __init__(self, store, *, direct=None, wallet=None, collectors=None):
        self.store = store
        self.direct_ingestion = StatusObject(direct or {})
        self.wallet_discovery = StatusObject(wallet or {})
        self.collectors = StatusObject(collectors or {})


@dataclass
class SyntheticSwap:
    signature: str
    slot: int
    observed_at: datetime
    received_at: datetime
    wallet: str = "wallet-a"
    token_mint: str = "mint-a"
    side: str = "buy"
    token_amount: float = 1.0
    native_amount_sol: float = 0.1
    reference_price_sol: float = 0.1
    ingestion_latency_ms: float = 100.0
    source: str = "PUMP_AMM_SYNTHETIC"


def _stage_rows(store: MemoryStore, surface: str, candidate_id: str):
    with store._lock:
        return store.db.execute(
            "SELECT stage,status,reason FROM v51_candidate_pipeline_audit "
            "WHERE surface=? AND candidate_id=? ORDER BY stage_index",
            (surface, candidate_id),
        ).fetchall()


def test_84_final_production_import_exposes_expected_final_graph() -> None:
    import solana_roi.production as production
    from solana_roi.direct_solana import DirectSolanaIngestionPlane
    from solana_roi.robinhood_chain_paper import RobinhoodChainPaperPlane

    assert production.app.state.roi_v51_final_economic_authority is True
    assert production.app.state.roi_v51_economic_composition_explicit is True
    assert production.app.state.roi_v51_system_proof_70_74 is True
    assert production.app.state.roi_v51_phase12_13_83_94 is True
    assert bool(getattr(DirectSolanaIngestionPlane._handle_notification, "_roi_cooperative_yield", False))
    assert bool(getattr(RobinhoodChainPaperPlane._maybe_open_v3, "_roi_v51_prelane_coverage", False))
    assert bool(getattr(RobinhoodChainPaperPlane.run, "_roi_robinhood_forward_only_run", False))


def test_85_black_box_final_contract_for_solana_and_fomo_after_production_composition() -> None:
    import solana_roi.production as production

    assert production.app.state.roi_v51_final_economic_authority is True
    store = MemoryStore()
    for surface, venue, candidate in (
        ("SOLANA", "PUMP_AMM", "bb-solana"),
        ("FOMO", "FOMO", "bb-fomo"),
    ):
        result = run_seeded_equivalence_case(
            store,
            {
                "surface": surface,
                "candidate_id": candidate,
                "token": f"token-{surface.lower()}",
                "venue": venue,
                "lifecycle": "forward_synthetic",
                "lane": "continuation",
                "entry_executable": True,
                "exit_executable": True,
                "latency_seconds": 2.0,
                "chase_fraction": 0.05,
                "round_trip_cost_fraction": 0.02,
                "settled_net_return": 0.08,
            },
        )
        assert result["decision"] == "paper_enter"
        rows = _stage_rows(store, surface, candidate)
        assert [row["stage"] for row in rows] == authority()["pipeline_stages"]
        assert rows[-1]["stage"] == "learning"
        assert rows[-1]["status"] == "complete"


@pytest.mark.parametrize("surface", ["SOLANA", "FOMO", "ROBINHOOD_CHAIN"])
@pytest.mark.parametrize(
    "override,reason",
    [
        ({"entry_executable": False}, "exact_entry_or_exit_execution_evidence_unavailable"),
        ({"exit_executable": False}, "exact_entry_or_exit_execution_evidence_unavailable"),
        ({"structurally_tradeable": False}, "mechanical_hard_stop"),
        ({"stale_risk": True}, "stale_risk"),
        ({"stale_candidate": True}, "stale_candidate"),
        ({"latency_seconds": 20.001}, "latency_chase_cost_or_hazard_context_has_no_accessible_bootstrap_size"),
        ({"chase_fraction": 0.401}, "latency_chase_cost_or_hazard_context_has_no_accessible_bootstrap_size"),
        ({"hazard_evidence_sufficient": False}, "hazard_insufficient_evidence"),
        ({"exposure_available": False}, "portfolio_exposure_exhausted"),
    ],
)
def test_86_every_negative_path_has_explicit_terminal_rejection(surface: str, override: dict, reason: str) -> None:
    store = MemoryStore()
    case = {
        "surface": surface,
        "candidate_id": f"negative-{surface}-{reason}-{json.dumps(override, sort_keys=True)}",
        "token": "token-negative",
        "venue": "PUMP_AMM" if surface == "SOLANA" else surface,
        "lifecycle": "forward_synthetic",
        "lane": "continuation",
        "entry_executable": True,
        "exit_executable": True,
        "latency_seconds": 2.0,
        "chase_fraction": 0.05,
        "round_trip_cost_fraction": 0.02,
    }
    case.update(override)
    result = run_seeded_equivalence_case(store, case)
    assert result["decision"] == "paper_reject"
    assert result["reason"] == reason
    rows = _stage_rows(store, surface, case["candidate_id"])
    decision = [row for row in rows if row["stage"] == "decision"]
    position = [row for row in rows if row["stage"] == "position"]
    assert len(decision) == 1 and decision[0]["status"] == "complete"
    assert len(position) == 1 and position[0]["status"] == "not_opened"


def test_87_one_hundred_candidates_have_one_hundred_canonical_and_terminal_records(tmp_path) -> None:
    store = ObservationEventStore(tmp_path / "lossless.sqlite3")
    now = datetime.now(timezone.utc)
    release = "a" * 40
    for index in range(100):
        candidate = f"synthetic-{index:03d}"
        assert record_solana_candidate(
            store,
            SyntheticSwap(signature=candidate, slot=index + 1, observed_at=now, received_at=now),
            release_commit=release,
        ) is True
        for stage, status, reason in (
            ("context", "complete", "synthetic_context"),
            ("execution_evidence", "complete", "synthetic_execution"),
            ("decision", "complete", "synthetic_terminal_reject"),
            ("position", "not_opened", "synthetic_terminal_reject"),
        ):
            record_stage_event(
                store,
                surface="SOLANA",
                candidate_id=candidate,
                release_commit=release,
                stage=stage,
                status=status,
                reason=reason,
            )
    with store._lock:
        canonical = store.db.execute(
            "SELECT COUNT(DISTINCT candidate_id) AS n FROM v51_candidates WHERE surface='SOLANA'"
        ).fetchone()["n"]
        terminal = store.db.execute(
            "SELECT COUNT(DISTINCT candidate_id) AS n FROM v51_candidate_current_state "
            "WHERE surface='SOLANA' AND stage='decision' AND status='complete'"
        ).fetchone()["n"]
        undecided = store.db.execute(
            "SELECT COUNT(*) AS n FROM v51_candidates c WHERE surface='SOLANA' AND NOT EXISTS ("
            "SELECT 1 FROM v51_candidate_current_state s WHERE s.surface=c.surface AND s.candidate_id=c.candidate_id "
            "AND s.stage='decision' AND s.status='complete')"
        ).fetchone()["n"]
    assert canonical == 100
    assert terminal == 100
    assert undecided == 0
    store.db.close()


def test_88_wrapper_order_regression_preserves_expected_predecessors() -> None:
    import solana_roi.production  # noqa: F401
    from solana_roi.robinhood_chain_paper import RobinhoodChainPaperPlane

    v3 = RobinhoodChainPaperPlane._maybe_open_v3
    v2 = RobinhoodChainPaperPlane._maybe_open_v2
    assert bool(getattr(v3, "_roi_v51_prelane_coverage", False))
    assert bool(getattr(v2, "_roi_v51_prelane_coverage", False))
    assert hasattr(v3, "__wrapped__") and inspect.unwrap(v3) is not v3
    assert hasattr(v2, "__wrapped__") and inspect.unwrap(v2) is not v2
    assert bool(getattr(RobinhoodChainPaperPlane.run, "_roi_robinhood_forward_only_run", False))


def test_89_real_fastapi_lifespan_starts_canonical_workers_without_unused_helius(monkeypatch) -> None:
    import solana_roi.production as production

    started: list[str] = []

    class Worker:
        def __init__(self, name: str):
            self.name = name

        async def run(self, stop: asyncio.Event) -> None:
            started.append(self.name)
            await stop.wait()

    class Runtime:
        direct_ingestion = Worker("direct")
        wallet_discovery = Worker("wallet")
        webhook_worker = Worker("helius")
        price_clock = Worker("clock")

    monkeypatch.setenv("SOLANA_ROI_DIRECT_SOLANA_ENABLED", "true")
    monkeypatch.delenv("SOLANA_ROI_LEGACY_WEBHOOK_WORKER_ENABLED", raising=False)
    monkeypatch.setenv("SOLANA_ROI_SHADOW_CLOCK_ENABLED", "false")
    monkeypatch.setattr(api, "ingestion_runtime", lambda: Runtime())

    async def exercise() -> None:
        async with api.lifespan(production.app):
            await asyncio.sleep(0)
            assert set(started) == {"direct", "wallet"}
            assert production.app.state.roi_legacy_helius_webhook_worker_enabled is False

    asyncio.run(exercise())


@pytest.mark.parametrize("snapshot", ["v3.1", "v4", "pre-v5", "pr179", "current"])
def test_90_prior_state_migration_preserves_append_only_paper_history(tmp_path, snapshot: str) -> None:
    path = tmp_path / f"{snapshot.replace('.', '-')}.sqlite3"
    legacy = AppendOnlyEventStore(path)
    observed = "2026-09-01T00:00:00+00:00"
    lineage = legacy.append("paper_settlement", observed, {"snapshot": snapshot, "net_return": 0.10})
    legacy.db.execute(f"CREATE TABLE IF NOT EXISTS legacy_{snapshot.replace('.', '_').replace('-', '_')} (id INTEGER PRIMARY KEY, note TEXT)")
    legacy.db.commit()
    legacy.db.close()

    upgraded = ObservationEventStore(path)
    with upgraded._lock:
        row = upgraded.db.execute(
            "SELECT event_type,observed_at,lineage_hash FROM events WHERE event_type='paper_settlement'"
        ).fetchone()
        assert row is not None
        assert row["observed_at"] == observed
        assert row["lineage_hash"] == lineage
    assert upgraded.verify() is True
    upgraded.db.close()


def test_91_direct_solana_disables_unused_legacy_worker_by_default(monkeypatch) -> None:
    monkeypatch.setenv("SOLANA_ROI_DIRECT_SOLANA_ENABLED", "true")
    monkeypatch.delenv("SOLANA_ROI_LEGACY_WEBHOOK_WORKER_ENABLED", raising=False)
    assert api.legacy_webhook_worker_enabled() is False
    monkeypatch.setenv("SOLANA_ROI_LEGACY_WEBHOOK_WORKER_ENABLED", "true")
    assert api.legacy_webhook_worker_enabled() is True


def test_92_93_resource_attribution_and_backpressure_fail_unhealthy_outpacing_worker(tmp_path) -> None:
    degraded = normalize_subsystem(
        "solana_ingestion",
        {
            "queue_depth": 12,
            "oldest_pending_age_seconds": 75.0,
            "events_per_second": 20.0,
            "processing_per_second": 10.0,
            "lag": 75.0,
            "dropped_count": 0,
            "retry_count": 3,
            "cycle_duration_seconds": 0.5,
            "processed_count": 100,
        },
    )
    assert degraded["producer_persistently_outpacing_consumer"] is True
    assert degraded["backpressure_healthy"] is False

    store = ObservationEventStore(tmp_path / "ops.sqlite3")
    runtime = FakeRuntime(
        store,
        direct={"processed_count": 10, "cycle_duration_seconds": 0.1},
        wallet={"processed_count": 4, "cycle_duration_seconds": 0.2},
        collectors={"processed_count": 7, "cycle_duration_seconds": 0.3},
    )
    proof = build_operations_proof(
        runtime,
        unified_status={"fomo": {"processed_count": 3, "cycle_duration_seconds": 0.4}},
        robinhood_status={"processed_count": 2, "cycle_duration_seconds": 0.5},
        proof_cycle_duration_seconds=0.6,
        http_request_count=9,
        http_total_duration_seconds=0.7,
    )
    expected = {"solana_ingestion", "wallet_discovery", "risk_enrichment", "fomo", "robinhood", "proof_publication", "http"}
    assert set(proof["resource_attribution"]) == expected
    for name in expected:
        assert "cycle_duration_seconds" in proof["resource_attribution"][name]
        assert "work_counter" in proof["resource_attribution"][name]
        worker = proof["backpressure"]["subsystems"][name]
        for field in ("queue_depth", "oldest_pending_age_seconds", "events_per_second", "processing_per_second", "lag", "dropped_count", "retry_count"):
            assert field in worker
    store.db.close()


def test_94_restart_continuity_tracks_release_without_new_economic_epoch(tmp_path, monkeypatch) -> None:
    path = tmp_path / "continuity.sqlite3"
    store = ObservationEventStore(path)
    runtime = FakeRuntime(store)
    monkeypatch.setenv("SOLANA_ROI_RELEASE_COMMIT", "b" * 40)
    first = build_operations_proof(runtime)
    first_row = first["continuity"]["subsystems"]["solana_ingestion"]
    assert first_row["current_release"] == "b" * 40
    assert first_row["previous_release"] is None
    assert first_row["continuity_epoch"] == ECONOMIC_FREEZE_EPOCH
    store.db.close()

    reopened = ObservationEventStore(path)
    runtime2 = FakeRuntime(reopened)
    monkeypatch.setenv("SOLANA_ROI_RELEASE_COMMIT", "c" * 40)
    second = build_operations_proof(runtime2)
    row = second["continuity"]["subsystems"]["solana_ingestion"]
    assert row["current_release"] == "c" * 40
    assert row["previous_release"] == "b" * 40
    assert row["continuity_epoch"] == ECONOMIC_FREEZE_EPOCH
    assert row["restart_changes_economic_epoch"] is False
    assert row["cursor_restore_status"]
    reopened.db.close()


def test_83_94_preserve_frozen_paper_only_authority() -> None:
    spec = authority()
    assert spec["execution"]["latency_hard_max_seconds"] == 20.0
    assert spec["paper_only"] is True
    assert spec["live_money_authority"] is False
    assert spec["signing_available"] is False
    assert spec["transaction_submission_available"] is False
