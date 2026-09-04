from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from solana_roi import candidate_certification_hotpath_repair as hotpath
from solana_roi import candidate_execution_evidence_plane as plane
from solana_roi.ingestion import IngestionDecision, NormalizedSwap
from solana_roi.observation_store import ObservationEventStore


WALLET = "ScoutWallet111111111111111111111111111111"
MINT = "Mint1111111111111111111111111111111111111"


def _swap(*, age_seconds: float = 1.0) -> NormalizedSwap:
    now = datetime.now(timezone.utc)
    observed = now - timedelta(seconds=age_seconds)
    received = observed + timedelta(milliseconds=100)
    return NormalizedSwap(
        signature="candidate-execution-plane-test",
        slot=123,
        observed_at=observed,
        received_at=received,
        wallet=WALLET,
        token_mint=MINT,
        side="buy",
        token_amount=1_000.0,
        native_amount_sol=1.0,
        reference_price_sol=0.001,
        source="solana-direct:RAYDIUM:buy",
    )


def test_direct_scout_ingest_is_queued_without_running_risk_quote_inline(monkeypatch):
    calls: list[str] = []

    async def original(_service, swap):
        calls.append(swap.signature)
        raise AssertionError("candidate delegate must not run in hydration worker")

    monkeypatch.setattr(plane, "_ORIGINAL_SERVICE_INGEST", original)
    candidate_plane = SimpleNamespace(scout_wallets=(WALLET,))
    service = SimpleNamespace(_roi_candidate_execution_plane=candidate_plane)
    swap = _swap()

    async def run():
        token = hotpath._CURRENT_HYDRATION_REASON.set("frozen_scout_processed_trigger")
        try:
            return await plane._route_candidate_ingest(service, swap)
        finally:
            hotpath._CURRENT_HYDRATION_REASON.reset(token)

    plane._STORAGE_PRESSURE.clear()
    decision = asyncio.run(run())

    assert calls == []
    assert decision.decision == "candidate_execution_queued"
    assert plane._candidate_queue(candidate_plane).qsize() == 1
    assert plane._STORAGE_PRESSURE.is_set() is True
    plane._STORAGE_PRESSURE.clear()


def test_non_candidate_ingest_stays_on_existing_service_path(monkeypatch):
    calls: list[str] = []
    expected = IngestionDecision(
        signature="fallback",
        token_mint=MINT,
        wallet=WALLET,
        decision="existing_path",
        reason="preserved",
        observed_at=datetime.now(timezone.utc),
        ingestion_latency_ms=1.0,
    )

    async def original(_service, swap):
        calls.append(swap.signature)
        return expected

    monkeypatch.setattr(plane, "_ORIGINAL_SERVICE_INGEST", original)
    candidate_plane = SimpleNamespace(scout_wallets=(WALLET,))
    service = SimpleNamespace(_roi_candidate_execution_plane=candidate_plane)

    result = asyncio.run(plane._route_candidate_ingest(service, _swap()))

    assert result is expected
    assert calls == ["candidate-execution-plane-test"]


def test_bulk_writer_is_sliced_and_candidate_pressure_gets_smaller_lock_holds(monkeypatch):
    chunks: list[int] = []

    def original(_obj, items):
        chunks.append(len(items))
        return len(items)

    monkeypatch.setattr(plane, "_ORIGINAL_FULL_SCOPE_BATCH", original)
    monkeypatch.setattr(plane, "_batch_contains_scout", lambda _items: False)
    monkeypatch.setattr(plane, "CANDIDATE_STORAGE_YIELD_SECONDS", 0.0)
    obj = SimpleNamespace()
    items = [object() for _ in range(40)]

    plane._STORAGE_PRESSURE.clear()
    assert plane._persist_full_scope_with_storage_slices(obj, items) == 40
    assert chunks == [16, 16, 8]

    chunks.clear()
    plane._STORAGE_PRESSURE.set()
    assert plane._persist_full_scope_with_storage_slices(obj, items[:10]) == 10
    assert chunks == [4, 4, 2]
    plane._STORAGE_PRESSURE.clear()


def test_candidate_launch_and_funding_start_concurrently_and_finalize_coverage_once(monkeypatch):
    launch_started = asyncio.Event()
    funding_started = asyncio.Event()
    marks: list[tuple[str, str]] = []

    class Collector:
        def __init__(self, name: str):
            self.name = name

        async def collect(self, _mint, _at):
            if self.name == "launch":
                launch_started.set()
                await funding_started.wait()
            else:
                funding_started.set()
                await launch_started.wait()
            return True

    class Collectors:
        coverage_asserted = True
        launch = Collector("launch")
        funding = Collector("funding")
        risk = SimpleNamespace(store=object())

        async def _safe_bool(self, _name, _mint, _at, awaitable):
            return bool(await awaitable)

    async def original_refresh(*args, **kwargs):
        raise AssertionError("candidate fanout must not fall back to sequential coverage")

    def original_mark(_store, mint, *, assessed_at):
        marks.append((mint, assessed_at))

    monkeypatch.setattr(plane, "_ORIGINAL_REFRESH_COVERAGE", original_refresh)
    monkeypatch.setattr(plane, "_ORIGINAL_MARK_FUNDING", original_mark)
    collectors = Collectors()
    now = datetime.now(timezone.utc)

    async def run():
        token = plane._CANDIDATE_EXECUTION_CONTEXT.set(True)
        try:
            await plane._refresh_coverage_with_candidate_fanout(
                collectors,
                MINT,
                now,
                current_swap=_swap(),
            )
        finally:
            plane._CANDIDATE_EXECUTION_CONTEXT.reset(token)

    asyncio.run(run())

    assert launch_started.is_set() is True
    assert funding_started.is_set() is True
    assert marks == [(MINT, now.isoformat())]


def test_funding_coverage_mark_is_deferred_inside_parallel_candidate_fanout(monkeypatch):
    direct_marks: list[tuple[str, str]] = []

    def original(_store, mint, *, assessed_at):
        direct_marks.append((mint, assessed_at))

    monkeypatch.setattr(plane, "_ORIGINAL_MARK_FUNDING", original)
    state: dict[str, object] = {}
    token = plane._DEFERRED_FUNDING_MARK.set(state)
    try:
        plane._mark_funding_with_candidate_defer(object(), MINT, assessed_at="2026-09-04T00:00:00+00:00")
    finally:
        plane._DEFERRED_FUNDING_MARK.reset(token)

    assert direct_marks == []
    assert state["requested"] is True
    assert state["token_mint"] == MINT


def test_candidate_snapshot_is_append_only_and_has_no_money_authority(tmp_path):
    store = ObservationEventStore(tmp_path / "candidate-plane.sqlite3")
    risk_provider = SimpleNamespace(
        readiness=lambda mint, *, as_of: {"complete": True, "fresh": True}
    )
    runtime_plane = SimpleNamespace(
        store=store,
        service=SimpleNamespace(risk_provider=risk_provider),
    )
    swap = _swap()
    queued_at = datetime.now(timezone.utc)
    job = plane.CandidateExecutionJob(
        swap=swap,
        reason="frozen_scout_processed_trigger",
        queued_at=queued_at,
        queued_monotonic=0.0,
    )
    started = queued_at + timedelta(milliseconds=10)
    completed = started + timedelta(milliseconds=50)
    decision = IngestionDecision(
        signature=swap.signature,
        token_mint=swap.token_mint,
        wallet=swap.wallet,
        decision="shadow_first_touch",
        reason="paper cohort disabled",
        observed_at=swap.observed_at,
        ingestion_latency_ms=swap.ingestion_latency_ms,
    )

    for _ in range(2):
        plane._persist_snapshot_sync(
            runtime_plane,
            job,
            started_at=started,
            completed_at=completed,
            queue_wait_ms=10.0,
            decision=decision,
            timed_out=False,
            error_type=None,
        )

    with store._lock:
        rows = store.db.execute(
            "SELECT signature,risk_complete,risk_fresh,paper_only,live_money_authority "
            "FROM candidate_execution_plane_snapshots"
        ).fetchall()
    assert len(rows) == 1
    assert str(rows[0]["signature"]) == swap.signature
    assert int(rows[0]["risk_complete"]) == 1
    assert int(rows[0]["risk_fresh"]) == 1
    assert int(rows[0]["paper_only"]) == 1
    assert int(rows[0]["live_money_authority"]) == 0


def test_redesign_preserves_all_non_negotiable_authority_and_gate_boundaries():
    assert plane.CANDIDATE_PROCESSING_TARGET_SECONDS == 5.0
    assert plane.CANDIDATE_ENTRY_WINDOW_SECONDS == 20.0
    assert plane.CANDIDATE_EXECUTION_WORKERS == 2
    assert plane.BACKGROUND_SQLITE_SLICE_ROWS < 128
    assert plane.CANDIDATE_SQLITE_SLICE_ROWS < plane.BACKGROUND_SQLITE_SLICE_ROWS
    assert plane.PAPER_ONLY is True
    assert plane.LIVE_MONEY_AUTHORITY is False
    assert plane.SIGNING_AVAILABLE is False
    assert plane.TRANSACTION_SUBMISSION_AVAILABLE is False
    assert plane.FULL_MARKET_OBSERVATION_REDUCED is False
    assert plane.CERTIFICATION_THRESHOLDS_CHANGED is False
