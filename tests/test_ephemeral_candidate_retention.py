from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

from solana_roi import ephemeral_candidate_retention as retention
from solana_roi.config import BASELINE
from solana_roi.direct_solana import DirectSolanaJournal
from solana_roi.observation_store import ObservationEventStore
from solana_roi.storage import AppendOnlyEventStore


def _now() -> datetime:
    return datetime.now(timezone.utc)


def test_stale_candidate_hydration_is_pruned_without_rpc(tmp_path, monkeypatch):
    store = ObservationEventStore(tmp_path / "stale.sqlite3")
    journal = DirectSolanaJournal(store)
    retention._ensure_schema(store)
    trigger = _now() - timedelta(minutes=10)
    journal.enqueue(
        signature="stale-scout-signature",
        slot=1,
        trigger_received_at=trigger,
        source_hint=None,
        priority=0,
        reason="frozen_scout_processed_trigger",
    )
    row = journal.claim()
    assert row is not None

    calls: list[str] = []

    async def original(_self, claimed):
        calls.append(str(claimed["signature"]))

    monkeypatch.setattr(retention, "_ORIGINAL_HYDRATE_ONE", original)
    plane = type("Plane", (), {})()
    plane.store = store
    plane.journal = journal

    asyncio.run(retention._bounded_ephemeral_hydrate_one(plane, row))

    assert calls == []
    with store._lock:
        assert store.db.execute(
            "SELECT COUNT(*) FROM direct_solana_hydration_queue WHERE signature='stale-scout-signature'"
        ).fetchone()[0] == 0
        outcome = store.db.execute(
            "SELECT SUM(count) FROM anonymous_certification_outcomes "
            "WHERE reason='frozen_scout_processed_trigger' AND outcome='expired_before_entry'"
        ).fetchone()[0]
    assert int(outcome or 0) == 1
    store.close()


def test_completed_candidate_hydration_tracks_only_ephemeral_strategy_state(tmp_path, monkeypatch):
    store = ObservationEventStore(tmp_path / "track.sqlite3")
    DirectSolanaJournal(store)
    retention._ensure_schema(store)
    trigger = _now()
    row = {
        "signature": "sig-track",
        "slot": 1,
        "trigger_received_at": trigger.isoformat(),
        "source_hint": "PUMP_FUN",
        "priority": 0,
        "reason": "frozen_scout_processed_trigger",
        "attempts": 0,
    }

    async def original(_self, claimed):
        store.record_swap(
            signature=str(claimed["signature"]),
            slot=1,
            observed_at=trigger.isoformat(),
            received_at=trigger.isoformat(),
            wallet="wallet-a",
            token_mint="MINT-TRACK",
            side="buy",
            token_amount=1.0,
            native_amount_sol=1.0,
            reference_price_sol=1.0,
            ingestion_latency_ms=1.0,
            source="solana-direct:PUMP_FUN:program",
        )

    monkeypatch.setattr(retention, "_ORIGINAL_HYDRATE_ONE", original)
    plane = type("Plane", (), {})()
    plane.store = store

    asyncio.run(retention._bounded_ephemeral_hydrate_one(plane, row))

    with store._lock:
        state = store.db.execute(
            "SELECT first_seen_at, expires_at FROM ephemeral_candidate_state WHERE token_mint='MINT-TRACK'"
        ).fetchone()
        swap_count = store.db.execute(
            "SELECT COUNT(*) FROM normalized_swaps WHERE token_mint='MINT-TRACK'"
        ).fetchone()[0]
    assert state is not None
    assert datetime.fromisoformat(str(state["expires_at"])) == trigger + timedelta(seconds=20)
    assert swap_count == 1
    store.close()


def test_expired_candidate_state_is_deleted_but_canonical_evidence_remains(tmp_path):
    store = ObservationEventStore(tmp_path / "durable.sqlite3")
    retention._ensure_schema(store)
    now = _now()
    old = now - timedelta(minutes=1)
    mint = "MINT-DURABLE"

    with store._lock, store.db:
        store.db.execute(
            "INSERT INTO ephemeral_candidate_state(token_mint, first_seen_at, expires_at, source) VALUES (?, ?, ?, ?)",
            (mint, old.isoformat(), (old + timedelta(seconds=20)).isoformat(), "PUMP_FUN"),
        )
    store.record_swap(
        signature="sig-durable",
        slot=1,
        observed_at=old.isoformat(),
        received_at=old.isoformat(),
        wallet="wallet-x",
        token_mint=mint,
        side="buy",
        token_amount=1.0,
        native_amount_sol=1.0,
        reference_price_sol=1.0,
        ingestion_latency_ms=1.0,
        source="solana-direct:PUMP_FUN:program",
    )
    store.claim_first_touch(
        token_mint=mint,
        signature="sig-durable",
        wallet="wallet-x",
        entity_id="entity-x",
        tier="S",
        observed_at=old.isoformat(),
        reference_price_sol=1.0,
    )
    store.record_risk_evidence(
        token_mint=mint,
        dimension="mint_authority",
        observed_at=old.isoformat(),
        received_at=old.isoformat(),
        source="rpc",
        payload={"safe": True},
    )
    store.append(
        "normalized_swap",
        old.isoformat(),
        {"token_mint": mint, "signature": "sig-durable"},
    )

    result = retention._reap_sqlite(store.path, now)
    assert result["candidate_mints"] == [mint]

    with store._lock:
        state_count = store.db.execute(
            "SELECT COUNT(*) FROM ephemeral_candidate_state WHERE token_mint=?", (mint,)
        ).fetchone()[0]
        swap_count = store.db.execute(
            "SELECT COUNT(*) FROM normalized_swaps WHERE token_mint=?", (mint,)
        ).fetchone()[0]
        touch_count = store.db.execute(
            "SELECT COUNT(*) FROM token_first_touches WHERE token_mint=?", (mint,)
        ).fetchone()[0]
        risk_count = store.db.execute(
            "SELECT COUNT(*) FROM risk_evidence WHERE token_mint=?", (mint,)
        ).fetchone()[0]
        event_count = store.db.execute("SELECT COUNT(*) FROM events").fetchone()[0]

    assert state_count == 0
    assert swap_count == 1
    assert touch_count == 1
    assert risk_count == 1
    assert event_count == 1
    store.close()


def test_unentered_engine_candidate_is_purged_but_position_is_never_removed():
    class Engine:
        def __init__(self):
            self.portfolio = type("Portfolio", (), {"positions": {"MINT-ENTERED": object()}})()
            self.strategy = type(
                "Strategy",
                (),
                {"candidates": {"MINT-ENTERED": object(), "MINT-STALE": object()}},
            )()
            self.marks = {"MINT-ENTERED": 1.0, "MINT-STALE": 2.0}
            self.saved = 0

        def _save_checkpoint(self):
            self.saved += 1

    engine = Engine()
    plane = type("Plane", (), {})()
    plane.service = type("Service", (), {"engine": engine})()

    retention._purge_engine_candidates(plane, ["MINT-ENTERED", "MINT-STALE"])

    assert "MINT-ENTERED" in engine.strategy.candidates
    assert "MINT-ENTERED" in engine.marks
    assert "MINT-STALE" not in engine.strategy.candidates
    assert "MINT-STALE" not in engine.marks
    assert engine.saved == 1


def test_append_only_observation_ledger_is_not_intercepted_by_retention(tmp_path):
    store = ObservationEventStore(tmp_path / "events.sqlite3")
    observed = _now().isoformat()
    lineage = store.append(
        "normalized_swap",
        observed,
        {"token_mint": "MINT-LEDGER", "signature": "sig-ledger"},
    )
    assert lineage and not lineage.startswith("ephemeral:")
    with store._lock:
        assert store.db.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 1
    assert not bool(getattr(AppendOnlyEventStore.append, "_roi_ephemeral_candidate_retention", False))
    store.close()


def test_frozen_strategy_and_recovery_invariants_unchanged():
    assert retention.ENTRY_WINDOW_SECONDS == BASELINE.confirmation_window_seconds == 20.0
    assert BASELINE.max_chase_fraction == 0.15
    assert "gap_backfill" not in retention.EPHEMERAL_HYDRATION_REASONS
