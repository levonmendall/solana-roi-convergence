from __future__ import annotations

import asyncio
import sqlite3
import threading
from datetime import datetime, timezone
from types import SimpleNamespace

from solana_roi import v51_measurement_integrity as measurement
from solana_roi.v51_candidate_ledger import (
    ensure_schema,
    record_solana_candidate,
    record_stage_event,
    refresh_candidate_pipeline,
)
from solana_roi.v51_measurement_integrity_hardening import _ensure_release_compatibility_fail_closed
from solana_roi.v51_strategy_api import _robinhood_coverage_text


class Store:
    def __init__(self) -> None:
        self.db = sqlite3.connect(":memory:", check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        self.events: list[tuple[str, str, dict]] = []

    def append(self, event_type: str, observed_at: str, payload: dict) -> None:
        self.events.append((event_type, observed_at, payload))


def _swap(signature: str = "sig-1") -> SimpleNamespace:
    observed = datetime(2026, 9, 5, 20, 0, tzinfo=timezone.utc)
    received = datetime(2026, 9, 5, 20, 0, 1, tzinfo=timezone.utc)
    return SimpleNamespace(
        signature=signature,
        slot=123,
        observed_at=observed,
        received_at=received,
        wallet="wallet-a",
        token_mint="mint-a",
        side="buy",
        token_amount=100.0,
        native_amount_sol=1.0,
        reference_price_sol=0.01,
        source="solana-direct:PUMP_AMM:buy",
        ingestion_latency_ms=1000.0,
    )


def test_canonical_candidate_ingress_and_stage_history_are_append_only(monkeypatch) -> None:
    monkeypatch.setenv("GITHUB_SHA", "a" * 40)
    store = Store()
    swap = _swap()

    assert record_solana_candidate(store, swap)
    assert not record_solana_candidate(store, swap)

    with store._lock:
        candidate_count = store.db.execute("SELECT COUNT(*) FROM v51_candidates").fetchone()[0]
        first_events = store.db.execute(
            "SELECT stage,status FROM v51_candidate_stage_events ORDER BY id"
        ).fetchall()
    assert candidate_count == 1
    assert [(row["stage"], row["status"]) for row in first_events] == [
        ("ingestion", "complete"),
        ("candidate", "complete"),
    ]

    assert record_stage_event(
        store,
        surface="SOLANA",
        candidate_id=swap.signature,
        release_commit="a" * 40,
        stage="context",
        status="coverage_debt",
        reason="context_pending",
    )
    assert record_stage_event(
        store,
        surface="SOLANA",
        candidate_id=swap.signature,
        release_commit="a" * 40,
        stage="context",
        status="complete",
        reason="context_ready",
    )
    assert not record_stage_event(
        store,
        surface="SOLANA",
        candidate_id=swap.signature,
        release_commit="a" * 40,
        stage="context",
        status="complete",
        reason="context_ready",
    )

    with store._lock:
        history = store.db.execute(
            "SELECT status,reason FROM v51_candidate_stage_events "
            "WHERE stage='context' ORDER BY id"
        ).fetchall()
        current = store.db.execute(
            "SELECT status,reason FROM v51_candidate_current_state "
            "WHERE surface='SOLANA' AND candidate_id=? AND stage='context'",
            (swap.signature,),
        ).fetchone()
    assert [(row["status"], row["reason"]) for row in history] == [
        ("coverage_debt", "context_pending"),
        ("complete", "context_ready"),
    ]
    assert dict(current) == {"status": "complete", "reason": "context_ready"}


def test_candidate_coverage_uses_canonical_ledger_not_wallet_discovery(monkeypatch) -> None:
    monkeypatch.setenv("GITHUB_SHA", "b" * 40)
    store = Store()
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE wallet_discovery_forward_observations ("
            "signature TEXT,side TEXT,copyable INTEGER,release_commit TEXT,token_mint TEXT,wallet TEXT,source TEXT,received_at TEXT)"
        )
        store.db.execute(
            "INSERT INTO wallet_discovery_forward_observations VALUES ('legacy-only','buy',1,?,?,?,?,?)",
            ("b" * 40, "mint-old", "wallet-old", "wallet-discovery", "2026-09-05T20:00:00+00:00"),
        )
    empty = refresh_candidate_pipeline(store)
    assert empty["source_candidates_seen"]["solana"] == 0
    assert empty["candidate_source_of_truth"].startswith("append_only_v51_candidates")

    record_solana_candidate(store, _swap("canonical-1"))
    populated = refresh_candidate_pipeline(store)
    assert populated["source_candidates_seen"]["solana"] == 1
    assert populated["coverage_debt_count"] == 1
    assert populated["coverage_complete"] is False


def test_measurement_compatibility_fails_known_and_unclassified_history_closed(monkeypatch) -> None:
    current = "c" * 40
    monkeypatch.setenv("GITHUB_SHA", current)
    store = Store()

    bad = _ensure_release_compatibility_fail_closed(
        store, "f74b9d3db7c1bae081ea91e2853dc1af6095ec2d"
    )
    assert bad is not None
    assert int(bad["promotion_eligible"]) == 0

    unknown_history = _ensure_release_compatibility_fail_closed(store, "d" * 40)
    assert unknown_history is not None
    assert int(unknown_history["promotion_eligible"]) == 0
    assert str(unknown_history["measurement_epoch"]) == "unclassified-historical-release"

    # The hardening helper delegates current-release registration to the base
    # function exactly as production does after installation.
    monkeypatch.setattr(
        measurement,
        "_ORIGINAL_ENSURE_RELEASE_COMPATIBILITY",
        measurement.ensure_release_compatibility,
        raising=False,
    )
    current_row = _ensure_release_compatibility_fail_closed(store, current)
    assert current_row is not None
    assert int(current_row["promotion_eligible"]) == 1
    assert str(current_row["measurement_epoch"]) == measurement.MEASUREMENT_EPOCH


def test_wallet_research_classification_keeps_v51_challenger_bands(monkeypatch) -> None:
    monkeypatch.setenv("GITHUB_SHA", "e" * 40)
    store = Store()
    ensure_schema(store)
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE wallet_discovery_forward_observations ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,signature TEXT UNIQUE,wallet TEXT,token_mint TEXT,side TEXT,"
            "token_amount REAL,observed_at TEXT,received_at TEXT,wallet_price_sol REAL,copyable_price_sol REAL,"
            "chase_fraction REAL,copyable INTEGER,observation_lag_ms REAL,risk_complete INTEGER,"
            "manipulation_flag INTEGER,side_wallet_flag INTEGER,source TEXT)"
        )

    async def legacy_insert(self, swap) -> bool:
        chase = float(swap.test_chase)
        with self.store._lock, self.store.db:
            cursor = self.store.db.execute(
                "INSERT OR IGNORE INTO wallet_discovery_forward_observations("
                "signature,wallet,token_mint,side,token_amount,observed_at,received_at,wallet_price_sol,copyable_price_sol,"
                "chase_fraction,copyable,observation_lag_ms,risk_complete,manipulation_flag,side_wallet_flag,source) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    swap.signature,
                    "wallet-a",
                    "mint-a",
                    "buy",
                    1.0,
                    "2026-09-05T20:00:00+00:00",
                    "2026-09-05T20:00:01+00:00",
                    1.0,
                    1.0 + chase,
                    chase,
                    1 if chase <= 0.15 else 0,
                    1000.0,
                    1,
                    0,
                    0,
                    "wallet-discovery-forward:test",
                ),
            )
        return cursor.rowcount == 1

    monkeypatch.setattr(measurement, "_ORIGINAL_WALLET_RECORD", legacy_insert)
    dummy = SimpleNamespace(
        store=store,
        policy=SimpleNamespace(max_observation_lag_seconds=20.0),
    )

    async def record(chase: float, signature: str) -> tuple[int, str]:
        swap = SimpleNamespace(signature=signature, test_chase=chase)
        await measurement._wallet_record_with_v51_research_lineage(dummy, swap)
        with store._lock:
            row = store.db.execute(
                "SELECT copyable FROM wallet_discovery_forward_observations WHERE signature=?",
                (signature,),
            ).fetchone()
            lineage = store.db.execute(
                "SELECT chase_band,research_eligible,measurement_epoch FROM v51_wallet_discovery_forward_lineage WHERE signature=?",
                (signature,),
            ).fetchone()
        assert lineage["measurement_epoch"] == measurement.MEASUREMENT_EPOCH
        return int(row["copyable"]), str(lineage["chase_band"])

    assert asyncio.run(record(0.20, "chase-20")) == (1, "challenger_15_25pct")
    assert asyncio.run(record(0.35, "chase-35")) == (1, "challenger_25_40pct")
    assert asyncio.run(record(0.41, "chase-41")) == (0, "observe_only_gt_40pct")


def test_proof_metadata_and_robinhood_wording_are_state_explicit(monkeypatch) -> None:
    monkeypatch.setenv("GITHUB_SHA", "f" * 40)
    store = Store()
    ensure_schema(store)
    meta = measurement.proof_metadata(store)
    for key in (
        "generated_at",
        "evidence_through",
        "release_commit",
        "economic_freeze_epoch",
        "measurement_epoch",
        "execution_model_epoch",
        "measurement_fingerprint",
        "execution_model_fingerprint",
        "proof_state",
        "proof_max_age_seconds",
    ):
        assert key in meta
    assert meta["measurement_epoch"] == measurement.MEASUREMENT_EPOCH
    assert _robinhood_coverage_text("unavailable", False).startswith("unavailable:")
    assert _robinhood_coverage_text("stale", False).startswith("stale:")
    assert _robinhood_coverage_text("epoch_mismatch", False).startswith("epoch_mismatch:")
    assert _robinhood_coverage_text("confirmed", True).startswith("confirmed:")
