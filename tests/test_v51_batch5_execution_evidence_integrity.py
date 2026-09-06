from __future__ import annotations

import sqlite3
import threading

import pytest

from solana_roi.strategy_v51_authority import AUTHORITY_ID, ECONOMIC_FREEZE_EPOCH
from solana_roi.v51_economic_certification import build_economic_certification
from solana_roi.v51_execution_evidence_integrity import (
    REQUIRED_LINEAGE_KEYS,
    dedupe_economic_samples,
    economic_sample_id,
    persist_complete_settlement_lineage,
    reconstruct_settlement_lineage,
    validate_settlement_lineage,
)
from solana_roi.v51_promotion_proof import event_cluster_id


class Store:
    def __init__(self) -> None:
        self.db = sqlite3.connect(":memory:", check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self._lock = threading.RLock()


def _lineage(**overrides):
    values = {
        "settlement_id": "settlement-1",
        "exit_quote_or_reason": "exit-quote-1",
        "position_id": "position-1",
        "entry_id": "entry-1",
        "entry_quote_id": "entry-quote-1",
        "authorization_id": "authorization-1",
        "sizing_id": "sizing-1",
        "strategy_evaluation_id": "evaluation-1",
        "candidate_id": "candidate-1",
        "wallet_entity_source_signal_id": "wallet-signal-1",
        "normalized_event_id": "normalized-1",
        "source_observation_signature": "signature-1",
        "release_commit": "release-1",
        "strategy_authority_id": AUTHORITY_ID,
        "economic_epoch": ECONOMIC_FREEZE_EPOCH,
        "measurement_epoch": "measurement-batch5",
        "economic_event_root_id": "economic-root-1",
        "token_mint": "TOKEN",
        "lifecycle": "continuation",
    }
    values.update(overrides)
    return values


def test_complete_settlement_lineage_requires_every_backward_trace_node() -> None:
    complete = validate_settlement_lineage(_lineage())
    assert complete["complete"] is True
    assert complete["evidence_eligible"] is True
    assert complete["missing"] == []

    for key in REQUIRED_LINEAGE_KEYS:
        broken = _lineage(**{key: None})
        status = validate_settlement_lineage(broken)
        assert status["complete"] is False
        assert status["evidence_eligible"] is False
        assert key in status["missing"]


def test_complete_lineage_persists_and_reconstructs_backward_exactly() -> None:
    store = Store()
    lineage = _lineage()
    assert persist_complete_settlement_lineage(store, lineage) is True
    rebuilt = reconstruct_settlement_lineage(
        store,
        release_commit=lineage["release_commit"],
        settlement_id=lineage["settlement_id"],
    )
    assert rebuilt is not None
    for key in REQUIRED_LINEAGE_KEYS:
        assert rebuilt[key] == lineage[key]
    assert rebuilt["economic_sample_id"] == economic_sample_id(lineage)
    assert persist_complete_settlement_lineage(store, lineage) is False


def test_incomplete_lineage_cannot_be_persisted_as_evidence() -> None:
    store = Store()
    with pytest.raises(ValueError, match="incomplete_evidence_lineage:exit_quote_or_reason"):
        persist_complete_settlement_lineage(store, _lineage(exit_quote_or_reason=None))


def test_family_analytics_stay_isolated_but_economic_sample_is_shared() -> None:
    left = {
        "token_mint": "TOKEN",
        "lifecycle": "continuation",
        "family": "PUMP_FUN",
        "surface": "SOLANA_ALPHA",
        "venue": "PUMP_FUN",
        "source_signature": "signature-pump",
    }
    right = {
        "token_mint": "TOKEN",
        "lifecycle": "continuation",
        "family": "RAYDIUM",
        "surface": "SOLANA_ALPHA",
        "venue": "RAYDIUM",
        "source_signature": "signature-raydium-migration",
    }
    assert event_cluster_id(left, family="PUMP_FUN") != event_cluster_id(right, family="RAYDIUM")
    assert economic_sample_id(left) == economic_sample_id(right)


def test_replay_recovery_and_venue_migration_do_not_inflate_samples() -> None:
    rows = [
        {
            "release_commit": "r1",
            "source_signature": "sig-a",
            "token_mint": "TOKEN",
            "lifecycle": "continuation",
            "family": "PUMP_FUN",
            "surface": "SOLANA_ALPHA",
            "settled_at": "2026-09-06T00:00:00+00:00",
        },
        {
            "release_commit": "r1",
            "source_signature": "sig-a-replayed",
            "token_mint": "TOKEN",
            "lifecycle": "continuation",
            "family": "PUMP_AMM",
            "surface": "SOLANA_ALPHA",
            "settled_at": "2026-09-06T00:00:01+00:00",
        },
        {
            "release_commit": "r1",
            "source_signature": "sig-a-fomo-context",
            "token_mint": "TOKEN",
            "lifecycle": "continuation",
            "family": "FOMO_CLEAN",
            "surface": "FOMO",
            "settled_at": "2026-09-06T00:00:02+00:00",
        },
        {
            "release_commit": "r1",
            "source_signature": "sig-b",
            "token_mint": "OTHER",
            "lifecycle": "continuation",
            "family": "RAYDIUM",
            "surface": "SOLANA_ALPHA",
            "settled_at": "2026-09-06T00:00:03+00:00",
        },
    ]
    kept, duplicates = dedupe_economic_samples(rows)
    assert len(kept) == 2
    assert len(duplicates) == 2
    assert len({row["economic_sample_id"] for row in kept}) == 2


def _seed_cross_family_duplicate(store: Store) -> None:
    with store._lock, store.db:
        store.db.executescript(
            "CREATE TABLE v51_economic_freeze_releases ("
            "release_commit TEXT NOT NULL,economic_freeze_epoch TEXT NOT NULL,authority_id TEXT NOT NULL);"
            "CREATE TABLE risk_conditioned_alpha_v5_outcomes ("
            "id INTEGER PRIMARY KEY,release_commit TEXT NOT NULL,source_signature TEXT NOT NULL,token_mint TEXT,"
            "lane TEXT,venue TEXT,lifecycle TEXT,regime TEXT,risk_signature TEXT,context_key TEXT,"
            "position_fraction REAL,net_return REAL,settled_at TEXT);"
            "CREATE TABLE risk_conditioned_alpha_v5_trials ("
            "id INTEGER PRIMARY KEY,release_commit TEXT NOT NULL,source_signature TEXT NOT NULL,lane TEXT,"
            "trigger_wallet TEXT,flow_state TEXT,risk_severity REAL,chase_band TEXT,latency_band TEXT,"
            "round_trip_cost_fraction REAL);"
            "CREATE TABLE fomo_paper_outcomes ("
            "id INTEGER PRIMARY KEY,release_commit TEXT NOT NULL,source_signature TEXT NOT NULL,"
            "token_mint TEXT,trigger_wallet TEXT,venue TEXT,lifecycle TEXT,regime TEXT,"
            "position_fraction REAL,net_return REAL,settled_at TEXT);"
            "CREATE TABLE fomo_paper_trials ("
            "id INTEGER PRIMARY KEY,release_commit TEXT NOT NULL,source_signature TEXT NOT NULL,"
            "fomo_state TEXT,signal_to_entry_seconds REAL,entry_cost_sol REAL);"
        )
        store.db.execute(
            "INSERT INTO v51_economic_freeze_releases VALUES (?,?,?)",
            ("canonical-release", ECONOMIC_FREEZE_EPOCH, AUTHORITY_ID),
        )
        store.db.execute(
            "INSERT INTO risk_conditioned_alpha_v5_trials VALUES (?,?,?,?,?,?,?,?,?,?)",
            (1, "canonical-release", "solana-sig", "lane", "wallet", "neutral", 0.0, "le_15pct", "le_2s", 0.02),
        )
        store.db.execute(
            "INSERT INTO risk_conditioned_alpha_v5_outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                1,
                "canonical-release",
                "solana-sig",
                "TOKEN",
                "lane",
                "PUMP_FUN",
                "continuation",
                "regime",
                "clean",
                "",
                0.01,
                0.10,
                "2026-09-06T00:00:00+00:00",
            ),
        )
        store.db.execute(
            "INSERT INTO fomo_paper_trials VALUES (?,?,?,?,?,?)",
            (1, "canonical-release", "fomo-sig", "active_fomo", 2.0, 0.001),
        )
        store.db.execute(
            "INSERT INTO fomo_paper_outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (
                1,
                "canonical-release",
                "fomo-sig",
                "TOKEN",
                "wallet",
                "PUMP_AMM",
                "continuation",
                "regime",
                0.01,
                0.20,
                "2026-09-06T00:00:01+00:00",
            ),
        )


def test_economic_certification_counts_cross_family_duplicate_once() -> None:
    store = Store()
    _seed_cross_family_duplicate(store)
    proof = build_economic_certification(store)
    assert proof["raw_closed_outcome_count"] == 2
    assert proof["closed_outcome_count"] == 1
    assert proof["duplicate_economic_sample_count"] == 1
    assert sum(int(item["closed_outcome_count"]) for item in proof["families"].values()) == 1
