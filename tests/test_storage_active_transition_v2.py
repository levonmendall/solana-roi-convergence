from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from pathlib import Path

import pytest

from solana_roi import storage_manifest
from solana_roi.active_runtime import ActiveDurablePaperTradingEngine, ActiveObservationEventStore
from solana_roi.active_storage import ActiveStorage
from solana_roi.certification_active_manifest import install_active_certification_manifest
from solana_roi.config import BASELINE
from solana_roi.storage_current_state_extractor import LegacyCurrentStateExtractor
from solana_roi.storage_shadow_migration import build_shadow_database, read_logical_truth
from solana_roi.storage_transition import (
    ACTIVE_PATH_ENV,
    ACTIVATE_ENV,
    LEGACY_PATH_ENV,
    build_checkpoint_payload,
    load_verified_checkpoint,
    persist_verified_checkpoint,
    select_runtime_database_from_environment,
    verify_semantic_equivalence,
)


RELEASE = "a" * 40
ANCHOR_ID = 41
ANCHOR_HASH = hashlib.sha256(b"sealed-legacy-head").hexdigest()


def _paper_state() -> dict[str, object]:
    return {
        "schema": "roi-convergence-paper-engine-checkpoint.v1",
        "strategy_version": BASELINE.version,
        "initial_capital_usd": BASELINE.initial_capital_usd,
        "cash_usd": BASELINE.initial_capital_usd,
        "marks": {},
        "trade_start_nav": {},
        "candidates": {},
        "positions": {},
        "closed": [],
    }


def _make_legacy(path: Path) -> None:
    state = _paper_state()
    raw = json.dumps(state, sort_keys=True, separators=(",", ":"), allow_nan=False)
    digest = hashlib.sha256(raw.encode()).hexdigest()
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE events(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                previous_hash TEXT,
                lineage_hash TEXT NOT NULL UNIQUE
            );
            CREATE TABLE paper_engine_checkpoint(
                id INTEGER PRIMARY KEY CHECK(id=1),
                saved_at TEXT NOT NULL,
                last_engine_event_id INTEGER NOT NULL,
                state_json TEXT NOT NULL,
                state_sha256 TEXT NOT NULL
            );
            CREATE TABLE wallet_profiles(
                wallet TEXT PRIMARY KEY, entity_id TEXT NOT NULL, tier TEXT NOT NULL,
                first_touch_sample_size INTEGER NOT NULL, historically_eligible INTEGER NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE v52_market_validation_features(
                id INTEGER PRIMARY KEY AUTOINCREMENT, lane TEXT NOT NULL,
                observed_at TEXT NOT NULL, discovery_route TEXT, market_state TEXT,
                market_archetype TEXT, feature TEXT NOT NULL, value REAL NOT NULL,
                paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL
            );
            CREATE TABLE v52_wallet_forward_validation(
                id INTEGER PRIMARY KEY AUTOINCREMENT, evaluated_at TEXT NOT NULL,
                status TEXT NOT NULL, strategy_influence_enabled INTEGER NOT NULL,
                influence_scope_json TEXT NOT NULL, reasons_json TEXT NOT NULL,
                windows_json TEXT NOT NULL, paper_only INTEGER NOT NULL,
                UNIQUE(evaluated_at)
            );
            """
        )
        conn.execute(
            "INSERT INTO events(id,event_type,observed_at,payload_json,previous_hash,lineage_hash) "
            "VALUES(?,?,?,?,?,?)",
            (ANCHOR_ID, "price", "2026-09-13T00:00:00+00:00", "{}", None, ANCHOR_HASH),
        )
        conn.execute(
            "INSERT INTO paper_engine_checkpoint(id,saved_at,last_engine_event_id,state_json,state_sha256) "
            "VALUES(1,?,?,?,?)",
            ("2026-09-13T00:00:01+00:00", ANCHOR_ID, raw, digest),
        )
        conn.execute(
            "INSERT INTO wallet_profiles VALUES(?,?,?,?,?,?)",
            ("wallet-a", "entity-a", "S", 100, 1, "2026-09-13T00:00:00+00:00"),
        )
        conn.executemany(
            "INSERT INTO v52_market_validation_features(lane,observed_at,feature,value,paper_only,live_money_authority) "
            "VALUES('pump_fun','2026-09-13T00:00:00+00:00','velocity',?,1,0)",
            [(float(i),) for i in range(300)],
        )
        conn.execute(
            "INSERT INTO v52_wallet_forward_validation(evaluated_at,status,strategy_influence_enabled,influence_scope_json,reasons_json,windows_json,paper_only) "
            "VALUES(?,?,?,?,?,?,1)",
            ("2026-09-13T00:00:00+00:00", "VALIDATION INCOMPLETE — MORE EVIDENCE REQUIRED", 0, "[]", "[]", "[]"),
        )
        conn.commit()


def _truth() -> dict[str, object]:
    return {
        "strategy": {},
        "wallet": {},
        "wallet_evidence_watermarks": {},
        "provider_source": {},
        "freshness": {},
        "latest_event_ids": {
            "paper_engine_event_id": ANCHOR_ID,
            "events": {
                "id": ANCHOR_ID,
                "event_type": "price",
                "observed_at": "2026-09-13T00:00:00+00:00",
                "lineage_hash": ANCHOR_HASH,
                "previous_hash": None,
            },
        },
        "active_candidates": {},
        "active_lifecycles": {},
        "portfolio": {
            "saved_at": "2026-09-13T00:00:01+00:00",
            "last_engine_event_id": ANCHOR_ID,
            "state_sha256": hashlib.sha256(
                json.dumps(_paper_state(), sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
            ).hexdigest(),
            "state": _paper_state(),
        },
        "replication_watermarks": {},
        "certification": {},
        "continuity": {},
    }


def _make_active_checkpoint(path: Path, truth: dict[str, object] | None = None) -> ActiveStorage:
    storage = ActiveStorage(path)
    storage.initialize(epoch_id="test-epoch")
    payload = build_checkpoint_payload(
        release_sha=RELEASE,
        current_truth=truth or _truth(),
        provenance={"test": True},
    )
    persist_verified_checkpoint(storage, checkpoint_payload=payload, source_truth=truth or _truth())
    return storage


def test_positive_manifest_has_current_v52_and_no_legacy_unclassified() -> None:
    required = {
        "v52_market_validation_features",
        "v52_market_validation_shadow_outcomes",
        "v52_market_validation_point_in_time",
        "v52_wallet_point_in_time_observations",
        "v52_wallet_forward_outcomes",
        "v52_wallet_integrity_snapshots",
        "v52_wallet_forward_validation",
        "wallet_intelligence_snapshots",
        "paper_engine_checkpoint",
    }
    assert required <= set(storage_manifest.RETENTION_REGISTRY)
    assert all(
        c.retention_class is not storage_manifest.RetentionClass.LEGACY_UNCLASSIFIED
        for c in storage_manifest.RETENTION_REGISTRY.values()
    )


def test_market_validation_pruning_preserves_exact_latest_250(tmp_path: Path) -> None:
    path = tmp_path / "active.sqlite3"
    storage = ActiveStorage(path)
    storage.initialize(epoch_id="prune")
    with storage.connect() as conn:
        conn.execute(
            "CREATE TABLE v52_market_validation_features("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,lane TEXT NOT NULL,observed_at TEXT NOT NULL,"
            "feature TEXT NOT NULL,value REAL NOT NULL)"
        )
        conn.executemany(
            "INSERT INTO v52_market_validation_features(lane,observed_at,feature,value) VALUES('pump_fun','2026-09-13', 'velocity', ?)",
            [(float(i),) for i in range(300)],
        )
        conn.commit()
    result = storage.prune_v52_market_validation()
    assert result["features"] == 50
    with storage.connect() as conn:
        rows = conn.execute(
            "SELECT value FROM v52_market_validation_features WHERE lane='pump_fun' AND feature='velocity' ORDER BY id DESC"
        ).fetchall()
    assert len(rows) == 250
    assert [float(r[0]) for r in rows] == [float(i) for i in range(299, 49, -1)]


def test_semantic_equivalence_detects_each_section_change() -> None:
    source = _truth()
    payload = build_checkpoint_payload(release_sha=RELEASE, current_truth=source, provenance={})
    assert verify_semantic_equivalence(source, payload).equivalent
    for section in (
        "strategy", "wallet", "wallet_evidence_watermarks", "provider_source", "freshness",
        "latest_event_ids", "active_candidates", "active_lifecycles", "portfolio",
        "replication_watermarks", "certification", "continuity",
    ):
        changed = dict(payload)
        changed[section] = {"deliberate_mismatch": section}
        verification = verify_semantic_equivalence(source, changed)
        assert not verification.equivalent
        assert section in verification.mismatched_sections


def test_release_bound_checkpoint_is_fail_closed(tmp_path: Path) -> None:
    path = tmp_path / "active.sqlite3"
    _make_active_checkpoint(path)
    assert load_verified_checkpoint(path, expected_release_sha=RELEASE)["release_sha"] == RELEASE
    with pytest.raises(RuntimeError, match="release SHA"):
        load_verified_checkpoint(path, expected_release_sha="b" * 40)


@pytest.mark.parametrize("size_gib", [1, 5, 10])
def test_active_selection_does_not_read_large_legacy_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, size_gib: int) -> None:
    active = tmp_path / "active.sqlite3"
    _make_active_checkpoint(active)
    legacy = tmp_path / "legacy.sqlite3"
    with legacy.open("wb") as handle:
        handle.truncate(size_gib * 1024**3)
    monkeypatch.setenv(ACTIVATE_ENV, "1")
    monkeypatch.setenv(ACTIVE_PATH_ENV, str(active))
    monkeypatch.setenv(LEGACY_PATH_ENV, str(legacy))
    assert select_runtime_database_from_environment(expected_release_sha=RELEASE) == active
    assert legacy.stat().st_size == size_gib * 1024**3


def test_extractor_requires_exact_paper_checkpoint_digest(tmp_path: Path) -> None:
    legacy = tmp_path / "legacy.sqlite3"
    _make_legacy(legacy)
    with sqlite3.connect(legacy) as conn:
        conn.execute("UPDATE paper_engine_checkpoint SET state_sha256='bad' WHERE id=1")
        conn.commit()
    with pytest.raises(RuntimeError, match="digest mismatch"):
        LegacyCurrentStateExtractor(legacy).extract()


def test_shadow_build_is_exact_and_bounded(tmp_path: Path) -> None:
    legacy = tmp_path / "legacy.sqlite3"
    active = tmp_path / "active.sqlite3"
    _make_legacy(legacy)
    report = build_shadow_database(
        legacy_path=legacy,
        active_path=active,
        release_sha=RELEASE,
    )
    assert report.equivalent
    assert report.mismatched_sections == ()
    assert report.copied_rows["v52_market_validation_features"] == 250
    assert report.active_size_bytes < 1024**3
    extraction = LegacyCurrentStateExtractor(legacy).extract()
    assert verify_semantic_equivalence(extraction.truth, read_logical_truth(active)).equivalent


def test_active_runtime_continues_event_ids_and_hash_without_legacy(tmp_path: Path) -> None:
    legacy = tmp_path / "legacy.sqlite3"
    active = tmp_path / "active.sqlite3"
    _make_legacy(legacy)
    build_shadow_database(legacy_path=legacy, active_path=active, release_sha=RELEASE)
    legacy.unlink()
    assert not legacy.exists()
    store = ActiveObservationEventStore(active, expected_release_sha=RELEASE)
    lineage = store.append("price", "2026-09-13T00:00:02+00:00", {"token_mint": "x", "price": 1.0})
    with store._lock:
        row = store.db.execute("SELECT id,previous_hash,lineage_hash FROM events ORDER BY id DESC LIMIT 1").fetchone()
    assert int(row["id"]) == ANCHOR_ID + 1
    assert str(row["previous_hash"]) == ANCHOR_HASH
    assert str(row["lineage_hash"]) == lineage
    assert store.verify()
    engine = ActiveDurablePaperTradingEngine(store=store)
    assert engine.portfolio.cash_usd == BASELINE.initial_capital_usd
    assert engine._last_engine_event_id == ANCHOR_ID
    store.close()


def test_certification_positive_manifest_excludes_arbitrary_history(tmp_path: Path) -> None:
    from solana_roi import certification_incremental_replication as replication

    path = tmp_path / "scope.sqlite3"
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE events(id INTEGER PRIMARY KEY AUTOINCREMENT,event_type TEXT,observed_at TEXT,payload_json TEXT,previous_hash TEXT,lineage_hash TEXT)")
        conn.execute("CREATE TABLE arbitrary_legacy_exhaust(id INTEGER PRIMARY KEY,payload TEXT)")
        conn.commit()
        install_active_certification_manifest()
        names = {str(row["name"]) for row in replication._ordinary_tables(conn)}
    assert "events" in names
    assert "arbitrary_legacy_exhaust" not in names


def test_active_checkpoint_corruption_never_falls_back_to_legacy(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    active = tmp_path / "active.sqlite3"
    legacy = tmp_path / "legacy.sqlite3"
    _make_active_checkpoint(active)
    legacy.write_bytes(b"legacy must not be opened")
    with sqlite3.connect(active) as conn:
        conn.execute("UPDATE checkpoint_current SET payload_hash='corrupt' WHERE verified=1")
        conn.commit()
    monkeypatch.setenv(ACTIVATE_ENV, "1")
    monkeypatch.setenv(ACTIVE_PATH_ENV, str(active))
    monkeypatch.setenv(LEGACY_PATH_ENV, str(legacy))
    with pytest.raises(RuntimeError, match="payload hash mismatch"):
        select_runtime_database_from_environment(expected_release_sha=RELEASE)
