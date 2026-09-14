from __future__ import annotations

import hashlib
import json
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
        "marks": {}, "trade_start_nav": {}, "candidates": {}, "positions": {}, "closed": [],
    }


def _paper_raw() -> tuple[str, str]:
    raw = json.dumps(_paper_state(), sort_keys=True, separators=(",", ":"), allow_nan=False)
    return raw, hashlib.sha256(raw.encode()).hexdigest()


def _make_legacy(path: Path) -> None:
    raw, digest = _paper_raw()
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE events(id INTEGER PRIMARY KEY AUTOINCREMENT,event_type TEXT NOT NULL,observed_at TEXT NOT NULL,payload_json TEXT NOT NULL,previous_hash TEXT,lineage_hash TEXT NOT NULL UNIQUE);
            CREATE TABLE paper_engine_checkpoint(id INTEGER PRIMARY KEY CHECK(id=1),saved_at TEXT NOT NULL,last_engine_event_id INTEGER NOT NULL,state_json TEXT NOT NULL,state_sha256 TEXT NOT NULL);
            CREATE TABLE wallet_profiles(wallet TEXT PRIMARY KEY,entity_id TEXT NOT NULL,tier TEXT NOT NULL,first_touch_sample_size INTEGER NOT NULL,historically_eligible INTEGER NOT NULL,updated_at TEXT NOT NULL);
            CREATE TABLE v52_market_validation_features(id INTEGER PRIMARY KEY AUTOINCREMENT,lane TEXT NOT NULL,observed_at TEXT NOT NULL,discovery_route TEXT,market_state TEXT,market_archetype TEXT,feature TEXT NOT NULL,value REAL NOT NULL,paper_only INTEGER NOT NULL,live_money_authority INTEGER NOT NULL);
            CREATE TABLE v52_wallet_forward_validation(id INTEGER PRIMARY KEY AUTOINCREMENT,evaluated_at TEXT NOT NULL,status TEXT NOT NULL,strategy_influence_enabled INTEGER NOT NULL,influence_scope_json TEXT NOT NULL,reasons_json TEXT NOT NULL,windows_json TEXT NOT NULL,paper_only INTEGER NOT NULL,UNIQUE(evaluated_at));
            """
        )
        conn.execute("INSERT INTO events(id,event_type,observed_at,payload_json,previous_hash,lineage_hash) VALUES(?,?,?,?,?,?)", (ANCHOR_ID,"price","2026-09-13T00:00:00+00:00","{}",None,ANCHOR_HASH))
        conn.execute("INSERT INTO paper_engine_checkpoint(id,saved_at,last_engine_event_id,state_json,state_sha256) VALUES(1,?,?,?,?)", ("2026-09-13T00:00:01+00:00",ANCHOR_ID,raw,digest))
        conn.execute("INSERT INTO wallet_profiles VALUES(?,?,?,?,?,?)", ("wallet-a","entity-a","S",100,1,"2026-09-13T00:00:00+00:00"))
        conn.executemany("INSERT INTO v52_market_validation_features(lane,observed_at,feature,value,paper_only,live_money_authority) VALUES('pump_fun','2026-09-13T00:00:00+00:00','velocity',?,1,0)", [(float(i),) for i in range(300)])
        conn.execute("INSERT INTO v52_wallet_forward_validation(evaluated_at,status,strategy_influence_enabled,influence_scope_json,reasons_json,windows_json,paper_only) VALUES(?,?,?,?,?,?,1)", ("2026-09-13T00:00:00+00:00","VALIDATION INCOMPLETE — MORE EVIDENCE REQUIRED",0,"[]","[]","[]"))
        conn.commit()


def _make_genesis_legacy(path: Path) -> None:
    _make_legacy(path)
    observed_at = "2026-09-13T00:00:00+00:00"
    raw = "{}"
    lineage = hashlib.sha256(f"|storage_probe|{observed_at}|{raw}".encode()).hexdigest()
    with sqlite3.connect(path) as conn:
        conn.execute(
            "UPDATE events SET event_type='storage_probe',previous_hash=NULL,lineage_hash=? WHERE id=?",
            (lineage, ANCHOR_ID),
        )
        conn.execute("DELETE FROM paper_engine_checkpoint WHERE id=1")
        conn.commit()


def _truth() -> dict[str, object]:
    raw, digest = _paper_raw()
    return {
        "strategy": {}, "wallet": {}, "wallet_evidence_watermarks": {}, "provider_source": {}, "freshness": {},
        "latest_event_ids": {"paper_engine_event_id": ANCHOR_ID, "events": {"id": ANCHOR_ID,"event_type":"price","observed_at":"2026-09-13T00:00:00+00:00","lineage_hash":ANCHOR_HASH,"previous_hash":None}},
        "active_candidates": {}, "active_lifecycles": {},
        "portfolio": {"saved_at":"2026-09-13T00:00:01+00:00","last_engine_event_id":ANCHOR_ID,"state_sha256":digest,"state":json.loads(raw)},
        "replication_watermarks": {}, "certification": {}, "continuity": {},
    }


def _make_active(path: Path) -> ActiveStorage:
    storage = ActiveStorage(path); storage.initialize(epoch_id="test")
    truth = _truth(); checkpoint = build_checkpoint_payload(release_sha=RELEASE,current_truth=truth,provenance={"test":True})
    persist_verified_checkpoint(storage,checkpoint_payload=checkpoint,source_truth=truth)
    return storage


def test_manifest_classifies_v52_and_never_allows_legacy_unclassified() -> None:
    required={"v52_market_validation_features","v52_market_validation_shadow_outcomes","v52_market_validation_point_in_time","v52_wallet_point_in_time_observations","v52_wallet_forward_outcomes","v52_wallet_integrity_snapshots","v52_wallet_forward_validation","wallet_intelligence_snapshots","paper_engine_checkpoint"}
    assert required <= set(storage_manifest.RETENTION_REGISTRY)
    assert all(c.retention_class is not storage_manifest.RetentionClass.LEGACY_UNCLASSIFIED for c in storage_manifest.RETENTION_REGISTRY.values())


def test_v52_feature_pruning_preserves_exact_latest_250(tmp_path: Path) -> None:
    storage=ActiveStorage(tmp_path/"a.sqlite3"); storage.initialize(epoch_id="p")
    with storage.connect() as conn:
        conn.execute("CREATE TABLE v52_market_validation_features(id INTEGER PRIMARY KEY AUTOINCREMENT,lane TEXT,observed_at TEXT,feature TEXT,value REAL)")
        conn.executemany("INSERT INTO v52_market_validation_features(lane,observed_at,feature,value) VALUES('pump_fun','2026-09-13','velocity',?)",[(float(i),) for i in range(300)]); conn.commit()
    assert storage.prune_v52_market_validation()["features"] == 50
    with storage.connect() as conn:
        values=[float(r[0]) for r in conn.execute("SELECT value FROM v52_market_validation_features ORDER BY id DESC")]
    assert values == [float(i) for i in range(299,49,-1)]


def test_semantic_equivalence_is_exact_for_every_section() -> None:
    source=_truth(); payload=build_checkpoint_payload(release_sha=RELEASE,current_truth=source,provenance={})
    assert verify_semantic_equivalence(source,payload).equivalent
    for section in ("strategy","wallet","wallet_evidence_watermarks","provider_source","freshness","latest_event_ids","active_candidates","active_lifecycles","portfolio","replication_watermarks","certification","continuity"):
        changed=dict(payload); changed[section]={"mismatch":section}
        result=verify_semantic_equivalence(source,changed)
        assert not result.equivalent and section in result.mismatched_sections


def test_checkpoint_is_release_bound_and_fail_closed(tmp_path: Path) -> None:
    path=tmp_path/"a.sqlite3"; _make_active(path)
    assert load_verified_checkpoint(path,expected_release_sha=RELEASE)["release_sha"] == RELEASE
    with pytest.raises(RuntimeError,match="release SHA"):
        load_verified_checkpoint(path,expected_release_sha="b"*40)


@pytest.mark.parametrize("size_gib",[1,5,10])
def test_active_selection_is_independent_of_legacy_size(tmp_path: Path,monkeypatch: pytest.MonkeyPatch,size_gib: int) -> None:
    active=tmp_path/"a.sqlite3"; _make_active(active)
    legacy=tmp_path/"legacy.sqlite3"
    with legacy.open("wb") as handle: handle.truncate(size_gib*1024**3)
    monkeypatch.setenv(ACTIVATE_ENV,"1"); monkeypatch.setenv(ACTIVE_PATH_ENV,str(active)); monkeypatch.setenv(LEGACY_PATH_ENV,str(legacy))
    assert select_runtime_database_from_environment(expected_release_sha=RELEASE) == active
    assert legacy.stat().st_size == size_gib*1024**3


def test_extractor_rejects_corrupt_paper_checkpoint(tmp_path: Path) -> None:
    legacy=tmp_path/"l.sqlite3"; _make_legacy(legacy)
    with sqlite3.connect(legacy) as conn: conn.execute("UPDATE paper_engine_checkpoint SET state_sha256='bad'"); conn.commit()
    with pytest.raises(RuntimeError,match="digest mismatch"): LegacyCurrentStateExtractor(legacy).extract()


def test_extractor_accepts_missing_checkpoint_only_at_proven_genesis(tmp_path: Path) -> None:
    legacy=tmp_path/"genesis.sqlite3"; _make_genesis_legacy(legacy)
    portfolio=LegacyCurrentStateExtractor(legacy).extract().truth["portfolio"]
    assert portfolio["last_engine_event_id"] == 0
    assert portfolio["state"] == _paper_state()
    assert portfolio["source_checkpoint_present"] is False
    assert portfolio["genesis_materialized"] is True


def test_extractor_rejects_missing_checkpoint_when_engine_history_exists(tmp_path: Path) -> None:
    legacy=tmp_path/"history.sqlite3"; _make_legacy(legacy)
    with sqlite3.connect(legacy) as conn:
        conn.execute("DELETE FROM paper_engine_checkpoint WHERE id=1")
        conn.commit()
    with pytest.raises(RuntimeError,match="engine history exists without a durable checkpoint"):
        LegacyCurrentStateExtractor(legacy).extract()


def test_shadow_materializes_proven_genesis_checkpoint_for_active_restart(tmp_path: Path) -> None:
    legacy=tmp_path/"genesis.sqlite3"; active=tmp_path/"active.sqlite3"; _make_genesis_legacy(legacy)
    report=build_shadow_database(legacy_path=legacy,active_path=active,release_sha=RELEASE)
    assert report.equivalent and report.copied_rows["paper_engine_checkpoint"] == 1
    with sqlite3.connect(active) as conn:
        row=conn.execute("SELECT last_engine_event_id,state_json,state_sha256 FROM paper_engine_checkpoint WHERE id=1").fetchone()
    assert row is not None and int(row[0]) == 0
    assert json.loads(str(row[1])) == _paper_state()
    assert hashlib.sha256(str(row[1]).encode()).hexdigest() == str(row[2])
    legacy.unlink()
    store=ActiveObservationEventStore(active,expected_release_sha=RELEASE)
    engine=ActiveDurablePaperTradingEngine(store=store)
    assert engine.portfolio.cash_usd == BASELINE.initial_capital_usd
    assert engine._last_engine_event_id == 0
    store.close()


def test_shadow_build_is_exact_and_bounded(tmp_path: Path) -> None:
    legacy=tmp_path/"l.sqlite3"; active=tmp_path/"a.sqlite3"; _make_legacy(legacy)
    report=build_shadow_database(legacy_path=legacy,active_path=active,release_sha=RELEASE)
    assert report.equivalent and report.mismatched_sections == ()
    assert report.copied_rows["v52_market_validation_features"] == 250
    assert report.active_size_bytes < 1024**3
    source=LegacyCurrentStateExtractor(legacy).extract().truth
    assert verify_semantic_equivalence(source,read_logical_truth(active)).equivalent


def test_runtime_continues_lineage_and_restores_without_legacy(tmp_path: Path) -> None:
    legacy=tmp_path/"l.sqlite3"; active=tmp_path/"a.sqlite3"; _make_legacy(legacy)
    build_shadow_database(legacy_path=legacy,active_path=active,release_sha=RELEASE); legacy.unlink()
    store=ActiveObservationEventStore(active,expected_release_sha=RELEASE)
    # storage_probe is intentionally not an engine event; an uncovered engine
    # event must still make DurablePaperTradingEngine fail closed.
    lineage=store.append("storage_probe","2026-09-13T00:00:02+00:00",{"probe":True})
    with store._lock: row=store.db.execute("SELECT id,previous_hash,lineage_hash FROM events ORDER BY id DESC LIMIT 1").fetchone()
    assert int(row["id"]) == ANCHOR_ID+1 and str(row["previous_hash"]) == ANCHOR_HASH and str(row["lineage_hash"]) == lineage
    assert store.verify()
    engine=ActiveDurablePaperTradingEngine(store=store)
    assert engine.portfolio.cash_usd == BASELINE.initial_capital_usd and engine._last_engine_event_id == ANCHOR_ID
    store.close()


def test_certification_scope_excludes_arbitrary_history(tmp_path: Path) -> None:
    from solana_roi import certification_incremental_replication as replication
    with sqlite3.connect(tmp_path/"s.sqlite3") as conn:
        conn.row_factory=sqlite3.Row
        conn.execute("CREATE TABLE events(id INTEGER PRIMARY KEY AUTOINCREMENT,event_type TEXT,observed_at TEXT,payload_json TEXT,previous_hash TEXT,lineage_hash TEXT)")
        conn.execute("CREATE TABLE arbitrary_legacy_exhaust(id INTEGER PRIMARY KEY,payload TEXT)"); conn.commit()
        install_active_certification_manifest(); names={str(r["name"]) for r in replication._ordinary_tables(conn)}
    assert "events" in names and "arbitrary_legacy_exhaust" not in names


def test_corrupt_active_checkpoint_never_falls_back_to_legacy(tmp_path: Path,monkeypatch: pytest.MonkeyPatch) -> None:
    active=tmp_path/"a.sqlite3"; legacy=tmp_path/"l.sqlite3"; _make_active(active); legacy.write_bytes(b"must-not-open")
    with sqlite3.connect(active) as conn: conn.execute("UPDATE checkpoint_current SET payload_hash='corrupt' WHERE verified=1"); conn.commit()
    monkeypatch.setenv(ACTIVATE_ENV,"1"); monkeypatch.setenv(ACTIVE_PATH_ENV,str(active)); monkeypatch.setenv(LEGACY_PATH_ENV,str(legacy))
    with pytest.raises(RuntimeError,match="payload hash mismatch"): select_runtime_database_from_environment(expected_release_sha=RELEASE)
