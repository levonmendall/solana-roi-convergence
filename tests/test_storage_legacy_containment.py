from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone

from solana_roi.legacy_storage_containment import LegacyContainedObservationEventStore
from solana_roi.storage_active_compat_pruning import prune_active_compatibility_database
from solana_roi.storage_runtime_persistence_reconciliation import RUNTIME_CONTRACTS


def _iso(value: datetime) -> str:
    return value.isoformat()


def test_legacy_containment_deletes_only_positive_old_rows_and_never_events(tmp_path):
    path = tmp_path / "legacy.sqlite3"
    store = LegacyContainedObservationEventStore(path, batch_rows=1024)
    now = datetime(2026, 9, 14, 12, 0, tzinfo=timezone.utc)
    old8 = _iso(now - timedelta(days=8))
    old32 = _iso(now - timedelta(days=32))
    future = _iso(now + timedelta(hours=1))
    recent = _iso(now - timedelta(hours=1))
    try:
        # Establish event lineage before creating any stale containment fixtures;
        # the first append may legitimately run an empty maintenance pass.
        lineage = store.append("containment_test", recent, {"paper_only": True})
        assert lineage
        before_event_count = int(store.db.execute("SELECT COUNT(*) FROM events").fetchone()[0])
        with store._lock, store.db:
            store.db.execute(
                "CREATE TABLE direct_solana_recent_receipts("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, expires_at TEXT NOT NULL)"
            )
            store.db.execute(
                "CREATE TABLE helius_webhook_inbox("
                "id INTEGER PRIMARY KEY AUTOINCREMENT,state TEXT NOT NULL,updated_at TEXT NOT NULL)"
            )
            store.db.execute(
                "CREATE TABLE semantic_candidate_events("
                "signature TEXT PRIMARY KEY,received_at TEXT NOT NULL)"
            )
            store.db.execute(
                "CREATE TABLE semantic_candidate_opportunities("
                "token_mint TEXT NOT NULL,venue TEXT NOT NULL,last_seen TEXT NOT NULL,PRIMARY KEY(token_mint,venue))"
            )
            store.db.execute(
                "CREATE TABLE semantic_candidate_risk_state("
                "token_mint TEXT NOT NULL,venue TEXT NOT NULL,assessed_at TEXT NOT NULL,PRIMARY KEY(token_mint,venue))"
            )
            store.db.execute("INSERT INTO direct_solana_recent_receipts(expires_at) VALUES(?)", (old8,))
            store.db.execute("INSERT INTO direct_solana_recent_receipts(expires_at) VALUES(?)", (future,))
            store.db.execute("INSERT INTO helius_webhook_inbox(state,updated_at) VALUES('complete',?)", (old8,))
            store.db.execute("INSERT INTO helius_webhook_inbox(state,updated_at) VALUES('pending',?)", (old8,))
            store.db.execute("INSERT INTO semantic_candidate_events(signature,received_at) VALUES('old',?)", (old32,))
            store.db.execute("INSERT INTO semantic_candidate_events(signature,received_at) VALUES('new',?)", (recent,))
            store.db.execute("INSERT INTO semantic_candidate_opportunities VALUES('old','PUMP_FUN',?)", (old32,))
            store.db.execute("INSERT INTO semantic_candidate_opportunities VALUES('new','PUMP_FUN',?)", (recent,))
            store.db.execute("INSERT INTO semantic_candidate_risk_state VALUES('old','PUMP_FUN',?)", (old32,))
            store.db.execute("INSERT INTO semantic_candidate_risk_state VALUES('new','PUMP_FUN',?)", (recent,))

        report = store.contain_once(now=now)

        assert report["deleted_rows"] == 5
        assert int(store.db.execute("SELECT COUNT(*) FROM events").fetchone()[0]) == before_event_count
        assert store.db.execute("SELECT COUNT(*) FROM direct_solana_recent_receipts").fetchone()[0] == 1
        assert store.db.execute("SELECT state FROM helius_webhook_inbox").fetchone()[0] == "pending"
        assert store.db.execute("SELECT signature FROM semantic_candidate_events").fetchone()[0] == "new"
        assert store.db.execute("SELECT token_mint FROM semantic_candidate_opportunities").fetchone()[0] == "new"
        assert store.db.execute("SELECT token_mint FROM semantic_candidate_risk_state").fetchone()[0] == "new"
        assert report["vacuum"] is False
        assert report["wal_checkpoint"] is False
        assert report["event_lineage_touched"] is False
        assert report["paper_authority_touched"] is False
    finally:
        store.close()


def test_legacy_containment_preserves_latest_500_certification_samples(tmp_path):
    path = tmp_path / "samples.sqlite3"
    store = LegacyContainedObservationEventStore(path, batch_rows=1024)
    now = datetime(2026, 9, 14, 12, 0, tzinfo=timezone.utc)
    old = _iso(now - timedelta(days=60))
    try:
        with store._lock, store.db:
            store.db.execute(
                "CREATE TABLE execution_quote_observations("
                "id INTEGER PRIMARY KEY AUTOINCREMENT,received_at TEXT NOT NULL)"
            )
            store.db.executemany(
                "INSERT INTO execution_quote_observations(received_at) VALUES(?)",
                [(old,)] * 502,
            )
        report = store.contain_once(now=now)
        ids = [int(row[0]) for row in store.db.execute("SELECT id FROM execution_quote_observations ORDER BY id")]
        assert ids == list(range(3, 503))
        assert report["deleted_by_table"]["execution_quote_observations"] == 2
    finally:
        store.close()


def test_active_pruning_uses_real_webhook_schema_and_preserves_chronology_conflict(tmp_path):
    path = tmp_path / "active.sqlite3"
    now = datetime(2026, 9, 14, 12, 0, tzinfo=timezone.utc)
    old = _iso(now - timedelta(days=40))
    with sqlite3.connect(path) as conn:
        conn.execute(
            "CREATE TABLE helius_webhook_inbox("
            "id INTEGER PRIMARY KEY,event_id TEXT,event_type TEXT,payload_json TEXT,payload_sha256 TEXT,"
            "received_at TEXT,state TEXT,attempts INTEGER,last_error TEXT,updated_at TEXT)"
        )
        conn.execute(
            "INSERT INTO helius_webhook_inbox VALUES(1,'e','x','{}','h',?,'complete',1,NULL,?)",
            (old, old),
        )
        conn.execute(
            "CREATE TABLE wallet_profiles(wallet TEXT PRIMARY KEY,tier TEXT,historically_eligible INTEGER)"
        )
        conn.execute(
            "CREATE TABLE token_first_touches(token_mint TEXT PRIMARY KEY,observed_at TEXT)"
        )
        conn.execute(
            "CREATE TABLE normalized_swaps("
            "id INTEGER PRIMARY KEY,token_mint TEXT,wallet TEXT,side TEXT,observed_at TEXT,received_at TEXT)"
        )
        conn.execute("INSERT INTO wallet_profiles VALUES('w','S',1)")
        conn.execute("INSERT INTO token_first_touches VALUES('m','2026-01-02T00:00:00+00:00')")
        conn.execute(
            "INSERT INTO normalized_swaps VALUES(1,'m','w','buy','2026-01-01T00:00:00+00:00',?)",
            (old,),
        )
        conn.commit()

    deleted = prune_active_compatibility_database(path, now=now)
    with sqlite3.connect(path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM helius_webhook_inbox").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM normalized_swaps").fetchone()[0] == 1
    assert deleted["helius_webhook_inbox"] == 1
    assert deleted["normalized_swaps"] == 0


def test_semantic_candidate_tables_are_positive_bounded_contracts():
    contracts = {contract.dataset: contract for contract in RUNTIME_CONTRACTS}
    for name in (
        "semantic_candidate_events",
        "semantic_candidate_opportunities",
        "semantic_candidate_risk_state",
    ):
        contract = contracts[name]
        assert contract.retention_class.value == "BOUNDED_WINDOW"
        assert contract.max_hot_age == "31d"
        assert contract.startup_access is True
        assert contract.certification_access is True
