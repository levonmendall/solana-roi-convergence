from __future__ import annotations

import hashlib
import sqlite3
import threading
from contextlib import closing

import pytest

from solana_roi import active_runtime as runtime


def _ledger(tmp_path):
    store = runtime.ActiveObservationEventStore.__new__(runtime.ActiveObservationEventStore)
    store.path = tmp_path / 'events.sqlite3'
    store._verify_lock = threading.Lock()
    store.transition_event_head_id = 100
    store.transition_event_head_hash = 'a' * 64
    store.transition_engine_event_id = 99
    with closing(sqlite3.connect(store.path)) as conn, conn:
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('CREATE TABLE events(id INTEGER PRIMARY KEY AUTOINCREMENT,event_type TEXT,observed_at TEXT,payload_json TEXT,previous_hash TEXT,lineage_hash TEXT)')
        previous = store.transition_event_head_hash
        for i in range(101, 108):
            event_type = 'price' if i == 104 else 'observation'
            lineage = hashlib.sha256(f'{previous}|{event_type}|2026-09-16|{{}}'.encode()).hexdigest()
            conn.execute('INSERT INTO events VALUES(?,?,?,?,?,?)', (i, event_type, '2026-09-16', '{}', previous, lineage))
            previous = lineage
    return store


@pytest.mark.parametrize('sql', [
    'UPDATE events SET id=id+100',
    'DELETE FROM events WHERE id=107',
    'DELETE FROM events',
    'DELETE FROM events WHERE id=104',
    "UPDATE events SET payload_json='forged' WHERE id=101",
    "UPDATE events SET payload_json='forged' WHERE id=104",
    "UPDATE events SET previous_hash='forged' WHERE id=104",
])
def test_every_retained_event_and_id_remains_authoritative(tmp_path, sql):
    store = _ledger(tmp_path)
    assert store.verify()
    with closing(sqlite3.connect(store.path)) as conn, conn:
        conn.execute(sql)
    assert not store.verify()


def test_chunk_readers_close_before_cache_advice_and_report_verified_frontier(tmp_path, monkeypatch):
    store = _ledger(tmp_path)
    monkeypatch.setattr(runtime, 'ACTIVE_VERIFY_CHUNK_ROWS', 2)
    monkeypatch.setattr(runtime, 'ACTIVE_VERIFY_CACHE_BYTES', 1)
    original = sqlite3.connect
    opened = []
    def track(*args, **kwargs):
        conn = original(*args, **kwargs)
        opened.append(conn)
        return conn
    def release():
        # The first connection is a data_version witness without a transaction.
        try:
            assert not opened[0].in_transaction
        except sqlite3.ProgrammingError:
            pass
        for conn in opened[1:]:
            with pytest.raises(sqlite3.ProgrammingError, match='closed'):
                conn.execute('SELECT 1')
    monkeypatch.setattr(runtime.sqlite3, 'connect', track)
    monkeypatch.setattr(store, '_release_verification_file_cache', release)
    assert store._verify_active_snapshot(reason='test') == (True, 107, 104)
    assert store.verification_status['rows'] == 7
    assert store.verification_status['chunks'] == 4
    assert store.verification_status['invocation'] == 1
    for conn in opened:
        with pytest.raises(sqlite3.ProgrammingError, match='closed'):
            conn.execute('SELECT 1')


def test_already_scanned_row_mutation_during_chunks_fails_closed(tmp_path, monkeypatch):
    store = _ledger(tmp_path)
    monkeypatch.setattr(runtime, 'ACTIVE_VERIFY_CHUNK_ROWS', 2)
    monkeypatch.setattr(runtime, 'ACTIVE_VERIFY_CACHE_BYTES', 1)
    changed = False
    def mutate():
        nonlocal changed
        if not changed:
            changed = True
            with closing(sqlite3.connect(store.path)) as conn, conn:
                conn.execute("UPDATE events SET payload_json='forged' WHERE id=101")
    monkeypatch.setattr(store, '_release_verification_file_cache', mutate)
    assert not store.verify()
    assert store.verification_status['failure_reason'] == 'database_changed_during_verification'


def test_no_tail_is_valid_only_at_the_verified_transition_sequence(tmp_path):
    store = _ledger(tmp_path)
    with closing(sqlite3.connect(store.path)) as conn, conn:
        conn.execute('DELETE FROM events')
        conn.execute("UPDATE sqlite_sequence SET seq=100 WHERE name='events'")
    assert store._verify_active_snapshot(reason='test') == (True, 100, 99)


def test_concurrent_append_cannot_advance_the_reported_verified_frontier(tmp_path, monkeypatch):
    store = _ledger(tmp_path)
    monkeypatch.setattr(runtime, 'ACTIVE_VERIFY_CHUNK_ROWS', 2)
    monkeypatch.setattr(runtime, 'ACTIVE_VERIFY_CACHE_BYTES', 1)
    changed = False
    def append():
        nonlocal changed
        if not changed:
            changed = True
            with closing(sqlite3.connect(store.path)) as conn, conn:
                previous = conn.execute('SELECT lineage_hash FROM events ORDER BY id DESC LIMIT 1').fetchone()[0]
                lineage = hashlib.sha256(f'{previous}|price|2026-09-16|{{}}'.encode()).hexdigest()
                conn.execute('INSERT INTO events VALUES(108,?,?,?,?,?)', ('price', '2026-09-16', '{}', previous, lineage))
    monkeypatch.setattr(store, '_release_verification_file_cache', append)
    assert store._verify_active_snapshot(reason='test') == (False, 0, None)
    assert store.verification_status['failure_reason'] == 'database_changed_during_verification'
    assert store._verify_active_snapshot(reason='retry') == (True, 108, 108)


def test_engine_restore_uses_the_exact_verified_frontier(tmp_path, monkeypatch):
    store = _ledger(tmp_path)
    engine = runtime.ActiveDurablePaperTradingEngine.__new__(runtime.ActiveDurablePaperTradingEngine)
    engine.store = store
    monkeypatch.setattr(store, '_verify_active_snapshot', lambda **_kwargs: (True, 107, 104))
    assert engine._verify_engine_snapshot() == (True, 107, 104)


def test_active_cache_advice_covers_database_wal_and_shm(tmp_path, monkeypatch):
    from solana_roi import durable_bootstrap_memory_repair as memory
    store = _ledger(tmp_path)
    advised = []
    monkeypatch.setattr(memory, '_advise_dontneed', lambda path: advised.append(path) or True)
    store._release_verification_file_cache()
    assert [str(path) for path in advised] == [str(store.path), str(store.path) + '-wal', str(store.path) + '-shm']
