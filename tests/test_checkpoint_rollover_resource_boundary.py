from __future__ import annotations

import hashlib
import sqlite3
from contextlib import closing

import pytest

from solana_roi import active_storage, storage_shadow_migration as migration
from solana_roi import storage_transition as transition


def _seed(tmp_path, *, large=False):
    budget = active_storage.ActiveStorageBudget(
        warning_bytes=6 * 1024**2, hard_bytes=8 * 1024**2,
        max_wal_bytes=8 * 1024**2,
    )
    storage = active_storage.ActiveStorage(tmp_path / 'active.sqlite3', budget=budget)
    storage.initialize(epoch_id='resource-boundary')
    truth = {section: {} for section in transition._SEMANTIC_SECTIONS}
    truth['portfolio'] = {'last_engine_event_id': 0}
    if large:
        truth['wallet'] = {'rows': ['x' * 1024 for _ in range(4608)]}
    migration._write_logical_truth(storage, truth)
    payload = transition.build_checkpoint_payload(
        release_sha='a' * 40, current_truth=truth, provenance={'test': True},
    )
    return storage, truth, payload


def test_checkpoint_fits_when_second_semantic_copy_would_exceed_page_ceiling(tmp_path):
    storage, truth, payload = _seed(tmp_path, large=True)
    transition.persist_verified_checkpoint(storage, checkpoint_payload=payload, source_truth=truth)
    with closing(storage.connect()) as conn:
        body_bytes = conn.execute('SELECT length(payload_json) FROM checkpoint_current WHERE verified=1').fetchone()[0]
        pages = conn.execute('PRAGMA page_count').fetchone()[0]
        page_size = conn.execute('PRAGMA page_size').fetchone()[0]
    assert body_bytes < 8192
    assert pages * page_size < storage.budget.hard_bytes
    assert transition.load_verified_checkpoint(storage.path)['wallet'] == truth['wallet']


def test_checkpoint_validation_and_publish_share_write_transaction(tmp_path, monkeypatch):
    storage, truth, payload = _seed(tmp_path)
    original = transition._read_current_payload
    observed = []

    def checked(conn, *args):
        observed.append(conn.in_transaction)
        return original(conn, *args)

    monkeypatch.setattr(transition, '_read_current_payload', checked)
    transition.persist_verified_checkpoint(storage, checkpoint_payload=payload, source_truth=truth)
    assert observed and all(observed), 'current rows could change between verification and checkpoint publication'


def test_checkpoint_readers_close_on_success_and_corruption(tmp_path, monkeypatch):
    storage, truth, payload = _seed(tmp_path)
    transition.persist_verified_checkpoint(storage, checkpoint_payload=payload, source_truth=truth)
    original_connect = sqlite3.connect
    opened = []

    def connect(*args, **kwargs):
        conn = original_connect(*args, **kwargs)
        opened.append(conn)
        return conn

    monkeypatch.setattr(transition.sqlite3, 'connect', connect)
    transition.load_verified_checkpoint(storage.path)
    with closing(original_connect(storage.path)) as conn:
        conn.execute("UPDATE checkpoint_current SET payload_hash='corrupt'")
        conn.commit()
    with pytest.raises(RuntimeError, match='payload hash mismatch'):
        transition.load_verified_checkpoint(storage.path)
    for conn in opened:
        with pytest.raises(sqlite3.ProgrammingError, match='closed'):
            conn.execute('SELECT 1')


def test_missing_current_section_cannot_publish_checkpoint(tmp_path):
    storage, truth, payload = _seed(tmp_path)
    with closing(storage.connect()) as conn:
        conn.execute("DELETE FROM wallet_current WHERE wallet_id='__transition_state__'")
        conn.commit()
    with pytest.raises(RuntimeError, match='semantic section missing: wallet'):
        transition.persist_verified_checkpoint(storage, checkpoint_payload=payload, source_truth=truth)
    with closing(storage.connect()) as conn:
        assert conn.execute('SELECT count(*) FROM checkpoint_current').fetchone()[0] == 0


def test_shadow_copy_streams_without_materializing_entire_query(tmp_path, monkeypatch):
    with closing(sqlite3.connect(tmp_path / 'source.sqlite3')) as source, closing(sqlite3.connect(tmp_path / 'dest.sqlite3')) as dest:
        source.row_factory = sqlite3.Row
        source.execute('CREATE TABLE wallet_profiles(wallet TEXT PRIMARY KEY, payload BLOB)')
        source.executemany('INSERT INTO wallet_profiles VALUES(?,?)', ((str(i), b'\x00\xff') for i in range(5000)))
        source.commit()

        def reject_full_query(*_args, **_kwargs):
            raise AssertionError('whole-query materialization is forbidden during shadow copy')

        monkeypatch.setattr(migration, '_query_dicts', reject_full_query)
        copied = migration._copy_query(source, dest, 'wallet_profiles', 'SELECT * FROM wallet_profiles ORDER BY wallet')
        assert copied == 5000
        assert dest.execute('SELECT count(*) FROM wallet_profiles WHERE payload=?', (b'\x00\xff',)).fetchone()[0] == 5000


@pytest.mark.parametrize('value', [
    {'unicode': 'é漢字', 'nested': [True, None, 1.5, -0.0, 1e-30]},
    {'a': {'z': 3, 'b': [2, 1]}, 'bytes': b'\x00\xff'},
    {'nonfinite': [float('nan'), float('inf'), -float('inf')]},
])
def test_streamed_hash_preserves_existing_canonical_bytes(value, monkeypatch):
    expected = hashlib.sha256(active_storage.canonical_json(value).encode('utf-8')).hexdigest()

    def reject_materialization(_value):
        raise AssertionError('hashing must not build the complete JSON string')

    monkeypatch.setattr(active_storage, 'canonical_json', reject_materialization)
    assert active_storage.payload_hash(value) == expected
