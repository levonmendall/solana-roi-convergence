from __future__ import annotations

import base64
import gc
import hashlib
import json
import sqlite3
from contextlib import closing

import pytest

from solana_roi import active_storage as active
from solana_roi import storage_shadow_migration as migration
from solana_roi import storage_transition as transition


def test_current_state_fits_warning_boundary_with_other_retained_evidence(tmp_path):
    budget = active.ActiveStorageBudget(warning_bytes=6 * 1024**2, hard_bytes=8 * 1024**2)
    storage = active.ActiveStorage(tmp_path / 'active.sqlite3', budget=budget)
    storage.initialize(epoch_id='bounded-successor')
    truth = {name: {} for name in transition._SEMANTIC_SECTIONS}
    truth['portfolio'] = {'last_engine_event_id': 0, 'cash_usd': 500.0}
    truth['wallet'] = {'rows': [{'wallet': str(i), 'evidence': 'x' * 1024} for i in range(4608)]}
    with closing(storage.connect()) as conn, conn:
        conn.execute('CREATE TABLE retained_fixture(payload BLOB)')
        conn.execute('INSERT INTO retained_fixture VALUES(zeroblob(?))', (3 * 1024**2,))
    migration._write_logical_truth(storage, truth)
    checkpoint = transition.build_checkpoint_payload(release_sha='a' * 40, current_truth=truth, provenance={})
    transition.persist_verified_checkpoint(storage, checkpoint_payload=checkpoint, source_truth=truth)
    with closing(storage.connect()) as conn:
        conn.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        retained = conn.execute('SELECT length(payload) FROM retained_fixture').fetchone()[0]
    assert retained == 3 * 1024**2
    assert storage.path.stat().st_size < budget.warning_bytes
    assert transition.verify_semantic_equivalence(truth, transition.load_verified_checkpoint(storage.path)).equivalent
    assert migration.read_logical_truth(storage.path) == truth


@pytest.mark.parametrize('value', [
    {'unicode': 'é漢字', 'nested': [True, None, -0.0, 1e-30], 'bytes': b'\x00\xff'},
    {'rows': [{'wallet': str(i), 'value': 'é漢字' * 100} for i in range(1500)]},
    {'$roi_current_payload': 'user value, not an encoding', 'nested': [1, 2]},
])
def test_encoding_preserves_exact_canonical_json(value):
    encoded, size = active.encode_current_payload(value)
    decoded = active.decode_current_payload(encoded)
    assert active.canonical_json(decoded) == active.canonical_json(value)
    assert size == len(active.canonical_json(value).encode('utf-8'))


def test_compressed_payload_rejects_truncation_trailing_bytes_and_wrong_size():
    encoded, _ = active.encode_current_payload({'rows': ['state' * 1024] * 100})
    envelope = json.loads(encoded)
    compressed = base64.b64decode(envelope['data'])
    for changed in (compressed[:-2], compressed + b'extra'):
        corrupt = {**envelope, 'data': base64.b64encode(changed).decode('ascii')}
        with pytest.raises(RuntimeError, match='current-state payload'):
            active.decode_current_payload(json.dumps(corrupt))
    for size in (0, envelope['utf8_bytes'] - 1, envelope['utf8_bytes'] + 1, 2**40):
        with pytest.raises(RuntimeError, match='current-state payload'):
            active.decode_current_payload(json.dumps({**envelope, 'utf8_bytes': size}))


def test_changed_compressed_semantics_cannot_publish_checkpoint(tmp_path):
    storage = active.ActiveStorage(tmp_path / 'active.sqlite3')
    storage.initialize(epoch_id='tamper-check')
    truth = {name: {} for name in transition._SEMANTIC_SECTIONS}
    truth['portfolio'] = {'last_engine_event_id': 0}
    truth['wallet'] = {'rows': ['truth' * 1024] * 100}
    migration._write_logical_truth(storage, truth)
    checkpoint = transition.build_checkpoint_payload(release_sha='a' * 40, current_truth=truth, provenance={})
    transition.persist_verified_checkpoint(storage, checkpoint_payload=checkpoint, source_truth=truth)
    changed, _ = active.encode_current_payload({'rows': ['forged' * 1024] * 100})
    digest = hashlib.sha256(changed.encode()).hexdigest()
    with closing(storage.connect()) as conn, conn:
        conn.execute("UPDATE wallet_current SET payload_json=?,payload_hash=? WHERE wallet_id='__transition_state__'", (changed, digest))
    with pytest.raises(RuntimeError, match='section hash mismatch: wallet'):
        transition.load_verified_checkpoint(storage.path)
    with pytest.raises(RuntimeError, match='semantic equivalence failed: wallet'):
        transition.persist_verified_checkpoint(storage, checkpoint_payload=checkpoint, source_truth=truth)


def test_current_payload_connections_close_without_garbage_collection(tmp_path, monkeypatch):
    storage = active.ActiveStorage(tmp_path / 'active.sqlite3')
    original = storage.connect
    opened = []
    def track():
        conn = original()
        opened.append(conn)
        return conn
    monkeypatch.setattr(storage, 'connect', track)
    enabled = gc.isenabled()
    gc.disable()
    try:
        storage.initialize(epoch_id='connection-lifetime')
        storage.replace_current('strategy_current', 'state_key', 'transition', {'value': 'x' * 100000})
        storage.assert_positive_schema()
        storage.checkpoint_wal()
        for conn in opened:
            with pytest.raises(sqlite3.ProgrammingError, match='closed'):
                conn.execute('SELECT 1')
    finally:
        if enabled:
            gc.enable()
        for conn in opened:
            conn.close()
