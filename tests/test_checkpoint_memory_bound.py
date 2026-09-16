from __future__ import annotations

import base64
import hashlib
import json
import math
import os
from pathlib import Path
import random
import sqlite3
import subprocess
import sys
import zlib

import pytest

from solana_roi.active_storage import ActiveStorage, canonical_json, encode_current_payload, payload_hash
from solana_roi.checkpoint_payload_stream import _CanonicalJSON, dictionary_shape, verify_section_shape
from solana_roi.storage_transition import (
    _ACTIVE_SECTION_ROWS, _SEMANTIC_SECTIONS, build_checkpoint_payload,
    load_verified_checkpoint, load_verified_checkpoint_shape, persist_verified_checkpoint,
)

RELEASE = 'a' * 40


def _checkpoint(path, truth=None):
    truth = truth or {key: {'nested': {'value': [1, 2, {'x': 'é'}]}} for key in _SEMANTIC_SECTIONS}
    storage = ActiveStorage(path)
    storage.initialize(epoch_id='bounded-checkpoint-fixture')
    for section, (table, column, key) in _ACTIVE_SECTION_ROWS.items():
        storage.replace_current(table, column, key, truth[section])
    payload = build_checkpoint_payload(release_sha=RELEASE, current_truth=truth, provenance={
        'legacy_path': str(path), 'legacy_size_bytes': 4096, 'legacy_schema_fingerprint': 'fixture',
    })
    persist_verified_checkpoint(storage, checkpoint_payload=payload, source_truth=truth)
    return truth


def _section(body, value):
    conn = sqlite3.connect(':memory:')
    conn.execute('CREATE TABLE section(k TEXT PRIMARY KEY,payload_json TEXT,payload_hash TEXT)')
    conn.execute('INSERT INTO section VALUES(?,?,?)', ('key', body, hashlib.sha256(body.encode()).hexdigest()))
    combined = hashlib.sha256(b'prefix')
    try:
        shape, result = verify_section_shape(conn, 'section', 'k', 'key', 'test', payload_hash(value), combined)
    finally:
        conn.close()
    assert shape == dictionary_shape(value)
    assert result.hexdigest() == hashlib.sha256(b'prefix' + canonical_json(value).encode()).hexdigest()
    assert combined.hexdigest() == hashlib.sha256(b'prefix').hexdigest()


@pytest.mark.parametrize('value', [None, True, False, -0.0, 1e-80, float('inf'), float('-inf'), float('nan'),
    '', 'a"\\\b\f\n\r\t\x00\x01\x0b\x1f', 'héllo 💰 🦔 中文 /', [], {},
    {'a': {'deep': 1}, 'b': [1, {'x': 2}], 'c': '\u2028\u2029'}, {'$roi_current_payload': 'ordinary logical value'}])
@pytest.mark.parametrize('chunk', [1, 3, 257])
def test_stream_grammar_and_hash_matches_canonical(value, chunk):
    body = canonical_json(value).encode()
    h = hashlib.sha256()
    parser = _CanonicalJSON(iter(body[i:i+chunk] for i in range(0,len(body),chunk)), h)
    assert parser.parse() == dictionary_shape(value)
    assert h.hexdigest() == payload_hash(value)
    _section(encode_current_payload(value)[0], value)


def test_random_canonical_values_match_materialized_parser():
    rand = random.Random(1138)
    def make(depth):
        if not depth or rand.randrange(3)==0:
            return rand.choice([None, True, False, rand.uniform(-1e9,1e9), rand.randrange(-10000,10000), '漢💰/\\\n\x1b'])
        if rand.randrange(2):
            return {f'k{i}{rand.randrange(99)}': make(depth-1) for i in range(rand.randrange(6))}
        return [make(depth-1) for _ in range(rand.randrange(6))]
    for _ in range(200):
        v=make(4); body=canonical_json(v).encode(); h=hashlib.sha256()
        assert _CanonicalJSON(iter(body[i:i+17] for i in range(0,len(body),17)),h).parse()==dictionary_shape(v)
        assert h.hexdigest()==payload_hash(v)


@pytest.mark.parametrize('body', ['{ "z": 2, "a": {"n": 1.00} }', '{"a":"\\u00e9"}', '{"x":1,"x":2}', '1.00', '1e2', ' { } ', '"\\/"'])
def test_noncanonical_legacy_json_keeps_existing_semantics(body):
    _section(body, json.loads(body))


@pytest.mark.parametrize('bad', ['{"a":}', '[1,]', '{"a":01}', 'truefalse', '"unterminated', '{"a":1}[]', '"\x01"'])
def test_invalid_json_cannot_be_accepted_with_updated_physical_hash(bad):
    with pytest.raises((ValueError, RuntimeError)):
        _section(bad, {})


def test_compressed_large_scalar_and_arrays_use_stream_not_decoder(monkeypatch):
    import solana_roi.checkpoint_payload_stream as module
    v={'bulk': 'x'*200_000, 'rows':[{'x':i,'y':'é💰\x1b'*500} for i in range(20)]}
    body,_=encode_current_payload(v)
    assert '$roi_current_payload' in body
    monkeypatch.setattr(module, 'decode_current_payload', lambda _body: pytest.fail('materializing fallback'))
    _section(body,v)


@pytest.mark.parametrize('mutation', ['length_short', 'length_long', 'trailing', 'truncated', 'base64', 'codec'])
def test_compressed_integrity_failures_protect_data(mutation):
    v={'text':'x'*150_000}; body,_=encode_current_payload(v); value=json.loads(body)
    if mutation=='length_short': value['utf8_bytes']-=1
    elif mutation=='length_long': value['utf8_bytes']+=1
    elif mutation=='trailing': value['data']=base64.b64encode(base64.b64decode(value['data'])+b'extra').decode()
    elif mutation=='truncated': value['data']=base64.b64encode(base64.b64decode(value['data'])[:-3]).decode()
    elif mutation=='base64': value['data']='!' + value['data'][1:]
    else: value['$roi_current_payload']='wrong-codec'
    with pytest.raises((ValueError, RuntimeError, zlib.error)):
        _section(canonical_json(value),v)


def test_full_and_shape_loaders_agree_and_do_not_write(tmp_path):
    p=tmp_path/'active.sqlite'; truth=_checkpoint(p)
    before=hashlib.sha256(p.read_bytes()).hexdigest()
    full=load_verified_checkpoint(p,expected_release_sha=RELEASE)
    shape=load_verified_checkpoint_shape(p,expected_release_sha=RELEASE)
    assert shape == {k: dictionary_shape(v) if k in _SEMANTIC_SECTIONS else v for k,v in full.items()}
    assert full['wallet'] == truth['wallet']  # full restoration API remains unchanged
    assert hashlib.sha256(p.read_bytes()).hexdigest()==before


@pytest.mark.parametrize('target', ['physical','section','combined','stored_combined','schema','migration','release','missing'])
def test_shape_loader_preserves_each_checkpoint_integrity_gate(tmp_path,target):
    p=tmp_path/'active.sqlite'; _checkpoint(p)
    with sqlite3.connect(p) as c:
        row=c.execute('SELECT payload_json FROM checkpoint_current').fetchone(); cp=json.loads(row[0])
        if target=='physical':
            c.execute("UPDATE wallet_current SET payload_hash='bad'")
        elif target=='section': cp['section_hashes']['wallet']='bad'
        elif target=='combined': cp['semantic_hash']='bad'
        elif target=='stored_combined': c.execute("UPDATE checkpoint_current SET semantic_hash='bad'")
        elif target=='schema': c.execute('UPDATE checkpoint_current SET schema_version=999')
        elif target=='migration': c.execute('UPDATE checkpoint_current SET migration_version=999')
        elif target=='release': c.execute("UPDATE checkpoint_current SET release_sha='bad'")
        elif target=='missing': c.execute("DELETE FROM wallet_current WHERE wallet_id='__transition_state__'")
        if target in ('section','combined'):
            body=canonical_json(cp); c.execute('UPDATE checkpoint_current SET payload_json=?,payload_hash=?',(body,hashlib.sha256(body.encode()).hexdigest()))
    for loader in (load_verified_checkpoint_shape,load_verified_checkpoint):
        with pytest.raises(RuntimeError): loader(p,expected_release_sha=RELEASE)


def test_release_rollforward_remains_explicit(tmp_path,monkeypatch):
    p=tmp_path/'active.sqlite'; _checkpoint(p); new='b'*40
    monkeypatch.delenv('SOLANA_ROI_ACTIVE_STORAGE_ENABLED',raising=False)
    with pytest.raises(RuntimeError,match='release SHA'): load_verified_checkpoint_shape(p,expected_release_sha=new)
    monkeypatch.setenv('SOLANA_ROI_ACTIVE_STORAGE_ENABLED','true'); monkeypatch.setenv('RENDER_GIT_COMMIT',new)
    monkeypatch.delenv('SOLANA_ROI_ACTIVE_STORAGE_FINALIZE_FROM_LEGACY',raising=False)
    assert load_verified_checkpoint_shape(p,expected_release_sha=new)['release_sha']==RELEASE
    monkeypatch.setenv('SOLANA_ROI_ACTIVE_STORAGE_FINALIZE_FROM_LEGACY','true')
    with pytest.raises(RuntimeError,match='release SHA'): load_verified_checkpoint_shape(p,expected_release_sha=new)


def test_sixteen_predecessor_real_chain_matches_prior_verifier_and_preserves_files(tmp_path,monkeypatch):
    from test_storage_recovery_startup_sequence import _seed_active,_append_generation,_advance_without_lifecycle_guard,PREVIOUS_RELEASE
    from solana_roi import sealed_epoch_reclamation as r
    monkeypatch.setenv('SOLANA_ROI_RELEASE_COMMIT',PREVIOUS_RELEASE)
    monkeypatch.setenv('RENDER_GIT_COMMIT',PREVIOUS_RELEASE)
    active=tmp_path/'active.sqlite'; _seed_active(active); paths=[]
    for i in range(1,17):
        _append_generation(active,i)
        paths.append(_advance_without_lifecycle_guard(active,i))
    before={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in [active,*paths]}
    new=r.preflight_sealed_epoch_reclamation(active,expected_release_sha=PREVIOUS_RELEASE)
    monkeypatch.setattr(r,'load_verified_checkpoint',load_verified_checkpoint)
    old=r.preflight_sealed_epoch_reclamation(active,expected_release_sha=PREVIOUS_RELEASE)
    assert new==old
    assert len(new['eligible_candidates']) == 16, new
    assert new['protected_candidate_count'] == 0
    assert {str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in [active,*paths]}==before
    assert new['sealed_source_deleted'] is False


def test_legacy_embedded_checkpoint_preserves_full_validation(tmp_path):
    p=tmp_path/'legacy.sqlite'; _checkpoint(p)
    full=load_verified_checkpoint(p)
    legacy=dict(full)
    legacy['migration_version']=2
    legacy.pop('sections_storage',None)
    raw=canonical_json(legacy)
    with sqlite3.connect(p) as conn:
        conn.execute('UPDATE checkpoint_current SET migration_version=2,payload_json=?,payload_hash=?',
                     (raw,hashlib.sha256(raw.encode()).hexdigest()))
    a=load_verified_checkpoint(p)
    b=load_verified_checkpoint_shape(p)
    assert b=={k:dictionary_shape(v) if k in _SEMANTIC_SECTIONS else v for k,v in a.items()}
    legacy['wallet']['nested']['value'][0]=999
    raw=canonical_json(legacy)
    with sqlite3.connect(p) as conn:
        conn.execute('UPDATE checkpoint_current SET payload_json=?,payload_hash=?',
                     (raw,hashlib.sha256(raw.encode()).hexdigest()))
    with pytest.raises(RuntimeError,match='section hash mismatch'):
        load_verified_checkpoint_shape(p)


def test_all_section_reads_share_one_snapshot(tmp_path,monkeypatch):
    import solana_roi.checkpoint_payload_stream as stream
    p=tmp_path/'active.sqlite'; _checkpoint(p)
    original=stream.verify_section_shape
    calls=[]
    def change_after_first_section(conn,*args):
        calls.append(conn.in_transaction)
        out=original(conn,*args)
        if len(calls)==1:
            with sqlite3.connect(p) as writer:
                writer.execute("UPDATE wallet_current SET payload_json='{}',payload_hash=? WHERE wallet_id='__transition_state__'",
                               (hashlib.sha256(b'{}').hexdigest(),))
        return out
    monkeypatch.setattr(stream,'verify_section_shape',change_after_first_section)
    assert load_verified_checkpoint_shape(p)['checkpoint_id']
    assert len(calls)==len(_SEMANTIC_SECTIONS) and all(calls)
    monkeypatch.setattr(stream,'verify_section_shape',original)
    # No cross-call cache: the next read must observe the corrupt replacement.
    with pytest.raises(RuntimeError,match='section hash mismatch'):
        load_verified_checkpoint_shape(p)


def test_connections_close_on_success_and_failed_proof(tmp_path,monkeypatch):
    p=tmp_path/'active.sqlite'; _checkpoint(p)
    original=sqlite3.connect; opened=[]
    def track(*args,**kwargs):
        conn=original(*args,**kwargs); opened.append(conn); return conn
    monkeypatch.setattr(sqlite3,'connect',track)
    load_verified_checkpoint_shape(p)
    with original(p) as c: c.execute("UPDATE checkpoint_current SET payload_hash='corrupt'")
    with pytest.raises(RuntimeError): load_verified_checkpoint_shape(p)
    for c in opened:
        with pytest.raises(sqlite3.ProgrammingError,match='closed'): c.execute('SELECT 1')


def test_checkpoint_allocation_regression_under_unchanged_768_mib(tmp_path):
    from checkpoint_memory_probe import fixture
    p=tmp_path/'large.sqlite'; digest=fixture(p)
    root=Path(__file__).resolve().parents[1]
    env={**os.environ,'PYTHONPATH':str(root/'src'),'PYTHONDONTWRITEBYTECODE':'1'}
    before=hashlib.sha256(p.read_bytes()).hexdigest()
    results={}
    for mode in ('baseline','repaired'):
        child=subprocess.run([sys.executable,str(root/'tests/checkpoint_memory_probe.py'),mode,str(p),'1'],
                             cwd=root,env=env,capture_output=True,text=True,timeout=90)
        assert child.returncode==(42 if mode=='baseline' else 0),child.stdout+child.stderr
        result=json.loads(child.stdout.splitlines()[-1]); results[mode]=result
        assert result['rlimit_as_bytes']==768*1024**2
    assert results['baseline']['outcome']=='MemoryError'
    assert results['repaired']['outcome']=='complete'
    assert results['repaired']['semantic_hash']==digest
    assert results['repaired']['ru_maxrss_kib']<256*1024
    assert hashlib.sha256(p.read_bytes()).hexdigest()==before
