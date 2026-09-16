"""Disposable checkpoint fixture and subprocess for the 768-MiB regression.

The fixture contains ordinary writer-format canonical zlib JSON, generated in
chunks so fixture construction itself does not require a huge parent process.
There is no strategy data, production disk, network, or deletion in this probe.
"""
from __future__ import annotations
import base64
import hashlib
import json
import os
from pathlib import Path
import resource
import sqlite3
import sys
import time
import zlib

from solana_roi.active_storage import ActiveStorage, canonical_json
from solana_roi.storage_transition import _ACTIVE_SECTION_ROWS, _SEMANTIC_SECTIONS


def pieces(large_bytes):
    yield b'{"bulk":"'
    block = b'x' * 65_536
    for _ in range(large_bytes // len(block)):
        yield block
    yield b'x' * (large_bytes % len(block))
    yield b'"}'


def fixture(path: Path, *, section_bytes: int = 160 * 1024**2) -> str:
    storage = ActiveStorage(path); storage.initialize(epoch_id='memory-probe')
    large_sections = {'wallet', 'provider_source', 'active_candidates'}
    compressor = zlib.compressobj(); compressed = bytearray(); large_hash=hashlib.sha256(); length=0
    for piece in pieces(section_bytes):
        large_hash.update(piece); length+=len(piece); compressed.extend(compressor.compress(piece))
    compressed.extend(compressor.flush())
    body = canonical_json({'$roi_current_payload':'zlib-utf8-v1','data':base64.b64encode(compressed).decode('ascii'),'utf8_bytes':length})
    combined = hashlib.sha256(); combined.update(b'{'); sections={}
    with storage.connect() as conn:
        for index, section in enumerate(sorted(_SEMANTIC_SECTIONS)):
            if index: combined.update(b',')
            combined.update(canonical_json(section).encode()+b':')
            large = section in large_sections
            section_body = body if large else '{}'
            for part in (pieces(section_bytes) if large else [b'{}']): combined.update(part)
            sections[section]=large_hash.hexdigest() if large else hashlib.sha256(b'{}').hexdigest()
            table,col,key = _ACTIVE_SECTION_ROWS[section]
            conn.execute(f'INSERT INTO "{table}"("{col}",payload_json,payload_hash,updated_at) VALUES(?,?,?,?)',
                (key,section_body,hashlib.sha256(section_body.encode()).hexdigest(),'2026-09-16T00:00:00Z'))
        combined.update(b'}')
        cp={'checkpoint_id':'memory-checkpoint','timestamp':'2026-09-16T00:00:00Z','schema_version':2,'migration_version':3,
            'release_sha':'a'*40,'provenance':{'legacy_path':str(path),'legacy_size_bytes':4096,'legacy_schema_fingerprint':'memory-fixture'},
            'section_hashes':sections,'semantic_hash':combined.hexdigest(),'sections_storage':'active_current_tables'}
        cp_body=canonical_json(cp)
        conn.execute('INSERT INTO checkpoint_current VALUES(?,?,?,?,?,?,?,?,1)',
            (cp['checkpoint_id'],cp['timestamp'],2,3,'a'*40,cp_body,hashlib.sha256(cp_body.encode()).hexdigest(),combined.hexdigest()))
        conn.commit()
    storage.checkpoint_wal()
    return combined.hexdigest()


def probe(path: Path, mode: str, iterations: int):
    resource.setrlimit(resource.RLIMIT_AS,(768*1024**2,768*1024**2))
    resource.setrlimit(resource.RLIMIT_CORE,(0,0))
    from solana_roi.storage_transition import load_verified_checkpoint,load_verified_checkpoint_shape
    loader = load_verified_checkpoint if mode=='baseline' else load_verified_checkpoint_shape
    started=time.monotonic(); result=None
    try:
        # Retain active/previous shape while traversing the next checkpoint, as
        # the actual predecessor preflight does. Never retain a history of values.
        active=loader(path,expected_release_sha='a'*40)
        for _ in range(iterations):
            result=None
            result=loader(path,expected_release_sha='a'*40)
        outcome='complete'; rc=0
    except MemoryError as exc:
        outcome='MemoryError'; rc=42
        print('EXPECTED_MEMORY_BOUNDARY '+str(exc),flush=True)
    status={}
    for line in Path('/proc/self/status').read_text().splitlines():
        if line.split(':')[0] in {'VmPeak','VmHWM','VmSize','VmRSS'}:
            key,value=line.split(':',1); status[key]=value.strip()
    print(json.dumps({'mode':mode,'outcome':outcome,'iterations':iterations,'elapsed_seconds':time.monotonic()-started,
        'rlimit_as_bytes':768*1024**2,'ru_maxrss_kib':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        'process':status,'semantic_hash':result.get('semantic_hash') if result else None}),flush=True)
    return rc


if __name__=='__main__':
    if sys.argv[1]=='fixture':
        print(fixture(Path(sys.argv[2])),flush=True)
    else:
        raise SystemExit(probe(Path(sys.argv[2]),sys.argv[1],int(sys.argv[3])))
