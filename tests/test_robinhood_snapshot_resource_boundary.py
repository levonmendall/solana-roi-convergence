from __future__ import annotations

import sqlite3
from contextlib import closing

import pytest

from solana_roi import batch9_continuity_frontier_proof_repair as proof
from solana_roi import certification_snapshot_memory_repair as bounded


def test_robinhood_snapshot_obeys_existing_raw_memory_guard(tmp_path, monkeypatch):
    source = tmp_path / 'live.sqlite3'
    snapshot = tmp_path / 'work.sqlite3'
    with closing(sqlite3.connect(source)) as db, db:
        db.execute('CREATE TABLE evidence(id INTEGER PRIMARY KEY,value TEXT)')
        db.execute("INSERT INTO evidence VALUES(1,'preserved')")
    before = source.read_bytes()
    built = []
    monkeypatch.setattr(proof, '_proof_snapshot_path', lambda _: snapshot)
    monkeypatch.setattr(proof, '_ORIGINAL_PROOF_REFRESH', lambda *a, **k: built.append(True) or {'available': True})
    monkeypatch.setattr(bounded, '_raw_cgroup_sample', lambda: {'current': 99, 'maximum': 100})
    monkeypatch.setattr(bounded, '_aggressive_cache_release', lambda *a: None)
    result = proof._snapshot_robinhood_proof_refresh(str(source))
    assert result['available'] is False
    assert result['error_type'] == 'MemoryError'
    assert built == []
    assert source.read_bytes() == before
    assert not snapshot.exists()


@pytest.mark.parametrize('bound,error', [('size', 'RuntimeError'), ('deadline', 'TimeoutError')])
def test_robinhood_snapshot_cannot_bypass_existing_export_bounds(tmp_path, monkeypatch, bound, error):
    from solana_roi import certification_service_split as split
    source = tmp_path / 'live.sqlite3'
    snapshot = tmp_path / 'work.sqlite3'
    with closing(sqlite3.connect(source)) as db, db:
        db.execute('CREATE TABLE evidence(value TEXT)')
        db.execute("INSERT INTO evidence VALUES('preserved')")
    before = source.read_bytes()
    built = []
    monkeypatch.setattr(proof, '_proof_snapshot_path', lambda _: snapshot)
    monkeypatch.setattr(proof, '_ORIGINAL_PROOF_REFRESH', lambda *a, **k: built.append(True) or {'available': True})
    monkeypatch.setattr(bounded, '_raw_cgroup_sample', lambda: {'current': 0, 'maximum': 2 * 1024**3})
    if bound == 'size':
        monkeypatch.setattr(split, '_snapshot_max_bytes', lambda: 1)
    else:
        monkeypatch.setattr(split, '_snapshot_deadline_seconds', lambda: -1)
    result = proof._snapshot_robinhood_proof_refresh(str(source))
    assert result['available'] is False
    assert result['error_type'] == error
    assert built == []
    assert source.read_bytes() == before
    assert not snapshot.exists()
