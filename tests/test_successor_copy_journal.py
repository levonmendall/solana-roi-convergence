from __future__ import annotations

import sqlite3
from contextlib import closing

import pytest

from solana_roi import storage_shadow_migration as migration
from solana_roi.active_storage import ActiveStorage
from test_storage_active_transition_v2 import _make_legacy, RELEASE


def test_uninstalled_copy_does_not_duplicate_successor_in_wal(tmp_path, monkeypatch):
    source, target = tmp_path / 'source.sqlite3', tmp_path / 'successor.sqlite3'
    _make_legacy(source)
    original_bytes = source.read_bytes()
    original = migration._copy_dict_rows
    modes = []
    def checked(src, dest, table, rows):
        mode = dest.execute('PRAGMA journal_mode').fetchone()[0]
        modes.append(mode)
        assert mode == 'delete'
        assert dest.execute('PRAGMA synchronous').fetchone()[0] == 2
        assert dest.in_transaction
        count = original(src, dest, table, rows)
        wal = target.with_name(target.name + '-wal')
        assert not wal.exists() or wal.stat().st_size == 0
        return count
    monkeypatch.setattr(migration, '_copy_dict_rows', checked)
    report = migration.build_shadow_database(legacy_path=source, active_path=target, release_sha=RELEASE)
    assert modes and report.equivalent
    assert source.read_bytes() == original_bytes
    with closing(ActiveStorage(target).connect()) as conn:
        assert conn.execute('PRAGMA journal_mode').fetchone()[0] == 'wal'
        assert conn.execute('SELECT wallet FROM wallet_profiles').fetchone()[0] == 'wallet-a'


def test_failed_private_copy_rolls_back_without_changing_source(tmp_path, monkeypatch):
    source, target = tmp_path / 'source.sqlite3', tmp_path / 'successor.sqlite3'
    _make_legacy(source)
    original_bytes = source.read_bytes()
    original = migration._copy_dict_rows
    def interrupted(src, dest, table, rows):
        count = original(src, dest, table, rows)
        if table == 'wallet_profiles' and count:
            raise RuntimeError('interrupted uninstalled copy')
        return count
    monkeypatch.setattr(migration, '_copy_dict_rows', interrupted)
    with pytest.raises(RuntimeError, match='interrupted uninstalled copy'):
        migration.build_shadow_database(legacy_path=source, active_path=target, release_sha=RELEASE)
    assert source.read_bytes() == original_bytes
    with closing(sqlite3.connect(target)) as conn:
        assert conn.execute('SELECT count(*) FROM wallet_profiles').fetchone()[0] == 0
