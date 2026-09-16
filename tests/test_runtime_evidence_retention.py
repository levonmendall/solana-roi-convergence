from __future__ import annotations

import sqlite3

import pytest

from solana_roi import storage_current_state_extractor as extractor
from solana_roi.active_storage import ActiveStorage
from solana_roi.storage_shadow_migration import build_shadow_database
from solana_roi.storage_transition import load_verified_checkpoint, semantic_hash
from test_storage_active_transition_v2 import _make_legacy


TABLES = ("v51_release_compatibility", "v52_tournament_exact_evidence")


def _seed(path):
    _make_legacy(path)
    with sqlite3.connect(path) as conn:
        conn.executescript(
            "CREATE TABLE v51_release_compatibility("
            "release_commit TEXT PRIMARY KEY, measurement_epoch TEXT, promotion_eligible INTEGER);"
            "CREATE TABLE v52_tournament_exact_evidence("
            "challenger_id TEXT,stream_id TEXT,evidence_ref TEXT,observed_at TEXT,"
            "PRIMARY KEY(challenger_id,stream_id));"
        )
        conn.execute("INSERT INTO v51_release_compatibility VALUES('old-release','invalid-epoch',0)")
        conn.execute("INSERT INTO v52_tournament_exact_evidence VALUES('challenger','stream','immutable-ref','2000-01-01')")


def test_runtime_evidence_admitted_without_admitting_unknown_tables(tmp_path):
    path = tmp_path / "source.sqlite3"
    _seed(path)
    ActiveStorage(path).assert_positive_schema()
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE v52_unknown_evidence(id INTEGER)")
    with pytest.raises(ValueError, match="v52_unknown_evidence"):
        ActiveStorage(path).assert_positive_schema()


def test_evidence_survives_two_successors_and_verified_checkpoint_reopen(tmp_path):
    source = tmp_path / "source.sqlite3"
    _seed(source)
    expected = extractor.LegacyCurrentStateExtractor(source).extract().truth
    for table in TABLES:
        assert expected["strategy"][table]
    for number in range(2):
        destination = tmp_path / f"successor-{number}.sqlite3"
        report = build_shadow_database(legacy_path=source, active_path=destination, release_sha="a" * 40)
        assert report.equivalent
        checkpoint = load_verified_checkpoint(destination)
        with sqlite3.connect(source) as before, sqlite3.connect(destination) as after:
            for table in TABLES:
                assert report.copied_rows[table] == 1
                assert checkpoint["strategy"][table] == expected["strategy"][table]
                assert after.execute(f'SELECT * FROM "{table}"').fetchall() == before.execute(f'SELECT * FROM "{table}"').fetchall()
        source = destination


@pytest.mark.parametrize("table", TABLES)
def test_evidence_changes_are_included_in_semantic_seal(tmp_path, table):
    source = tmp_path / "source.sqlite3"
    _seed(source)
    before = extractor.LegacyCurrentStateExtractor(source).extract().truth
    with sqlite3.connect(source) as conn:
        conn.execute(f'DELETE FROM "{table}"')
    after = extractor.LegacyCurrentStateExtractor(source).extract().truth
    assert semantic_hash(before) != semantic_hash(after)


@pytest.mark.parametrize("table", TABLES)
def test_row_budget_excess_fails_closed_without_truncating_evidence(tmp_path, monkeypatch, table):
    source = tmp_path / "source.sqlite3"
    _seed(source)
    original = source.read_bytes()
    sections = dict(extractor.SECTION_TABLES)
    sections["strategy"] = tuple((name, 0 if name == table else count) for name, count in sections["strategy"])
    monkeypatch.setattr(extractor, "SECTION_TABLES", sections)
    with pytest.raises(RuntimeError, match=f"{table} exceeds bounded row contract"):
        extractor.LegacyCurrentStateExtractor(source).extract()
    assert source.read_bytes() == original
