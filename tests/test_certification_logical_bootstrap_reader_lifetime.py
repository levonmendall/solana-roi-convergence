import sqlite3
from types import SimpleNamespace

from solana_roi import certification_logical_bootstrap as bootstrap


def _store_with_rows(tmp_path):
    db_path = tmp_path / "canonical.sqlite"
    writer = sqlite3.connect(db_path)
    try:
        writer.execute("PRAGMA journal_mode=WAL")
        writer.execute("CREATE TABLE sample(value TEXT NOT NULL)")
        writer.executemany("INSERT INTO sample(value) VALUES (?)", [("one",), ("two",)])
        writer.commit()
    finally:
        writer.close()
    return SimpleNamespace(path=db_path)


def test_bootstrap_reader_does_not_hold_page_wide_transaction(tmp_path):
    store = _store_with_rows(tmp_path)

    reader = bootstrap._pinned_reader(store)
    try:
        assert reader.execute("SELECT COUNT(*) FROM sample").fetchone()[0] == 2
        assert reader.in_transaction is False
    finally:
        reader.close()


def test_page_keeps_keyset_resume_and_revalidates_after_row_snapshot(tmp_path, monkeypatch):
    store = _store_with_rows(tmp_path)
    validations = []

    def validate(reader, epoch, fingerprint):
        validations.append((epoch, fingerprint, reader.in_transaction))
        return {}

    monkeypatch.setattr(bootstrap, "_validate_identity", validate)
    monkeypatch.setattr(
        bootstrap.replication,
        "_ordinary_tables",
        lambda _reader: [{"name": "sample", "without_rowid": False}],
    )
    monkeypatch.setattr(
        bootstrap.replication,
        "_columns",
        lambda _reader, _table: [{"name": "value", "hidden": 0, "pk": 0}],
    )
    monkeypatch.setattr(bootstrap.split, "_release_commit", lambda: "test-release")
    monkeypatch.setattr(bootstrap.split, "_drop_file_cache", lambda _path: None)

    first = bootstrap._page(
        store,
        table_name="sample",
        epoch="epoch-12345678",
        fingerprint="f" * 64,
        cursor=None,
        limit=1,
    )
    second = bootstrap._page(
        store,
        table_name="sample",
        epoch="epoch-12345678",
        fingerprint="f" * 64,
        cursor=first["next_cursor"],
        limit=1,
    )

    assert first["rows"] == [{"rowid": 1, "values": ["one"]}]
    assert first["done"] is False
    assert first["next_cursor"]
    assert second["rows"] == [{"rowid": 2, "values": ["two"]}]
    assert second["done"] is True
    assert second["next_cursor"] is None
    assert len(validations) == 4
    assert all(in_transaction is False for _epoch, _fingerprint, in_transaction in validations)
