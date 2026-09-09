from __future__ import annotations

import inspect

from solana_roi import storage
from solana_roi.storage import AppendOnlyEventStore


def _seed(store: AppendOnlyEventStore, count: int = 256) -> None:
    for index in range(count):
        store.append(
            "test_event",
            f"2026-09-08T00:00:{index % 60:02d}+00:00",
            {"index": index, "payload": "x" * 128},
        )


def test_verify_streams_complete_lineage_and_uses_file_specific_cache_advice(tmp_path, monkeypatch) -> None:
    path = tmp_path / "lineage.sqlite3"
    store = AppendOnlyEventStore(path)
    _seed(store)

    opened: list[tuple[object, int]] = []
    advised: list[tuple[int, int, int, int]] = []
    closed: list[int] = []

    monkeypatch.setattr(storage.os, "POSIX_FADV_DONTNEED", 4, raising=False)
    monkeypatch.setattr(storage.os, "open", lambda target, flags: opened.append((target, flags)) or 12345)
    monkeypatch.setattr(
        storage.os,
        "posix_fadvise",
        lambda fd, offset, length, advice: advised.append((fd, offset, length, advice)),
        raising=False,
    )
    monkeypatch.setattr(storage.os, "close", lambda fd: closed.append(fd))

    assert store.verify() is True
    assert opened == [(path, storage.os.O_RDONLY)]
    assert advised == [(12345, 0, 0, 4)]
    assert closed == [12345]

    source = inspect.getsource(AppendOnlyEventStore.verify)
    assert ".fetchall()" not in source
    assert "cursor.fetchone()" in source
    assert "_verify_lock" in source
    store.close()


def test_verify_preserves_exact_hash_chain_tamper_detection(tmp_path) -> None:
    path = tmp_path / "tamper.sqlite3"
    store = AppendOnlyEventStore(path)
    _seed(store, count=8)
    assert store.verify() is True

    with store._lock, store.db:
        store.db.execute(
            "UPDATE events SET previous_hash=? WHERE id=?",
            ("tampered-previous-hash", 4),
        )

    assert store.verify() is False
    store.close()


def test_file_cache_advice_failure_is_safe_noop(tmp_path, monkeypatch) -> None:
    path = tmp_path / "advice-failure.sqlite3"
    store = AppendOnlyEventStore(path)
    _seed(store, count=4)

    closed: list[int] = []
    monkeypatch.setattr(storage.os, "POSIX_FADV_DONTNEED", 4, raising=False)
    monkeypatch.setattr(storage.os, "open", lambda _target, _flags: 99)

    def _fail_advice(_fd: int, _offset: int, _length: int, _advice: int) -> None:
        raise OSError("advisory unsupported")

    monkeypatch.setattr(storage.os, "posix_fadvise", _fail_advice, raising=False)
    monkeypatch.setattr(storage.os, "close", lambda fd: closed.append(fd))

    assert store.verify() is True
    assert closed == [99]
    store.close()


def test_verification_repair_does_not_change_authority_or_certification_thresholds() -> None:
    source = inspect.getsource(storage)
    assert "drop_caches" not in source
    assert "SOLANA_ROI_CERTIFICATION" not in source
    assert "live_money_authority" not in source
    assert "transaction_submission" not in source
    assert "signing" not in source
