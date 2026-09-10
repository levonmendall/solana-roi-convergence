from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

import pytest
from fastapi import HTTPException

from solana_roi import certification_incremental_replication as replication


class _Store:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.RLock()
        self.db = sqlite3.connect(path, check_same_thread=False)
        with self.db:
            self.db.execute("CREATE TABLE sample(id INTEGER PRIMARY KEY,value TEXT NOT NULL)")

    def close(self) -> None:
        self.db.close()


def test_pinned_reader_rejects_schema_version_change_after_store_lock_release(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _Store(tmp_path / "source.sqlite3")
    try:
        identity = replication.prepare_bootstrap(store)
        original_ensure = replication._ensure_tracking_locked

        def stale_configured_schema(connection: sqlite3.Connection):
            meta, reconfigured = original_ensure(connection)
            assert reconfigured is False
            stale = dict(meta)
            stale["configured_schema_version"] = str(int(meta["configured_schema_version"]) - 1)
            return stale, False

        monkeypatch.setattr(replication, "_ensure_tracking_locked", stale_configured_schema)
        with pytest.raises(HTTPException) as excinfo:
            replication._delta_payload(
                store,
                from_watermark=int(identity["watermark"]),
                epoch=str(identity["epoch"]),
                schema_fingerprint=str(identity["schema_fingerprint"]),
            )
        assert excinfo.value.status_code == 409
        assert excinfo.value.detail == "certification_replica_bootstrap_required:schema_changed"
    finally:
        store.close()
