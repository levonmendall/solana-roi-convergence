from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path
from types import SimpleNamespace

from solana_roi import e2e_status_read_boundary_repair as repair


def _fake_payload() -> dict:
    return {
        "status_contract_version": "all-strategy-e2e-status-v1",
        "release_commit": "release-a",
        "solana": {"runtime_ready": True, "blockers": []},
        "fomo": {"runtime_ready": True, "blockers": []},
        "robinhood": {"runtime_ready": True, "blockers": []},
        "overall": {
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        },
        "read_boundary": {},
    }


def test_http_cache_fails_closed_without_running_deep_builder(monkeypatch) -> None:
    monkeypatch.setattr(repair, "_SNAPSHOT", None)
    monkeypatch.setattr(repair, "_SNAPSHOT_PUBLISHED_MONOTONIC", None)
    monkeypatch.setenv("RENDER_GIT_COMMIT", "release-a")

    started = time.monotonic()
    payload = repair._cached_e2e_status()
    elapsed = time.monotonic() - started

    assert elapsed < 0.1
    assert payload["release_commit"] == "release-a"
    assert payload["overall"]["paper_only"] is True
    assert payload["overall"]["live_money_authority"] is False
    assert payload["overall"]["signing_available"] is False
    assert payload["overall"]["transaction_submission_available"] is False
    assert payload["overall"]["blockers"] == ["e2e_status_snapshot_not_ready"]
    assert payload["read_boundary"]["http_request_executes_deep_status_builder"] is False


def test_http_cache_returns_immutable_fresh_snapshot(monkeypatch) -> None:
    monkeypatch.setattr(repair, "_SNAPSHOT", None)
    monkeypatch.setattr(repair, "_SNAPSHOT_PUBLISHED_MONOTONIC", None)
    repair._publish_snapshot(_fake_payload())

    first = repair._cached_e2e_status()
    first["solana"]["runtime_ready"] = False
    second = repair._cached_e2e_status()

    assert second["solana"]["runtime_ready"] is True
    assert second["read_boundary"]["state"] == "ready"
    assert second["read_boundary"]["snapshot_age_seconds"] >= 0.0
    assert second["read_boundary"]["http_request_executes_deep_status_builder"] is False


def test_http_cache_rejects_stale_snapshot(monkeypatch) -> None:
    monkeypatch.setattr(repair, "_SNAPSHOT", _fake_payload())
    monkeypatch.setattr(
        repair,
        "_SNAPSHOT_PUBLISHED_MONOTONIC",
        time.monotonic() - repair.SNAPSHOT_STALE_SECONDS - 1.0,
    )
    monkeypatch.setenv("RENDER_GIT_COMMIT", "release-a")

    payload = repair._cached_e2e_status()

    assert payload["overall"]["blockers"] == ["e2e_status_snapshot_stale"]
    assert payload["read_boundary"]["state"] == "failed_closed"


def test_probe_status_read_does_not_create_schema(tmp_path: Path) -> None:
    db = sqlite3.connect(tmp_path / "probe.sqlite3")
    db.row_factory = sqlite3.Row
    store = SimpleNamespace(db=db, _lock=threading.RLock())
    try:
        payload = repair._empty_probe_status(store, "release-a")
        assert all(not row["completed"] for row in payload.values())
        table = db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='regime_paper_e2e_probes'"
        ).fetchone()
        assert table is None
    finally:
        db.close()


def test_snapshot_worker_records_failure_without_erasing_last_good_snapshot(monkeypatch) -> None:
    original = _fake_payload()
    monkeypatch.setattr(repair, "_SNAPSHOT", None)
    monkeypatch.setattr(repair, "_SNAPSHOT_PUBLISHED_MONOTONIC", None)
    repair._publish_snapshot(original)
    stop = threading.Event()

    def failed_builder(*_args, **_kwargs):
        stop.set()
        raise RuntimeError("synthetic background failure")

    monkeypatch.setattr(repair, "build_bounded_e2e_status", failed_builder)
    repair._snapshot_thread_main(SimpleNamespace(), lambda: {}, stop)

    payload = repair._cached_e2e_status()
    assert payload["release_commit"] == "release-a"
    assert payload["solana"]["runtime_ready"] is True
    assert repair._cache_state()["last_error_type"] == "RuntimeError"
