from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from solana_roi import release_bound_scout_reaper_atomicity as repair


def test_sqlite_busy_defers_cleanup_instead_of_losing_failure_evidence(monkeypatch, tmp_path):
    path = tmp_path / "busy.sqlite3"
    sqlite3.connect(path).close()
    original_calls: list[Path] = []

    monkeypatch.setattr(
        repair.release_bound,
        "_snapshot_expired_pending_scout_rows",
        lambda _path, _at: [
            {
                "signature": "sig-expired",
                "reason": "frozen_scout_processed_trigger",
                "trigger_received_at": "2026-09-04T22:00:00+00:00",
            }
        ],
    )
    monkeypatch.setattr(
        repair,
        "_record_expired_rows_before_cleanup",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(sqlite3.OperationalError("database is locked")),
    )
    old = repair._ORIGINAL_REAP
    repair._ORIGINAL_REAP = lambda original_path, _at: (
        original_calls.append(original_path) or {"busy": False, "queue_rows": 1, "candidate_mints": []}
    )
    try:
        result = repair._reap_after_release_bound_accounting(
            path,
            datetime(2026, 9, 4, 22, 1, tzinfo=timezone.utc),
        )
    finally:
        repair._ORIGINAL_REAP = old

    assert result["busy"] is True
    assert result["release_bound_accounting_deferred"] is True
    assert original_calls == []


def test_cleanup_runs_only_after_release_bound_evidence_commits(monkeypatch, tmp_path):
    path = tmp_path / "ordered.sqlite3"
    sqlite3.connect(path).close()
    events: list[str] = []

    monkeypatch.setattr(
        repair.release_bound,
        "_snapshot_expired_pending_scout_rows",
        lambda _path, _at: [{"signature": "sig", "reason": "frozen_scout_processed_trigger", "trigger_received_at": "2026-09-04T22:00:00+00:00"}],
    )
    monkeypatch.setattr(
        repair,
        "_record_expired_rows_before_cleanup",
        lambda *_args, **_kwargs: events.append("evidence"),
    )
    old = repair._ORIGINAL_REAP
    repair._ORIGINAL_REAP = lambda _path, _at: (events.append("cleanup") or {"busy": False, "queue_rows": 1, "candidate_mints": []})
    try:
        result = repair._reap_after_release_bound_accounting(
            path,
            datetime(2026, 9, 4, 22, 1, tzinfo=timezone.utc),
        )
    finally:
        repair._ORIGINAL_REAP = old

    assert result["busy"] is False
    assert events == ["evidence", "cleanup"]
