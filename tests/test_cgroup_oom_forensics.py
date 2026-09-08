from __future__ import annotations

import json
from pathlib import Path

from solana_roi import cgroup_oom_forensics as forensics


def test_atomic_snapshot_overwrite_is_bounded(tmp_path: Path) -> None:
    path = tmp_path / "runtime-memory-forensics.json"
    forensics._atomic_write(path, {"sequence": 1})
    forensics._atomic_write(path, {"sequence": 2})

    assert json.loads(path.read_text(encoding="utf-8")) == {"sequence": 2}
    assert sorted(item.name for item in tmp_path.iterdir()) == ["runtime-memory-forensics.json"]


def test_capture_persists_cgroup_and_store_artifact_context(tmp_path: Path, monkeypatch) -> None:
    db = tmp_path / "solana-roi.sqlite3"
    db.write_bytes(b"db")
    Path(str(db) + "-wal").write_bytes(b"wal")
    monkeypatch.setenv("SOLANA_ROI_DB_PATH", str(db))

    snapshot = forensics.capture_snapshot("test_capture")
    persisted = json.loads((tmp_path / forensics.CURRENT_FILENAME).read_text(encoding="utf-8"))

    assert snapshot["reason"] == "test_capture"
    assert persisted["reason"] == "test_capture"
    assert persisted["database_bytes"] == 2
    assert persisted["wal_bytes"] == 3
    assert "memory_current_bytes" in persisted
    assert "memory_max_bytes" in persisted
    assert "memory_peak_bytes" in persisted
    assert "memory_events" in persisted
    assert "memory_stat" in persisted
    assert "memory_pressure" in persisted
    assert "process_rss_bytes" in persisted
    assert persisted["paper_only"] is True
    assert persisted["live_money_authority"] is False
    assert persisted["signing_available"] is False
    assert persisted["transaction_submission_available"] is False


def test_malformed_previous_snapshot_fails_soft(tmp_path: Path) -> None:
    path = tmp_path / "runtime-memory-forensics.json"
    path.write_text("{not-json", encoding="utf-8")
    assert forensics._read_json(path) is None


def test_phase_records_active_work_without_changing_authority(tmp_path: Path, monkeypatch) -> None:
    db = tmp_path / "solana-roi.sqlite3"
    db.write_bytes(b"")
    monkeypatch.setenv("SOLANA_ROI_DB_PATH", str(db))

    with forensics.phase("production_proof_build"):
        snapshot = forensics.capture_snapshot("inside_phase")

    assert "production_proof_build" in snapshot["active_phases"]
    assert forensics.CERTIFICATION_THRESHOLDS_CHANGED is False
    assert forensics.ECONOMIC_THRESHOLDS_CHANGED is False
    assert forensics.CANONICAL_EVIDENCE_RESET is False
    assert forensics.PAPER_ONLY is True
    assert forensics.LIVE_MONEY_AUTHORITY is False
