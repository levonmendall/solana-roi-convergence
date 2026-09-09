from __future__ import annotations

import asyncio
import json
import sqlite3

from solana_roi import certification_service_split as split
from solana_roi import certifier_service
from solana_roi import e2e_status_read_boundary_repair as e2e
from solana_roi import render_runtime_bootstrap_repair as render_bootstrap


class _Response:
    def __init__(self, payload: dict):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self) -> bytes:
        return json.dumps(self.payload).encode("utf-8")


class _ForbiddenLock:
    def __enter__(self):
        raise AssertionError("certification snapshot must not hold the live runtime store lock")

    def __exit__(self, exc_type, exc, tb):
        return False


def test_split_runtime_worker_bypasses_all_local_certification_publishers(monkeypatch) -> None:
    calls: list[str] = []

    async def base(runtime, stop):
        _ = runtime
        _ = stop
        calls.append("base")

    async def previous(runtime, stop):
        _ = runtime
        _ = stop
        calls.append("previous")

    monkeypatch.setattr(e2e, "_ORIGINAL_RUNTIME_WORKERS", base)
    monkeypatch.setattr(render_bootstrap, "_run_runtime_workers", previous)

    split._strip_local_certification_workers()
    current = render_bootstrap._run_runtime_workers
    asyncio.run(current(object(), asyncio.Event()))

    assert calls == ["base"]
    assert getattr(current, "_roi_certification_split_runtime") is True
    assert getattr(current, "_roi_local_certification_builders_disabled") is True
    assert getattr(current, "_roi_e2e_status_snapshot_worker") is True
    assert getattr(current, "_roi_production_proof_snapshot_worker") is True
    assert getattr(current, "_roi_forward_certification_snapshot_worker") is True


def test_snapshot_uses_separate_read_connection_without_runtime_store_lock(tmp_path) -> None:
    source = tmp_path / "canonical.sqlite3"
    target = tmp_path / "snapshot.sqlite3"
    connection = sqlite3.connect(source)
    try:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("CREATE TABLE evidence(id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
        connection.execute("INSERT INTO evidence(value) VALUES ('canonical')")
        connection.commit()
    finally:
        connection.close()

    class Store:
        path = source
        _lock = _ForbiddenLock()

    size = split._snapshot_store_to_file(Store(), target)
    assert size > 0
    copied = sqlite3.connect(target)
    try:
        row = copied.execute("SELECT value FROM evidence").fetchone()
    finally:
        copied.close()
    assert row == ("canonical",)
    status = split.status()
    assert status["snapshot_holds_runtime_store_lock"] is False
    assert status["snapshot_export"] == "sqlite_online_backup_point_in_time_read_only_connection"


def test_remote_certification_fails_closed_on_exact_release_mismatch(monkeypatch) -> None:
    monkeypatch.setenv("SOLANA_ROI_CERTIFIER_URL", "https://certifier.invalid")
    monkeypatch.setenv("SOLANA_ROI_CERTIFICATION_SHARED_TOKEN", "test-token")
    monkeypatch.setenv("RENDER_GIT_COMMIT", "expected-release")
    monkeypatch.setattr(
        split.urllib.request,
        "urlopen",
        lambda request, timeout: _Response(
            {
                "release_commit": "wrong-release",
                "overall": {
                    "paper_only": True,
                    "live_money_authority": False,
                    "signing_available": False,
                    "transaction_submission_available": False,
                },
            }
        ),
    )

    payload = split._remote_surface("/v1/strategy/e2e-status")
    assert payload["overall"]["all_regimes_e2e_proven"] is False
    boundary = payload["certification_service_split"]
    assert boundary["state"] == "failed_closed"
    assert "remote_certification_release_mismatch" in boundary["reason"]
    assert boundary["runtime_executes_local_certification_builders"] is False


def test_remote_certification_accepts_exact_release_read_only_artifact(monkeypatch) -> None:
    monkeypatch.setenv("SOLANA_ROI_CERTIFIER_URL", "https://certifier.invalid")
    monkeypatch.setenv("SOLANA_ROI_CERTIFICATION_SHARED_TOKEN", "test-token")
    monkeypatch.setenv("RENDER_GIT_COMMIT", "exact-release")
    monkeypatch.setattr(
        split.urllib.request,
        "urlopen",
        lambda request, timeout: _Response(
            {
                "release_commit": "exact-release",
                "overall": {
                    "paper_only": True,
                    "live_money_authority": False,
                    "signing_available": False,
                    "transaction_submission_available": False,
                },
            }
        ),
    )

    payload = split._remote_surface("/v1/strategy/e2e-status")
    boundary = payload["certification_service_split"]
    assert boundary["state"] == "ready"
    assert boundary["release_commit"] == "exact-release"
    assert boundary["runtime_executes_local_certification_builders"] is False
    assert boundary["live_money_authority"] is False


def test_isolated_certifier_publishes_only_exact_release_artifact(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("RENDER_GIT_COMMIT", "exact-release")
    with certifier_service._LOCK:
        certifier_service._ARTIFACTS.clear()
        certifier_service._PUBLISHED.clear()

    path = tmp_path / "e2e.json"
    path.write_text(json.dumps({"release_commit": "wrong"}), encoding="utf-8")
    assert certifier_service._publish_file("e2e", path, "exact-release") is False

    path.write_text(
        json.dumps(
            {
                "release_commit": "exact-release",
                "overall": {
                    "paper_only": True,
                    "live_money_authority": False,
                    "signing_available": False,
                    "transaction_submission_available": False,
                },
            }
        ),
        encoding="utf-8",
    )
    assert certifier_service._publish_file("e2e", path, "exact-release") is True
    payload = certifier_service._cached("e2e")
    assert payload["release_commit"] == "exact-release"
    assert payload["isolated_certifier"]["child_process_isolation"] is True
    assert payload["isolated_certifier"]["shared_writable_disk"] is False


def test_split_status_preserves_paper_only_authority(monkeypatch) -> None:
    monkeypatch.setenv("SOLANA_ROI_CERTIFICATION_SPLIT_RUNTIME", "true")
    status = split.status()
    assert status["enabled"] is True
    assert status["canonical_sqlite_owner"] == "authoritative_runtime"
    assert status["shared_writable_disk"] is False
    assert status["paper_only"] is True
    assert status["live_money_authority"] is False
    assert status["signing_available"] is False
    assert status["transaction_submission_available"] is False
    assert status["strategy_thresholds_changed"] is False
    assert status["certification_thresholds_changed"] is False
