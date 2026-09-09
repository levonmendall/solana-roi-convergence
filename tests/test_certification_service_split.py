from __future__ import annotations

import asyncio
import json
import sqlite3

from fastapi import FastAPI

from solana_roi import certification_generation_runtime_repair as certification_runtime
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


def test_split_runtime_keeps_only_local_forward_publisher(monkeypatch) -> None:
    calls: list[str] = []

    async def base(runtime, stop):
        _ = runtime
        _ = stop
        calls.append("base")

    async def previous(runtime, stop):
        _ = runtime
        _ = stop
        calls.append("previous")

    async def forward_only(runtime, stop):
        calls.append("forward")
        delegate = certification_runtime._ORIGINAL_RUNTIME_WORKERS
        assert delegate is base
        await delegate(runtime, stop)

    monkeypatch.setattr(e2e, "_ORIGINAL_RUNTIME_WORKERS", base)
    monkeypatch.setattr(render_bootstrap, "_run_runtime_workers", previous)
    monkeypatch.setattr(certification_runtime, "_ORIGINAL_RUNTIME_WORKERS", previous)
    monkeypatch.setattr(certification_runtime, "_runtime_workers_with_forward_snapshot", forward_only)

    split._strip_local_certification_workers()
    current = render_bootstrap._run_runtime_workers
    asyncio.run(current(object(), asyncio.Event()))

    assert calls == ["forward", "base"]
    assert "previous" not in calls
    assert getattr(current, "_roi_certification_split_runtime") is True
    assert getattr(current, "_roi_local_certification_builders_disabled") is False
    assert getattr(current, "_roi_local_heavy_certification_builders_disabled") is True
    assert getattr(current, "_roi_local_forward_publisher_retained") is True
    assert getattr(current, "_roi_e2e_status_snapshot_worker") is True
    assert getattr(current, "_roi_production_proof_snapshot_worker") is True
    assert getattr(current, "_roi_forward_certification_snapshot_worker") is True


def test_split_keeps_forward_route_local_and_proxies_only_heavy_surfaces(monkeypatch) -> None:
    monkeypatch.setenv("SOLANA_ROI_CERTIFICATION_SPLIT_RUNTIME", "true")
    app = FastAPI()

    @app.get("/v1/strategy/e2e-status")
    def e2e_status() -> dict:
        return {"surface": "e2e"}

    @app.get("/v1/strategy/forward-certification")
    def forward_status() -> dict:
        return {"surface": "forward"}

    @app.get("/v1/strategy/production-proof")
    def production_status() -> dict:
        return {"surface": "production"}

    monkeypatch.setattr(split, "_install_snapshot_route", lambda app, runtime_provider: None)
    monkeypatch.setattr(split, "_strip_local_certification_workers", lambda: None)

    split.install_certification_service_split(app, lambda: object())

    routes = {getattr(route, "path", None): route for route in app.routes}
    assert routes["/v1/strategy/forward-certification"].endpoint is forward_status
    assert getattr(routes["/v1/strategy/e2e-status"].endpoint, "_roi_remote_certification_proxy") is True
    assert getattr(routes["/v1/strategy/production-proof"].endpoint, "_roi_remote_certification_proxy") is True
    assert not bool(
        getattr(routes["/v1/strategy/forward-certification"].endpoint, "_roi_remote_certification_proxy", False)
    )
    assert app.state.roi_certification_local_heavy_workers_disabled is True
    assert app.state.roi_certification_local_forward_publisher is True


def test_snapshot_uses_pinned_read_transaction_without_runtime_store_lock(tmp_path) -> None:
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

    size, estimated = split._snapshot_store_to_file(Store(), target)
    assert size > 0
    assert estimated >= size
    copied = sqlite3.connect(target)
    try:
        row = copied.execute("SELECT value FROM evidence").fetchone()
    finally:
        copied.close()
    assert row == ("canonical",)
    status = split.status()
    assert status["snapshot_holds_runtime_store_lock"] is False
    assert status["snapshot_export"] == "pinned_wal_read_transaction_bounded_online_backup"
    assert status["snapshot_uses_runtime_persistent_disk"] is True
    assert status["snapshot_shared_writable_disk"] is False
    assert status["snapshot_single_flight"] is True


def test_snapshot_deadline_aborts_instead_of_running_unbounded(tmp_path, monkeypatch) -> None:
    source = tmp_path / "canonical.sqlite3"
    target = tmp_path / "snapshot.sqlite3"
    connection = sqlite3.connect(source)
    try:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("CREATE TABLE evidence(id INTEGER PRIMARY KEY, value BLOB NOT NULL)")
        connection.executemany(
            "INSERT INTO evidence(value) VALUES (zeroblob(32768))",
            [() for _ in range(128)],
        )
        connection.commit()
    finally:
        connection.close()

    class Store:
        path = source

    clock = iter([0.0, 0.0, 100.0, 100.0, 100.0])
    monkeypatch.setattr(split.time, "monotonic", lambda: next(clock, 100.0))
    monkeypatch.setattr(split, "_snapshot_deadline_seconds", lambda: 1.0)
    monkeypatch.setattr(split, "_snapshot_pages_per_step", lambda: 1)

    try:
        split._snapshot_store_to_file(Store(), target)
    except TimeoutError as exc:
        assert "bounded deadline" in str(exc)
    else:
        raise AssertionError("snapshot backup should fail closed at its deadline")


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
    assert status["forward_stale_threshold_changed"] is False
    assert status["runtime_executes_local_heavy_certification_builders"] is False
    assert status["runtime_executes_local_forward_publisher"] is True
    assert status["forward_publication_interval_seconds"] == 15.0
    assert status["forward_publication_stale_seconds"] == 45.0
    assert status["snapshot_deadline_seconds"] > 0
    assert status["snapshot_free_reserve_bytes"] > 0
