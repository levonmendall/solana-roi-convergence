from __future__ import annotations

import asyncio
from pathlib import Path

from fastapi import FastAPI

from solana_roi import production_cleanup_runtime_install as runtime_cleanup


def test_probe_runs_after_disk_lease_before_cleanup_gate_and_runtime(
    tmp_path: Path, monkeypatch
) -> None:
    app = FastAPI()
    database = tmp_path / "production.sqlite3"
    monkeypatch.setenv("SOLANA_ROI_DB_PATH", str(database))
    monkeypatch.setenv("RENDER_GIT_COMMIT", "probe-order-release")
    monkeypatch.delenv(runtime_cleanup.cleanup.ENABLED_ENV, raising=False)
    calls: list[str] = []

    async def original(_stop: asyncio.Event) -> None:
        calls.append("runtime")
        runtime_cleanup.bootstrap._BOOTSTRAP_STATE["state"] = "full_runtime"

    async def gate(_app: FastAPI, path: Path) -> bool:
        assert path == database
        assert app.state.roi_production_disk_ownership["owned"] is True
        calls.append("gate")
        return True

    def probe(path: Path):
        assert path == database
        assert app.state.roi_production_disk_ownership["owned"] is True
        calls.append("probe")
        return {
            "status": "ok",
            "database_path": str(path),
            "target_table": "anonymous_candidate_latency_failures",
            "read_only": True,
        }

    real_mark = runtime_cleanup.disk_ownership.mark_same_release_established

    def mark(path: Path, lease):
        calls.append("marker")
        return real_mark(path, lease)

    monkeypatch.setattr(runtime_cleanup.bootstrap, "_bootstrap_and_run", original)
    monkeypatch.setattr(runtime_cleanup, "_run_cleanup_if_enabled", gate)
    monkeypatch.setattr(runtime_cleanup, "probe_cleanup_target", probe)
    monkeypatch.setattr(runtime_cleanup.disk_ownership, "mark_same_release_established", mark)

    runtime_cleanup.install_production_cleanup_runtime(app)
    stop = asyncio.Event()
    asyncio.run(runtime_cleanup.bootstrap._bootstrap_and_run(stop))

    assert calls == ["probe", "gate", "runtime", "marker"]
    assert app.state.roi_cleanup_target_probe["phase"] == "post_disk_lease_prebootstrap"
    assert app.state.roi_cleanup_target_probe["read_only"] is True
    assert runtime_cleanup.disk_ownership.same_release_established(database) is True
    assert app.state.roi_production_disk_ownership["owned"] is False


def test_probe_failure_is_visible_but_does_not_establish_destructive_release_early(
    tmp_path: Path, monkeypatch
) -> None:
    app = FastAPI()
    database = tmp_path / "production.sqlite3"
    monkeypatch.setenv("SOLANA_ROI_DB_PATH", str(database))
    monkeypatch.setenv("RENDER_GIT_COMMIT", "probe-failure-release")
    monkeypatch.delenv(runtime_cleanup.cleanup.ENABLED_ENV, raising=False)
    runtime_cleanup.bootstrap._BOOTSTRAP_STATE["state"] = "starting"

    async def original(_stop: asyncio.Event) -> None:
        # Simulate the current production failure boundary: runtime exits before
        # reaching full_runtime. The read-only probe must still have happened, but
        # the release must not become eligible for destructive maintenance.
        runtime_cleanup.bootstrap._BOOTSTRAP_STATE["state"] = "failed_closed"

    def failing_probe(_path: Path):
        raise RuntimeError("probe metadata unavailable")

    monkeypatch.setattr(runtime_cleanup.bootstrap, "_bootstrap_and_run", original)
    monkeypatch.setattr(runtime_cleanup, "probe_cleanup_target", failing_probe)

    runtime_cleanup.install_production_cleanup_runtime(app)
    stop = asyncio.Event()
    asyncio.run(runtime_cleanup.bootstrap._bootstrap_and_run(stop))

    assert app.state.roi_cleanup_target_probe["status"] == "probe_failed"
    assert app.state.roi_cleanup_target_probe["phase"] == "post_disk_lease_prebootstrap"
    assert runtime_cleanup.disk_ownership.same_release_established(database) is False
