from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

from solana_roi import startup_retention_cleanup as retention


class _FakeApp:
    def __init__(self) -> None:
        self.state = SimpleNamespace()
        self.routes: list[SimpleNamespace] = []
        self.lifecycle_events: list[str] = []

        @asynccontextmanager
        async def _base_lifespan(app):
            self.lifecycle_events.append("base_start")
            yield {"canonical": True}
            self.lifecycle_events.append("base_stop")

        self.router = SimpleNamespace(lifespan_context=_base_lifespan)

    def get(self, path: str):
        def _decorator(func):
            self.routes.append(SimpleNamespace(path=path, endpoint=func))
            return func

        return _decorator


def _completed_state() -> dict[str, object]:
    return {
        "version": retention.CLEANUP_VERSION,
        "installed": True,
        "startup_stale_export_cleanup": {
            "version": retention.CLEANUP_VERSION,
            "examined": 1,
            "removed": 1,
            "skipped": 0,
            "bounded": True,
            "outcomes": {"removed": 1},
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        },
        "scope": ["stale_certification_exports"],
        "candidate_selection_changed": False,
        "provenance_ambiguous_data_deleted": False,
        "replication_journal_deleted": False,
        "robinhood_history_deleted": False,
        "event_ledger_deleted": False,
        "wallet_history_deleted": False,
        "strategy_thresholds_changed": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


def _run_lifespan(app: _FakeApp) -> None:
    async def _run() -> None:
        async with app.router.lifespan_context(app) as state:
            assert state == {"canonical": True}
            app.lifecycle_events.append("serving")

    asyncio.run(_run())


def test_registration_is_storage_non_mutating_until_real_lifespan(monkeypatch, tmp_path: Path) -> None:
    calls: list[Path] = []

    def _fake_cleanup(app, cleanup_runtime):
        app.lifecycle_events.append("cleanup")
        calls.append(Path(cleanup_runtime.store.path))
        state = _completed_state()
        app.state.roi_safe_retention_cleanup = state
        return state

    monkeypatch.setattr(retention, "_run_cleanup", _fake_cleanup)
    configured_path = tmp_path / "solana-roi.sqlite3"
    monkeypatch.setenv("SOLANA_ROI_DB_PATH", str(configured_path))
    app = _FakeApp()

    def guarded_runtime_accessor():
        raise AssertionError("retention cleanup must not construct the guarded runtime")

    registered = retention.install_startup_retention_cleanup(app, guarded_runtime_accessor)

    assert calls == []
    assert registered["startup_pending"] is True
    assert [route.path for route in app.routes] == ["/v1/operations/safe-retention-cleanup"]

    _run_lifespan(app)

    assert calls == [configured_path]
    assert app.lifecycle_events == ["cleanup", "base_start", "serving", "base_stop"]
    assert app.state.roi_safe_retention_cleanup["startup_pending"] is False
    assert app.state.roi_safe_retention_cleanup["store_path_source"] == "environment_contract"
    assert app.state.roi_safe_retention_cleanup["paper_only"] is True
    assert app.state.roi_safe_retention_cleanup["live_money_authority"] is False


def test_concrete_runtime_store_path_wins_without_environment_lookup(monkeypatch, tmp_path: Path) -> None:
    calls: list[Path] = []

    def _fake_cleanup(app, cleanup_runtime):
        calls.append(Path(cleanup_runtime.store.path))
        state = _completed_state()
        app.state.roi_safe_retention_cleanup = state
        return state

    monkeypatch.setattr(retention, "_run_cleanup", _fake_cleanup)
    monkeypatch.setenv("SOLANA_ROI_DB_PATH", str(tmp_path / "wrong.sqlite3"))
    runtime_path = tmp_path / "runtime.sqlite3"
    runtime = SimpleNamespace(store=SimpleNamespace(path=runtime_path))
    app = _FakeApp()

    retention.install_startup_retention_cleanup(app, runtime)
    _run_lifespan(app)

    assert calls == [runtime_path]
    assert app.state.roi_safe_retention_cleanup["store_path_source"] == "runtime_store"


def test_duplicate_registration_does_not_wrap_or_run_a_second_cleanup(monkeypatch, tmp_path: Path) -> None:
    calls: list[Path] = []

    def _fake_cleanup(app, cleanup_runtime):
        calls.append(Path(cleanup_runtime.store.path))
        state = _completed_state()
        app.state.roi_safe_retention_cleanup = state
        return state

    monkeypatch.setattr(retention, "_run_cleanup", _fake_cleanup)
    configured_path = tmp_path / "solana-roi.sqlite3"
    monkeypatch.setenv("SOLANA_ROI_DB_PATH", str(configured_path))
    app = _FakeApp()
    guarded_accessor = lambda: (_ for _ in ()).throw(AssertionError("must not be called"))

    retention.install_startup_retention_cleanup(app, guarded_accessor)
    first_wrapper = app.router.lifespan_context
    retention.install_startup_retention_cleanup(app, guarded_accessor)

    assert calls == []
    assert app.router.lifespan_context is first_wrapper
    assert len(app.routes) == 1

    _run_lifespan(app)

    assert calls == [configured_path]


def test_startup_failure_is_observed_without_weakening_authority(monkeypatch, tmp_path: Path) -> None:
    def _raise_cleanup(app, cleanup_runtime):
        app.lifecycle_events.append("cleanup_failed")
        raise RuntimeError("cleanup failed")

    monkeypatch.setattr(retention, "_run_cleanup", _raise_cleanup)
    monkeypatch.setenv("SOLANA_ROI_DB_PATH", str(tmp_path / "solana-roi.sqlite3"))
    app = _FakeApp()

    retention.install_startup_retention_cleanup(app, object())
    _run_lifespan(app)

    state = app.state.roi_safe_retention_cleanup
    assert app.lifecycle_events == ["cleanup_failed", "base_start", "serving", "base_stop"]
    assert state["startup_pending"] is False
    assert state["startup_error"] == "RuntimeError:cleanup failed"
    assert state["paper_only"] is True
    assert state["live_money_authority"] is False
    assert state["signing_available"] is False
    assert state["transaction_submission_available"] is False
    assert state["replication_journal_deleted"] is False
    assert state["robinhood_history_deleted"] is False
