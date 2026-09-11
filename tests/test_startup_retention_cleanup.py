from __future__ import annotations

from types import SimpleNamespace

from solana_roi import startup_retention_cleanup as retention


class _FakeApp:
    def __init__(self) -> None:
        self.state = SimpleNamespace()
        self.routes: list[SimpleNamespace] = []
        self.startup_handlers: list[object] = []

    def get(self, path: str):
        def _decorator(func):
            self.routes.append(SimpleNamespace(path=path, endpoint=func))
            return func

        return _decorator

    def add_event_handler(self, event_type: str, handler) -> None:
        assert event_type == "startup"
        self.startup_handlers.append(handler)


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


def test_registration_is_storage_non_mutating_until_startup(monkeypatch) -> None:
    calls: list[object] = []

    def _fake_cleanup(app, ingestion_runtime):
        calls.append(ingestion_runtime)
        state = _completed_state()
        app.state.roi_safe_retention_cleanup = state
        return state

    monkeypatch.setattr(retention, "_run_cleanup", _fake_cleanup)
    app = _FakeApp()
    runtime = object()

    registered = retention.install_startup_retention_cleanup(app, runtime)

    assert calls == []
    assert registered["startup_pending"] is True
    assert len(app.startup_handlers) == 1
    assert [route.path for route in app.routes] == ["/v1/operations/safe-retention-cleanup"]

    app.startup_handlers[0]()

    assert calls == [runtime]
    assert app.state.roi_safe_retention_cleanup["startup_pending"] is False
    assert app.state.roi_safe_retention_cleanup["paper_only"] is True
    assert app.state.roi_safe_retention_cleanup["live_money_authority"] is False


def test_duplicate_registration_does_not_add_or_run_a_second_cleanup(monkeypatch) -> None:
    calls: list[object] = []

    def _fake_cleanup(app, ingestion_runtime):
        calls.append(ingestion_runtime)
        state = _completed_state()
        app.state.roi_safe_retention_cleanup = state
        return state

    monkeypatch.setattr(retention, "_run_cleanup", _fake_cleanup)
    app = _FakeApp()
    runtime = object()

    retention.install_startup_retention_cleanup(app, runtime)
    retention.install_startup_retention_cleanup(app, runtime)

    assert calls == []
    assert len(app.startup_handlers) == 1
    assert len(app.routes) == 1

    app.startup_handlers[0]()

    assert calls == [runtime]


def test_startup_failure_is_observed_without_weakening_authority(monkeypatch) -> None:
    def _raise_cleanup(app, ingestion_runtime):
        raise RuntimeError("cleanup failed")

    monkeypatch.setattr(retention, "_run_cleanup", _raise_cleanup)
    app = _FakeApp()

    retention.install_startup_retention_cleanup(app, object())
    app.startup_handlers[0]()

    state = app.state.roi_safe_retention_cleanup
    assert state["startup_pending"] is False
    assert state["startup_error"] == "RuntimeError:cleanup failed"
    assert state["paper_only"] is True
    assert state["live_money_authority"] is False
    assert state["signing_available"] is False
    assert state["transaction_submission_available"] is False
    assert state["replication_journal_deleted"] is False
    assert state["robinhood_history_deleted"] is False
