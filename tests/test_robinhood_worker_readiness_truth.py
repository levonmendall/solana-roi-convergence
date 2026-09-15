from __future__ import annotations

from types import SimpleNamespace

from solana_roi import robinhood_worker_isolation_repair as repair


def _module() -> SimpleNamespace:
    return SimpleNamespace(ROBINHOOD_V5_VERSION="test-v5", _STATE={})


def _plane(**updates: object) -> SimpleNamespace:
    base: dict[str, object] = {
        "enabled": True,
        "_cursor": 100,
        "_latest_block": 100,
        "_caught_up": True,
        "_last_error": None,
        "_last_poll_at": "poll",
        "_last_success_at": "success",
        "_rpc_failures": 0,
        "_roi_market_log_legacy_equivalent_requests": 0,
        "_roi_market_log_actual_requests": 0,
        "v3_pools": {},
        "v2_curves": {},
    }
    base.update(updates)
    return SimpleNamespace(**base)


def test_pre_live_rpc_error_revokes_cached_runtime_readiness(monkeypatch) -> None:
    monkeypatch.setattr(repair, "_runtime_install_module", _module)
    status = repair._fast_live_status(_plane(_last_error="TimeoutError: provider unavailable"))

    assert status["worker_process_ready"] is True
    assert status["runtime_ready"] is False
    assert status["caught_up_for_paper_decisions"] is False
    assert status["paper_decision_transport_ready"] is False
    assert status["error"] == "TimeoutError: provider unavailable"


def test_live_epoch_ignores_unrelated_historical_backfill_error(monkeypatch) -> None:
    monkeypatch.setattr(repair, "_runtime_install_module", _module)
    status = repair._fast_live_status(
        _plane(
            _last_error="historical backfill failed",
            _roi_live_epoch_cursor=100,
            _roi_live_epoch_ready=True,
            _roi_live_epoch_suppress_entries=False,
            _roi_live_epoch_last_error_type=None,
        )
    )

    assert status["worker_process_ready"] is True
    assert status["runtime_ready"] is True
    assert status["paper_decision_transport_ready"] is True
    assert status["error"] is None


def test_live_epoch_error_revokes_cached_runtime_readiness(monkeypatch) -> None:
    monkeypatch.setattr(repair, "_runtime_install_module", _module)
    status = repair._fast_live_status(
        _plane(
            _roi_live_epoch_cursor=100,
            _roi_live_epoch_ready=True,
            _roi_live_epoch_suppress_entries=False,
            _roi_live_epoch_last_error_type="ReadTimeout",
        )
    )

    assert status["worker_process_ready"] is True
    assert status["runtime_ready"] is False
    assert status["caught_up_for_paper_decisions"] is False
    assert status["paper_decision_transport_ready"] is False
    assert status["error"] == "ReadTimeout"
