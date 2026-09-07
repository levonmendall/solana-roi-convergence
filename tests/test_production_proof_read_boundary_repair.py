from __future__ import annotations

import threading
from types import SimpleNamespace

from solana_roi import production_proof_read_boundary_repair as repair


def _reset_snapshot(monkeypatch) -> None:
    monkeypatch.setattr(repair, "_SNAPSHOT", None)
    monkeypatch.setattr(repair, "_SNAPSHOT_PUBLISHED_MONOTONIC", None)
    monkeypatch.setattr(
        repair,
        "_SNAPSHOT_STATS",
        {
            "attempts": 0,
            "successes": 0,
            "failures": 0,
            "consecutive_failures": 0,
            "last_started_at": None,
            "last_completed_at": None,
            "last_duration_seconds": None,
            "last_error_type": None,
        },
    )


def _fake_app(original):
    other = lambda: {"health": True}
    other_route = SimpleNamespace(
        path="/v1/strategy/forward-certification",
        endpoint=other,
        dependant=SimpleNamespace(call=other),
    )
    proof_route = SimpleNamespace(
        path="/v1/strategy/production-proof",
        endpoint=original,
        dependant=SimpleNamespace(call=original),
    )
    app = SimpleNamespace(routes=[other_route, proof_route], state=SimpleNamespace())
    return app, other_route, proof_route, other


def test_installer_replaces_only_production_proof_http_route(monkeypatch) -> None:
    _reset_snapshot(monkeypatch)
    calls = {"count": 0}

    def deep_builder():
        calls["count"] += 1
        raise AssertionError("deep production proof builder must not run from HTTP")

    app, other_route, proof_route, other = _fake_app(deep_builder)
    repair._publish_snapshot(
        {
            "state": "INSUFFICIENT_EVIDENCE",
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        }
    )

    repair.install_production_proof_read_boundary_repair(app)

    payload = proof_route.endpoint()
    assert calls["count"] == 0
    assert proof_route.endpoint is repair._cached_production_proof
    assert proof_route.dependant.call is repair._cached_production_proof
    assert other_route.endpoint is other
    assert other_route.dependant.call is other
    assert payload["read_boundary"]["http_request_executes_deep_proof_builder"] is False
    assert payload["read_boundary"]["snapshot_precomputed_off_request_path"] is True
    assert payload["read_boundary"]["strategy_contract_or_gate_relaxed"] is False
    assert getattr(app.state, "roi_production_proof_read_boundary") is True
    assert getattr(app.state, "roi_production_proof_http_deep_builder_disabled") is True
    assert getattr(app.state, "roi_production_proof_strategy_contract_relaxed") is False


def test_snapshot_builder_runs_off_request_path_and_publishes(monkeypatch) -> None:
    _reset_snapshot(monkeypatch)
    stop = threading.Event()
    calls = {"count": 0}

    def builder():
        calls["count"] += 1
        stop.set()
        return {
            "state": "INSUFFICIENT_EVIDENCE",
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        }

    repair._snapshot_thread_main(builder, stop)
    payload = repair._cached_production_proof()

    assert calls["count"] == 1
    assert payload["read_boundary"]["state"] == "ready"
    assert payload["read_boundary"]["http_request_executes_deep_proof_builder"] is False
    cache = payload["read_boundary"]["cache"]
    assert cache["snapshot_available"] is True
    assert cache["snapshot_fresh"] is True
    assert cache["successes"] == 1


def test_unavailable_snapshot_fails_closed_without_relaxing_authority(monkeypatch) -> None:
    _reset_snapshot(monkeypatch)
    monkeypatch.setenv("RENDER_GIT_COMMIT", "a" * 40)

    payload = repair._cached_production_proof()

    assert payload["state"] == "DEGRADED"
    assert payload["production_proof_pass"] is False
    assert payload["ready_for_forward_proof"] is False
    assert payload["release"]["release_commit"] == "a" * 40
    assert payload["paper_only"] is True
    assert payload["live_money_authority"] is False
    assert payload["signing_available"] is False
    assert payload["transaction_submission_available"] is False
    assert payload["read_only_observability"] is True
    assert payload["changes_strategy_authority"] is False
    assert payload["changes_economic_thresholds"] is False
    assert payload["surface_attestation_policy"]["surface_scoped_attestation_required"] is True
    assert payload["surface_attestation_policy"]["aggregate_attestation_fallback_allowed"] is False
    assert payload["read_boundary"]["reason"] == "production_proof_snapshot_not_ready"


def test_production_composition_owns_both_nonblocking_proof_boundaries() -> None:
    from pathlib import Path
    from solana_roi import production_system

    source = Path(production_system.__file__).read_text(encoding="utf-8")
    assert "install_e2e_status_read_boundary_repair(app, ingestion_runtime)" in source
    assert "install_production_proof_read_boundary_repair(app)" in source
    assert '"paper_only": PAPER_ONLY' in source
    assert '"live_money_authority": LIVE_MONEY_AUTHORITY' in source
    assert '"signing_available": SIGNING_AVAILABLE' in source
    assert '"transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE' in source
