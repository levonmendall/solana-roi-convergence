from __future__ import annotations

from types import SimpleNamespace

from solana_roi import e2e_status_read_boundary_repair as repair
from solana_roi import unified_strategy_status as unified


class _Status:
    def __init__(self, payload: dict[str, object]) -> None:
        self.payload = payload
        self.calls = 0

    def status(self) -> dict[str, object]:
        self.calls += 1
        return dict(self.payload)


def test_bounded_e2e_status_uses_only_required_live_status_inputs(monkeypatch) -> None:
    direct = _Status({"enabled": True, "continuity_ok": True})
    wallet = _Status({"profit_first_entity_strategy": {}})
    runtime = SimpleNamespace(direct_ingestion=direct, wallet_discovery=wallet)
    robinhood_calls = {"count": 0}
    captured: dict[str, object] = {}

    def robinhood_status() -> dict[str, object]:
        robinhood_calls["count"] += 1
        return {"runtime_ready": True, "paper_only": True, "live_money_authority": False}

    def fake_builder(base, received_runtime, robinhood):
        captured["base"] = base
        captured["runtime"] = received_runtime
        captured["robinhood"] = robinhood
        return {
            "status_contract_version": "test-contract",
            "release_commit": "abc123",
            "solana": {"all_regimes_e2e_achievable": True},
            "fomo": {"all_regimes_e2e_achievable": True},
            "robinhood": {"all_regimes_e2e_achievable": True},
            "overall": {"paper_only": True, "live_money_authority": False},
        }

    monkeypatch.setattr(unified, "build_unified_strategy_status", fake_builder)
    result = repair.build_bounded_e2e_status(lambda: runtime, robinhood_status)

    assert captured["runtime"] is runtime
    assert captured["base"] == {
        "data_plane": "direct-solana",
        "direct_solana": {"enabled": True, "continuity_ok": True},
        "wallet_discovery": {"profit_first_entity_strategy": {}},
    }
    assert "event_chain_valid" not in captured["base"]
    assert "evidence_counts" not in captured["base"]
    assert direct.calls == 1
    assert wallet.calls == 1
    assert robinhood_calls["count"] == 1
    assert result["read_boundary"]["full_ingestion_status_invoked"] is False
    assert result["read_boundary"]["full_event_chain_verification_invoked"] is False
    assert result["read_boundary"]["strategy_contract_or_gate_relaxed"] is False
    assert result["read_boundary"]["paper_only"] is True
    assert result["read_boundary"]["live_money_authority"] is False
    assert result["read_boundary"]["signing_available"] is False
    assert result["read_boundary"]["transaction_submission_available"] is False


def test_installer_replaces_only_dedicated_e2e_route(monkeypatch) -> None:
    original_ingestion = lambda: {"full": "audit"}
    original_e2e = lambda: {"old": True}
    ingestion_route = SimpleNamespace(
        path="/v1/ingestion/status",
        endpoint=original_ingestion,
        dependant=SimpleNamespace(call=original_ingestion),
    )
    e2e_route = SimpleNamespace(
        path="/v1/strategy/e2e-status",
        endpoint=original_e2e,
        dependant=SimpleNamespace(call=original_e2e),
    )
    app = SimpleNamespace(
        routes=[ingestion_route, e2e_route],
        state=SimpleNamespace(),
    )
    runtime = SimpleNamespace()

    monkeypatch.setattr(
        repair,
        "build_bounded_e2e_status",
        lambda runtime_provider, robinhood_provider: {"bounded": runtime_provider() is runtime},
    )

    repair.install_e2e_status_read_boundary_repair(app, lambda: runtime)

    assert ingestion_route.endpoint is original_ingestion
    assert ingestion_route.dependant.call is original_ingestion
    assert e2e_route.endpoint() == {"bounded": True}
    assert e2e_route.dependant.call is e2e_route.endpoint
    assert getattr(app.state, "roi_e2e_status_read_boundary") is True
    assert getattr(app.state, "roi_e2e_status_read_boundary_version") == repair.REPAIR_VERSION


def test_production_composition_owns_the_read_boundary() -> None:
    from pathlib import Path
    from solana_roi import production_system

    source = Path(production_system.__file__).read_text(encoding="utf-8")
    assert "install_e2e_status_read_boundary_repair(app, ingestion_runtime)" in source
    assert 'production_entrypoint": "solana_roi.production:app"' in source
    assert '"paper_only": PAPER_ONLY' in source
    assert '"live_money_authority": LIVE_MONEY_AUTHORITY' in source
    assert '"signing_available": SIGNING_AVAILABLE' in source
    assert '"transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE' in source
