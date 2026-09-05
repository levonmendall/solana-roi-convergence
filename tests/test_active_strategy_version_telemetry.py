from solana_roi import api


def test_active_strategy_version_falls_back_to_baseline_when_v51_not_installed(monkeypatch):
    monkeypatch.setattr(api.risk_v51, "_INSTALLED", False)

    assert api._active_strategy_version() == api.BASELINE.version


def test_active_strategy_version_reports_composed_v51(monkeypatch):
    monkeypatch.setattr(api.risk_v51, "_INSTALLED", True)
    monkeypatch.setattr(api.risk_v5, "STRATEGY_VERSION", api.risk_v51.V51_VERSION)

    assert api._active_strategy_version() == api.risk_v51.V51_VERSION


def test_legacy_health_route_keeps_baseline_lineage(monkeypatch):
    monkeypatch.setattr(api.risk_v51, "_INSTALLED", True)
    monkeypatch.setattr(api.risk_v5, "STRATEGY_VERSION", api.risk_v51.V51_VERSION)

    assert api.health() == {
        "status": "ok",
        "liveness_only": True,
        "paper_only": True,
        "live_money_authority": False,
        "strategy_version": api.BASELINE.version,
    }
