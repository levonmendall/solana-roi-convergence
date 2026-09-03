from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace

import pytest

from solana_roi import wallet_intelligence_startup_repair as repair


@dataclass
class _Policy:
    min_forward_episodes: int = 30


def test_constructor_and_status_do_not_bootstrap_sqlite(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = 0

    class ExplodingIntelligence:
        def __init__(self, store, policy):
            nonlocal calls
            calls += 1
            raise RuntimeError("database is locked")

    monkeypatch.setattr(repair, "_ORIGINAL_INTELLIGENCE", ExplodingIntelligence)
    proxy = repair.StartupIsolatedWalletIntelligence(SimpleNamespace(), policy=_Policy())

    status = proxy.status()

    assert calls == 0
    assert status["startup_state"] == "deferred"
    assert status["research_lane"] is True
    assert status["paper_only"] is True
    assert status["live_money_authority"] is False
    assert status["signing_or_submission_available"] is False
    assert status["active_strategy_mutation_allowed"] is False


def test_first_background_method_contains_bootstrap_state(monkeypatch: pytest.MonkeyPatch) -> None:
    class ExplodingIntelligence:
        def __init__(self, store, policy):
            raise RuntimeError("database is locked")

    monkeypatch.setattr(repair, "_ORIGINAL_INTELLIGENCE", ExplodingIntelligence)
    proxy = repair.StartupIsolatedWalletIntelligence(SimpleNamespace(), policy=_Policy())

    with pytest.raises(RuntimeError, match="database is locked"):
        proxy.propose_next_cohort(parent_version="v", strategy_version="v-next")

    status = proxy.status()
    assert status["startup_state"] == "bootstrap_failed"
    assert status["startup_attempts"] == 1
    assert status["startup_error_type"] == "RuntimeError"
    assert "database is locked" in status["startup_error_message"]


def test_successful_lazy_bootstrap_delegates(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = 0

    class HealthyIntelligence:
        def __init__(self, store, policy):
            nonlocal calls
            calls += 1

        def propose_next_cohort(self, *, parent_version: str, strategy_version: str):
            return {"proposed": False, "parent_version": parent_version, "strategy_version": strategy_version}

        def status(self):
            return {"research_lane": True, "paper_only": True, "observed_wallets": 7}

    monkeypatch.setattr(repair, "_ORIGINAL_INTELLIGENCE", HealthyIntelligence)
    proxy = repair.StartupIsolatedWalletIntelligence(SimpleNamespace(), policy=_Policy())

    result = proxy.propose_next_cohort(parent_version="v", strategy_version="v-next")
    status = proxy.status()

    assert calls == 1
    assert result["proposed"] is False
    assert status["startup_state"] == "ready"
    assert status["startup_attempts"] == 1
    assert status["observed_wallets"] == 7


def test_installer_replaces_runtime_constructor(monkeypatch: pytest.MonkeyPatch) -> None:
    from solana_roi import runtime as runtime_module

    original = runtime_module.ContinuousWalletIntelligence
    try:
        repair.install_wallet_intelligence_startup_isolation()
        assert runtime_module.ContinuousWalletIntelligence is repair.StartupIsolatedWalletIntelligence
    finally:
        monkeypatch.setattr(runtime_module, "ContinuousWalletIntelligence", original)
