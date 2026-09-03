from __future__ import annotations

import asyncio
from types import SimpleNamespace

from solana_roi import wallet_discovery_startup_repair as repair


class _Store:
    def __init__(self) -> None:
        self.events: list[tuple[str, str, dict]] = []

    def append(self, kind: str, observed_at: str, payload: dict) -> None:
        self.events.append((kind, observed_at, payload))


class _Intelligence:
    def status(self) -> dict:
        return {"research_lane": True, "paper_only": True}


def _kwargs() -> dict:
    return {
        "store": _Store(),
        "rpc": object(),
        "entity_resolver": object(),
        "risk": object(),
        "risk_collectors": object(),
        "intelligence": _Intelligence(),
        "policy": SimpleNamespace(),
        "enabled": True,
    }


def test_constructor_defers_research_bootstrap(monkeypatch) -> None:
    called = False

    class ExplodingDiscovery:
        def __init__(self, **_kwargs):
            nonlocal called
            called = True
            raise RuntimeError("persistent schema mismatch")

    monkeypatch.setattr(repair, "_ORIGINAL_DISCOVERY", ExplodingDiscovery)
    proxy = repair.StartupIsolatedWalletDiscovery(**_kwargs())

    assert called is False
    status = proxy.status()
    assert status["enabled"] is True
    assert status["operational"] is False
    assert status["startup_state"] == "deferred"
    assert status["paper_only"] is True
    assert status["live_money_authority"] is False
    assert status["signing_or_submission_available"] is False
    assert status["active_strategy_mutation_allowed"] is False


def test_bootstrap_failure_is_contained_and_visible(monkeypatch) -> None:
    class ExplodingDiscovery:
        def __init__(self, **_kwargs):
            raise RuntimeError("persistent schema mismatch")

    monkeypatch.setattr(repair, "_ORIGINAL_DISCOVERY", ExplodingDiscovery)
    kwargs = _kwargs()
    proxy = repair.StartupIsolatedWalletDiscovery(**kwargs)

    ready = asyncio.run(proxy._attempt_bootstrap())

    assert ready is False
    status = proxy.status()
    assert status["startup_state"] == "bootstrap_failed"
    assert status["startup_error_type"] == "RuntimeError"
    assert "persistent schema mismatch" in status["startup_error_message"]
    assert status["broad_program_receipt_sampling"] is False
    assert kwargs["store"].events[-1][0] == "wallet_discovery_startup_error"


def test_successful_bootstrap_delegates_status(monkeypatch) -> None:
    class HealthyDiscovery:
        def __init__(self, **_kwargs):
            pass

        def status(self) -> dict:
            return {
                "enabled": True,
                "paper_only": True,
                "live_money_authority": False,
                "broad_program_receipt_sampling": True,
            }

    monkeypatch.setattr(repair, "_ORIGINAL_DISCOVERY", HealthyDiscovery)
    proxy = repair.StartupIsolatedWalletDiscovery(**_kwargs())

    assert asyncio.run(proxy._attempt_bootstrap()) is True
    status = proxy.status()
    assert status["startup_state"] == "ready"
    assert status["startup_error_type"] is None
    assert status["broad_program_receipt_sampling"] is True
    assert status["paper_only"] is True
    assert status["live_money_authority"] is False
