from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

from solana_roi import robinhood_provider_pool_throughput_repair as repair


@pytest.fixture(autouse=True)
def _reset_stats():
    with repair._TELEMETRY_LOCK:
        repair._RESEARCH_STATS.update(
            {
                "passes": 0,
                "failures": 0,
                "provider_switches": 0,
                "last_provider_kind": None,
                "last_generation": None,
                "last_error_type": None,
            }
        )
    yield


def _drpc() -> SimpleNamespace:
    return SimpleNamespace(
        name="backup",
        http="https://lb.drpc.org/ogrpc?network=robinhood",
        ws="wss://lb.drpc.org/ogws?network=robinhood",
    )


def _alchemy() -> SimpleNamespace:
    return SimpleNamespace(
        name="primary",
        http="https://robinhood-mainnet.g.alchemy.com/v2/example",
        ws="wss://robinhood-mainnet.g.alchemy.com/v2/example",
    )


def test_drpc_uses_full_provider_pool_cap(monkeypatch) -> None:
    monkeypatch.setenv("ROBINHOOD_PROVIDER_POOL_LIVE_MARKET_CAP", "64")
    monkeypatch.setenv("ROBINHOOD_RPC_URL", _alchemy().http)
    monkeypatch.setattr(repair.failover, "active_provider", _drpc)
    assert repair._configured_pool_cap() == 64
    assert repair._effective_live_market_cap() == 64


def test_alchemy_fallback_retains_legacy_safety_ceiling(monkeypatch) -> None:
    alchemy = _alchemy()
    monkeypatch.setenv("ROBINHOOD_PROVIDER_POOL_LIVE_MARKET_CAP", "64")
    monkeypatch.setenv("ROBINHOOD_RPC_URL", alchemy.http)
    monkeypatch.setattr(repair.failover, "active_provider", lambda: alchemy)
    assert repair._effective_live_market_cap() == 16


def test_drpc_carries_broad_research_instead_of_public_rpc(monkeypatch) -> None:
    drpc = _drpc()
    monkeypatch.setenv("ROBINHOOD_RPC_URL", _alchemy().http)
    monkeypatch.setenv("ROBINHOOD_PROVIDER_POOL_RESEARCH_POLL_SECONDS", "1.0")
    monkeypatch.setattr(repair.failover, "active_provider", lambda: drpc)
    monkeypatch.setattr(repair.failover, "generation", lambda: 7)
    url, kind, poll_seconds, generation = repair._research_target()
    assert url == drpc.http
    assert kind == "drpc"
    assert poll_seconds == 1.0
    assert generation == 7


def test_alchemy_carries_no_broad_research_load(monkeypatch) -> None:
    alchemy = _alchemy()
    monkeypatch.setenv("ROBINHOOD_RPC_URL", alchemy.http)
    monkeypatch.setattr(repair.failover, "active_provider", lambda: alchemy)
    url, kind, poll_seconds, generation = repair._research_target()
    assert url == repair.runtime.ROBINHOOD_PUBLIC_RPC
    assert kind == "public_rpc"
    assert poll_seconds == 5.0
    assert generation is None


def test_research_loop_rebinds_to_active_private_provider_and_proves_success(monkeypatch, capsys) -> None:
    drpc = _drpc()
    stop = SimpleNamespace(is_set=lambda: False)
    calls: list[str] = []

    class _Rpc:
        def __init__(self, *, rpc_url: str, timeout_seconds: float) -> None:
            calls.append(rpc_url)
            self.rpc_url = rpc_url

        async def close(self) -> None:
            return None

    async def _pass(_self, _rpc) -> None:
        stop.is_set = lambda: True

    plane = SimpleNamespace()
    monkeypatch.setenv("ROBINHOOD_RPC_URL", _alchemy().http)
    monkeypatch.setenv("ROBINHOOD_PROVIDER_POOL_LIVE_MARKET_CAP", "64")
    monkeypatch.setattr(repair.failover, "active_provider", lambda: drpc)
    monkeypatch.setattr(repair.failover, "generation", lambda: 3)
    monkeypatch.setattr(repair.runtime, "RobinhoodRpc", _Rpc)
    monkeypatch.setattr(repair.budget, "_research_pass", _pass)
    monkeypatch.setattr(repair.budget, "_update_research_state", lambda *_args, **_kwargs: None)
    asyncio.run(repair._provider_pool_research_async(plane, stop))

    output = capsys.readouterr().out
    assert calls == [drpc.http]
    assert "ROBINHOOD_RESEARCH_PROVIDER_ACTIVE provider_kind=drpc generation=3" in output
    assert "ROBINHOOD_RESEARCH_PROVIDER_TRAFFIC provider_kind=drpc generation=3 successful_passes=1" in output
    assert "live_market_cap=64" in output
    assert "lb.drpc" not in output


def test_research_failure_is_visible_without_endpoint_or_secret(monkeypatch, capsys) -> None:
    drpc = _drpc()
    stop = SimpleNamespace(is_set=lambda: False)

    class _Rpc:
        def __init__(self, *, rpc_url: str, timeout_seconds: float) -> None:
            self.rpc_url = rpc_url

        async def close(self) -> None:
            return None

    async def _fail(_self, _rpc) -> None:
        stop.is_set = lambda: True
        raise TimeoutError("synthetic-secret-should-not-appear")

    plane = SimpleNamespace()
    monkeypatch.setenv("ROBINHOOD_RPC_URL", _alchemy().http)
    monkeypatch.setattr(repair.failover, "active_provider", lambda: drpc)
    monkeypatch.setattr(repair.failover, "generation", lambda: 4)
    monkeypatch.setattr(repair.runtime, "RobinhoodRpc", _Rpc)
    monkeypatch.setattr(repair.budget, "_research_pass", _fail)
    monkeypatch.setattr(repair.budget, "_research_state", lambda *_args, **_kwargs: {"rpc_failures": 0})
    monkeypatch.setattr(repair.budget, "_update_research_state", lambda *_args, **_kwargs: None)
    asyncio.run(repair._provider_pool_research_async(plane, stop))

    output = capsys.readouterr().out
    assert "ROBINHOOD_RESEARCH_PROVIDER_FAILED provider_kind=drpc generation=4 error_type=TimeoutError failures=1" in output
    assert "synthetic-secret-should-not-appear" not in output
    assert "lb.drpc" not in output


def test_installation_source_unifies_provider_budget_caps_without_import_side_effects() -> None:
    source = Path(repair.__file__).read_text(encoding="utf-8")
    assert "budget._live_market_cap = _effective_live_market_cap" in source
    assert "alchemy_guard._provider_pool_live_market_cap = _effective_live_market_cap" in source
    assert 'budget.BUDGET_VERSION = "robinhood-production-ws-transport-v4-provider-pool-throughput"' in source


def test_provider_finalizer_installs_repair_before_budget_transport() -> None:
    finalizer_path = Path(repair.__file__).with_name("robinhood_production_provider_finalizer.py")
    source = finalizer_path.read_text(encoding="utf-8")
    repair_install = source.index("install_robinhood_provider_pool_throughput_repair()")
    budget_install = source.index("install_robinhood_provider_budget_transport()")
    assert repair_install < budget_install


def test_production_facade_remains_installer_free() -> None:
    production_path = Path(repair.__file__).with_name("production.py")
    source = production_path.read_text(encoding="utf-8")
    assert "install_robinhood_provider_pool_throughput_repair()" not in source
    assert "from .production_system import" in source


def test_throughput_repair_preserves_paper_only_authority() -> None:
    status = repair.status()
    assert status["paper_only"] is True
    assert status["live_money_authority"] is False
    assert status["signing_available"] is False
    assert status["transaction_submission_available"] is False
