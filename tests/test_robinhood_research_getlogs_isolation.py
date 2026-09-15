from __future__ import annotations

import asyncio
from types import SimpleNamespace

from solana_roi import robinhood_chain_core as core
from solana_roi import robinhood_getlogs_provider_guard as guard
from solana_roi import robinhood_research_getlogs_isolation as isolation


def test_explicit_public_research_getlogs_bypasses_validation_cloud_dispatch(monkeypatch) -> None:
    calls: list[str] = []

    async def public_get_logs(_self, **_kwargs):
        calls.append("public")
        return [{"source": "public"}]

    async def governed_dispatch(_self, **_kwargs):
        calls.append("governed")
        return [{"source": "governed"}]

    monkeypatch.setattr(guard, "_ORIGINAL_GET_LOGS", public_get_logs)
    monkeypatch.setattr(isolation, "_ORIGINAL_DISPATCH", governed_dispatch)
    rpc = SimpleNamespace(rpc_url=core.ROBINHOOD_PUBLIC_RPC)

    rows = asyncio.run(
        isolation._research_isolated_dispatch(
            rpc,
            from_block=100,
            to_block=110,
            addresses=["0x" + "11" * 20],
            topics=["0xabc"],
        )
    )

    assert rows == [{"source": "public"}]
    assert calls == ["public"]


def test_private_production_getlogs_preserves_governed_dispatch(monkeypatch) -> None:
    calls: list[str] = []

    async def public_get_logs(_self, **_kwargs):
        calls.append("public")
        return [{"source": "public"}]

    async def governed_dispatch(_self, **_kwargs):
        calls.append("governed")
        return [{"source": "governed"}]

    monkeypatch.setattr(guard, "_ORIGINAL_GET_LOGS", public_get_logs)
    monkeypatch.setattr(isolation, "_ORIGINAL_DISPATCH", governed_dispatch)
    rpc = SimpleNamespace(rpc_url="https://private-robinhood.example")

    rows = asyncio.run(
        isolation._research_isolated_dispatch(
            rpc,
            from_block=100,
            to_block=110,
            addresses=["0x" + "22" * 20],
            topics=["0xdef"],
        )
    )

    assert rows == [{"source": "governed"}]
    assert calls == ["governed"]


def test_isolation_installs_before_provider_guard_without_raising(monkeypatch) -> None:
    original_dispatch = isolation._ORIGINAL_DISPATCH
    installed = isolation._INSTALLED
    try:
        monkeypatch.setattr(isolation, "_INSTALLED", False)
        monkeypatch.setattr(isolation, "_ORIGINAL_DISPATCH", None)
        monkeypatch.setattr(guard, "_dispatch_range", original_dispatch or guard._dispatch_range)
        monkeypatch.setattr(guard, "_ORIGINAL_GET_LOGS", None)

        isolation.install_robinhood_research_getlogs_isolation()

        assert isolation._INSTALLED is True
        assert getattr(
            guard._dispatch_range,
            "_roi_robinhood_research_getlogs_isolation",
            False,
        ) is True
    finally:
        isolation._INSTALLED = installed


def test_final_robinhood_composition_installs_public_research_isolation() -> None:
    from solana_roi.robinhood_chain_paper import RobinhoodChainPaperPlane

    assert getattr(
        RobinhoodChainPaperPlane,
        "_roi_post177_forward_pipeline_composition_compat_installed",
        False,
    ) is True
    assert getattr(
        guard._dispatch_range,
        "_roi_robinhood_research_getlogs_isolation",
        False,
    ) is True

    status = isolation.status()
    assert status["installed"] is True
    assert status["composition_order_independent"] is True
    assert status["public_research_getlogs_uses_official_public_rpc"] is True
    assert status["public_research_getlogs_uses_validation_cloud"] is False
    assert status["private_production_getlogs_preserves_governed_guard"] is True
    assert status["candidate_universe_reduced"] is False
    assert status["strategy_thresholds_changed"] is False
    assert status["paper_only"] is True
    assert status["live_money_authority"] is False
