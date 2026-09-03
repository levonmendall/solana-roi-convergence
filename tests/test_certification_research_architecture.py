from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from solana_roi import live_poll_redundancy as live_poll
from solana_roi import poll_recoverability_lease as lease
from solana_roi.certification_research_architecture import (
    _deadline_aware_hydrate_one,
    _isolated_research_pressure_reason,
    _new_research_rpc,
)
from solana_roi.config import BASELINE
from solana_roi.direct_solana import DirectSolanaIngestionPlane
from solana_roi.rpc_workload_governor import WORKLOAD_RESEARCH
from solana_roi.solana_rpc import RpcEndpoint, SolanaRpcPool


def _core_pool_with_alchemy() -> SolanaRpcPool:
    endpoints = (
        RpcEndpoint("publicnode", "https://solana-rpc.publicnode.com", "wss://solana-rpc.publicnode.com"),
        RpcEndpoint("solana-mainnet", "https://api.mainnet.solana.com", "wss://api.mainnet.solana.com"),
        RpcEndpoint(
            "alchemy",
            "https://solana-mainnet.g.alchemy.com/v2/test-key",
            "wss://solana-mainnet.streaming.alchemy.com/v2/test-key",
        ),
    )
    return SolanaRpcPool(endpoints)


async def _close_pool(pool: SolanaRpcPool) -> None:
    await asyncio.gather(
        *(client.aclose() for client in getattr(pool, "_clients", {}).values()),
        return_exceptions=True,
    )


def test_wallet_research_does_not_silently_enable_metered_alchemy(monkeypatch):
    monkeypatch.delenv("SOLANA_ROI_ENABLE_METERED_ALCHEMY", raising=False)
    core = _core_pool_with_alchemy()
    research = _new_research_rpc(core)
    try:
        assert research is not core
        assert getattr(research, "_roi_wallet_research_pool", False) is True
        assert [row.name for row in research.endpoints] == ["publicnode", "solana-mainnet"]
        assert getattr(research, "_roi_metered_alchemy_enabled", True) is False
    finally:
        asyncio.run(_close_pool(research))
        asyncio.run(_close_pool(core))


def test_wallet_research_can_use_alchemy_only_after_explicit_opt_in(monkeypatch):
    monkeypatch.setenv("SOLANA_ROI_ENABLE_METERED_ALCHEMY", "true")
    core = _core_pool_with_alchemy()
    research = _new_research_rpc(core)
    try:
        assert [row.name for row in research.endpoints] == ["publicnode", "solana-mainnet", "alchemy"]
        assert getattr(research, "_roi_metered_alchemy_enabled", False) is True
    finally:
        asyncio.run(_close_pool(research))
        asyncio.run(_close_pool(core))


def test_stale_non_authoritative_market_sample_expires_without_rpc(monkeypatch):
    from solana_roi import certification_research_architecture as architecture

    original_calls: list[str] = []

    async def original(_self, row):
        original_calls.append(str(row["signature"]))

    monkeypatch.setattr(architecture, "_ORIGINAL_DIRECT_HYDRATE_ONE", original)
    monkeypatch.setenv("SOLANA_ROI_BACKGROUND_HYDRATION_MAX_AGE_SECONDS", "120")

    finished: list[tuple[str, str | None, bool]] = []

    class Journal:
        def finish(self, signature, *, error=None, retry=False):
            finished.append((signature, error, retry))

    plane = SimpleNamespace(journal=Journal())
    old = datetime.now(timezone.utc) - timedelta(minutes=10)
    row = {
        "signature": "old-market-sample",
        "trigger_received_at": old.isoformat(),
        "reason": "deterministic_market_sample",
    }

    asyncio.run(_deadline_aware_hydrate_one(plane, row))

    assert original_calls == []
    assert finished == [
        ("old-market-sample", "expired_non_authoritative_background_hydration", False)
    ]
    assert getattr(plane, "_roi_expired_background_hydrations", 0) == 1


def test_launch_and_frozen_scout_work_never_use_background_expiry(monkeypatch):
    from solana_roi import certification_research_architecture as architecture

    calls: list[str] = []

    async def original(_self, row):
        calls.append(str(row["reason"]))

    monkeypatch.setattr(architecture, "_ORIGINAL_DIRECT_HYDRATE_ONE", original)
    old = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    plane = SimpleNamespace(journal=SimpleNamespace(finish=lambda *args, **kwargs: None))

    asyncio.run(
        _deadline_aware_hydrate_one(
            plane,
            {"signature": "launch", "trigger_received_at": old, "reason": "prospective_launch"},
        )
    )
    asyncio.run(
        _deadline_aware_hydrate_one(
            plane,
            {
                "signature": "scout",
                "trigger_received_at": old,
                "reason": "frozen_scout_processed_trigger",
            },
        )
    )

    assert calls == ["prospective_launch", "frozen_scout_processed_trigger"]


def test_isolated_wallet_research_ignores_core_raw_queue_pressure(monkeypatch):
    from solana_roi import certification_research_architecture as architecture

    rpc = SimpleNamespace(_roi_wallet_research_pool=True)
    discovery = SimpleNamespace(rpc=rpc)
    monkeypatch.setattr(architecture.capacity, "_rpc_redundancy_degraded", lambda _rpc: False)

    assert _isolated_research_pressure_reason(discovery) is None


def test_production_composition_preserves_strategy_and_continuity_boundaries():
    from solana_roi.production import app  # noqa: F401

    assert getattr(SolanaRpcPool._call_endpoint, "_roi_rpc_workload_governor", False) is True
    assert getattr(DirectSolanaIngestionPlane._hydrate_one, "_roi_certification_research_architecture", False) is True
    assert live_poll._poll_target is lease._leased_poll_target
    assert lease.POLL_RECOVERABILITY_LEASE_SECONDS == 12.0
    assert live_poll.POLL_CURSOR_MAX_PAGES == 3
    assert live_poll.POLL_LIMIT == 1000
    assert BASELINE.max_chase_fraction == 0.15
    assert WORKLOAD_RESEARCH == "research"
