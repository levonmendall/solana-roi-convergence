from __future__ import annotations

import asyncio

import httpx

from solana_roi import candidate_rpc_hedge_repair as repair
from solana_roi import production_capacity_repair as capacity
from solana_roi import rpc_workload_governor as governor
from solana_roi.solana_rpc import RpcEndpoint, SolanaRpcPool


class FakeResponse:
    def __init__(self, result, *, url: str):
        self._result = result
        self.request = httpx.Request("POST", url)

    def raise_for_status(self) -> None:
        return None

    def json(self):
        return {"jsonrpc": "2.0", "id": 1, "result": self._result}


class FakeClient:
    def __init__(self, result, *, delay: float = 0.0):
        self.result = result
        self.delay = delay
        self.calls = 0

    async def post(self, url, *, json):
        self.calls += 1
        if self.delay:
            await asyncio.sleep(self.delay)
        return FakeResponse(self.result, url=url)


def _pool() -> tuple[SolanaRpcPool, FakeClient, FakeClient]:
    endpoints = (
        RpcEndpoint("publicnode", "https://solana-rpc.publicnode.com", "wss://solana-rpc.publicnode.com"),
        RpcEndpoint("solana-mainnet", "https://api.mainnet.solana.com", "wss://api.mainnet.solana.com"),
    )
    primary = FakeClient({"slot": 1}, delay=0.05)
    official = FakeClient({"slot": 2}, delay=0.0)
    pool = SolanaRpcPool(
        endpoints,
        hedge_delay_seconds=0.001,
        clients={"publicnode": primary, "solana-mainnet": official},
    )
    return pool, primary, official


def test_candidate_read_may_hedge_to_official_public_secondary(monkeypatch):
    pool, primary, official = _pool()
    monkeypatch.setattr(repair, "_ORIGINAL_CALL_WITH_META", capacity._capacity_call_with_meta)
    governor._VALID_WORKLOADS = frozenset(set(governor._VALID_WORKLOADS) | {"candidate"})

    async def run():
        with governor.rpc_workload("candidate"):
            return await repair._candidate_hedged_call_with_meta(pool, "getTransaction", ["sig"], hedge=True)

    result, provider, _latency = asyncio.run(run())
    assert result == {"slot": 2}
    assert provider == "solana-mainnet"
    assert primary.calls == 1
    assert official.calls == 1


def test_non_candidate_keeps_sequential_official_public_fallback(monkeypatch):
    pool, primary, official = _pool()
    monkeypatch.setattr(repair, "_ORIGINAL_CALL_WITH_META", capacity._capacity_call_with_meta)

    async def run():
        with governor.rpc_workload(governor.WORKLOAD_CERTIFICATION):
            return await repair._candidate_hedged_call_with_meta(pool, "getTransaction", ["sig"], hedge=True)

    result, provider, _latency = asyncio.run(run())
    assert result == {"slot": 1}
    assert provider == "publicnode"
    assert primary.calls == 1
    assert official.calls == 0


def test_candidate_hedge_does_not_change_certification_or_authority_boundaries():
    from solana_roi import live_poll_redundancy as live_poll
    from solana_roi import poll_recoverability_lease as lease
    from solana_roi.config import BASELINE

    assert lease.POLL_RECOVERABILITY_LEASE_SECONDS == 12.0
    assert live_poll.POLL_CURSOR_MAX_PAGES == 3
    assert live_poll.POLL_LIMIT == 1000
    assert BASELINE.max_chase_fraction == 0.15
