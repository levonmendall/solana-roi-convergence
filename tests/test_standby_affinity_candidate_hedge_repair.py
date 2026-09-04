from __future__ import annotations

import asyncio
from types import SimpleNamespace

import httpx

from solana_roi import continuity_storage_capacity_repair as storage
from solana_roi import poll_recoverability_lease as lease
from solana_roi import live_poll_redundancy as live_poll
from solana_roi import production_capacity_repair as capacity
from solana_roi import rpc_workload_governor as governor
from solana_roi.candidate_initial_transaction_hedge_repair import (
    _CANDIDATE_INITIAL_TRANSACTION_HEDGE,
    _call_with_candidate_initial_hedge,
)
from solana_roi.candidate_rpc_priority_repair import WORKLOAD_CANDIDATE
from solana_roi.continuity_high_volume_poll_affinity_repair import (
    _assigned_endpoint_with_high_volume_affinity,
)
from solana_roi.direct_solana import DirectSolanaIngestionPlane, WatchTarget
from solana_roi.solana_rpc import RpcEndpoint, SolanaRpcPool


class FakeResponse:
    def __init__(self, result, *, status_code: int = 200, url: str = "https://rpc.example"):
        self._result = result
        self.status_code = status_code
        self.request = httpx.Request("POST", url)
        self._response = httpx.Response(status_code, request=self.request)

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise httpx.HTTPStatusError(
                f"http {self.status_code}",
                request=self.request,
                response=self._response,
            )

    def json(self):
        return {"jsonrpc": "2.0", "id": 1, "result": self._result}


class FakeClient:
    def __init__(self, result, *, delay: float = 0.0, status_code: int = 200):
        self.result = result
        self.delay = delay
        self.status_code = status_code
        self.calls = 0

    async def post(self, url, *, json):
        self.calls += 1
        if self.delay:
            await asyncio.sleep(self.delay)
        return FakeResponse(self.result, status_code=self.status_code, url=url)


def _public_pair() -> tuple[RpcEndpoint, RpcEndpoint]:
    return (
        RpcEndpoint(
            "publicnode",
            "https://solana-rpc.publicnode.com",
            "wss://solana-rpc.publicnode.com",
        ),
        RpcEndpoint(
            "solana-mainnet",
            "https://api.mainnet.solana.com",
            "wss://api.mainnet.solana.com",
        ),
    )


def test_high_volume_pump_targets_do_not_use_official_public_routine_primary(monkeypatch):
    endpoints = _public_pair()
    targets = (
        WatchTarget("scout", "scout-a", None),
        WatchTarget("program", "pump-fun", "PUMP_FUN"),
        WatchTarget("program", "ray-a", "RAYDIUM"),
        WatchTarget("program", "pump-amm", "PUMP_AMM"),
    )
    plane = SimpleNamespace(
        rpc=SimpleNamespace(endpoints=endpoints),
        endpoints=endpoints,
        watch_targets=targets,
    )

    # Freeze the underlying PR #77 assignment for this regression so import order
    # cannot make the helper recursively call an already-installed production wrapper.
    original = storage._assigned_endpoint
    module = __import__(
        "solana_roi.continuity_high_volume_poll_affinity_repair",
        fromlist=["_ORIGINAL_ASSIGNED_ENDPOINT"],
    )
    monkeypatch.setattr(module, "_ORIGINAL_ASSIGNED_ENDPOINT", original)

    # Under the original 5/5 index shard these odd-indexed targets would both land
    # on api.mainnet.solana.com. The repair keeps them on the non-official primary.
    assert original(plane, targets[1]).name == "solana-mainnet"
    assert original(plane, targets[3]).name == "solana-mainnet"
    assert _assigned_endpoint_with_high_volume_affinity(plane, targets[1]).name == "publicnode"
    assert _assigned_endpoint_with_high_volume_affinity(plane, targets[3]).name == "publicnode"

    # Lower-volume targets retain the established deterministic shard unchanged.
    assert _assigned_endpoint_with_high_volume_affinity(plane, targets[0]).name == original(plane, targets[0]).name
    assert _assigned_endpoint_with_high_volume_affinity(plane, targets[2]).name == original(plane, targets[2]).name


def test_high_volume_affinity_never_invents_a_provider(monkeypatch):
    official = _public_pair()[1]
    target = WatchTarget("program", "pump-fun", "PUMP_FUN")
    plane = SimpleNamespace(
        rpc=SimpleNamespace(endpoints=(official,)),
        endpoints=(official,),
        watch_targets=(target,),
    )
    original = storage._assigned_endpoint
    module = __import__(
        "solana_roi.continuity_high_volume_poll_affinity_repair",
        fromlist=["_ORIGINAL_ASSIGNED_ENDPOINT"],
    )
    monkeypatch.setattr(module, "_ORIGINAL_ASSIGNED_ENDPOINT", original)

    assert _assigned_endpoint_with_high_volume_affinity(plane, target) is official


def _candidate_pool(*, primary_delay: float = 0.03) -> tuple[SolanaRpcPool, FakeClient, FakeClient]:
    endpoints = _public_pair()
    primary = FakeClient({"slot": 1}, delay=primary_delay)
    official = FakeClient({"slot": 2})
    pool = SolanaRpcPool(
        endpoints,
        hedge_delay_seconds=0.001,
        clients={"publicnode": primary, "solana-mainnet": official},
    )
    return pool, primary, official


def test_frozen_scout_initial_transaction_can_hedge_to_official_secondary(monkeypatch):
    import solana_roi.candidate_initial_transaction_hedge_repair as repair

    pool, primary, official = _candidate_pool()
    monkeypatch.setattr(repair, "_PREVIOUS_CALL_WITH_META", capacity._capacity_call_with_meta)

    async def run():
        with governor.rpc_workload(WORKLOAD_CANDIDATE):
            token = _CANDIDATE_INITIAL_TRANSACTION_HEDGE.set(True)
            try:
                return await _call_with_candidate_initial_hedge(
                    pool,
                    "getTransaction",
                    ["sig-a", {"commitment": "confirmed"}],
                    hedge=True,
                )
            finally:
                _CANDIDATE_INITIAL_TRANSACTION_HEDGE.reset(token)

    result, provider, _latency = asyncio.run(run())
    assert result == {"slot": 2}
    assert provider == "solana-mainnet"
    assert primary.calls == 1
    assert official.calls == 1


def test_broad_public_pair_hedging_policy_remains_sequential(monkeypatch):
    import solana_roi.candidate_initial_transaction_hedge_repair as repair

    pool, primary, official = _candidate_pool()
    monkeypatch.setattr(repair, "_PREVIOUS_CALL_WITH_META", capacity._capacity_call_with_meta)

    async def run():
        with governor.rpc_workload(governor.WORKLOAD_CERTIFICATION):
            return await _call_with_candidate_initial_hedge(
                pool,
                "getTransaction",
                ["sig-a", {"commitment": "confirmed"}],
                hedge=True,
            )

    result, provider, _latency = asyncio.run(run())
    assert result == {"slot": 1}
    assert provider == "publicnode"
    assert primary.calls == 1
    assert official.calls == 0


def test_candidate_exception_is_limited_to_initial_get_transaction(monkeypatch):
    import solana_roi.candidate_initial_transaction_hedge_repair as repair

    pool, primary, official = _candidate_pool()
    monkeypatch.setattr(repair, "_PREVIOUS_CALL_WITH_META", capacity._capacity_call_with_meta)

    async def run():
        with governor.rpc_workload(WORKLOAD_CANDIDATE):
            token = _CANDIDATE_INITIAL_TRANSACTION_HEDGE.set(True)
            try:
                return await _call_with_candidate_initial_hedge(
                    pool,
                    "getSlot",
                    [],
                    hedge=True,
                )
            finally:
                _CANDIDATE_INITIAL_TRANSACTION_HEDGE.reset(token)

    result, provider, _latency = asyncio.run(run())
    assert result == {"slot": 1}
    assert provider == "publicnode"
    assert primary.calls == 1
    assert official.calls == 0


def test_production_composition_installs_both_repairs_and_preserves_hard_bounds():
    from solana_roi.production import app  # noqa: F401

    assert getattr(storage._assigned_endpoint, "_roi_high_volume_poll_affinity", False) is True
    assert getattr(SolanaRpcPool.call_with_meta, "_roi_candidate_initial_transaction_hedge", False) is True
    assert getattr(DirectSolanaIngestionPlane._get_transaction_ready, "_roi_candidate_initial_transaction_hedge", False) is True
    assert lease.POLL_RECOVERABILITY_LEASE_SECONDS == 12.0
    assert live_poll.POLL_CURSOR_MAX_PAGES == 3
    assert live_poll.POLL_LIMIT == 1000
