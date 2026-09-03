from __future__ import annotations

import asyncio
from types import SimpleNamespace

from solana_roi import funding_rpc_freshness_repair as funding_repair
from solana_roi import launch_context_rpc_repair as context_repair
from solana_roi import launch_coverage_bridge as bridge
from solana_roi.launch_funding import LaunchFundingPolicy


class NullThenSecondaryRpc:
    def __init__(self, transaction: dict):
        self.transaction = transaction
        self.primary_calls = 0
        self.secondary_calls = 0
        self.primary = SimpleNamespace(name="primary")
        self.secondary = SimpleNamespace(name="secondary")

    async def get_transaction(self, signature: str, *, hedge: bool = True):
        self.primary_calls += 1
        return None, "primary", 1.0

    def _ordered(self, method: str):
        assert method == "getTransaction"
        return [self.primary, self.secondary]

    async def _call_endpoint(self, endpoint, method: str, params):
        assert method == "getTransaction"
        self.secondary_calls += 1
        if endpoint.name == "secondary":
            return self.transaction, "secondary", 2.0
        return None, endpoint.name, 1.0


class AlwaysNullRpc(NullThenSecondaryRpc):
    async def _call_endpoint(self, endpoint, method: str, params):
        self.secondary_calls += 1
        return None, endpoint.name, 2.0


def test_launch_context_recovers_non_null_transaction_from_sibling_backend() -> None:
    async def scenario() -> None:
        tx = {"blockTime": 123, "transaction": {"message": {"instructions": []}}, "meta": {}}
        rpc = NullThenSecondaryRpc(tx)
        plane = SimpleNamespace(rpc=rpc)
        result, provider, latency = await context_repair._context_transaction_ready(plane, "sig")
        assert result is tx
        assert provider == "secondary"
        assert latency == 2.0
        assert rpc.primary_calls == 1
        assert rpc.secondary_calls == 1
        assert plane._roi_launch_context_rpc_secondary_non_null_recoveries == 1

    asyncio.run(scenario())


def test_funding_recovers_non_null_transaction_from_sibling_backend() -> None:
    async def scenario() -> None:
        tx = {"blockTime": 123, "transaction": {"message": {"instructions": []}}, "meta": {}}
        rpc = NullThenSecondaryRpc(tx)
        collector = SimpleNamespace(rpc=rpc)
        result = await funding_repair._transaction_with_freshness_retry(collector, "sig")
        assert result is tx
        assert rpc.primary_calls == 1
        assert rpc.secondary_calls == 1
        assert collector._roi_funding_provenance_transaction_secondary_non_null_recoveries == 1

    asyncio.run(scenario())


def test_context_read_remains_bounded_when_every_backend_is_null() -> None:
    async def scenario() -> None:
        rpc = AlwaysNullRpc({})
        plane = SimpleNamespace(rpc=rpc)
        original_delay = context_repair.CONTEXT_TRANSACTION_RETRY_DELAY_SECONDS
        context_repair.CONTEXT_TRANSACTION_RETRY_DELAY_SECONDS = 0.0
        try:
            try:
                await context_repair._context_transaction_ready(plane, "sig")
            except RuntimeError:
                pass
            else:
                raise AssertionError("missing confirmed transaction must fail closed")
        finally:
            context_repair.CONTEXT_TRANSACTION_RETRY_DELAY_SECONDS = original_delay
        assert rpc.primary_calls == context_repair.CONTEXT_TRANSACTION_RPC_ROUNDS
        assert plane._roi_launch_context_rpc_exhausted == 1

    asyncio.run(scenario())


def test_reliability_repairs_do_not_change_governed_launch_boundaries() -> None:
    policy = LaunchFundingPolicy()
    assert policy.launch_window_seconds == 8.0
    assert policy.max_pair_stream_lag_seconds == 3.0
    assert bridge.LAUNCH_CONTEXT_DEADLINE_SECONDS == 35.0
    assert context_repair.CONTEXT_TRANSACTION_RPC_ROUNDS == 3
    assert funding_repair.FUNDING_TRANSACTION_RPC_ROUNDS == 3
