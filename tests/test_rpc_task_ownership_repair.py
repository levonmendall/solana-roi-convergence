from __future__ import annotations

import asyncio
import gc
from types import SimpleNamespace

import pytest

from solana_roi import rpc_task_ownership_repair as repair
from solana_roi.solana_rpc import RpcEndpoint, SolanaRpcPool


_ENDPOINT = RpcEndpoint(
    name="official-public",
    http_url="https://api.mainnet.solana.com",
    ws_url="wss://api.mainnet.solana.com",
)


@pytest.fixture(autouse=True)
def _reset(monkeypatch):
    original = SolanaRpcPool._call_endpoint
    repair._reset_state_for_tests()
    yield
    SolanaRpcPool._call_endpoint = original
    repair._reset_state_for_tests()


def test_detached_hedge_endpoint_failure_is_terminally_observed(monkeypatch) -> None:
    class Simulated429(RuntimeError):
        pass

    async def failing_endpoint(self, endpoint, method, params):
        del self, endpoint, method, params
        raise Simulated429("429 Too Many Requests")

    monkeypatch.setattr(SolanaRpcPool, "_call_endpoint", failing_endpoint)
    repair.install_rpc_task_ownership_repair()

    async def run() -> tuple[list[dict[str, object]], dict[str, object]]:
        loop = asyncio.get_running_loop()
        unhandled: list[dict[str, object]] = []
        previous = loop.get_exception_handler()
        loop.set_exception_handler(lambda _loop, context: unhandled.append(dict(context)))
        try:
            # Model the exact production failure shape: a distinct endpoint task is
            # allowed to finish with a 429 after its outer owner has lost the task.
            task = asyncio.create_task(
                repair._owned_call_endpoint(SimpleNamespace(), _ENDPOINT, "getSlot", [])
            )
            await asyncio.sleep(0)
            assert task.done()
            del task
            gc.collect()
            await asyncio.sleep(0)
            return unhandled, repair.status()
        finally:
            loop.set_exception_handler(previous)

    unhandled, state = asyncio.run(run())
    assert unhandled == []
    assert state["owned_endpoint_tasks"] == 1
    assert state["terminal_failures_observed"] == 1


def test_terminal_observer_does_not_change_awaited_failure_semantics(monkeypatch) -> None:
    class ExpectedFailure(RuntimeError):
        pass

    async def failing_endpoint(self, endpoint, method, params):
        del self, endpoint, method, params
        raise ExpectedFailure("preserve caller-visible failure")

    monkeypatch.setattr(SolanaRpcPool, "_call_endpoint", failing_endpoint)
    repair.install_rpc_task_ownership_repair()

    async def run() -> None:
        task = asyncio.create_task(
            repair._owned_call_endpoint(SimpleNamespace(), _ENDPOINT, "getTransaction", [])
        )
        with pytest.raises(ExpectedFailure, match="preserve caller-visible failure"):
            await task
        await asyncio.sleep(0)

    asyncio.run(run())
    state = repair.status()
    assert state["terminal_failures_observed"] == 1


def test_sequential_direct_await_does_not_attach_observer_to_parent(monkeypatch) -> None:
    async def successful_endpoint(self, endpoint, method, params):
        del self, method, params
        return {"ok": True}, endpoint.name, 1.0

    monkeypatch.setattr(SolanaRpcPool, "_call_endpoint", successful_endpoint)
    repair.install_rpc_task_ownership_repair()

    async def run() -> tuple[object, str, float]:
        # There is no child endpoint Task here; the endpoint coroutine is directly
        # awaited by this parent task and remains owned by normal call semantics.
        return await repair._owned_call_endpoint(SimpleNamespace(), _ENDPOINT, "getSlot", [])

    result, provider, latency = asyncio.run(run())
    assert result == {"ok": True}
    assert provider == _ENDPOINT.name
    assert latency == 1.0
    assert repair.status()["owned_endpoint_tasks"] == 0


def test_installer_composes_after_existing_capacity_wrapper(monkeypatch) -> None:
    calls: list[str] = []

    async def capacity_wrapper(self, endpoint, method, params):
        del self, endpoint, params
        calls.append(method)
        return {"ok": True}, "capacity", 2.0

    setattr(capacity_wrapper, "_roi_production_capacity_repair", True)
    monkeypatch.setattr(SolanaRpcPool, "_call_endpoint", capacity_wrapper)
    repair.install_rpc_task_ownership_repair()

    installed = SolanaRpcPool._call_endpoint
    assert installed is repair._owned_call_endpoint
    assert getattr(installed, "_roi_production_capacity_repair", False) is True
    assert getattr(installed, "_roi_rpc_task_terminal_ownership", False) is True

    async def run() -> None:
        task = asyncio.create_task(installed(SimpleNamespace(), _ENDPOINT, "getSlot", []))
        result, provider, latency = await task
        assert result == {"ok": True}
        assert provider == "capacity"
        assert latency == 2.0
        await asyncio.sleep(0)

    asyncio.run(run())
    assert calls == ["getSlot"]
    state = repair.status()
    assert state["terminal_successes_observed"] == 1
    assert state["paper_only"] is True
    assert state["live_money_authority"] is False
    assert state["signing_available"] is False
    assert state["transaction_submission_available"] is False
    assert state["certification_thresholds_changed"] is False
    assert state["economic_thresholds_changed"] is False
    assert state["canonical_evidence_reset"] is False
