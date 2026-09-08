from __future__ import annotations

import asyncio
import gc
from types import SimpleNamespace

import pytest

from solana_roi import production_capacity_repair as capacity
from solana_roi import rpc_task_ownership_repair as repair
from solana_roi.solana_rpc import RpcEndpoint, SolanaRpcPool


_ENDPOINT = RpcEndpoint(
    name="public",
    http_url="https://api.mainnet.solana.com",
    ws_url="wss://api.mainnet.solana.com",
)


@pytest.fixture(autouse=True)
def _preserve_composed_rpc_state():
    original_class_method = SolanaRpcPool._call_endpoint
    original_capacity_global = capacity._capacity_call_endpoint
    original_delegate = repair._ORIGINAL_CALL_ENDPOINT
    original_capacity_delegate = repair._ORIGINAL_CAPACITY_CALL_ENDPOINT
    with repair._STATE_LOCK:
        original_state = dict(repair._STATE)
    try:
        yield
    finally:
        SolanaRpcPool._call_endpoint = original_class_method
        capacity._capacity_call_endpoint = original_capacity_global
        repair._ORIGINAL_CALL_ENDPOINT = original_delegate
        repair._ORIGINAL_CAPACITY_CALL_ENDPOINT = original_capacity_delegate
        with repair._STATE_LOCK:
            repair._STATE.clear()
            repair._STATE.update(original_state)


def _install_with_late_capacity_root(monkeypatch, failure: BaseException) -> None:
    async def simulated_capacity(self, endpoint, method, params):
        del self, endpoint, method, params
        raise failure

    setattr(simulated_capacity, "_roi_production_capacity_repair", True)
    monkeypatch.setattr(capacity, "_capacity_call_endpoint", simulated_capacity)
    monkeypatch.setattr(SolanaRpcPool, "_call_endpoint", simulated_capacity)
    repair._ORIGINAL_CALL_ENDPOINT = None
    repair._ORIGINAL_CAPACITY_CALL_ENDPOINT = None
    with repair._STATE_LOCK:
        for key in tuple(repair._STATE):
            repair._STATE[key] = False if key == "installed" else 0

    repair.install_rpc_task_ownership_repair()

    # Reproduce the live topology that escaped v1: after ownership installation,
    # a later capacity composition makes the capacity wrapper itself the Task root.
    SolanaRpcPool._call_endpoint = capacity._capacity_call_endpoint


def test_late_capacity_root_connect_timeout_is_terminally_observed(monkeypatch) -> None:
    class SimulatedConnectTimeout(RuntimeError):
        pass

    _install_with_late_capacity_root(monkeypatch, SimulatedConnectTimeout("connect timeout"))

    async def run() -> list[dict[str, object]]:
        loop = asyncio.get_running_loop()
        unhandled: list[dict[str, object]] = []
        previous = loop.get_exception_handler()
        loop.set_exception_handler(lambda _loop, context: unhandled.append(dict(context)))
        try:
            task = asyncio.create_task(
                SolanaRpcPool._call_endpoint(SimpleNamespace(), _ENDPOINT, "getTransaction", [])
            )
            await asyncio.sleep(0)
            assert task.done()
            del task
            gc.collect()
            await asyncio.sleep(0)
            return unhandled
        finally:
            loop.set_exception_handler(previous)

    assert asyncio.run(run()) == []
    state = repair.status()
    assert state["owned_capacity_root_tasks"] == 1
    assert state["terminal_failures_observed"] == 1
    assert state["active_call_endpoint_terminally_owned"] is True
    assert state["capacity_call_endpoint_terminally_owned"] is True
    assert state["late_capacity_composition_guarded"] is True


def test_late_capacity_root_awaited_failure_semantics_are_unchanged(monkeypatch) -> None:
    class Simulated429(RuntimeError):
        pass

    _install_with_late_capacity_root(monkeypatch, Simulated429("429 Too Many Requests"))

    async def run() -> None:
        task = asyncio.create_task(
            SolanaRpcPool._call_endpoint(SimpleNamespace(), _ENDPOINT, "getSlot", [])
        )
        with pytest.raises(Simulated429, match="429 Too Many Requests"):
            await task
        await asyncio.sleep(0)

    asyncio.run(run())
    state = repair.status()
    assert state["owned_capacity_root_tasks"] == 1
    assert state["terminal_failures_observed"] == 1
    assert state["paper_only"] is True
    assert state["live_money_authority"] is False
    assert state["signing_available"] is False
    assert state["transaction_submission_available"] is False
    assert state["certification_thresholds_changed"] is False
    assert state["economic_thresholds_changed"] is False
    assert state["canonical_evidence_reset"] is False
