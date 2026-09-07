from __future__ import annotations

import asyncio
import gc

from solana_roi.solana_rpc import RpcEndpoint, SolanaRpcPool


class _SimultaneousPool(SolanaRpcPool):
    def __init__(self) -> None:
        endpoints = (
            RpcEndpoint("failed", "https://failed.example", "wss://failed.example"),
            RpcEndpoint("good", "https://good.example", "wss://good.example"),
        )
        super().__init__(endpoints, hedge_delay_seconds=0.001, clients={"failed": object(), "good": object()})
        self._started: set[str] = set()
        self._release = asyncio.Event()

    async def _call_endpoint(self, endpoint, method, params):  # type: ignore[override]
        self._started.add(endpoint.name)
        if len(self._started) == 2:
            self._release.set()
        await self._release.wait()
        if endpoint.name == "failed":
            raise TimeoutError("simulated simultaneous hedge failure")
        return 777, endpoint.name, 1.0


def test_simultaneous_hedge_success_drains_already_done_failed_sibling() -> None:
    async def run() -> tuple[int, str, list[dict[str, object]]]:
        pool = _SimultaneousPool()
        loop = asyncio.get_running_loop()
        unhandled: list[dict[str, object]] = []
        previous = loop.get_exception_handler()
        loop.set_exception_handler(lambda _loop, context: unhandled.append(dict(context)))
        try:
            result, provider, _latency = await pool.call_with_meta("getSlot", [], hedge=True)
            await asyncio.sleep(0)
            gc.collect()
            await asyncio.sleep(0)
            return int(result), str(provider), unhandled
        finally:
            loop.set_exception_handler(previous)

    result, provider, unhandled = asyncio.run(run())
    assert result == 777
    assert provider == "good"
    assert unhandled == []
