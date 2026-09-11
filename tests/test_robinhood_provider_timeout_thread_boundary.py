from __future__ import annotations

import asyncio
import json
import threading

import httpx

from solana_roi import robinhood_chain_core as core
from solana_roi import robinhood_provider_failover as failover


_ALCHEMY_HTTP = "https://robinhood-mainnet.g.alchemy.com/v2/redacted"
_ALCHEMY_WS = "wss://robinhood-mainnet.g.alchemy.com/v2/redacted"
_DRPC_HTTP = "https://lb.drpc.live/robinhood/redacted"
_DRPC_WS = "wss://lb.drpc.live/robinhood/redacted"
_STRESS_ITERATIONS = 128


def _configure_alchemy_drpc_pool(monkeypatch) -> None:
    monkeypatch.setenv(
        "ROBINHOOD_RPC_ENDPOINTS_JSON",
        json.dumps(
            [
                {"name": "alchemy", "http": _ALCHEMY_HTTP, "ws": _ALCHEMY_WS},
                {"name": "drpc", "http": _DRPC_HTTP, "ws": _DRPC_WS},
            ]
        ),
    )
    monkeypatch.delenv("ROBINHOOD_RPC_URL", raising=False)
    monkeypatch.delenv("ROBINHOOD_WS_URL", raising=False)
    monkeypatch.delenv("ROBINHOOD_BACKUP_RPC_URL", raising=False)
    monkeypatch.delenv("ROBINHOOD_BACKUP_WS_URL", raising=False)
    monkeypatch.delenv("ROBINHOOD_PROVIDER_PRIMARY", raising=False)
    # A timeout must be enough to leave a bad provider immediately. The stress loop
    # resets provider state between iterations so it repeatedly exercises the same
    # production Alchemy -> verified dRPC failover boundary.
    monkeypatch.setenv("ROBINHOOD_PROVIDER_FAILOVER_ERROR_THRESHOLD", "1")
    monkeypatch.setenv("ROBINHOOD_PROVIDER_FAILOVER_COOLDOWN_SECONDS", "30")
    failover.reset_for_tests()


def test_repeated_alchemy_getlogs_timeouts_fail_over_to_drpc_without_thread_growth(
    monkeypatch,
) -> None:
    _configure_alchemy_drpc_pool(monkeypatch)
    calls: list[tuple[str, str]] = []

    async def scenario() -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            payload = json.loads(request.content.decode("utf-8"))
            method = str(payload["method"])
            host = request.url.host or ""
            calls.append((host, method))

            if "alchemy.com" in host:
                assert method == "eth_getLogs"
                raise httpx.ReadTimeout(
                    "deterministic Alchemy read timeout",
                    request=request,
                )
            if host == "lb.drpc.live" and method == "eth_chainId":
                return httpx.Response(
                    200,
                    request=request,
                    json={
                        "jsonrpc": "2.0",
                        "id": payload["id"],
                        "result": hex(core.ROBINHOOD_CHAIN_ID),
                    },
                )
            if host == "lb.drpc.live" and method == "eth_getLogs":
                return httpx.Response(
                    200,
                    request=request,
                    json={
                        "jsonrpc": "2.0",
                        "id": payload["id"],
                        "result": [{"blockNumber": "0x1", "logIndex": "0x0"}],
                    },
                )
            raise AssertionError(f"unexpected provider/method: {host} {method}")

        rpc = core.RobinhoodRpc(_ALCHEMY_HTTP, timeout_seconds=0.05)
        await rpc.client.aclose()
        rpc.client = httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            timeout=0.05,
        )
        wrapped = failover._rpc_wrapper(core.RobinhoodRpc.rpc)

        async def forbidden_to_thread(*_args, **_kwargs):
            raise AssertionError("Robinhood provider failover must not enter asyncio.to_thread")

        monkeypatch.setattr(asyncio, "to_thread", forbidden_to_thread)
        baseline_threads = {
            thread.ident
            for thread in threading.enumerate()
            if thread.is_alive() and thread.ident is not None
        }
        peak_thread_count = len(baseline_threads)

        try:
            for _ in range(_STRESS_ITERATIONS):
                failover.reset_for_tests()
                provider = failover.active_provider()
                assert provider is not None
                assert provider.name == "alchemy"
                rpc.rpc_url = provider.http

                result = await wrapped(
                    rpc,
                    "eth_getLogs",
                    [{"fromBlock": "0x1", "toBlock": "0x1"}],
                )
                assert result == [{"blockNumber": "0x1", "logIndex": "0x0"}]
                assert failover.active_name() == "drpc"
                peak_thread_count = max(peak_thread_count, threading.active_count())
                await asyncio.sleep(0)

            after_threads = {
                thread.ident
                for thread in threading.enumerate()
                if thread.is_alive() and thread.ident is not None
            }
            assert after_threads - baseline_threads == set()
            assert peak_thread_count == len(baseline_threads)
        finally:
            await rpc.close()
            failover.reset_for_tests()

    asyncio.run(scenario())

    assert len(calls) == _STRESS_ITERATIONS * 3
    for offset in range(0, len(calls), 3):
        assert calls[offset : offset + 3] == [
            ("robinhood-mainnet.g.alchemy.com", "eth_getLogs"),
            ("lb.drpc.live", "eth_chainId"),
            ("lb.drpc.live", "eth_getLogs"),
        ]
