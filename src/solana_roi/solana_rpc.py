from __future__ import annotations

import asyncio
import itertools
import json
import os
import time
from dataclasses import asdict, dataclass
from typing import Any

import httpx


@dataclass(frozen=True, slots=True)
class RpcEndpoint:
    name: str
    http_url: str
    ws_url: str


DEFAULT_RPC_ENDPOINTS: tuple[RpcEndpoint, ...] = (
    RpcEndpoint(
        name="publicnode",
        http_url="https://solana-rpc.publicnode.com",
        ws_url="wss://solana-rpc.publicnode.com",
    ),
    RpcEndpoint(
        name="onfinality",
        http_url="https://solana.api.onfinality.io/public",
        ws_url="wss://solana.api.onfinality.io/public-ws",
    ),
)


def rpc_endpoints_from_env(env: dict[str, str] | None = None) -> tuple[RpcEndpoint, ...]:
    values = env if env is not None else os.environ
    raw = str(values.get("SOLANA_ROI_RPC_ENDPOINTS_JSON") or "").strip()
    if not raw:
        return DEFAULT_RPC_ENDPOINTS
    payload = json.loads(raw)
    if not isinstance(payload, list) or not payload:
        raise ValueError("SOLANA_ROI_RPC_ENDPOINTS_JSON must be a non-empty JSON array")
    rows: list[RpcEndpoint] = []
    seen_names: set[str] = set()
    seen_http: set[str] = set()
    seen_ws: set[str] = set()
    for item in payload:
        if not isinstance(item, dict):
            raise ValueError("RPC endpoint entries must be objects")
        name = str(item.get("name") or "").strip()
        http_url = str(item.get("http") or item.get("http_url") or "").strip()
        ws_url = str(item.get("ws") or item.get("ws_url") or "").strip()
        if not name or not http_url.startswith("https://") or not ws_url.startswith("wss://"):
            raise ValueError("each RPC endpoint requires name, https http URL, and wss URL")
        if name in seen_names or http_url in seen_http or ws_url in seen_ws:
            raise ValueError("RPC endpoints must be distinct")
        seen_names.add(name)
        seen_http.add(http_url)
        seen_ws.add(ws_url)
        rows.append(RpcEndpoint(name=name, http_url=http_url, ws_url=ws_url))
    return tuple(rows)


class SolanaRpcPool:
    """Redundant read-only Solana JSON-RPC pool with latency hedging.

    This client never signs or submits transactions. Read requests are tried on
    independent endpoints, and latency-critical reads are hedged after a short
    delay so a slow free endpoint does not dominate p95/p99 candidate latency.
    """

    def __init__(
        self,
        endpoints: tuple[RpcEndpoint, ...] | None = None,
        *,
        timeout_seconds: float = 2.5,
        hedge_delay_seconds: float = 0.15,
        clients: dict[str, Any] | None = None,
    ):
        self.endpoints = endpoints or rpc_endpoints_from_env()
        if not self.endpoints:
            raise ValueError("at least one Solana RPC endpoint is required")
        self.timeout_seconds = max(0.25, float(timeout_seconds))
        self.hedge_delay_seconds = max(0.0, float(hedge_delay_seconds))
        self._ids = itertools.count(1)
        self._clients: dict[str, Any] = {}
        supplied = clients or {}
        for endpoint in self.endpoints:
            self._clients[endpoint.name] = supplied.get(endpoint.name) or httpx.AsyncClient(
                timeout=self.timeout_seconds,
                headers={"Accept": "application/json", "Content-Type": "application/json"},
            )
        self._health: dict[str, dict[str, Any]] = {
            endpoint.name: {
                "successes": 0,
                "failures": 0,
                "last_latency_ms": None,
                "ewma_latency_ms": None,
                "last_error_type": None,
                "last_success_at_monotonic": None,
            }
            for endpoint in self.endpoints
        }

    def _ordered(self) -> list[RpcEndpoint]:
        def score(endpoint: RpcEndpoint) -> tuple[int, float]:
            state = self._health[endpoint.name]
            failures = int(state["failures"])
            successes = int(state["successes"])
            failure_penalty = 1 if failures > successes + 2 else 0
            latency = state["ewma_latency_ms"]
            return failure_penalty, float(latency if latency is not None else 1_000.0)
        return sorted(self.endpoints, key=score)

    async def _call_endpoint(self, endpoint: RpcEndpoint, method: str, params: list[Any]) -> tuple[Any, str, float]:
        started = time.perf_counter()
        payload = {
            "jsonrpc": "2.0",
            "id": next(self._ids),
            "method": method,
            "params": params,
        }
        try:
            response = await self._clients[endpoint.name].post(endpoint.http_url, json=payload)
            response.raise_for_status()
            body = response.json()
            if not isinstance(body, dict):
                raise RuntimeError("Solana RPC response is not an object")
            if body.get("error"):
                raise RuntimeError(f"Solana RPC {method} returned an error")
            elapsed = max(0.0, (time.perf_counter() - started) * 1000.0)
            state = self._health[endpoint.name]
            state["successes"] = int(state["successes"]) + 1
            state["last_latency_ms"] = elapsed
            previous = state["ewma_latency_ms"]
            state["ewma_latency_ms"] = elapsed if previous is None else 0.8 * float(previous) + 0.2 * elapsed
            state["last_error_type"] = None
            state["last_success_at_monotonic"] = time.monotonic()
            return body.get("result"), endpoint.name, elapsed
        except Exception as exc:
            state = self._health[endpoint.name]
            state["failures"] = int(state["failures"]) + 1
            state["last_error_type"] = type(exc).__name__
            raise

    async def call_with_meta(
        self,
        method: str,
        params: list[Any],
        *,
        hedge: bool = False,
    ) -> tuple[Any, str, float]:
        ordered = self._ordered()
        if len(ordered) == 1 or not hedge:
            last_error: Exception | None = None
            for endpoint in ordered:
                try:
                    return await self._call_endpoint(endpoint, method, params)
                except Exception as exc:
                    last_error = exc
            raise RuntimeError(f"all Solana RPC endpoints failed for {method}") from last_error

        primary = asyncio.create_task(self._call_endpoint(ordered[0], method, params))
        hedge_task: asyncio.Task[tuple[Any, str, float]] | None = None
        try:
            done, _ = await asyncio.wait({primary}, timeout=self.hedge_delay_seconds)
            if primary in done:
                return primary.result()
            hedge_task = asyncio.create_task(self._call_endpoint(ordered[1], method, params))
            pending: set[asyncio.Task[tuple[Any, str, float]]] = {primary, hedge_task}
            errors: list[Exception] = []
            while pending:
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    try:
                        result = task.result()
                    except Exception as exc:
                        errors.append(exc)
                        continue
                    for loser in pending:
                        loser.cancel()
                    if pending:
                        await asyncio.gather(*pending, return_exceptions=True)
                    return result
            for endpoint in ordered[2:]:
                try:
                    return await self._call_endpoint(endpoint, method, params)
                except Exception as exc:
                    errors.append(exc)
            raise RuntimeError(f"all Solana RPC endpoints failed for {method}") from (errors[-1] if errors else None)
        finally:
            for task in (primary, hedge_task):
                if task is not None and not task.done():
                    task.cancel()

    async def call(self, method: str, params: list[Any], *, hedge: bool = False) -> Any:
        result, _provider, _latency = await self.call_with_meta(method, params, hedge=hedge)
        return result

    async def get_transaction(self, signature: str, *, hedge: bool = True) -> tuple[Any, str, float]:
        return await self.call_with_meta(
            "getTransaction",
            [
                signature,
                {
                    "encoding": "jsonParsed",
                    "commitment": "confirmed",
                    "maxSupportedTransactionVersion": 0,
                },
            ],
            hedge=hedge,
        )

    async def get_signatures_for_address(
        self,
        address: str,
        *,
        before: str | None = None,
        limit: int = 1000,
        hedge: bool = False,
    ) -> tuple[list[dict[str, Any]], str, float]:
        config: dict[str, Any] = {"commitment": "confirmed", "limit": max(1, min(1000, int(limit)))}
        if before:
            config["before"] = before
        result, provider, latency = await self.call_with_meta(
            "getSignaturesForAddress",
            [address, config],
            hedge=hedge,
        )
        rows = [row for row in result if isinstance(row, dict)] if isinstance(result, list) else []
        return rows, provider, latency

    def status(self) -> dict[str, Any]:
        return {
            "configured": True,
            "endpoint_count": len(self.endpoints),
            "redundant": len(self.endpoints) >= 2,
            "hedge_delay_ms": self.hedge_delay_seconds * 1000.0,
            "endpoints": [
                {
                    "name": endpoint.name,
                    "http_host": endpoint.http_url.split("/", 3)[2],
                    "ws_host": endpoint.ws_url.split("/", 3)[2],
                    **self._health[endpoint.name],
                }
                for endpoint in self.endpoints
            ],
        }

    def endpoint_summary(self) -> list[dict[str, str]]:
        return [asdict(endpoint) for endpoint in self.endpoints]
