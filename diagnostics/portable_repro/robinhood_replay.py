#!/usr/bin/env python3
"""Deterministic bounded loopback HTTPS/WSS replay for Robinhood providers.

The harness redirects the canonical Alchemy/dRPC configuration to these loopback
endpoints. HTTP and WebSocket handling is asyncio-based: the replay does not create
a client, executor, or native thread per request, so it does not contaminate the
thread/memory attribution experiment it is meant to support.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import ssl
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import websockets

CHAIN_ID = 4663
CHAIN_ID_HEX = hex(CHAIN_ID)
DEFAULT_HEAD = 9_250_000
MAX_BODY = 8 * 1024 * 1024
SCENARIOS = (
    "steady",
    "alchemy-429-then-drpc",
    "alchemy-500-then-drpc",
    "alchemy-timeout-then-drpc",
    "alchemy-retry-then-success",
    "alchemy-cancel-delay",
    "drpc-503-then-alchemy",
    "drpc-timeout-then-alchemy",
    "concurrency-burst",
)
RECOMMENDED_PRIMARY = {
    "drpc-503-then-alchemy": "drpc",
    "drpc-timeout-then-alchemy": "drpc",
}


@dataclass(frozen=True)
class FaultAction:
    status: int = 200
    delay_seconds: float = 0.0
    error_code: int | None = None
    message: str | None = None


@dataclass
class ReplayState:
    scenario: str = "alchemy-429-then-drpc"
    head: int = DEFAULT_HEAD
    request_counts: dict[tuple[str, str, str], int] = field(default_factory=dict)
    active_http: int = 0
    max_active_http: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)

    def next_count(self, provider: str, transport: str, method: str) -> int:
        key = (provider, transport, method)
        with self.lock:
            value = self.request_counts.get(key, 0) + 1
            self.request_counts[key] = value
            return value

    def enter_http(self) -> tuple[int, int]:
        with self.lock:
            self.active_http += 1
            self.max_active_http = max(self.max_active_http, self.active_http)
            return self.active_http, self.max_active_http

    def exit_http(self) -> None:
        with self.lock:
            self.active_http = max(0, self.active_http - 1)

    def advance_head(self) -> int:
        with self.lock:
            self.head += 1
            return self.head


def _hex32(value: int) -> str:
    return "0x" + int(value).to_bytes(32, "big", signed=False).hex()


def _selector_result(raw_hex: str) -> str:
    raw = str(raw_hex or "0x").removeprefix("0x")
    try:
        payload = bytes.fromhex(raw)
    except ValueError:
        payload = raw.encode("utf-8", "replace")
    return "0x" + hashlib.sha3_256(payload).hexdigest()


def jsonrpc_result(method: str, params: list[Any], *, head: int) -> Any:
    if method == "eth_chainId":
        return CHAIN_ID_HEX
    if method == "eth_blockNumber":
        return hex(head)
    if method == "eth_gasPrice":
        return hex(1_000_000_000)
    if method == "eth_getLogs":
        return []
    if method == "eth_getTransactionByHash":
        return {"from": "0x" + "1" * 40}
    if method == "eth_call":
        return _hex32(0)
    if method == "web3_sha3":
        return _selector_result(str(params[0] if params else "0x"))
    return "0x0"


def scenario_action(
    scenario: str,
    *,
    provider: str,
    method: str,
    count: int,
    fault_delay: float = 15.0,
) -> FaultAction:
    if method != "eth_chainId":
        return FaultAction()
    if scenario == "alchemy-429-then-drpc" and provider == "alchemy" and count == 1:
        return FaultAction(429, error_code=-32005, message="synthetic alchemy rate limit")
    if scenario == "alchemy-500-then-drpc" and provider == "alchemy" and count == 1:
        return FaultAction(500, error_code=-32603, message="synthetic alchemy server error")
    if scenario == "alchemy-timeout-then-drpc" and provider == "alchemy" and count == 1:
        return FaultAction(delay_seconds=fault_delay)
    if scenario == "alchemy-retry-then-success" and provider == "alchemy" and count == 1:
        return FaultAction(503, error_code=-32603, message="synthetic retryable alchemy outage")
    if scenario == "alchemy-cancel-delay" and provider == "alchemy":
        return FaultAction(delay_seconds=fault_delay)
    if scenario == "drpc-503-then-alchemy" and provider == "drpc" and count == 1:
        return FaultAction(503, error_code=-32603, message="synthetic dRPC outage")
    if scenario == "drpc-timeout-then-alchemy" and provider == "drpc" and count == 1:
        return FaultAction(delay_seconds=fault_delay)
    return FaultAction()


def _response_for_action(action: FaultAction, *, request_id: Any, method: str, params: list[Any], head: int) -> tuple[int, dict[str, Any]]:
    if action.status != 200:
        return action.status, {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {"code": action.error_code or -32603, "message": action.message or "synthetic replay fault"},
        }
    return 200, {"jsonrpc": "2.0", "id": request_id, "result": jsonrpc_result(method, params, head=head)}


def http_response(
    state: ReplayState,
    *,
    provider: str,
    method: str,
    params: list[Any],
    request_id: Any,
) -> tuple[int, dict[str, Any]]:
    """Pure response helper retained for unit tests; network delay is handled by server."""
    count = state.next_count(provider, "http", method)
    action = scenario_action(state.scenario, provider=provider, method=method, count=count)
    return _response_for_action(action, request_id=request_id, method=method, params=params, head=state.head)


class EvidenceWriter:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.Lock()

    def write(self, payload: dict[str, Any]) -> None:
        line = json.dumps({"ts_unix": time.time(), **payload}, sort_keys=True) + "\n"
        with self.lock, self.path.open("a", encoding="utf-8") as handle:
            handle.write(line)
            handle.flush()


def _parse_headers(raw: bytes) -> tuple[str, dict[str, str]]:
    lines = raw.decode("iso-8859-1").split("\r\n")
    request_line = lines[0] if lines else ""
    headers: dict[str, str] = {}
    for line in lines[1:]:
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        headers[key.strip().lower()] = value.strip()
    return request_line, headers


def _http_bytes(status: int, body: dict[str, Any]) -> bytes:
    encoded = json.dumps(body, separators=(",", ":")).encode("utf-8")
    reasons = {200: "OK", 429: "Too Many Requests", 500: "Internal Server Error", 503: "Service Unavailable"}
    reason = reasons.get(status, "Replay")
    return (
        f"HTTP/1.1 {status} {reason}\r\n"
        "content-type: application/json\r\n"
        f"content-length: {len(encoded)}\r\n"
        "connection: close\r\n\r\n"
    ).encode("ascii") + encoded


async def handle_http(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    *,
    provider: str,
    state: ReplayState,
    evidence: EvidenceWriter,
    fault_delay: float,
) -> None:
    active, peak = state.enter_http()
    method = "<unknown>"
    count = 0
    disconnected = False
    try:
        header_raw = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=10.0)
        request_line, headers = _parse_headers(header_raw[:-4])
        if not request_line.startswith("POST "):
            raise ValueError("only POST is supported")
        length = int(headers.get("content-length", "0"))
        if length < 0 or length > MAX_BODY:
            raise ValueError("invalid content-length")
        raw = await asyncio.wait_for(reader.readexactly(length), timeout=10.0)
        request = json.loads(raw.decode("utf-8"))
        method = str(request.get("method") or "")
        params = list(request.get("params") or [])
        request_id = request.get("id")
        count = state.next_count(provider, "http", method)
        action = scenario_action(
            state.scenario,
            provider=provider,
            method=method,
            count=count,
            fault_delay=fault_delay,
        )
        evidence.write({
            "scenario": state.scenario,
            "provider": provider,
            "transport": "http",
            "method": method,
            "request_count": count,
            "planned_status": action.status,
            "planned_delay_seconds": action.delay_seconds,
            "active_http": active,
            "max_active_http": peak,
            "request_bytes": len(raw),
        })
        if action.delay_seconds > 0:
            await asyncio.sleep(action.delay_seconds)
        status, body = _response_for_action(
            action,
            request_id=request_id,
            method=method,
            params=params,
            head=state.head,
        )
        writer.write(_http_bytes(status, body))
        await writer.drain()
        evidence.write({
            "scenario": state.scenario,
            "provider": provider,
            "transport": "http",
            "method": method,
            "request_count": count,
            "status": status,
            "outcome": "response_sent",
        })
    except (BrokenPipeError, ConnectionResetError, asyncio.IncompleteReadError) as exc:
        disconnected = True
        evidence.write({
            "scenario": state.scenario,
            "provider": provider,
            "transport": "http",
            "method": method,
            "request_count": count,
            "outcome": "client_disconnect_or_cancel",
            "error": type(exc).__name__,
        })
    except Exception as exc:
        evidence.write({
            "scenario": state.scenario,
            "provider": provider,
            "transport": "http",
            "method": method,
            "request_count": count,
            "outcome": "replay_error",
            "error": type(exc).__name__,
        })
        if not writer.is_closing():
            body = {"jsonrpc": "2.0", "id": None, "error": {"code": -32603, "message": "replay parse error"}}
            writer.write(_http_bytes(500, body))
            try:
                await writer.drain()
            except (BrokenPipeError, ConnectionResetError):
                disconnected = True
    finally:
        state.exit_http()
        if not writer.is_closing():
            writer.close()
        try:
            await writer.wait_closed()
        except (BrokenPipeError, ConnectionResetError):
            disconnected = True
        if disconnected:
            evidence.write({
                "scenario": state.scenario,
                "provider": provider,
                "transport": "http",
                "method": method,
                "request_count": count,
                "outcome": "connection_closed_by_client",
            })


async def _head_pump(ws: Any, provider: str, subscription: str, state: ReplayState, evidence: EvidenceWriter) -> None:
    while True:
        await asyncio.sleep(1.0)
        head = state.advance_head()
        payload = {
            "jsonrpc": "2.0",
            "method": "eth_subscription",
            "params": {
                "subscription": subscription,
                "result": {
                    "number": hex(head),
                    "hash": "0x" + f"{head:064x}"[-64:],
                    "parentHash": "0x" + f"{max(0, head - 1):064x}"[-64:],
                    "timestamp": hex(int(time.time())),
                },
            },
        }
        await ws.send(json.dumps(payload, separators=(",", ":")))
        evidence.write({"provider": provider, "transport": "wss", "method": "eth_subscription:newHeads", "head": head})


async def ws_handler(ws: Any, provider: str, state: ReplayState, evidence: EvidenceWriter) -> None:
    head_task: asyncio.Task[None] | None = None
    try:
        async for raw in ws:
            request = json.loads(raw)
            method = str(request.get("method") or "")
            params = list(request.get("params") or [])
            request_id = request.get("id")
            count = state.next_count(provider, "wss", method)
            if method == "eth_chainId":
                result: Any = CHAIN_ID_HEX
            elif method == "eth_subscribe":
                kind = str(params[0] if params else "unknown")
                result = f"portable-{provider}-{kind}"
            else:
                result = jsonrpc_result(method, params, head=state.head)
            await ws.send(json.dumps({"jsonrpc": "2.0", "id": request_id, "result": result}, separators=(",", ":")))
            evidence.write({
                "scenario": state.scenario,
                "provider": provider,
                "transport": "wss",
                "method": method,
                "request_count": count,
                "subscription_kind": str(params[0]) if method == "eth_subscribe" and params else None,
            })
            if method == "eth_subscribe" and params and params[0] == "newHeads" and head_task is None:
                head_task = asyncio.create_task(_head_pump(ws, provider, str(result), state, evidence))
    finally:
        if head_task is not None:
            head_task.cancel()
            try:
                await head_task
            except asyncio.CancelledError:
                pass


async def run(args: argparse.Namespace) -> None:
    state = ReplayState(scenario=args.scenario, head=args.head)
    evidence = EvidenceWriter(Path(args.evidence))
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ssl_context.load_cert_chain(certfile=args.cert, keyfile=args.key)

    alchemy_http = await asyncio.start_server(
        lambda r, w: handle_http(r, w, provider="alchemy", state=state, evidence=evidence, fault_delay=args.fault_delay),
        "127.0.0.1",
        args.alchemy_http_port,
        ssl=ssl_context,
    )
    drpc_http = await asyncio.start_server(
        lambda r, w: handle_http(r, w, provider="drpc", state=state, evidence=evidence, fault_delay=args.fault_delay),
        "127.0.0.1",
        args.drpc_http_port,
        ssl=ssl_context,
    )
    alchemy_ws = await websockets.serve(
        lambda ws: ws_handler(ws, "alchemy", state, evidence),
        "127.0.0.1",
        args.alchemy_ws_port,
        ssl=ssl_context,
        max_size=MAX_BODY,
    )
    drpc_ws = await websockets.serve(
        lambda ws: ws_handler(ws, "drpc", state, evidence),
        "127.0.0.1",
        args.drpc_ws_port,
        ssl=ssl_context,
        max_size=MAX_BODY,
    )
    Path(args.ready).write_text(
        json.dumps({
            "ready": True,
            "scenario": args.scenario,
            "recommended_primary": RECOMMENDED_PRIMARY.get(args.scenario, "alchemy"),
            "fault_delay_seconds": args.fault_delay,
            "chain_id": CHAIN_ID,
            "http_server_model": "asyncio-bounded-no-per-request-thread",
            "alchemy": {"http": args.alchemy_http_port, "wss": args.alchemy_ws_port},
            "drpc": {"http": args.drpc_http_port, "wss": args.drpc_ws_port},
        }, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    try:
        await asyncio.Future()
    finally:
        alchemy_http.close()
        drpc_http.close()
        await alchemy_http.wait_closed()
        await drpc_http.wait_closed()
        alchemy_ws.close()
        drpc_ws.close()
        await alchemy_ws.wait_closed()
        await drpc_ws.wait_closed()


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    p.add_argument("--cert", required=True)
    p.add_argument("--key", required=True)
    p.add_argument("--evidence", required=True)
    p.add_argument("--ready", required=True)
    p.add_argument("--scenario", choices=SCENARIOS, default="alchemy-429-then-drpc")
    p.add_argument("--fault-delay", type=float, default=15.0)
    p.add_argument("--head", type=int, default=DEFAULT_HEAD)
    p.add_argument("--alchemy-http-port", type=int, default=18443)
    p.add_argument("--alchemy-ws-port", type=int, default=18444)
    p.add_argument("--drpc-http-port", type=int, default=19443)
    p.add_argument("--drpc-ws-port", type=int, default=19444)
    return p


def main() -> int:
    args = parser().parse_args()
    if args.fault_delay <= 0:
        raise SystemExit("fault-delay must be positive")
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
