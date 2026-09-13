#!/usr/bin/env python3
"""Deterministic loopback HTTPS/WSS replay for canonical Robinhood transports.

This server exists only for the portable reproduction harness. It doesn't patch
production code: the harness points the canonical provider-pool environment at these
private HTTPS/WSS endpoints. The default scenario reproduces an Alchemy HTTP 429 on
the first ``eth_chainId`` request so the real provider-failover wrapper must switch
to the backup provider and verify Robinhood chain id 4663 there.
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
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import websockets

CHAIN_ID = 4663
CHAIN_ID_HEX = hex(CHAIN_ID)
DEFAULT_HEAD = 9_250_000


@dataclass
class ReplayState:
    scenario: str = "alchemy-429-then-drpc"
    head: int = DEFAULT_HEAD
    request_counts: dict[tuple[str, str, str], int] = field(default_factory=dict)
    lock: threading.Lock = field(default_factory=threading.Lock)

    def next_count(self, provider: str, transport: str, method: str) -> int:
        key = (provider, transport, method)
        with self.lock:
            value = self.request_counts.get(key, 0) + 1
            self.request_counts[key] = value
            return value

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
    # Structural replay only. Python's SHA3-256 isn't Ethereum Keccak; the replay
    # manifest must not classify this as recorded/high-fidelity selector behavior.
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
        # A single zero word is a safe structural default. Quote/state calls that
        # require richer ABI data remain a declared fidelity gap.
        return _hex32(0)
    if method == "web3_sha3":
        return _selector_result(str(params[0] if params else "0x"))
    return "0x0"


def http_response(
    state: ReplayState,
    *,
    provider: str,
    method: str,
    params: list[Any],
    request_id: Any,
) -> tuple[int, dict[str, Any]]:
    count = state.next_count(provider, "http", method)
    if (
        state.scenario == "alchemy-429-then-drpc"
        and provider == "alchemy"
        and method == "eth_chainId"
        and count == 1
    ):
        return 429, {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {"code": -32005, "message": "synthetic alchemy rate limit"},
        }
    return 200, {
        "jsonrpc": "2.0",
        "id": request_id,
        "result": jsonrpc_result(method, params, head=state.head),
    }


class EvidenceWriter:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.Lock()

    def write(self, payload: dict[str, Any]) -> None:
        line = json.dumps({"ts_unix": time.time(), **payload}, sort_keys=True) + "\n"
        with self.lock, self.path.open("a", encoding="utf-8") as handle:
            handle.write(line)


def _http_handler(provider: str, state: ReplayState, evidence: EvidenceWriter):
    class Handler(BaseHTTPRequestHandler):
        server_version = "PortableRobinhoodReplay/1"

        def log_message(self, fmt: str, *args: Any) -> None:
            return

        def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
            try:
                length = int(self.headers.get("content-length") or "0")
                raw = self.rfile.read(max(0, min(length, 8 * 1024 * 1024)))
                request = json.loads(raw.decode("utf-8"))
                method = str(request.get("method") or "")
                params = list(request.get("params") or [])
                request_id = request.get("id")
                status, body = http_response(
                    state,
                    provider=provider,
                    method=method,
                    params=params,
                    request_id=request_id,
                )
                evidence.write(
                    {
                        "provider": provider,
                        "transport": "http",
                        "method": method,
                        "status": status,
                        "request_bytes": len(raw),
                    }
                )
            except Exception as exc:
                status = 500
                body = {
                    "jsonrpc": "2.0",
                    "id": None,
                    "error": {"code": -32603, "message": f"replay error: {type(exc).__name__}"},
                }
                evidence.write(
                    {
                        "provider": provider,
                        "transport": "http",
                        "method": "<parse-error>",
                        "status": status,
                        "error": type(exc).__name__,
                    }
                )
            encoded = json.dumps(body, separators=(",", ":")).encode("utf-8")
            self.send_response(status)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

    return Handler


def start_https_server(
    provider: str,
    port: int,
    *,
    state: ReplayState,
    evidence: EvidenceWriter,
    cert: Path,
    key: Path,
) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer(("127.0.0.1", port), _http_handler(provider, state, evidence))
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certfile=str(cert), keyfile=str(key))
    server.socket = context.wrap_socket(server.socket, server_side=True)
    thread = threading.Thread(target=server.serve_forever, name=f"replay-{provider}-https", daemon=True)
    thread.start()
    return server


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
        evidence.write(
            {
                "provider": provider,
                "transport": "wss",
                "method": "eth_subscription:newHeads",
                "head": head,
            }
        )


async def ws_handler(ws: Any, provider: str, state: ReplayState, evidence: EvidenceWriter) -> None:
    head_task: asyncio.Task[None] | None = None
    try:
        async for raw in ws:
            request = json.loads(raw)
            method = str(request.get("method") or "")
            params = list(request.get("params") or [])
            request_id = request.get("id")
            state.next_count(provider, "wss", method)
            if method == "eth_chainId":
                result: Any = CHAIN_ID_HEX
            elif method == "eth_subscribe":
                kind = str(params[0] if params else "unknown")
                result = f"portable-{provider}-{kind}"
            else:
                result = jsonrpc_result(method, params, head=state.head)
            await ws.send(json.dumps({"jsonrpc": "2.0", "id": request_id, "result": result}, separators=(",", ":")))
            evidence.write(
                {
                    "provider": provider,
                    "transport": "wss",
                    "method": method,
                    "subscription_kind": str(params[0]) if method == "eth_subscribe" and params else None,
                }
            )
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
    cert = Path(args.cert)
    key = Path(args.key)
    http_servers = [
        start_https_server("alchemy", args.alchemy_http_port, state=state, evidence=evidence, cert=cert, key=key),
        start_https_server("drpc", args.drpc_http_port, state=state, evidence=evidence, cert=cert, key=key),
    ]
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ssl_context.load_cert_chain(certfile=str(cert), keyfile=str(key))
    alchemy_ws = await websockets.serve(
        lambda ws: ws_handler(ws, "alchemy", state, evidence),
        "127.0.0.1",
        args.alchemy_ws_port,
        ssl=ssl_context,
        max_size=8 * 1024 * 1024,
    )
    drpc_ws = await websockets.serve(
        lambda ws: ws_handler(ws, "drpc", state, evidence),
        "127.0.0.1",
        args.drpc_ws_port,
        ssl=ssl_context,
        max_size=8 * 1024 * 1024,
    )
    Path(args.ready).write_text(
        json.dumps(
            {
                "ready": True,
                "scenario": args.scenario,
                "chain_id": CHAIN_ID,
                "alchemy": {"http": args.alchemy_http_port, "wss": args.alchemy_ws_port},
                "drpc": {"http": args.drpc_http_port, "wss": args.drpc_ws_port},
            },
            sort_keys=True,
        ) + "\n",
        encoding="utf-8",
    )
    try:
        await asyncio.Future()
    finally:
        alchemy_ws.close()
        drpc_ws.close()
        await alchemy_ws.wait_closed()
        await drpc_ws.wait_closed()
        for server in http_servers:
            server.shutdown()
            server.server_close()


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    p.add_argument("--cert", required=True)
    p.add_argument("--key", required=True)
    p.add_argument("--evidence", required=True)
    p.add_argument("--ready", required=True)
    p.add_argument("--scenario", choices=("steady", "alchemy-429-then-drpc"), default="alchemy-429-then-drpc")
    p.add_argument("--head", type=int, default=DEFAULT_HEAD)
    p.add_argument("--alchemy-http-port", type=int, default=18443)
    p.add_argument("--alchemy-ws-port", type=int, default=18444)
    p.add_argument("--drpc-http-port", type=int, default=19443)
    p.add_argument("--drpc-ws-port", type=int, default=19444)
    return p


def main() -> int:
    args = parser().parse_args()
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
