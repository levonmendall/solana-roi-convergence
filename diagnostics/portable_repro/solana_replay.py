#!/usr/bin/env python3
"""Deterministic loopback Solana JSON-RPC/WSS replay for harness validation.

This is intentionally *structural* replay. It exercises the canonical configured
endpoint pool, HTTP fallback/hedging, logsSubscribe acknowledgements and optional
notifications without claiming production transaction or event fidelity.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import ssl
import threading
import time
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import websockets

DEFAULT_SLOT = 350_000_000


@dataclass
class ReplayState:
    slot: int = DEFAULT_SLOT
    request_counts: dict[tuple[str, str, str], int] = field(default_factory=dict)
    lock: threading.Lock = field(default_factory=threading.Lock)

    def count(self, provider: str, transport: str, method: str) -> int:
        key = (provider, transport, method)
        with self.lock:
            value = self.request_counts.get(key, 0) + 1
            self.request_counts[key] = value
            return value

    def next_slot(self) -> int:
        with self.lock:
            self.slot += 1
            return self.slot


class EvidenceWriter:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.Lock()

    def write(self, payload: dict[str, Any]) -> None:
        line = json.dumps({"ts_unix": time.time(), **payload}, sort_keys=True) + "\n"
        with self.lock, self.path.open("a", encoding="utf-8") as handle:
            handle.write(line)


def result_for(method: str, params: list[Any], *, slot: int) -> Any:
    if method in {"getSlot", "getBlockHeight", "getFirstAvailableBlock"}:
        return slot
    if method == "getTransaction":
        # Recorded/production-shaped parsed transactions are still required for
        # hydration fidelity. None preserves the canonical retry/not-ready path.
        return None
    if method == "getSignaturesForAddress":
        return []
    if method == "getLatestBlockhash":
        return {
            "context": {"slot": slot},
            "value": {
                "blockhash": "PortableReplay111111111111111111111111111111",
                "lastValidBlockHeight": slot + 150,
            },
        }
    if method == "getHealth":
        return "ok"
    if method == "getVersion":
        return {"solana-core": "portable-replay", "feature-set": 0}
    return None


def _handler(provider: str, state: ReplayState, evidence: EvidenceWriter):
    class Handler(BaseHTTPRequestHandler):
        server_version = "PortableSolanaReplay/1"

        def log_message(self, fmt: str, *args: Any) -> None:
            return

        def do_POST(self) -> None:  # noqa: N802
            length = int(self.headers.get("content-length") or "0")
            raw = self.rfile.read(max(0, min(length, 16 * 1024 * 1024)))
            try:
                request = json.loads(raw.decode("utf-8"))
                method = str(request.get("method") or "")
                params = list(request.get("params") or [])
                request_id = request.get("id")
                state.count(provider, "http", method)
                body = {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": result_for(method, params, slot=state.slot),
                }
                status = 200
            except Exception as exc:
                method = "<parse-error>"
                body = {
                    "jsonrpc": "2.0",
                    "id": None,
                    "error": {"code": -32603, "message": f"replay error: {type(exc).__name__}"},
                }
                status = 500
            evidence.write(
                {
                    "provider": provider,
                    "transport": "http",
                    "method": method,
                    "status": status,
                    "request_bytes": len(raw),
                }
            )
            encoded = json.dumps(body, separators=(",", ":")).encode("utf-8")
            self.send_response(status)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

    return Handler


def start_https(provider: str, port: int, *, state: ReplayState, evidence: EvidenceWriter, cert: Path, key: Path) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer(("127.0.0.1", port), _handler(provider, state, evidence))
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certfile=str(cert), keyfile=str(key))
    server.socket = context.wrap_socket(server.socket, server_side=True)
    threading.Thread(target=server.serve_forever, name=f"solana-replay-{provider}-https", daemon=True).start()
    return server


async def _notification_pump(
    ws: Any,
    provider: str,
    subscriptions: dict[int, int],
    state: ReplayState,
    evidence: EvidenceWriter,
    interval: float,
) -> None:
    sequence = 0
    while True:
        await asyncio.sleep(interval)
        slot = state.next_slot()
        for subscription in list(subscriptions.values()):
            sequence += 1
            signature = f"portable{provider}{sequence:056d}"[-64:]
            payload = {
                "jsonrpc": "2.0",
                "method": "logsNotification",
                "params": {
                    "subscription": subscription,
                    "result": {
                        "context": {"slot": slot},
                        "value": {"signature": signature, "err": None, "logs": []},
                    },
                },
            }
            await ws.send(json.dumps(payload, separators=(",", ":")))
            evidence.write(
                {
                    "provider": provider,
                    "transport": "wss",
                    "method": "logsNotification",
                    "slot": slot,
                    "subscription": subscription,
                }
            )


async def ws_handler(
    ws: Any,
    provider: str,
    state: ReplayState,
    evidence: EvidenceWriter,
    notification_interval: float,
) -> None:
    subscriptions: dict[int, int] = {}
    next_subscription = 1000
    pump: asyncio.Task[None] | None = None
    try:
        async for raw in ws:
            request = json.loads(raw)
            method = str(request.get("method") or "")
            request_id = int(request.get("id") or 0)
            state.count(provider, "wss", method)
            if method == "logsSubscribe":
                next_subscription += 1
                subscriptions[request_id] = next_subscription
                result: Any = next_subscription
            elif method == "logsUnsubscribe":
                result = True
            else:
                result = result_for(method, list(request.get("params") or []), slot=state.slot)
            await ws.send(json.dumps({"jsonrpc": "2.0", "id": request_id, "result": result}, separators=(",", ":")))
            evidence.write(
                {
                    "provider": provider,
                    "transport": "wss",
                    "method": method,
                    "subscription": result if method == "logsSubscribe" else None,
                }
            )
            if method == "logsSubscribe" and notification_interval > 0 and pump is None:
                pump = asyncio.create_task(
                    _notification_pump(ws, provider, subscriptions, state, evidence, notification_interval)
                )
    finally:
        if pump is not None:
            pump.cancel()
            try:
                await pump
            except asyncio.CancelledError:
                pass


async def run(args: argparse.Namespace) -> None:
    state = ReplayState(slot=args.slot)
    evidence = EvidenceWriter(Path(args.evidence))
    cert, key = Path(args.cert), Path(args.key)
    http_servers = [
        start_https("rpc-a", args.rpc_a_http_port, state=state, evidence=evidence, cert=cert, key=key),
        start_https("rpc-b", args.rpc_b_http_port, state=state, evidence=evidence, cert=cert, key=key),
    ]
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certfile=str(cert), keyfile=str(key))
    a_ws = await websockets.serve(
        lambda ws: ws_handler(ws, "rpc-a", state, evidence, args.notification_interval),
        "127.0.0.1", args.rpc_a_ws_port, ssl=context, max_size=16 * 1024 * 1024,
    )
    b_ws = await websockets.serve(
        lambda ws: ws_handler(ws, "rpc-b", state, evidence, args.notification_interval),
        "127.0.0.1", args.rpc_b_ws_port, ssl=context, max_size=16 * 1024 * 1024,
    )
    Path(args.ready).write_text(
        json.dumps(
            {
                "ready": True,
                "rpc_a": {"http": args.rpc_a_http_port, "wss": args.rpc_a_ws_port},
                "rpc_b": {"http": args.rpc_b_http_port, "wss": args.rpc_b_ws_port},
                "notification_interval": args.notification_interval,
                "structural_only": True,
            },
            sort_keys=True,
        ) + "\n",
        encoding="utf-8",
    )
    try:
        await asyncio.Future()
    finally:
        a_ws.close(); b_ws.close()
        await a_ws.wait_closed(); await b_ws.wait_closed()
        for server in http_servers:
            server.shutdown(); server.server_close()


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    p.add_argument("--cert", required=True)
    p.add_argument("--key", required=True)
    p.add_argument("--evidence", required=True)
    p.add_argument("--ready", required=True)
    p.add_argument("--slot", type=int, default=DEFAULT_SLOT)
    p.add_argument("--notification-interval", type=float, default=0.0)
    p.add_argument("--rpc-a-http-port", type=int, default=20443)
    p.add_argument("--rpc-a-ws-port", type=int, default=20444)
    p.add_argument("--rpc-b-http-port", type=int, default=21443)
    p.add_argument("--rpc-b-ws-port", type=int, default=21444)
    return p


def main() -> int:
    args = parser().parse_args()
    if args.notification_interval < 0:
        raise SystemExit("notification interval cannot be negative")
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
