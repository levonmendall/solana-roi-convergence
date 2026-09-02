from __future__ import annotations

import asyncio
import json

from solana_roi.direct_solana import WatchTarget
from solana_roi.handshake_pump import (
    MAX_INFLIGHT_NOTIFICATION_HANDLERS,
    RequestIdentifierError,
    _pumped_stream_endpoint,
    _request_id_key,
)
from solana_roi.solana_rpc import RpcEndpoint


def test_request_ids_are_provider_type_agnostic():
    assert _request_id_key(7) == "7"
    assert _request_id_key("7") == "7"
    try:
        _request_id_key(True)
    except RequestIdentifierError:
        pass
    else:
        raise AssertionError("boolean request id must fail closed")


def test_dedicated_reader_handles_interleaved_notifications_and_string_ack_ids(monkeypatch):
    async def scenario() -> None:
        stop = asyncio.Event()
        notifications: list[str] = []
        connection_states: list[bool] = []

        class Journal:
            def set_provider(self, _provider, *, connected, error_type=None):
                connection_states.append(bool(connected))

        class FakeWebSocket:
            def __init__(self):
                self.queue: asyncio.Queue[str] = asyncio.Queue()
                self.sent = 0

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

            async def send(self, raw):
                request = json.loads(raw)
                self.sent += 1
                request_id = request["id"]
                subscription_id = f"sub-{request_id}"
                # Public providers are allowed to echo JSON-RPC ids as strings.
                await self.queue.put(json.dumps({"jsonrpc": "2.0", "id": str(request_id), "result": subscription_id}))
                # Interleave live traffic immediately after the first acknowledgement;
                # this must not block acknowledgement of the second target.
                if self.sent == 1:
                    await self.queue.put(
                        json.dumps(
                            {
                                "jsonrpc": "2.0",
                                "method": "logsNotification",
                                "params": {
                                    "subscription": subscription_id,
                                    "result": {
                                        "context": {"slot": 1},
                                        "value": {"signature": "sig-1", "err": None, "logs": []},
                                    },
                                },
                            }
                        )
                    )

            async def recv(self):
                return await self.queue.get()

        class FakeConnect:
            def __init__(self):
                self.ws = FakeWebSocket()

            def __call__(self, *_args, **_kwargs):
                return self.ws

        class Plane:
            watch_targets = (
                WatchTarget("scout", "scout-a", None),
                WatchTarget("program", "program-a", "RAYDIUM"),
            )
            journal = Journal()

            async def _handle_notification(self, _provider, _targets, message):
                notifications.append(str(message["params"]["result"]["value"]["signature"]))

            async def _connection_state(self, _provider, connected, error_type=None):
                connection_states.append(bool(connected))
                if connected:
                    asyncio.get_running_loop().call_later(0.01, stop.set)

        from solana_roi import direct_solana as direct_solana_module

        fake_connect = FakeConnect()
        monkeypatch.setattr(direct_solana_module.websockets, "connect", fake_connect)
        plane = Plane()
        endpoint = RpcEndpoint(name="fake", http_url="https://fake.invalid", ws_url="wss://fake.invalid")
        await asyncio.wait_for(_pumped_stream_endpoint(plane, endpoint, stop), timeout=1.0)

        assert True in connection_states
        assert notifications == ["sig-1"]
        state = plane._roi_subscription_setup["fake"]
        assert state["ready"] is True
        assert state["acknowledged_count"] == 2

    asyncio.run(scenario())


def test_notification_dispatch_capacity_is_bounded():
    assert MAX_INFLIGHT_NOTIFICATION_HANDLERS == 32
