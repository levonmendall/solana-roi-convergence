from __future__ import annotations

import asyncio
import json

from solana_roi import direct_solana as direct_solana_module
from solana_roi import target_quorum
from solana_roi import target_stream_fanout as fanout
from solana_roi import alchemy_multiplexed_stream as multiplex
from solana_roi.direct_solana import WatchTarget
from solana_roi.solana_rpc import RpcEndpoint


def _targets() -> tuple[WatchTarget, ...]:
    return tuple(
        WatchTarget("scout" if index < 3 else "program", f"target-{index}", None if index < 3 else "RAYDIUM")
        for index in range(10)
    )


def _alchemy() -> RpcEndpoint:
    return RpcEndpoint(
        "alchemy",
        "https://solana-mainnet.g.alchemy.com/v2/test-key",
        "wss://solana-mainnet.streaming.alchemy.com/v2/test-key",
    )


def test_alchemy_detection_and_installation_are_provider_specific():
    assert multiplex._is_alchemy_endpoint(_alchemy()) is True
    assert multiplex._is_alchemy_endpoint(
        RpcEndpoint("custom", "https://example.invalid", "wss://example.invalid")
    ) is False
    assert multiplex._is_alchemy_endpoint(
        RpcEndpoint("custom", "https://solana-mainnet.g.alchemy.com/v2/key", "wss://other.invalid")
    ) is True
    assert bool(getattr(fanout._provider_fanout, "_roi_alchemy_provider_specific", False))


def test_alchemy_multiplexing_preserves_existing_per_provider_memory_ceiling():
    assert multiplex._alchemy_max_queue(10) == 10 * fanout.TARGET_WS_MAX_QUEUE
    old_ceiling = 10 * fanout.TARGET_WS_MAX_QUEUE * fanout.TARGET_WS_MAX_SIZE_BYTES
    new_ceiling = multiplex._alchemy_max_queue(10) * fanout.TARGET_WS_MAX_SIZE_BYTES
    assert new_ceiling == old_ceiling


def test_alchemy_uses_one_websocket_for_all_ten_sequential_subscriptions(monkeypatch):
    async def scenario() -> None:
        stop = asyncio.Event()
        calls: list[dict[str, object]] = []
        state_calls: list[tuple[str, bool, str | None, int | None]] = []

        class Plane:
            watch_targets = _targets()

            async def _handle_notification(self, _provider, _subscription_targets, _message):
                return None

        class FakeWs:
            def __init__(self):
                self.responses: asyncio.Queue[str] = asyncio.Queue()
                self.sent: list[dict[str, object]] = []

            async def send(self, raw: str) -> None:
                payload = json.loads(raw)
                self.sent.append(payload)
                request_id = int(payload["id"])
                await self.responses.put(
                    json.dumps({"jsonrpc": "2.0", "id": request_id, "result": 1000 + request_id})
                )

            async def recv(self) -> str:
                response = await self.responses.get()
                if len(self.sent) == 10:
                    stop.set()
                return response

            async def ping(self):
                async def done():
                    return None

                return done()

        ws = FakeWs()

        class Context:
            async def __aenter__(self):
                return ws

            async def __aexit__(self, exc_type, exc, tb):
                return False

        def connect(_url: str, **kwargs):
            calls.append(dict(kwargs))
            return Context()

        async def record_state(
            _self,
            endpoint,
            target,
            *,
            connected,
            error_type=None,
            error_code=None,
            error_message=None,
        ):
            assert error_message is None
            state_calls.append((target.address, bool(connected), error_type, error_code))

        monkeypatch.setattr(direct_solana_module.websockets, "connect", connect)
        monkeypatch.setattr(target_quorum, "_quorum_set_target_state", record_state)
        monkeypatch.setattr(multiplex, "SUBSCRIPTION_INTER_TARGET_DELAY_SECONDS", 0.0)

        await multiplex._alchemy_multiplexed_stream(Plane(), _alchemy(), stop)

        assert len(calls) == 1
        assert calls[0]["max_queue"] == 80
        assert calls[0]["max_size"] == fanout.TARGET_WS_MAX_SIZE_BYTES
        assert len(ws.sent) == 10
        assert [row["method"] for row in ws.sent] == ["logsSubscribe"] * 10
        assert [row[1] for row in state_calls] == [True] * 10
        assert [row[0] for row in state_calls] == [target.address for target in _targets()]

    asyncio.run(scenario())


def test_alchemy_connection_429_is_sanitized_and_fails_all_targets_closed(monkeypatch):
    async def scenario() -> None:
        stop = asyncio.Event()
        states: list[tuple[bool, str | None, int | None, object]] = []

        class Plane:
            watch_targets = _targets()

        class Response:
            status_code = 429

        class InvalidStatus(Exception):
            def __init__(self):
                super().__init__("wss://solana-mainnet.streaming.alchemy.com/v2/secret-key")
                self.response = Response()

        class Context:
            async def __aenter__(self):
                stop.set()
                raise InvalidStatus()

            async def __aexit__(self, exc_type, exc, tb):
                return False

        def connect(_url: str, **_kwargs):
            return Context()

        async def record_state(
            _self,
            _endpoint,
            _target,
            *,
            connected,
            error_type=None,
            error_code=None,
            error_message=None,
        ):
            states.append((bool(connected), error_type, error_code, error_message))

        monkeypatch.setattr(direct_solana_module.websockets, "connect", connect)
        monkeypatch.setattr(target_quorum, "_quorum_set_target_state", record_state)

        await multiplex._alchemy_multiplexed_stream(Plane(), _alchemy(), stop)

        assert len(states) == 10
        assert all(connected is False for connected, _kind, _code, _message in states)
        assert all(kind == "InvalidStatus" for _connected, kind, _code, _message in states)
        assert all(code == 429 for _connected, _kind, code, _message in states)
        assert all(message is None for _connected, _kind, _code, message in states)
        assert "secret-key" not in repr(states)

    asyncio.run(scenario())


def test_status_reports_twenty_one_physical_connections_and_thirty_logical_subscriptions():
    configured_endpoints = (
        RpcEndpoint("publicnode", "https://public.invalid", "wss://public.invalid"),
        RpcEndpoint("solana-mainnet", "https://solana.invalid", "wss://solana.invalid"),
        _alchemy(),
    )

    class Plane:
        watch_targets = _targets()

    plane = Plane()
    plane.endpoints = configured_endpoints

    original = lambda _self: {
        "target_stream_fanout": {
            "providers": {
                endpoint.name: {"ready": False, "connected_target_count": 0, "target_count": 10}
                for endpoint in configured_endpoints
            }
        },
        "subscription_setup": {"alchemy": {}},
        "production_memory_boundary": {},
        "provider_runtime_policy": {},
    }
    status = multiplex._status_with_alchemy_multiplexing(original)(plane)

    stream = status["target_stream_fanout"]
    assert stream["total_websocket_connections"] == 21
    assert stream["total_logs_subscriptions"] == 30
    assert stream["providers"]["alchemy"]["websocket_connection_count"] == 1
    assert stream["providers"]["publicnode"]["websocket_connection_count"] == 10
    assert stream["providers"]["solana-mainnet"]["websocket_connection_count"] == 10
    assert stream["providers"]["alchemy"]["logs_subscription_count"] == 10
    assert status["subscription_setup"]["alchemy"]["topology"] == multiplex.ALCHEMY_TOPOLOGY

    boundary = status["production_memory_boundary"]
    assert boundary["physical_websocket_connection_count"] == 21
    assert boundary["logical_logs_subscription_count"] == 30
    assert boundary["memory_ceiling_increased_by_alchemy_multiplexing"] is False
    assert boundary["receive_payload_ceiling_bytes_per_provider"] == 10 * 8 * 1024 * 1024
    assert boundary["receive_payload_ceiling_bytes_all_providers"] == 3 * 10 * 8 * 1024 * 1024

    policy = status["provider_runtime_policy"]
    assert policy["alchemy_physical_websocket_count"] == 1
    assert policy["alchemy_logs_subscriptions_per_websocket"] == 10
    assert policy["alchemy_target_quorum_semantics_unchanged"] is True
    assert policy["live_poll_recoverability_lease_seconds_unchanged"] == 12.0
