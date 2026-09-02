from __future__ import annotations

import asyncio
import json

from solana_roi import alchemy_handshake_pump as pump
from solana_roi import alchemy_multiplexed_stream as multiplex
from solana_roi import direct_solana as direct_solana_module
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


def test_install_replaces_only_the_alchemy_multiplexed_receive_path():
    assert bool(getattr(multiplex._alchemy_multiplexed_stream, "_roi_alchemy_handshake_pumped", False))
    assert bool(getattr(multiplex._provider_specific_fanout, "_roi_alchemy_provider_specific", False))


def test_cooperative_capacity_drains_lightweight_handlers_without_raising_limit():
    async def scenario() -> None:
        handled: list[int] = []

        async def lightweight(index: int) -> None:
            handled.append(index)

        tasks = {
            asyncio.create_task(lightweight(index))
            for index in range(pump.MAX_INFLIGHT_NOTIFICATION_HANDLERS)
        }
        assert len(tasks) == pump.MAX_INFLIGHT_NOTIFICATION_HANDLERS
        assert await pump._cooperative_dispatch_capacity(tasks) is True
        await asyncio.gather(*tasks, return_exceptions=True)
        assert len(handled) == pump.MAX_INFLIGHT_NOTIFICATION_HANDLERS
        assert len(tasks) < pump.MAX_INFLIGHT_NOTIFICATION_HANDLERS

    asyncio.run(scenario())


def test_cooperative_capacity_still_fails_closed_for_genuinely_blocked_handlers():
    async def scenario() -> None:
        gate = asyncio.Event()

        async def blocked() -> None:
            await gate.wait()

        tasks = {
            asyncio.create_task(blocked())
            for _ in range(pump.MAX_INFLIGHT_NOTIFICATION_HANDLERS)
        }
        try:
            assert await pump._cooperative_dispatch_capacity(tasks) is False
            assert len(tasks) == pump.MAX_INFLIGHT_NOTIFICATION_HANDLERS
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    asyncio.run(scenario())


def test_ack_reader_is_not_blocked_by_live_notification_handler(monkeypatch):
    async def scenario() -> None:
        stop = asyncio.Event()
        notification_started = asyncio.Event()
        connected_targets: list[str] = []
        connect_calls: list[dict[str, object]] = []

        class Plane:
            watch_targets = _targets()

            async def _handle_notification(self, _provider, _subscription_targets, _message):
                notification_started.set()
                # Deliberately never complete during setup. The dedicated reader
                # must still consume and resolve the remaining subscription ACKs.
                await asyncio.Event().wait()

        class FakeWs:
            def __init__(self):
                self.queue: asyncio.Queue[str] = asyncio.Queue()
                self.sent: list[dict[str, object]] = []

            async def send(self, raw: str) -> None:
                payload = json.loads(raw)
                self.sent.append(payload)
                request_id = int(payload["id"])
                if request_id == 2:
                    await self.queue.put(
                        json.dumps(
                            {
                                "jsonrpc": "2.0",
                                "method": "logsNotification",
                                "params": {
                                    "subscription": 1001,
                                    "result": {
                                        "context": {"slot": 1},
                                        "value": {"signature": "sig-1", "err": None, "logs": []},
                                    },
                                },
                            }
                        )
                    )
                await self.queue.put(
                    json.dumps({"jsonrpc": "2.0", "id": request_id, "result": 1000 + request_id})
                )

            async def recv(self) -> str:
                return await self.queue.get()

        ws = FakeWs()

        class Context:
            async def __aenter__(self):
                return ws

            async def __aexit__(self, exc_type, exc, tb):
                return False

        def connect(_url: str, **kwargs):
            connect_calls.append(dict(kwargs))
            return Context()

        async def record_target_state(
            _self,
            _endpoint,
            target,
            *,
            connected,
            error_type=None,
            error_code=None,
        ):
            assert error_type is None
            assert error_code is None
            if connected:
                connected_targets.append(target.address)
                if len(connected_targets) == 10:
                    stop.set()

        monkeypatch.setattr(direct_solana_module.websockets, "connect", connect)
        monkeypatch.setattr(multiplex, "_set_target_state", record_target_state)
        monkeypatch.setattr(pump, "SUBSCRIPTION_INTER_TARGET_DELAY_SECONDS", 0.0)

        plane = Plane()
        await asyncio.wait_for(
            pump._pumped_alchemy_multiplexed_stream(plane, _alchemy(), stop),
            timeout=1.0,
        )

        assert len(connect_calls) == 1
        assert connect_calls[0]["max_queue"] == 80
        assert len(ws.sent) == 10
        assert notification_started.is_set()
        assert connected_targets == [target.address for target in _targets()]
        setup = plane._roi_subscription_setup["alchemy"]
        assert setup["acknowledged_count"] == 10
        assert setup["ack_receive_path"] == "dedicated-websocket-reader"
        assert setup["notification_dispatch_path"] == "bounded-concurrent-handlers"

    asyncio.run(scenario())


def test_status_reports_alchemy_handshake_pump_without_changing_lease():
    class Plane:
        endpoints = (_alchemy(),)

    original = lambda _self: {"provider_runtime_policy": {}}
    status = pump._status_with_alchemy_handshake_pump(original)(Plane())
    policy = status["provider_runtime_policy"]
    assert policy["alchemy_ack_receive_path"] == "dedicated-websocket-reader"
    assert policy["alchemy_inline_ack_receive_removed"] is True
    assert policy["alchemy_max_inflight_notification_handlers"] == pump.MAX_INFLIGHT_NOTIFICATION_HANDLERS
    assert policy["alchemy_backpressure_cooperative_drain_before_failure"] is True
    assert policy["alchemy_backpressure_limit_unchanged"] == pump.MAX_INFLIGHT_NOTIFICATION_HANDLERS
    assert policy["alchemy_notification_drop_on_backpressure"] is False
    assert policy["alchemy_backpressure_failure_remains_fail_closed"] is True
    assert policy["alchemy_target_quorum_semantics_unchanged"] is True
    assert policy["live_poll_recoverability_lease_seconds_unchanged"] == 12.0
