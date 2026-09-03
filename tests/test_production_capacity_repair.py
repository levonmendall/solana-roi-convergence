from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from types import SimpleNamespace

import httpx

from solana_roi.direct_solana import DirectSolanaJournal, WatchTarget
from solana_roi.observation_store import ObservationEventStore
from solana_roi.production_capacity_repair import (
    RATE_LIMIT_INITIAL_COOLDOWN_SECONDS,
    _capacity_call_endpoint,
    _capacity_call_with_meta,
    _capacity_status,
    _persist_background_batch,
    _research_pressure_reason,
)
from solana_roi.solana_rpc import RpcEndpoint, SolanaRpcPool


class FakeResponse:
    def __init__(self, result, *, status_code: int = 200, url: str = "https://rpc.example"):
        self._result = result
        self.status_code = status_code
        self.request = httpx.Request("POST", url)
        self._response = httpx.Response(status_code, request=self.request)

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise httpx.HTTPStatusError(
                f"http {self.status_code}",
                request=self.request,
                response=self._response,
            )

    def json(self):
        return {"jsonrpc": "2.0", "id": 1, "result": self._result}


class FakeClient:
    def __init__(self, result, *, delay: float = 0.0, status_code: int = 200):
        self.result = result
        self.delay = delay
        self.status_code = status_code
        self.calls = 0

    async def post(self, url, *, json):
        self.calls += 1
        if self.delay:
            await asyncio.sleep(self.delay)
        return FakeResponse(self.result, status_code=self.status_code, url=url)


def _public_pair() -> tuple[RpcEndpoint, RpcEndpoint]:
    return (
        RpcEndpoint("publicnode", "https://solana-rpc.publicnode.com", "wss://solana-rpc.publicnode.com"),
        RpcEndpoint("solana-mainnet", "https://api.mainnet.solana.com", "wss://api.mainnet.solana.com"),
    )


def test_official_public_secondary_is_not_proactively_hedged_when_primary_succeeds():
    endpoints = _public_pair()
    primary = FakeClient(123, delay=0.03)
    official = FakeClient(456)
    pool = SolanaRpcPool(
        endpoints,
        hedge_delay_seconds=0.001,
        clients={"publicnode": primary, "solana-mainnet": official},
    )

    # Call the capacity implementation directly so the regression remains stable
    # regardless of whether another test imported the production composition first.
    result, provider, _latency = asyncio.run(
        _capacity_call_with_meta(pool, "getSlot", [], hedge=True)
    )
    assert result == 123
    assert provider == "publicnode"
    assert primary.calls == 1
    assert official.calls == 0


def test_http_429_enters_bounded_cooldown_and_skips_immediate_repeat():
    endpoint = _public_pair()[1]
    client = FakeClient(None, status_code=429)
    pool = SolanaRpcPool((endpoint,), clients={endpoint.name: client})

    try:
        asyncio.run(_capacity_call_endpoint(pool, endpoint, "getSlot", []))
    except httpx.HTTPStatusError:
        pass
    else:
        raise AssertionError("expected HTTP 429")

    assert client.calls == 1
    before = time.monotonic()
    try:
        asyncio.run(_capacity_call_endpoint(pool, endpoint, "getSlot", []))
    except Exception as exc:
        assert type(exc).__name__ == "RpcEndpointCoolingDown"
    else:
        raise AssertionError("cooling endpoint should not be called")
    assert client.calls == 1

    status = _capacity_status(pool)["capacity_control"]
    row = status["endpoints"][0]
    assert row["rate_limit_events"] == 1
    assert row["cooling_down"] is True
    assert row["cooldown_remaining_seconds"] <= RATE_LIMIT_INITIAL_COOLDOWN_SECONDS
    assert row["cooldown_remaining_seconds"] > max(0.0, RATE_LIMIT_INITIAL_COOLDOWN_SECONDS - (time.monotonic() - before) - 1.0)


def _dispatch_item(*, signature: str, slot: int, received_at: datetime, sequence: int = 0):
    target = WatchTarget(kind="program", address="program", source_hint="PUMP_FUN")
    message = {
        "method": "logsNotification",
        "params": {
            "subscription": 1,
            "result": {
                "context": {"slot": slot},
                "value": {"signature": signature, "err": None, "logs": []},
            },
        },
    }
    return (10, time.monotonic(), sequence, received_at, "publicnode", {1: target}, message)


def test_background_microbatch_persists_every_unique_receipt_and_minute_count(tmp_path):
    store = ObservationEventStore(tmp_path / "capacity.sqlite3")
    journal = DirectSolanaJournal(store)
    journal.set_provider("publicnode", connected=True)
    plane = SimpleNamespace(store=store, journal=journal)
    at = datetime(2026, 9, 3, 6, 30, tzinfo=timezone.utc)
    items = [
        _dispatch_item(signature="sig-a", slot=100, received_at=at, sequence=0),
        _dispatch_item(signature="sig-b", slot=101, received_at=at, sequence=1),
    ]

    assert _persist_background_batch(plane, items) == 2
    with store._lock:
        receipt_count = store.db.execute(
            "SELECT COUNT(*) FROM direct_solana_recent_receipts WHERE source_key='PUMP_FUN'"
        ).fetchone()[0]
        minute = store.db.execute(
            "SELECT receipt_count, first_received_at, last_received_at, last_slot, rolling_sha256 "
            "FROM direct_solana_minute_receipts WHERE source='PUMP_FUN'"
        ).fetchone()
    assert receipt_count == 2
    assert int(minute["receipt_count"]) == 2
    assert int(minute["last_slot"]) == 101
    assert len(str(minute["rolling_sha256"])) == 64

    # The durable UNIQUE(signature, source_key) contract remains authoritative.
    assert _persist_background_batch(plane, [items[0]]) == 0
    with store._lock:
        assert store.db.execute(
            "SELECT COUNT(*) FROM direct_solana_recent_receipts WHERE source_key='PUMP_FUN'"
        ).fetchone()[0] == 2


def test_research_lane_yields_when_rpc_redundancy_is_rate_limited():
    endpoints = _public_pair()
    pool = SolanaRpcPool(
        endpoints,
        clients={endpoint.name: FakeClient(1) for endpoint in endpoints},
    )
    cooldowns = {endpoints[1].name: time.monotonic() + 30.0}
    setattr(pool, "_roi_capacity_cooldown_until", cooldowns)
    discovery = SimpleNamespace(rpc=pool)

    assert _research_pressure_reason(discovery) == "critical_rpc_redundancy_degraded"
