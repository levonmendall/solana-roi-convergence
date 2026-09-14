from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from types import SimpleNamespace

import httpx
import pytest

from solana_roi import robinhood_catchup_capacity_repair as catchup
from solana_roi import robinhood_chain_runtime as runtime
from solana_roi import robinhood_provider_efficiency_repair as efficiency
from solana_roi import robinhood_provider_failover as failover
from solana_roi import robinhood_provider_runtime_proof as proof


def _addr(value: int) -> str:
    return f"0x{value:040x}"


class _FakeRpc:
    def __init__(self, *, v3_address: str, v2_address: str) -> None:
        self.calls: list[dict[str, object]] = []
        self.v3_address = v3_address
        self.v2_address = v2_address

    async def chain_id(self) -> int:
        return runtime.ROBINHOOD_CHAIN_ID

    async def block_number(self) -> int:
        return 1_000

    async def get_logs(self, *, from_block: int, to_block: int, addresses: list[str], topics=None):
        self.calls.append(
            {
                "from_block": from_block,
                "to_block": to_block,
                "addresses": list(addresses),
                "topics": topics,
            }
        )
        if topics is None:
            return []
        return [
            {
                "address": self.v3_address,
                "topics": [runtime.V3_SWAP_TOPIC],
                "blockNumber": "0x3e8",
                "transactionIndex": "0x0",
                "logIndex": "0x0",
            },
            {
                "address": self.v2_address,
                "topics": [runtime.PONS_V2_CURVE_BUY_TOPIC],
                "blockNumber": "0x3e8",
                "transactionIndex": "0x0",
                "logIndex": "0x1",
            },
        ]


class _FakePlane:
    def __init__(self, *, duplicate_address: bool = False) -> None:
        v3_addresses = [_addr(index + 1) for index in range(32)]
        v2_addresses = [_addr(index + 1_000) for index in range(32)]
        if duplicate_address:
            v2_addresses[0] = v3_addresses[0]
        self.rpc = _FakeRpc(v3_address=v3_addresses[0], v2_address=v2_addresses[0])
        self.v3_pools = {
            address: SimpleNamespace(pool=address)
            for address in v3_addresses
        }
        self.v2_curves = {
            address: SimpleNamespace(curve=address)
            for address in v2_addresses
        }
        self._cursor = 900
        self._latest_block = None
        self._caught_up = False
        self._last_poll_at = None
        self._last_success_at = None
        self._last_error = None
        self.processed: list[str] = []

    async def _process_factory_log(self, log) -> None:
        raise AssertionError("factory response is empty in this regression")

    async def _process_v3_swap(self, pool, log, *, live: bool, observed_at: str) -> None:
        self.processed.append("v3")

    async def _process_v2_curve_log(self, curve, log, *, live: bool, observed_at: str) -> None:
        self.processed.append("v2")

    async def _settle_open_positions(self) -> None:
        return None

    def _set_cursor(self, value: int) -> None:
        self._cursor = int(value)


def test_cross_venue_market_logs_share_one_64_address_request_without_scope_loss(monkeypatch) -> None:
    monkeypatch.delenv("ROBINHOOD_COMBINED_LOG_ADDRESS_BATCH_SIZE", raising=False)
    efficiency.install_robinhood_provider_efficiency_repair()
    assert catchup._fetch_market_logs is efficiency._combined_fetch_market_logs
    plane = _FakePlane()

    asyncio.run(catchup._capacity_poll_once(plane))

    assert len(plane.rpc.calls) == 2
    factory_call, market_call = plane.rpc.calls
    assert factory_call["topics"] is None
    assert len(factory_call["addresses"]) == 4
    assert len(market_call["addresses"]) == 64
    assert market_call["from_block"] == 901
    assert market_call["to_block"] == 1_000
    assert market_call["topics"] == [[
        runtime.V3_SWAP_TOPIC,
        runtime.PONS_V2_CURVE_BUY_TOPIC,
        runtime.PONS_V2_CURVE_SELL_TOPIC,
    ]]
    assert set(market_call["addresses"]) == set(plane.v3_pools) | set(plane.v2_curves)
    assert plane.processed == ["v3", "v2"]
    assert plane._roi_market_log_legacy_equivalent_requests == 2
    assert plane._roi_market_log_actual_requests == 1
    assert plane._cursor == 1_000


def test_cross_venue_batching_fails_closed_on_address_classification_collision() -> None:
    efficiency.install_robinhood_provider_efficiency_repair()
    plane = _FakePlane(duplicate_address=True)
    with pytest.raises(RuntimeError, match="robinhood_market_address_classification_collision"):
        asyncio.run(catchup._capacity_poll_once(plane))
    assert plane._cursor == 900
    assert len(plane.rpc.calls) == 1


def _alchemy_provider() -> failover.ProviderEndpoint:
    return failover.ProviderEndpoint(
        name="alchemy",
        http="https://robinhood-mainnet.g.alchemy.com/v2/test-key",
        ws="wss://robinhood-mainnet.g.alchemy.com/v2/test-key",
    )


def _http_429(message: str) -> httpx.HTTPStatusError:
    request = httpx.Request("POST", "https://robinhood-mainnet.g.alchemy.com/v2/test-key")
    response = httpx.Response(
        429,
        request=request,
        content=json.dumps({"error": {"message": message}}).encode(),
    )
    return httpx.HTTPStatusError("429", request=request, response=response)


def test_alchemy_monthly_quota_is_distinguished_from_transient_429() -> None:
    provider = _alchemy_provider()
    assert proof._alchemy_monthly_quota_exhausted(
        provider,
        _http_429("Monthly capacity limit exceeded for this account"),
    )
    assert not proof._alchemy_monthly_quota_exhausted(
        provider,
        _http_429("Too many requests; retry shortly"),
    )


def test_monthly_quota_quarantine_targets_next_utc_month(monkeypatch) -> None:
    fixed = datetime(2026, 9, 14, 19, 30, tzinfo=timezone.utc).timestamp()
    expected = datetime(2026, 10, 1, 0, 0, tzinfo=timezone.utc).timestamp()
    assert proof._next_utc_month_epoch(fixed) == expected

    failover.reset_for_tests()
    monkeypatch.setattr(proof.time, "time", lambda: fixed)
    monkeypatch.setattr(proof.time, "monotonic", lambda: 10_000.0)
    proof._record_probe_failure(
        _alchemy_provider(),
        _http_429("Monthly capacity limit exceeded for this account"),
    )
    state = failover._state_for_locked("alchemy")
    assert state["quota_exhausted_until"] == expected
    assert state["quota_recovery_probe_required"] is True
    assert state["chain_verified"] is False
    assert state["read_capability_verified"] is False
    assert state["cooldown_until"] > 10_000.0 + 60.0
    assert state["capability_quarantine_reason"] == "alchemy_monthly_quota_exhausted"


def test_alchemy_backup_rejoins_only_after_chain_and_head_probe_without_forcing_primary(monkeypatch) -> None:
    endpoints = [
        {
            "name": "chainstack",
            "http": "https://robinhood.example.chainstack.com/key",
            "ws": "wss://robinhood.example.chainstack.com/key",
        },
        {
            "name": "alchemy",
            "http": "https://robinhood-mainnet.g.alchemy.com/v2/test-key",
            "ws": "wss://robinhood-mainnet.g.alchemy.com/v2/test-key",
        },
    ]
    monkeypatch.setenv("ROBINHOOD_RPC_ENDPOINTS_JSON", json.dumps(endpoints))
    monkeypatch.setenv("ROBINHOOD_PROVIDER_PRIMARY", "chainstack")
    failover.reset_for_tests()
    assert failover.active_name() == "chainstack"

    with failover._LOCK:
        state = failover._state_for_locked("alchemy")
        state["quota_recovery_probe_required"] = True
        state["quota_exhausted_until"] = 1.0
        state["cooldown_until"] = 0.0
        state["chain_verified"] = False
        state["read_capability_verified"] = False

    calls: list[tuple[str, str]] = []

    async def inner(rpc_self, method: str, params: list[object]):
        calls.append((str(rpc_self.rpc_url), method))
        assert "alchemy.com" in str(rpc_self.rpc_url)
        if method == "eth_chainId":
            return hex(runtime.ROBINHOOD_CHAIN_ID)
        if method == "eth_blockNumber":
            return hex(12_345)
        raise AssertionError(method)

    failover._ORIGINAL_RPC = inner
    rpc = SimpleNamespace(rpc_url=endpoints[0]["http"])

    assert asyncio.run(proof._verify_quota_recovery_if_needed(rpc)) is True

    with failover._LOCK:
        state = failover._state_for_locked("alchemy")
        assert state["quota_recovery_probe_required"] is False
        assert state["quota_exhausted_until"] is None
        assert state["chain_verified"] is True
        assert state["read_capability_verified"] is True
        assert state["last_verified_block"] == 12_345
    assert [method for _, method in calls] == ["eth_chainId", "eth_blockNumber"]
    assert failover.active_name() == "chainstack"
    assert rpc.rpc_url == endpoints[0]["http"]
