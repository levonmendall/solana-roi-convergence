from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

import pytest

from solana_roi import robinhood_provider_failover as failover


def _pool(monkeypatch) -> None:
    monkeypatch.setenv(
        "ROBINHOOD_RPC_ENDPOINTS_JSON",
        json.dumps(
            [
                {
                    "name": "alchemy",
                    "http": "https://robinhood-mainnet.g.alchemy.com/v2/redacted",
                    "ws": "wss://robinhood-mainnet.g.alchemy.com/v2/redacted",
                },
                {
                    "name": "chainstack",
                    "http": "https://nd-123-456.p2pify.com/redacted",
                    "ws": "wss://ws-nd-123-456.p2pify.com/redacted",
                },
            ]
        ),
    )
    monkeypatch.delenv("ROBINHOOD_RPC_URL", raising=False)
    monkeypatch.delenv("ROBINHOOD_WS_URL", raising=False)
    monkeypatch.delenv("ROBINHOOD_BACKUP_RPC_URL", raising=False)
    monkeypatch.delenv("ROBINHOOD_BACKUP_WS_URL", raising=False)
    monkeypatch.delenv("ROBINHOOD_PROVIDER_PRIMARY", raising=False)
    monkeypatch.setenv("ROBINHOOD_PROVIDER_FAILOVER_ERROR_THRESHOLD", "2")
    monkeypatch.setenv("ROBINHOOD_PROVIDER_FAILOVER_COOLDOWN_SECONDS", "30")
    failover.reset_for_tests()


def test_provider_pool_requires_private_https_and_wss(monkeypatch) -> None:
    _pool(monkeypatch)

    assert [item.name for item in failover.providers()] == ["alchemy", "chainstack"]
    assert failover.production_provider_configured() is True
    assert failover.endpoint_kind() == "configured_production_provider_pool"


def test_public_robinhood_transport_cannot_enter_authoritative_pool(monkeypatch) -> None:
    monkeypatch.setenv(
        "ROBINHOOD_RPC_ENDPOINTS_JSON",
        json.dumps(
            [
                {
                    "name": "public",
                    "http": failover.runtime.ROBINHOOD_PUBLIC_RPC,
                    "ws": failover.transport.PUBLIC_SEQUENCER_FEED,
                }
            ]
        ),
    )
    monkeypatch.delenv("ROBINHOOD_RPC_URL", raising=False)
    monkeypatch.delenv("ROBINHOOD_WS_URL", raising=False)
    failover.reset_for_tests()

    assert failover.providers() == ()
    assert failover.production_provider_configured() is False
    assert failover.active_provider() is None


def test_alchemy_budget_exhaustion_immediately_fails_over_and_verifies_chain(monkeypatch) -> None:
    _pool(monkeypatch)
    calls: list[tuple[str, str]] = []

    class RobinhoodAlchemyBudgetExceeded(RuntimeError):
        pass

    async def original(rpc_self, method, params):
        calls.append((rpc_self.rpc_url, method))
        if "alchemy.com" in rpc_self.rpc_url and method == "eth_call":
            raise RobinhoodAlchemyBudgetExceeded("budget exhausted")
        if method == "eth_chainId":
            return hex(failover.runtime.ROBINHOOD_CHAIN_ID)
        if method == "eth_call":
            return "0xbeef"
        raise AssertionError(method)

    wrapped = failover._rpc_wrapper(original)
    rpc = SimpleNamespace(rpc_url=failover.active_provider().http)

    result = asyncio.run(wrapped(rpc, "eth_call", [{"to": "0x" + "1" * 40}, "latest"]))

    assert result == "0xbeef"
    assert failover.active_name() == "chainstack"
    assert failover.generation() == 1
    assert calls == [
        ("https://robinhood-mainnet.g.alchemy.com/v2/redacted", "eth_call"),
        ("https://nd-123-456.p2pify.com/redacted", "eth_chainId"),
        ("https://nd-123-456.p2pify.com/redacted", "eth_call"),
    ]


def test_application_error_does_not_trigger_provider_failover(monkeypatch) -> None:
    _pool(monkeypatch)

    async def original(rpc_self, method, params):
        raise RuntimeError("eth_call: execution reverted")

    wrapped = failover._rpc_wrapper(original)
    rpc = SimpleNamespace(rpc_url=failover.active_provider().http)

    with pytest.raises(RuntimeError, match="execution reverted"):
        asyncio.run(wrapped(rpc, "eth_call", []))

    assert failover.active_name() == "alchemy"
    assert failover.generation() == 0


def test_repeated_websocket_failures_switch_provider_and_stop_old_generation(monkeypatch) -> None:
    _pool(monkeypatch)
    stop = failover.threading.Event()
    old_generation = failover.generation()
    proxy = failover._ProviderGenerationStop(stop, old_generation)

    assert proxy.is_set() is False
    failover._switch_from(
        "alchemy",
        failure_type="ConnectionClosedError",
        immediate=False,
        transport_kind="ws",
    )
    assert failover.active_name() == "alchemy"
    assert proxy.is_set() is False

    failover._switch_from(
        "alchemy",
        failure_type="ConnectionClosedError",
        immediate=False,
        transport_kind="ws",
    )
    assert failover.active_name() == "chainstack"
    assert failover.generation() == old_generation + 1
    assert proxy.is_set() is True


def test_reader_ready_requires_active_provider_generation(monkeypatch) -> None:
    _pool(monkeypatch)
    state = {
        "provider_pool_generation": failover.generation(),
        "provider_name": failover.active_name(),
    }
    monkeypatch.setattr(failover.transport, "_state", lambda self: dict(state))
    wrapped = failover._reader_ready_wrapper(lambda self: True)
    plane = SimpleNamespace()

    assert wrapped(plane) is True

    failover._switch_from(
        "alchemy",
        failure_type="QuotaExceeded",
        immediate=True,
        transport_kind="http",
    )

    # The old reader remains non-authoritative until the replacement WSS generation
    # has re-established its chain id/subscription and stamped the new provider state.
    assert wrapped(plane) is False


def test_default_rpc_uses_private_pool_while_explicit_public_rpc_remains_research(monkeypatch) -> None:
    _pool(monkeypatch)

    class FakeRpc:
        def __init__(self) -> None:
            self.rpc_url = ""

    def original(self, rpc_url_arg=None, *, timeout_seconds=4.0):
        self.rpc_url = rpc_url_arg or failover.runtime.ROBINHOOD_PUBLIC_RPC

    wrapped = failover._init_wrapper(original)
    default = FakeRpc()
    wrapped(default, None)
    public = FakeRpc()
    wrapped(public, failover.runtime.ROBINHOOD_PUBLIC_RPC)

    assert default.rpc_url == "https://robinhood-mainnet.g.alchemy.com/v2/redacted"
    assert public.rpc_url == failover.runtime.ROBINHOOD_PUBLIC_RPC


def test_status_redacts_provider_endpoints_and_preserves_paper_only_authority(monkeypatch) -> None:
    _pool(monkeypatch)

    status = failover.status()
    encoded = json.dumps(status, sort_keys=True)

    assert status["provider_names"] == ["alchemy", "chainstack"]
    assert status["automatic_http_failover"] is True
    assert status["automatic_websocket_failover"] is True
    assert status["paired_http_websocket_switching"] is True
    assert status["public_rpc_can_be_decision_authoritative"] is False
    assert status["public_sequencer_can_be_decision_authoritative"] is False
    assert status["paper_only"] is True
    assert status["live_money_authority"] is False
    assert status["signing_available"] is False
    assert status["transaction_submission_available"] is False
    assert "redacted" not in encoded
    assert "p2pify.com" not in encoded
