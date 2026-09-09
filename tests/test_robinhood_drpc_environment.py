from __future__ import annotations

import asyncio
import json
import os
from types import SimpleNamespace

import pytest

from solana_roi import robinhood_drpc_environment as drpc
from solana_roi import robinhood_provider_failover as failover


_TOUCHED_ENV = (
    "ROBINHOOD_DRPC_API_KEY",
    "DRPC_API_KEY",
    "DRPC_KEY",
    "SOLANA_ROI_DRPC_API_KEY",
    "ROBINHOOD_RPC_ENDPOINTS_JSON",
    "ROBINHOOD_RPC_URL",
    "ROBINHOOD_WS_URL",
    "ROBINHOOD_BACKUP_RPC_URL",
    "ROBINHOOD_BACKUP_WS_URL",
    "ROBINHOOD_PROVIDER_PRIMARY",
    "ROBINHOOD_PROVIDER_FAILOVER_ERROR_THRESHOLD",
    "ROBINHOOD_PROVIDER_FAILOVER_COOLDOWN_SECONDS",
)


@pytest.fixture(autouse=True)
def _restore_robinhood_environment():
    original = {name: os.environ.get(name) for name in _TOUCHED_ENV}
    try:
        yield
    finally:
        for name, value in original.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
        failover.reset_for_tests()


def _clear(monkeypatch) -> None:
    for name in _TOUCHED_ENV:
        monkeypatch.delenv(name, raising=False)
    failover.reset_for_tests()


def test_drpc_key_materializes_private_https_and_wss_backup(monkeypatch) -> None:
    _clear(monkeypatch)
    monkeypatch.setenv("ROBINHOOD_DRPC_API_KEY", "secret-test-key")

    assert drpc.configure_robinhood_drpc_backup() is True
    assert os.getenv("ROBINHOOD_BACKUP_RPC_URL") == "https://lb.drpc.live/robinhood/secret-test-key"
    assert os.getenv("ROBINHOOD_BACKUP_WS_URL") == "wss://lb.drpc.live/robinhood/secret-test-key"


def test_drpc_key_alias_is_supported(monkeypatch) -> None:
    _clear(monkeypatch)
    monkeypatch.setenv("DRPC_KEY", "alias-key")

    assert drpc.configure_robinhood_drpc_backup() is True
    assert os.getenv("ROBINHOOD_BACKUP_RPC_URL") == "https://lb.drpc.live/robinhood/alias-key"
    assert os.getenv("ROBINHOOD_BACKUP_WS_URL") == "wss://lb.drpc.live/robinhood/alias-key"


def test_explicit_backup_pair_retains_precedence(monkeypatch) -> None:
    _clear(monkeypatch)
    monkeypatch.setenv("ROBINHOOD_DRPC_API_KEY", "secret-test-key")
    monkeypatch.setenv("ROBINHOOD_BACKUP_RPC_URL", "https://backup.example/rpc")
    monkeypatch.setenv("ROBINHOOD_BACKUP_WS_URL", "wss://backup.example/ws")

    assert drpc.configure_robinhood_drpc_backup() is True
    assert os.getenv("ROBINHOOD_BACKUP_RPC_URL") == "https://backup.example/rpc"
    assert os.getenv("ROBINHOOD_BACKUP_WS_URL") == "wss://backup.example/ws"


def test_drpc_appends_to_existing_json_pool_without_reordering_primary(monkeypatch) -> None:
    _clear(monkeypatch)
    monkeypatch.setenv("ROBINHOOD_DRPC_API_KEY", "secret-test-key")
    monkeypatch.setenv(
        "ROBINHOOD_RPC_ENDPOINTS_JSON",
        json.dumps(
            [
                {
                    "name": "alchemy",
                    "http": "https://robinhood-mainnet.g.alchemy.com/v2/redacted",
                    "ws": "wss://robinhood-mainnet.g.alchemy.com/v2/redacted",
                }
            ]
        ),
    )

    assert drpc.configure_robinhood_drpc_backup() is True
    payload = json.loads(os.getenv("ROBINHOOD_RPC_ENDPOINTS_JSON") or "[]")
    assert [item["name"] for item in payload] == ["alchemy", "drpc"]


def test_alchemy_budget_failure_fails_over_to_drpc_after_chain_4663_verification(monkeypatch) -> None:
    _clear(monkeypatch)
    monkeypatch.setenv(
        "ROBINHOOD_RPC_URL",
        "https://robinhood-mainnet.g.alchemy.com/v2/redacted",
    )
    monkeypatch.setenv(
        "ROBINHOOD_WS_URL",
        "wss://robinhood-mainnet.g.alchemy.com/v2/redacted",
    )
    monkeypatch.setenv("ROBINHOOD_DRPC_API_KEY", "secret-test-key")
    monkeypatch.setenv("ROBINHOOD_PROVIDER_FAILOVER_ERROR_THRESHOLD", "2")
    monkeypatch.setenv("ROBINHOOD_PROVIDER_FAILOVER_COOLDOWN_SECONDS", "30")
    assert drpc.configure_robinhood_drpc_backup() is True
    failover.reset_for_tests()

    calls: list[tuple[str, str]] = []

    class RobinhoodAlchemyBudgetExceeded(RuntimeError):
        pass

    async def original(rpc_self, method, params):
        calls.append((rpc_self.rpc_url, method))
        if "alchemy.com" in rpc_self.rpc_url and method == "eth_call":
            raise RobinhoodAlchemyBudgetExceeded("budget exhausted")
        if "lb.drpc.live" in rpc_self.rpc_url and method == "eth_chainId":
            return hex(4663)
        if "lb.drpc.live" in rpc_self.rpc_url and method == "eth_call":
            return "0xbeef"
        raise AssertionError((rpc_self.rpc_url, method, params))

    wrapped = failover._rpc_wrapper(original)
    rpc = SimpleNamespace(rpc_url=failover.active_provider().http)

    assert asyncio.run(wrapped(rpc, "eth_call", [])) == "0xbeef"
    assert failover.active_provider() is not None
    assert failover.active_provider().http.startswith("https://lb.drpc.live/robinhood/")
    assert failover.generation() == 1
    assert calls[1][1] == "eth_chainId"
    assert failover.status()["chain_id_required"] == 4663
    assert failover.status()["paper_only"] is True
    assert failover.status()["live_money_authority"] is False
    assert failover.status()["signing_available"] is False
    assert failover.status()["transaction_submission_available"] is False
    assert "secret-test-key" not in json.dumps(failover.status(), sort_keys=True)


def test_wrong_drpc_chain_id_fails_closed(monkeypatch) -> None:
    _clear(monkeypatch)
    monkeypatch.setenv(
        "ROBINHOOD_RPC_URL",
        "https://robinhood-mainnet.g.alchemy.com/v2/redacted",
    )
    monkeypatch.setenv(
        "ROBINHOOD_WS_URL",
        "wss://robinhood-mainnet.g.alchemy.com/v2/redacted",
    )
    monkeypatch.setenv("ROBINHOOD_DRPC_API_KEY", "secret-test-key")
    assert drpc.configure_robinhood_drpc_backup() is True
    failover.reset_for_tests()

    class RobinhoodAlchemyBudgetExceeded(RuntimeError):
        pass

    async def original(rpc_self, method, params):
        if "alchemy.com" in rpc_self.rpc_url:
            raise RobinhoodAlchemyBudgetExceeded("budget exhausted")
        if method == "eth_chainId":
            return hex(1)
        raise AssertionError(method)

    wrapped = failover._rpc_wrapper(original)
    rpc = SimpleNamespace(rpc_url=failover.active_provider().http)

    with pytest.raises(RobinhoodAlchemyBudgetExceeded):
        asyncio.run(wrapped(rpc, "eth_call", []))
    assert failover.active_provider() is None
