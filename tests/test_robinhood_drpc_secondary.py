from __future__ import annotations

import json

from solana_roi import robinhood_chain_runtime as runtime
from solana_roi import robinhood_production_provider_finalizer as finalizer
from solana_roi import robinhood_provider_failover as failover


def _clear_secondary_env(monkeypatch) -> None:
    for name in (
        "ROBINHOOD_RPC_ENDPOINTS_JSON",
        "ROBINHOOD_BACKUP_RPC_URL",
        "ROBINHOOD_BACKUP_WS_URL",
        "DRPC_API_KEY",
        "ROBINHOOD_DRPC_API_KEY",
        "DRPC_KEY",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(finalizer, "_DRPC_BACKUP_BOOTSTRAPPED", False)
    failover.reset_for_tests()


def _private_alchemy_primary(monkeypatch) -> None:
    monkeypatch.setenv(
        "ROBINHOOD_RPC_URL",
        "https://robinhood-mainnet.g.alchemy.com/v2/example-primary",
    )
    monkeypatch.setenv(
        "ROBINHOOD_WS_URL",
        "wss://robinhood-mainnet.g.alchemy.com/v2/example-primary",
    )


def test_drpc_key_bootstraps_paired_robinhood_secondary_only(monkeypatch) -> None:
    _clear_secondary_env(monkeypatch)
    _private_alchemy_primary(monkeypatch)
    monkeypatch.setenv("DRPC_API_KEY", "test-drpc-secret")

    assert finalizer._install_drpc_backup_from_key() is True
    assert (
        finalizer.os.environ["ROBINHOOD_BACKUP_RPC_URL"]
        == "https://lb.drpc.live/robinhood/test-drpc-secret"
    )
    assert (
        finalizer.os.environ["ROBINHOOD_BACKUP_WS_URL"]
        == "wss://lb.drpc.live/robinhood/test-drpc-secret"
    )

    failover.reset_for_tests()
    providers = failover.providers()
    assert [item.name for item in providers] == ["primary", "backup"]
    assert providers[0].http.startswith("https://robinhood-mainnet.g.alchemy.com/")
    assert providers[1].http == "https://lb.drpc.live/robinhood/test-drpc-secret"
    assert providers[1].ws == "wss://lb.drpc.live/robinhood/test-drpc-secret"
    assert failover.active_name() == "primary"
    assert failover.endpoint_kind() == "configured_production_provider_pool"


def test_drpc_key_never_promotes_without_valid_private_primary(monkeypatch) -> None:
    _clear_secondary_env(monkeypatch)
    monkeypatch.setenv("DRPC_API_KEY", "test-drpc-secret")
    monkeypatch.setenv("ROBINHOOD_RPC_URL", runtime.ROBINHOOD_PUBLIC_RPC)
    monkeypatch.delenv("ROBINHOOD_WS_URL", raising=False)

    assert finalizer._install_drpc_backup_from_key() is False
    assert "ROBINHOOD_BACKUP_RPC_URL" not in finalizer.os.environ
    assert "ROBINHOOD_BACKUP_WS_URL" not in finalizer.os.environ


def test_explicit_backup_pair_takes_precedence_over_drpc_key(monkeypatch) -> None:
    _clear_secondary_env(monkeypatch)
    _private_alchemy_primary(monkeypatch)
    monkeypatch.setenv("DRPC_API_KEY", "test-drpc-secret")
    monkeypatch.setenv("ROBINHOOD_BACKUP_RPC_URL", "https://backup.example/rpc")
    monkeypatch.setenv("ROBINHOOD_BACKUP_WS_URL", "wss://backup.example/ws")

    assert finalizer._install_drpc_backup_from_key() is False
    assert finalizer.os.environ["ROBINHOOD_BACKUP_RPC_URL"] == "https://backup.example/rpc"
    assert finalizer.os.environ["ROBINHOOD_BACKUP_WS_URL"] == "wss://backup.example/ws"


def test_partial_explicit_backup_remains_fail_closed(monkeypatch) -> None:
    _clear_secondary_env(monkeypatch)
    _private_alchemy_primary(monkeypatch)
    monkeypatch.setenv("DRPC_API_KEY", "test-drpc-secret")
    monkeypatch.setenv("ROBINHOOD_BACKUP_RPC_URL", "https://partial.example/rpc")

    assert finalizer._install_drpc_backup_from_key() is False
    assert finalizer.os.environ["ROBINHOOD_BACKUP_RPC_URL"] == "https://partial.example/rpc"
    assert "ROBINHOOD_BACKUP_WS_URL" not in finalizer.os.environ


def test_explicit_provider_json_remains_authoritative(monkeypatch) -> None:
    _clear_secondary_env(monkeypatch)
    _private_alchemy_primary(monkeypatch)
    monkeypatch.setenv("DRPC_API_KEY", "test-drpc-secret")
    monkeypatch.setenv(
        "ROBINHOOD_RPC_ENDPOINTS_JSON",
        json.dumps(
            [
                {
                    "name": "configured",
                    "http": "https://configured.example/rpc",
                    "ws": "wss://configured.example/ws",
                }
            ]
        ),
    )

    assert finalizer._install_drpc_backup_from_key() is False
    assert "ROBINHOOD_BACKUP_RPC_URL" not in finalizer.os.environ
    assert "ROBINHOOD_BACKUP_WS_URL" not in finalizer.os.environ


def test_drpc_compatibility_alias_and_status_never_expose_secret(monkeypatch) -> None:
    _clear_secondary_env(monkeypatch)
    _private_alchemy_primary(monkeypatch)
    monkeypatch.setenv("ROBINHOOD_DRPC_API_KEY", "alias-secret-value")

    assert finalizer._install_drpc_backup_from_key() is True
    failover.reset_for_tests()
    encoded = json.dumps(finalizer.status(), sort_keys=True)

    assert finalizer.status()["drpc_secret_secondary_supported"] is True
    assert finalizer.status()["drpc_secret_secondary_bootstrapped"] is True
    assert finalizer.status()["drpc_secondary_network"] == "robinhood"
    assert "alias-secret-value" not in encoded
    assert "lb.drpc.live" not in encoded
