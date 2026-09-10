from __future__ import annotations

import json
from pathlib import Path

from solana_roi import robinhood_drpc_environment as drpc
from solana_roi import robinhood_provider_failover as failover


_ENV_NAMES = (
    *drpc.DRPC_KEY_ENV_NAMES,
    "ROBINHOOD_RPC_ENDPOINTS_JSON",
    "ROBINHOOD_RPC_URL",
    "ROBINHOOD_WS_URL",
    "ROBINHOOD_BACKUP_RPC_URL",
    "ROBINHOOD_BACKUP_WS_URL",
    "ROBINHOOD_PROVIDER_PRIMARY",
)


def _clear(monkeypatch) -> None:
    for name in _ENV_NAMES:
        monkeypatch.delenv(name, raising=False)
    failover.reset_for_tests()


def test_missing_key_fails_closed_without_provider_mutation(monkeypatch) -> None:
    _clear(monkeypatch)

    assert drpc.configure_robinhood_drpc_backup() is False
    assert "ROBINHOOD_RPC_ENDPOINTS_JSON" not in drpc.os.environ
    assert "ROBINHOOD_BACKUP_RPC_URL" not in drpc.os.environ
    assert "ROBINHOOD_BACKUP_WS_URL" not in drpc.os.environ


def test_drpc_is_appended_after_existing_json_provider(monkeypatch) -> None:
    _clear(monkeypatch)
    monkeypatch.setenv("ROBINHOOD_DRPC_API_KEY", "key / with spaces")
    monkeypatch.setenv(
        "ROBINHOOD_RPC_ENDPOINTS_JSON",
        json.dumps(
            [
                {
                    "name": "alchemy",
                    "http": "https://alchemy.example/rpc",
                    "ws": "wss://alchemy.example/ws",
                }
            ]
        ),
    )
    monkeypatch.setenv("ROBINHOOD_PROVIDER_PRIMARY", "drpc")

    assert drpc.configure_robinhood_drpc_backup() is True
    providers = json.loads(drpc.os.environ["ROBINHOOD_RPC_ENDPOINTS_JSON"])
    assert providers[0]["name"] == "alchemy"
    assert providers[1]["name"] == "drpc"
    assert providers[1]["http"].startswith("https://lb.drpc.live/robinhood/")
    assert providers[1]["ws"].startswith("wss://lb.drpc.live/robinhood/")
    assert " " not in providers[1]["http"]
    assert drpc.os.environ["ROBINHOOD_PROVIDER_PRIMARY"] == "drpc"
    assert drpc.configure_robinhood_drpc_backup() is True
    assert len(json.loads(drpc.os.environ["ROBINHOOD_RPC_ENDPOINTS_JSON"])) == 2


def test_legacy_drpc_backup_resolves_semantic_preference_and_is_actually_active(monkeypatch) -> None:
    _clear(monkeypatch)
    monkeypatch.setenv("ROBINHOOD_DRPC_API_KEY", "secret")
    monkeypatch.setenv(
        "ROBINHOOD_RPC_URL",
        "https://robinhood-mainnet.g.alchemy.com/v2/redacted",
    )
    monkeypatch.setenv(
        "ROBINHOOD_WS_URL",
        "wss://robinhood-mainnet.g.alchemy.com/v2/redacted",
    )
    monkeypatch.setenv("ROBINHOOD_PROVIDER_PRIMARY", "drpc")

    assert drpc.configure_robinhood_drpc_backup() is True
    assert "ROBINHOOD_RPC_ENDPOINTS_JSON" not in drpc.os.environ
    assert drpc.os.environ["ROBINHOOD_BACKUP_RPC_URL"].startswith(
        "https://lb.drpc.live/robinhood/"
    )
    assert drpc.os.environ["ROBINHOOD_BACKUP_WS_URL"].startswith(
        "wss://lb.drpc.live/robinhood/"
    )
    assert drpc.os.environ["ROBINHOOD_PROVIDER_PRIMARY"] == "backup"

    failover.reset_for_tests()
    assert [item.name for item in failover.providers()] == ["primary", "backup"]
    assert failover.active_name() == "backup"
    active = failover.active_provider()
    assert active is not None
    assert active.http.startswith("https://lb.drpc.live/robinhood/")
    assert active.ws.startswith("wss://lb.drpc.live/robinhood/")


def test_explicit_drpc_backup_pair_resolves_semantic_preference(monkeypatch) -> None:
    _clear(monkeypatch)
    monkeypatch.setenv("DRPC_API_KEY", "secret")
    monkeypatch.setenv(
        "ROBINHOOD_BACKUP_RPC_URL",
        "https://lb.drpc.live/robinhood/existing-secret",
    )
    monkeypatch.setenv(
        "ROBINHOOD_BACKUP_WS_URL",
        "wss://lb.drpc.live/robinhood/existing-secret",
    )
    monkeypatch.setenv("ROBINHOOD_PROVIDER_PRIMARY", "drpc")

    assert drpc.configure_robinhood_drpc_backup() is True
    assert drpc.os.environ["ROBINHOOD_PROVIDER_PRIMARY"] == "backup"
    assert "ROBINHOOD_RPC_ENDPOINTS_JSON" not in drpc.os.environ


def test_explicit_non_drpc_backup_pair_keeps_precedence(monkeypatch) -> None:
    _clear(monkeypatch)
    monkeypatch.setenv("DRPC_API_KEY", "secret")
    monkeypatch.setenv("ROBINHOOD_BACKUP_RPC_URL", "https://backup.example/rpc")
    monkeypatch.setenv("ROBINHOOD_BACKUP_WS_URL", "wss://backup.example/ws")
    monkeypatch.setenv("ROBINHOOD_PROVIDER_PRIMARY", "drpc")

    assert drpc.configure_robinhood_drpc_backup() is True
    assert drpc.os.environ["ROBINHOOD_BACKUP_RPC_URL"] == "https://backup.example/rpc"
    assert drpc.os.environ["ROBINHOOD_BACKUP_WS_URL"] == "wss://backup.example/ws"
    assert drpc.os.environ["ROBINHOOD_PROVIDER_PRIMARY"] == "drpc"
    assert "ROBINHOOD_RPC_ENDPOINTS_JSON" not in drpc.os.environ


def test_partial_explicit_backup_fails_closed_without_overwrite(monkeypatch) -> None:
    _clear(monkeypatch)
    monkeypatch.setenv("DRPC_KEY", "secret")
    monkeypatch.setenv("ROBINHOOD_BACKUP_RPC_URL", "https://partial.example/rpc")

    assert drpc.configure_robinhood_drpc_backup() is False
    assert drpc.os.environ["ROBINHOOD_BACKUP_RPC_URL"] == "https://partial.example/rpc"
    assert "ROBINHOOD_BACKUP_WS_URL" not in drpc.os.environ
    assert "ROBINHOOD_RPC_ENDPOINTS_JSON" not in drpc.os.environ


def test_production_bootstraps_drpc_before_composition_import() -> None:
    production = Path(__file__).parents[1] / "src" / "solana_roi" / "production.py"
    source = production.read_text(encoding="utf-8")
    configure_position = source.index("configure_robinhood_drpc_backup()")
    composition_position = source.index("from .production_system import")

    assert configure_position < composition_position
