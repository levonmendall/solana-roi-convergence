from __future__ import annotations

import json
from pathlib import Path

from solana_roi import robinhood_drpc_environment as drpc


_ENV_NAMES = (
    *drpc.DRPC_KEY_ENV_NAMES,
    "ROBINHOOD_RPC_ENDPOINTS_JSON",
    "ROBINHOOD_BACKUP_RPC_URL",
    "ROBINHOOD_BACKUP_WS_URL",
)


def _clear(monkeypatch) -> None:
    for name in _ENV_NAMES:
        monkeypatch.delenv(name, raising=False)


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

    assert drpc.configure_robinhood_drpc_backup() is True
    providers = json.loads(drpc.os.environ["ROBINHOOD_RPC_ENDPOINTS_JSON"])
    assert providers[0]["name"] == "alchemy"
    assert providers[1]["name"] == "drpc"
    assert providers[1]["http"].startswith("https://lb.drpc.live/robinhood/")
    assert providers[1]["ws"].startswith("wss://lb.drpc.live/robinhood/")
    assert " " not in providers[1]["http"]
    assert drpc.configure_robinhood_drpc_backup() is True
    assert len(json.loads(drpc.os.environ["ROBINHOOD_RPC_ENDPOINTS_JSON"])) == 2


def test_explicit_backup_pair_keeps_precedence(monkeypatch) -> None:
    _clear(monkeypatch)
    monkeypatch.setenv("DRPC_API_KEY", "secret")
    monkeypatch.setenv("ROBINHOOD_BACKUP_RPC_URL", "https://backup.example/rpc")
    monkeypatch.setenv("ROBINHOOD_BACKUP_WS_URL", "wss://backup.example/ws")

    assert drpc.configure_robinhood_drpc_backup() is True
    assert drpc.os.environ["ROBINHOOD_BACKUP_RPC_URL"] == "https://backup.example/rpc"
    assert drpc.os.environ["ROBINHOOD_BACKUP_WS_URL"] == "wss://backup.example/ws"


def test_partial_explicit_backup_fails_closed_without_overwrite(monkeypatch) -> None:
    _clear(monkeypatch)
    monkeypatch.setenv("DRPC_KEY", "secret")
    monkeypatch.setenv("ROBINHOOD_BACKUP_RPC_URL", "https://partial.example/rpc")

    assert drpc.configure_robinhood_drpc_backup() is False
    assert drpc.os.environ["ROBINHOOD_BACKUP_RPC_URL"] == "https://partial.example/rpc"
    assert "ROBINHOOD_BACKUP_WS_URL" not in drpc.os.environ


def test_production_bootstraps_drpc_before_composition_import() -> None:
    production = Path(__file__).parents[1] / "src" / "solana_roi" / "production.py"
    source = production.read_text(encoding="utf-8")
    configure_position = source.index("configure_robinhood_drpc_backup()")
    composition_position = source.index("from .production_system import")

    assert configure_position < composition_position
