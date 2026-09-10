from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

from solana_roi import robinhood_drpc_http_failure_diagnostic as diagnostic
from solana_roi import robinhood_provider_failover as failover


class _SyntheticHttpStatusError(RuntimeError):
    def __init__(self, status_code: int) -> None:
        super().__init__("synthetic response containing https://lb.drpc.live/robinhood/SECRET")
        self.response = SimpleNamespace(status_code=status_code)


@pytest.fixture(autouse=True)
def _reset(monkeypatch):
    monkeypatch.setattr(diagnostic, "_INSTALLED", False)
    monkeypatch.setattr(failover, "_ORIGINAL_RPC", None)
    yield
    diagnostic._INSTALLED = False
    failover._ORIGINAL_RPC = None


def test_drpc_probe_failure_logs_only_safe_method_status_and_type(capsys) -> None:
    async def original(_rpc_self, _method, _params):
        raise _SyntheticHttpStatusError(403)

    wrapped = diagnostic._diagnostic_rpc_wrapper(original)
    rpc = SimpleNamespace(rpc_url="https://lb.drpc.live/robinhood/SECRET")

    with pytest.raises(_SyntheticHttpStatusError):
        asyncio.run(wrapped(rpc, "eth_blockNumber", []))

    output = capsys.readouterr().out
    assert "ROBINHOOD_DRPC_HTTP_FAILURE" in output
    assert "method=eth_blockNumber" in output
    assert "error_type=_SyntheticHttpStatusError" in output
    assert "http_status=403" in output
    assert "SECRET" not in output
    assert "lb.drpc.live" not in output
    assert "synthetic response" not in output


def test_non_drpc_failure_emits_no_diagnostic(capsys) -> None:
    async def original(_rpc_self, _method, _params):
        raise _SyntheticHttpStatusError(429)

    wrapped = diagnostic._diagnostic_rpc_wrapper(original)
    rpc = SimpleNamespace(rpc_url="https://rpc.mainnet.chain.robinhood.com")

    with pytest.raises(_SyntheticHttpStatusError):
        asyncio.run(wrapped(rpc, "eth_chainId", []))

    assert "ROBINHOOD_DRPC_HTTP_FAILURE" not in capsys.readouterr().out


def test_unknown_method_is_redacted_to_other(capsys) -> None:
    async def original(_rpc_self, _method, _params):
        raise _SyntheticHttpStatusError(500)

    wrapped = diagnostic._diagnostic_rpc_wrapper(original)
    rpc = SimpleNamespace(rpc_url="https://lb.drpc.live/robinhood/SECRET")

    with pytest.raises(_SyntheticHttpStatusError):
        asyncio.run(wrapped(rpc, "eth_getLogs_with_sensitive_suffix", []))

    output = capsys.readouterr().out
    assert "method=other" in output
    assert "eth_getLogs_with_sensitive_suffix" not in output


def test_installer_wraps_saved_raw_provider_rpc_once() -> None:
    async def original(_rpc_self, _method, _params):
        return "0x1"

    failover._ORIGINAL_RPC = original
    diagnostic.install_robinhood_drpc_http_failure_diagnostic()
    first = failover._ORIGINAL_RPC
    diagnostic.install_robinhood_drpc_http_failure_diagnostic()

    assert diagnostic._INSTALLED is True
    assert first is failover._ORIGINAL_RPC
    assert first is not original
    assert bool(getattr(first, "_roi_robinhood_drpc_http_failure_diagnostic", False)) is True
    assert diagnostic.status()["live_money_authority"] is False
    assert diagnostic.status()["logs_credentials"] is False


def test_diagnostic_is_installed_only_inside_robinhood_provider_finalizer() -> None:
    root = Path(__file__).parents[1] / "src" / "solana_roi"
    production = (root / "production.py").read_text(encoding="utf-8")
    finalizer = (root / "robinhood_production_provider_finalizer.py").read_text(encoding="utf-8")

    assert "install_robinhood_drpc_http_failure_diagnostic()" not in production
    install_body = finalizer[finalizer.index("def install_robinhood_production_provider_finalizer("):]
    assert install_body.index("install_robinhood_provider_runtime_proof()") < install_body.index(
        "install_robinhood_drpc_http_failure_diagnostic()"
    ) < install_body.index("_preserve_bounded_transport_aliases()")
