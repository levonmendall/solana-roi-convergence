from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from solana_roi import robinhood_drpc_block_number_compat as compat


@pytest.fixture(autouse=True)
def _reset(monkeypatch):
    monkeypatch.setattr(compat, "_INSTALLED", False)
    monkeypatch.setattr(compat, "_FALLBACK_ACTIVE", False)
    monkeypatch.setattr(compat, "_FALLBACK_SUCCESSES", 0)
    monkeypatch.setattr(compat, "_FALLBACK_FAILURES", 0)
    yield


def _http_error(status: int) -> httpx.HTTPStatusError:
    request = httpx.Request("POST", "https://redacted.invalid")
    response = httpx.Response(status, request=request)
    return httpx.HTTPStatusError("redacted", request=request, response=response)


def test_drpc_eth_blocknumber_400_uses_equivalent_latest_block_read(capsys) -> None:
    calls: list[tuple[str, list[object]]] = []

    async def original(_rpc_self, method, params):
        calls.append((method, list(params)))
        if method == "eth_blockNumber":
            raise _http_error(400)
        if method == "eth_getBlockByNumber":
            return {"number": "0x12345", "hash": "0xabc"}
        raise AssertionError(method)

    wrapped = compat._compat_rpc_wrapper(original)
    rpc = SimpleNamespace(rpc_url="https://lb.drpc.live/robinhood/REDACTED")

    assert asyncio.run(wrapped(rpc, "eth_blockNumber", [])) == "0x12345"
    assert calls == [
        ("eth_blockNumber", []),
        ("eth_getBlockByNumber", ["latest", False]),
    ]
    output = capsys.readouterr().out
    assert "ROBINHOOD_DRPC_BLOCK_NUMBER_COMPATIBILITY" in output
    assert "fallback=eth_getBlockByNumber" in output
    assert "REDACTED" not in output
    assert compat.status()["fallback_active"] is True


def test_after_success_drpc_routes_blocknumber_directly_to_equivalent_read() -> None:
    compat._FALLBACK_ACTIVE = True
    calls: list[tuple[str, list[object]]] = []

    async def original(_rpc_self, method, params):
        calls.append((method, list(params)))
        if method == "eth_getBlockByNumber":
            return {"number": "0x999"}
        raise AssertionError(method)

    wrapped = compat._compat_rpc_wrapper(original)
    rpc = SimpleNamespace(rpc_url="https://lb.drpc.live/robinhood/REDACTED")

    assert asyncio.run(wrapped(rpc, "eth_blockNumber", [])) == "0x999"
    assert calls == [("eth_getBlockByNumber", ["latest", False])]


@pytest.mark.parametrize("status", [401, 403, 429, 500])
def test_non_400_http_errors_do_not_activate_compatibility(status: int) -> None:
    async def original(_rpc_self, _method, _params):
        raise _http_error(status)

    wrapped = compat._compat_rpc_wrapper(original)
    rpc = SimpleNamespace(rpc_url="https://lb.drpc.live/robinhood/REDACTED")

    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(wrapped(rpc, "eth_blockNumber", []))
    assert compat.status()["fallback_active"] is False


def test_non_drpc_eth_blocknumber_400_is_unchanged() -> None:
    calls = 0

    async def original(_rpc_self, _method, _params):
        nonlocal calls
        calls += 1
        raise _http_error(400)

    wrapped = compat._compat_rpc_wrapper(original)
    rpc = SimpleNamespace(rpc_url="https://robinhood-mainnet.g.alchemy.com/v2/REDACTED")

    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(wrapped(rpc, "eth_blockNumber", []))
    assert calls == 1
    assert compat.status()["fallback_active"] is False


def test_fallback_requires_valid_latest_block_number() -> None:
    async def original(_rpc_self, method, _params):
        if method == "eth_blockNumber":
            raise _http_error(400)
        if method == "eth_getBlockByNumber":
            return {"hash": "0xabc"}
        raise AssertionError(method)

    wrapped = compat._compat_rpc_wrapper(original)
    rpc = SimpleNamespace(rpc_url="https://lb.drpc.live/robinhood/REDACTED")

    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(wrapped(rpc, "eth_blockNumber", []))
    assert compat.status()["fallback_active"] is False
    assert compat.status()["fallback_failures"] == 1


def test_compatibility_installed_inside_finalizer_before_failover() -> None:
    root = Path(__file__).parents[1] / "src" / "solana_roi"
    production = (root / "production.py").read_text(encoding="utf-8")
    finalizer = (root / "robinhood_production_provider_finalizer.py").read_text(encoding="utf-8")

    assert "install_robinhood_drpc_block_number_compat" not in production
    install_body = finalizer[finalizer.index("def install_robinhood_production_provider_finalizer("):]
    assert install_body.index("install_robinhood_drpc_block_number_compat(") < install_body.index(
        "install_robinhood_provider_failover()"
    ) < install_body.index("install_robinhood_provider_runtime_proof()")
    assert compat.status()["live_money_authority"] is False
    assert compat.status()["signing_available"] is False
    assert compat.status()["transaction_submission_available"] is False
