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
    monkeypatch.setattr(compat, "_MODE", None)
    monkeypatch.setattr(compat, "_FALLBACK_SUCCESSES", 0)
    monkeypatch.setattr(compat, "_FALLBACK_FAILURES", 0)
    yield


def _http_error(status: int) -> httpx.HTTPStatusError:
    request = httpx.Request("POST", "https://redacted.invalid")
    response = httpx.Response(status, request=request)
    return httpx.HTTPStatusError("redacted", request=request, response=response)


class _Client:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls: list[dict[str, object]] = []

    async def post(self, url: str, *, json: dict[str, object]):
        self.calls.append({"url": url, "json": dict(json)})
        item = self.responses.pop(0)
        if isinstance(item, BaseException):
            raise item
        request = httpx.Request("POST", url)
        return httpx.Response(int(item[0]), request=request, json=item[1])


def _rpc(client: _Client):
    return SimpleNamespace(
        rpc_url="https://lb.drpc.live/robinhood/REDACTED",
        client=client,
        _request_id=0,
    )


def test_drpc_eth_blocknumber_400_retries_documented_paramless_form(capsys) -> None:
    async def original(_rpc_self, method, _params):
        assert method == "eth_blockNumber"
        raise _http_error(400)

    client = _Client([(200, {"jsonrpc": "2.0", "id": 1, "result": "0x12345"})])
    wrapped = compat._compat_rpc_wrapper(original)
    rpc = _rpc(client)

    assert asyncio.run(wrapped(rpc, "eth_blockNumber", [])) == "0x12345"
    assert client.calls[0]["json"] == {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_blockNumber",
    }
    output = capsys.readouterr().out
    assert "ROBINHOOD_DRPC_BLOCK_NUMBER_COMPATIBILITY" in output
    assert "mode=eth_blockNumber_without_params" in output
    assert "REDACTED" not in output
    assert compat.status()["fallback_mode"] == "eth_blockNumber_without_params"


def test_paramless_400_uses_documented_finalized_block_form(capsys) -> None:
    async def original(_rpc_self, method, _params):
        assert method == "eth_blockNumber"
        raise _http_error(400)

    client = _Client([
        (400, {"error": "bad request"}),
        (200, {"jsonrpc": "2.0", "id": 2, "result": {"number": "0x999", "hash": "0xabc"}}),
    ])
    wrapped = compat._compat_rpc_wrapper(original)
    rpc = _rpc(client)

    assert asyncio.run(wrapped(rpc, "eth_blockNumber", [])) == "0x999"
    assert client.calls[0]["json"] == {"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber"}
    assert client.calls[1]["json"] == {
        "jsonrpc": "2.0",
        "id": 2,
        "method": "eth_getBlockByNumber",
        "params": ["finalized", False],
    }
    output = capsys.readouterr().out
    assert "stage=eth_blockNumber_without_params" in output
    assert "http_status=400" in output
    assert "mode=eth_getBlockByNumber_finalized_false" in output
    assert compat.status()["fallback_mode"] == "eth_getBlockByNumber_finalized_false"


def test_after_success_routes_blocknumber_directly_to_proven_form() -> None:
    compat._MODE = compat._MODE_PARAMLESS
    client = _Client([(200, {"jsonrpc": "2.0", "id": 1, "result": "0x777"})])

    async def original(_rpc_self, _method, _params):
        raise AssertionError("original should not be called")

    wrapped = compat._compat_rpc_wrapper(original)
    rpc = _rpc(client)
    assert asyncio.run(wrapped(rpc, "eth_blockNumber", [])) == "0x777"
    assert client.calls[0]["json"] == {"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber"}


@pytest.mark.parametrize("status", [401, 403, 429, 500])
def test_non_400_http_errors_do_not_activate_compatibility(status: int) -> None:
    async def original(_rpc_self, _method, _params):
        raise _http_error(status)

    wrapped = compat._compat_rpc_wrapper(original)
    rpc = _rpc(_Client([]))

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
    rpc = SimpleNamespace(
        rpc_url="https://robinhood-mainnet.g.alchemy.com/v2/REDACTED",
        client=_Client([]),
        _request_id=0,
    )

    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(wrapped(rpc, "eth_blockNumber", []))
    assert calls == 1
    assert compat.status()["fallback_active"] is False


def test_all_compatibility_forms_must_return_valid_block_number(capsys) -> None:
    async def original(_rpc_self, method, _params):
        assert method == "eth_blockNumber"
        raise _http_error(400)

    client = _Client([
        (400, {"error": "bad request"}),
        (200, {"jsonrpc": "2.0", "id": 2, "result": {"hash": "0xabc"}}),
    ])
    wrapped = compat._compat_rpc_wrapper(original)
    rpc = _rpc(client)

    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(wrapped(rpc, "eth_blockNumber", []))
    output = capsys.readouterr().out
    assert "stage=eth_blockNumber_without_params" in output
    assert "stage=eth_getBlockByNumber_finalized_false" in output
    assert "REDACTED" not in output
    assert compat.status()["fallback_active"] is False
    assert compat.status()["fallback_failures"] == 2


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
