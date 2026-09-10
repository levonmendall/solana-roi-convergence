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


def _http_error(status: int, *, code: int | None = None) -> httpx.HTTPStatusError:
    request = httpx.Request("POST", "https://redacted.invalid")
    body = {"error": {"code": code, "message": "SECRET response body"}} if code is not None else {}
    response = httpx.Response(status, request=request, json=body)
    return httpx.HTTPStatusError("SECRET exception", request=request, response=response)


class _Client:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls: list[dict[str, object]] = []

    async def post(
        self,
        url: str,
        *,
        json: dict[str, object],
        headers: dict[str, str] | None = None,
    ):
        self.calls.append({"url": url, "json": dict(json), "headers": dict(headers or {})})
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
    assert client.calls[0]["json"] == {"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber"}
    output = capsys.readouterr().out
    assert "ROBINHOOD_DRPC_BLOCK_NUMBER_COMPATIBILITY" in output
    assert "mode=eth_blockNumber_without_params" in output
    assert "REDACTED" not in output


def test_paramless_400_uses_documented_finalized_block_form(capsys) -> None:
    async def original(_rpc_self, method, _params):
        assert method == "eth_blockNumber"
        raise _http_error(400)

    client = _Client([
        (400, {"error": {"code": 5, "message": "bad request"}}),
        (200, {"jsonrpc": "2.0", "id": 2, "result": {"number": "0x999", "hash": "0xabc"}}),
    ])
    wrapped = compat._compat_rpc_wrapper(original)
    rpc = _rpc(client)
    assert asyncio.run(wrapped(rpc, "eth_blockNumber", [])) == "0x999"
    assert client.calls[1]["json"] == {
        "jsonrpc": "2.0",
        "id": 2,
        "method": "eth_getBlockByNumber",
        "params": ["finalized", False],
    }
    output = capsys.readouterr().out
    assert "stage=eth_blockNumber_without_params" in output
    assert "mode=eth_getBlockByNumber_finalized_false" in output


def test_path_forms_400_use_ogrpc_header_gateway_and_prove_block(capsys) -> None:
    async def original(_rpc_self, method, _params):
        assert method == "eth_blockNumber"
        raise _http_error(400)

    client = _Client([
        (400, {"error": {"code": 5, "message": "path invalid"}}),
        (400, {"error": {"code": 13, "message": "path cannot route"}}),
        (200, {"jsonrpc": "2.0", "id": 3, "result": "0xabc"}),
    ])
    wrapped = compat._compat_rpc_wrapper(original)
    rpc = _rpc(client)
    assert asyncio.run(wrapped(rpc, "eth_blockNumber", [])) == "0xabc"
    gateway = client.calls[2]
    assert gateway["url"] == "https://lb.drpc.org/ogrpc?network=robinhood"
    assert gateway["json"] == {"jsonrpc": "2.0", "id": 3, "method": "eth_blockNumber", "params": []}
    assert gateway["headers"] == {"Drpc-Key": "REDACTED", "Content-Type": "application/json"}
    output = capsys.readouterr().out
    assert "mode=ogrpc_header_robinhood" in output
    assert "REDACTED" not in output
    assert "path invalid" not in output
    assert "path cannot route" not in output


def test_method_not_found_block_routes_salvage_latest_height_via_path_fee_history(capsys) -> None:
    async def original(_rpc_self, method, _params):
        assert method == "eth_blockNumber"
        raise _http_error(400, code=-32601)

    client = _Client([
        (400, {"error": {"code": -32601, "message": "not found"}}),
        (400, {"error": {"code": -32601, "message": "not found"}}),
        (400, {"error": {"code": -32601, "message": "not found"}}),
        (200, {"jsonrpc": "2.0", "id": 4, "result": {"oldestBlock": "0x1237", "baseFeePerGas": ["0x1", "0x1"], "gasUsedRatio": [0.5]}}),
    ])
    wrapped = compat._compat_rpc_wrapper(original)
    rpc = _rpc(client)

    assert asyncio.run(wrapped(rpc, "eth_blockNumber", [])) == "0x1237"
    fee_call = client.calls[3]
    assert fee_call["url"] == "https://lb.drpc.live/robinhood/REDACTED"
    assert fee_call["json"] == {
        "jsonrpc": "2.0",
        "id": 4,
        "method": "eth_feeHistory",
        "params": [1, "latest", []],
    }
    assert fee_call["headers"] == {}
    output = capsys.readouterr().out
    assert "drpc_code=-32601" in output
    assert "mode=eth_feeHistory_latest_one_path" in output
    assert "not found" not in output
    assert "REDACTED" not in output
    assert compat.status()["fallback_mode"] == compat._MODE_FEE_HISTORY_PATH


def test_path_fee_history_failure_can_salvage_via_ogrpc_fee_history(capsys) -> None:
    async def original(_rpc_self, method, _params):
        assert method == "eth_blockNumber"
        raise _http_error(400, code=-32601)

    fail = (400, {"error": {"code": -32601, "message": "not found"}})
    client = _Client([
        fail,
        fail,
        fail,
        fail,
        (200, {"jsonrpc": "2.0", "id": 5, "result": {"oldestBlock": "0x4567", "baseFeePerGas": ["0x1", "0x1"], "gasUsedRatio": [0.25]}}),
    ])
    wrapped = compat._compat_rpc_wrapper(original)
    rpc = _rpc(client)

    assert asyncio.run(wrapped(rpc, "eth_blockNumber", [])) == "0x4567"
    fee_call = client.calls[4]
    assert fee_call["url"] == "https://lb.drpc.org/ogrpc?network=robinhood"
    assert fee_call["json"] == {
        "jsonrpc": "2.0",
        "id": 5,
        "method": "eth_feeHistory",
        "params": [1, "latest", []],
    }
    assert fee_call["headers"] == {"Drpc-Key": "REDACTED", "Content-Type": "application/json"}
    output = capsys.readouterr().out
    assert "mode=eth_feeHistory_latest_one_ogrpc" in output
    assert "REDACTED" not in output
    assert compat.status()["fallback_mode"] == compat._MODE_FEE_HISTORY_OGRPC


def test_after_path_fee_history_proof_block_head_reuses_fee_history_but_other_methods_stay_canonical() -> None:
    compat._MODE = compat._MODE_FEE_HISTORY_PATH
    client = _Client([
        (200, {"jsonrpc": "2.0", "id": 1, "result": {"oldestBlock": "0x777", "baseFeePerGas": ["0x1", "0x1"], "gasUsedRatio": [0.1]}}),
    ])
    original_calls: list[tuple[str, list[object]]] = []

    async def original(_rpc_self, method, params):
        original_calls.append((method, list(params)))
        return "0x2a"

    wrapped = compat._compat_rpc_wrapper(original)
    rpc = _rpc(client)
    assert asyncio.run(wrapped(rpc, "eth_blockNumber", [])) == "0x777"
    assert client.calls[0]["json"] == {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_feeHistory",
        "params": [1, "latest", []],
    }
    assert asyncio.run(wrapped(rpc, "eth_gasPrice", [])) == "0x2a"
    assert original_calls == [("eth_gasPrice", [])]


def test_after_ogrpc_fee_history_proof_only_block_head_uses_generic_gateway() -> None:
    compat._MODE = compat._MODE_FEE_HISTORY_OGRPC
    client = _Client([
        (200, {"jsonrpc": "2.0", "id": 1, "result": {"oldestBlock": "0x888", "baseFeePerGas": ["0x1", "0x1"], "gasUsedRatio": [0.1]}}),
    ])
    original_calls: list[tuple[str, list[object]]] = []

    async def original(_rpc_self, method, params):
        original_calls.append((method, list(params)))
        return "0x3b"

    wrapped = compat._compat_rpc_wrapper(original)
    rpc = _rpc(client)
    assert asyncio.run(wrapped(rpc, "eth_blockNumber", [])) == "0x888"
    assert client.calls[0]["url"] == "https://lb.drpc.org/ogrpc?network=robinhood"
    assert asyncio.run(wrapped(rpc, "eth_gasPrice", [])) == "0x3b"
    assert original_calls == [("eth_gasPrice", [])]


@pytest.mark.parametrize(
    "history",
    [
        None,
        [],
        {},
        {"oldestBlock": None},
        {"oldestBlock": "garbage"},
        {"oldestBlock": "-1"},
    ],
)
def test_fee_history_must_contain_valid_nonnegative_latest_height(history) -> None:
    with pytest.raises(RuntimeError):
        compat._extract_fee_history_head(history)


def test_after_ogrpc_proof_all_drpc_http_reads_use_same_gateway() -> None:
    compat._MODE = compat._MODE_OGRPC
    client = _Client([(200, {"jsonrpc": "2.0", "id": 1, "result": "0x2a"})])

    async def original(_rpc_self, _method, _params):
        raise AssertionError("path-based RPC must not be reused after generic gateway proof")

    wrapped = compat._compat_rpc_wrapper(original)
    rpc = _rpc(client)
    assert asyncio.run(wrapped(rpc, "eth_gasPrice", [])) == "0x2a"
    assert client.calls[0]["url"] == "https://lb.drpc.org/ogrpc?network=robinhood"


def test_numeric_drpc_error_code_is_logged_without_response_body_or_key(capsys) -> None:
    compat._MODE = compat._MODE_OGRPC
    client = _Client([_http_error(400, code=29)])

    async def original(_rpc_self, _method, _params):
        raise AssertionError("not used")

    wrapped = compat._compat_rpc_wrapper(original)
    rpc = _rpc(client)
    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(wrapped(rpc, "eth_blockNumber", []))
    output = capsys.readouterr().out
    assert "http_status=400" in output
    assert "drpc_code=29" in output
    assert "SECRET" not in output
    assert "REDACTED" not in output


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


def test_missing_path_key_keeps_ogrpc_fail_closed() -> None:
    compat._MODE = compat._MODE_OGRPC
    client = _Client([])

    async def original(_rpc_self, _method, _params):
        raise AssertionError("not used")

    wrapped = compat._compat_rpc_wrapper(original)
    rpc = SimpleNamespace(
        rpc_url="https://lb.drpc.org/ogrpc?network=robinhood",
        client=client,
        _request_id=0,
    )
    with pytest.raises(RuntimeError, match="MissingDrpcPathKeyForOgrpc"):
        asyncio.run(wrapped(rpc, "eth_blockNumber", []))


def test_compatibility_installed_inside_finalizer_before_failover() -> None:
    root = Path(__file__).parents[1] / "src" / "solana_roi"
    production = (root / "production.py").read_text(encoding="utf-8")
    finalizer = (root / "robinhood_production_provider_finalizer.py").read_text(encoding="utf-8")
    assert "install_robinhood_drpc_block_number_compat" not in production
    install_body = finalizer[finalizer.index("def install_robinhood_production_provider_finalizer("):]
    assert install_body.index("install_robinhood_drpc_block_number_compat(") < install_body.index(
        "install_robinhood_provider_failover()"
    ) < install_body.index("install_robinhood_provider_runtime_proof()")
    state = compat.status()
    assert state["ogrpc_routes_all_drpc_http_after_direct_block_proof"] is True
    assert state["fee_history_proof_routes_only_block_head"] is True
    assert state["fee_history_contract"] == "blockCount=1,newestBlock=latest,rewardPercentiles=[]"
    assert state["logs_only_numeric_drpc_error_code"] is True
    assert state["live_money_authority"] is False
    assert state["signing_available"] is False
    assert state["transaction_submission_available"] is False
