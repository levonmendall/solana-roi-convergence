from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REPLAY = ROOT / "diagnostics" / "portable_repro" / "robinhood_replay.py"


def _load():
    spec = importlib.util.spec_from_file_location("portable_robinhood_replay", REPLAY)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_alchemy_first_chainid_is_429_then_structural_success():
    mod = _load()
    state = mod.ReplayState(scenario="alchemy-429-then-drpc")
    status, first = mod.http_response(
        state,
        provider="alchemy",
        method="eth_chainId",
        params=[],
        request_id=1,
    )
    assert status == 429
    assert first["error"]["code"] == -32005
    status, second = mod.http_response(
        state,
        provider="alchemy",
        method="eth_chainId",
        params=[],
        request_id=2,
    )
    assert status == 200
    assert second["result"] == hex(4663)


def test_drpc_chain_verification_succeeds_immediately():
    mod = _load()
    state = mod.ReplayState(scenario="alchemy-429-then-drpc")
    status, payload = mod.http_response(
        state,
        provider="drpc",
        method="eth_chainId",
        params=[],
        request_id=10,
    )
    assert status == 200
    assert int(payload["result"], 16) == 4663


def test_getlogs_is_structurally_empty_and_block_number_is_valid_quantity():
    mod = _load()
    state = mod.ReplayState(scenario="steady", head=9_250_123)
    assert mod.jsonrpc_result("eth_getLogs", [{}], head=state.head) == []
    assert int(mod.jsonrpc_result("eth_blockNumber", [], head=state.head), 16) == 9_250_123


def test_eth_call_is_exactly_one_zero_word_structural_default():
    mod = _load()
    result = mod.jsonrpc_result("eth_call", [{}, "latest"], head=1)
    assert result == "0x" + "00" * 32
