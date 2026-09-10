from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

import httpx
import pytest

from solana_roi import robinhood_getlogs_capability_repair as repair
from solana_roi import robinhood_provider_failover as failover


@dataclass
class _Rpc:
    rpc_url: str
    calls: list[tuple[str, int, int]] = field(default_factory=list)


def _provider(name: str, host: str) -> failover.ProviderEndpoint:
    return failover.ProviderEndpoint(name=name, http=f"https://{host}/credential", ws=f"wss://{host}/credential")


def _blocks(params: list[Any]) -> tuple[int, int]:
    query = params[0]
    return int(query["fromBlock"], 16), int(query["toBlock"], 16)


def _forbidden(url: str, message: str = "forbidden") -> httpx.HTTPStatusError:
    request = httpx.Request("POST", url)
    response = httpx.Response(403, request=request, json={"error": {"code": -32000, "message": message}})
    return httpx.HTTPStatusError("forbidden", request=request, response=response)


def _configure(monkeypatch: pytest.MonkeyPatch, rpc: _Rpc, providers: tuple[failover.ProviderEndpoint, ...], original_rpc: Any) -> None:
    failover.reset_for_tests()
    monkeypatch.setattr(failover, "providers", lambda: providers)
    monkeypatch.setattr(failover, "active_provider", lambda: providers[0])
    monkeypatch.setattr(failover, "_ORIGINAL_RPC", original_rpc)
    with failover._LOCK:
        for provider in providers:
            state = failover._state_for_locked(provider.name)
            state["chain_verified"] = True
            state["read_capability_verified"] = True


def _run(rpc: _Rpc, start: int, end: int) -> list[dict[str, Any]]:
    return asyncio.run(
        repair._capability_request_range(
            rpc,
            from_block=start,
            to_block=end,
            addresses=["0xmarket"],
            topics=["0xtopic"],
        )
    )


def test_span1_403_marks_chainstack_basic_only_and_falls_back_exactly(monkeypatch, capsys) -> None:
    chainstack = _provider("chainstack", "robinhood-mainnet.core.chainstack.com")
    alchemy = _provider("alchemy", "robinhood-mainnet.g.alchemy.com")
    rpc = _Rpc(chainstack.http)

    async def original(rpc_self: _Rpc, method: str, params: list[Any]) -> Any:
        assert method == "eth_getLogs"
        start, end = _blocks(params)
        kind = "chainstack" if "chainstack" in rpc_self.rpc_url else "alchemy"
        rpc_self.calls.append((kind, start, end))
        if kind == "chainstack":
            raise _forbidden(
                rpc_self.rpc_url,
                "token=supersecret https://robinhood-mainnet.core.chainstack.com/credential",
            )
        return [{"blockNumber": hex(block)} for block in range(start, end + 1)]

    _configure(monkeypatch, rpc, (chainstack, alchemy), original)
    rows = _run(rpc, 500, 500)

    assert [int(row["blockNumber"], 16) for row in rows] == [500]
    assert rpc.calls == [("chainstack", 500, 500), ("alchemy", 500, 500)]
    with failover._LOCK:
        state = failover._state_for_locked("chainstack")
        assert state["getlogs_capability"] == "basic_evm_rpc_only"
        assert state["getlogs_success_streak"] == 0
    output = capsys.readouterr().out
    assert "capability=basic_evm_rpc_only" in output
    assert "supersecret" not in output
    assert "/credential" not in output
    assert "https://" not in output


def test_chainstack_discovers_accepted_range_then_uses_contiguous_chunks(monkeypatch) -> None:
    chainstack = _provider("chainstack", "robinhood-mainnet.core.chainstack.com")
    alchemy = _provider("alchemy", "robinhood-mainnet.g.alchemy.com")
    rpc = _Rpc(chainstack.http)

    async def original(rpc_self: _Rpc, method: str, params: list[Any]) -> Any:
        assert method == "eth_getLogs"
        start, end = _blocks(params)
        kind = "chainstack" if "chainstack" in rpc_self.rpc_url else "alchemy"
        rpc_self.calls.append((kind, start, end))
        span = end - start + 1
        if kind == "chainstack" and span > 4:
            raise _forbidden(rpc_self.rpc_url, "block range is too large")
        return [{"blockNumber": hex(block)} for block in range(start, end + 1)]

    _configure(monkeypatch, rpc, (chainstack, alchemy), original)
    rows = _run(rpc, 100, 109)

    assert [int(row["blockNumber"], 16) for row in rows] == list(range(100, 110))
    # The failed 10-block request is followed by the decisive span-1 probe and
    # methodical boundary discovery. Final retrieval is exactly 4,4,2 blocks.
    assert ("chainstack", 100, 100) in rpc.calls
    assert rpc.calls[-3:] == [
        ("chainstack", 100, 103),
        ("chainstack", 104, 107),
        ("chainstack", 108, 109),
    ]
    with failover._LOCK:
        state = failover._state_for_locked("chainstack")
        assert state["getlogs_safe_max_blocks"] == 4
        assert state["getlogs_capability"] == "healthy"
        assert state["getlogs_success_streak"] >= repair.RECOVERY_SUCCESS_STREAK
        assert state["getlogs_span1_proven"] is True


def test_failed_fallback_chunk_does_not_retrieve_later_range(monkeypatch) -> None:
    chainstack = _provider("chainstack", "robinhood-mainnet.core.chainstack.com")
    alchemy = _provider("alchemy", "robinhood-mainnet.g.alchemy.com")
    rpc = _Rpc(chainstack.http)

    async def original(rpc_self: _Rpc, method: str, params: list[Any]) -> Any:
        assert method == "eth_getLogs"
        start, end = _blocks(params)
        kind = "chainstack" if "chainstack" in rpc_self.rpc_url else "alchemy"
        rpc_self.calls.append((kind, start, end))
        if kind == "chainstack":
            raise _forbidden(rpc_self.rpc_url)
        if start == 710:
            raise httpx.ReadTimeout("fallback timeout")
        return [{"blockNumber": hex(block)} for block in range(start, end + 1)]

    _configure(monkeypatch, rpc, (chainstack, alchemy), original)

    with pytest.raises(httpx.ReadTimeout):
        _run(rpc, 700, 724)

    alchemy_calls = [(start, end) for kind, start, end in rpc.calls if kind == "alchemy"]
    assert alchemy_calls == [(700, 709), (710, 719)]
    assert (720, 724) not in alchemy_calls


def test_getlogs_recovery_requires_three_consecutive_successes_and_failure_resets(monkeypatch) -> None:
    provider = _provider("chainstack", "robinhood-mainnet.core.chainstack.com")
    failover.reset_for_tests()
    monkeypatch.setattr(failover, "providers", lambda: (provider,))
    with failover._LOCK:
        state = failover._state_for_locked(provider.name)
        state["chain_verified"] = True
        state["read_capability_verified"] = True
        state["getlogs_capability"] = "degraded"

    repair._record_success(provider, span=1)
    repair._record_success(provider, span=1)
    assert repair.status()["providers"]["chainstack"]["fully_healthy"] is False

    repair._record_success(provider, span=1)
    proof = repair.status()["providers"]["chainstack"]
    assert proof["eth_getlogs_success_streak"] == 3
    assert proof["eth_getlogs_capability"] == "healthy"
    assert proof["fully_healthy"] is True

    repair._record_failure(provider, _forbidden(provider.http), span=2)
    proof = repair.status()["providers"]["chainstack"]
    assert proof["eth_getlogs_success_streak"] == 0
    assert proof["eth_getlogs_capability"] == "degraded"
    assert proof["fully_healthy"] is False


def test_basic_rpc_verification_alone_never_reports_full_health(monkeypatch) -> None:
    provider = _provider("chainstack", "robinhood-mainnet.core.chainstack.com")
    failover.reset_for_tests()
    monkeypatch.setattr(failover, "providers", lambda: (provider,))
    with failover._LOCK:
        state = failover._state_for_locked(provider.name)
        state["chain_verified"] = True
        state["read_capability_verified"] = True

    proof = repair.status()
    assert proof["basic_rpc_success_does_not_imply_full_health"] is True
    assert proof["providers"]["chainstack"]["fully_healthy"] is False
    assert proof["mandatory_capabilities"] == ["eth_chainId", "eth_blockNumber", "eth_getLogs"]
    assert proof["paper_only"] is True
    assert proof["live_money_authority"] is False
