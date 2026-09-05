from __future__ import annotations

import asyncio
import os
from types import SimpleNamespace

import pytest

from solana_roi import robinhood_blockscout_pro_repair as blockscout
from solana_roi import robinhood_live_frontier_verification_repair as frontier


def _clear_blockscout_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in list(os.environ):
        if "BLOCKSCOUT" in name.upper():
            monkeypatch.delenv(name, raising=False)


def test_blockscout_dynamic_secret_name_is_used_only_when_unambiguous(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_blockscout_env(monkeypatch)
    monkeypatch.setenv("MY_BLOCKSCOUT_API_ACCESS_KEY", "dynamic-secret")

    name, value = blockscout._api_key_source()
    assert name == "MY_BLOCKSCOUT_API_ACCESS_KEY"
    assert value == "dynamic-secret"

    monkeypatch.setenv("SECOND_BLOCKSCOUT_API_TOKEN", "second-secret")
    name, value = blockscout._api_key_source()
    assert name is None
    assert value == ""


def test_blockscout_canonical_secret_still_has_precedence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_blockscout_env(monkeypatch)
    monkeypatch.setenv("MY_BLOCKSCOUT_API_ACCESS_KEY", "dynamic-secret")
    monkeypatch.setenv("BLOCKSCOUT_API_KEY", "canonical-secret")

    name, value = blockscout._api_key_source()
    assert name == "BLOCKSCOUT_API_KEY"
    assert value == "canonical-secret"


def test_fresh_head_verification_clears_stale_caught_up_authority() -> None:
    class Rpc:
        async def block_number(self) -> int:
            return 110

    plane = SimpleNamespace(rpc=Rpc(), _cursor=100, _latest_block=100, _caught_up=True)
    ready = asyncio.run(frontier._fresh_head_ready(plane))

    assert ready is False
    assert plane._caught_up is False
    assert plane._latest_block == 110
    assert plane._roi_live_frontier_last_lag == 10
    assert plane._roi_live_frontier_stale_ready_corrections == 1


def test_fresh_head_verification_allows_only_existing_two_block_boundary() -> None:
    class Rpc:
        async def block_number(self) -> int:
            return 102

    plane = SimpleNamespace(rpc=Rpc(), _cursor=100, _latest_block=100, _caught_up=True)
    ready = asyncio.run(frontier._fresh_head_ready(plane))

    assert ready is True
    assert plane._caught_up is True
    assert plane._roi_live_frontier_last_lag == 2
    assert plane._roi_live_frontier_ready_checks == 1


def test_entry_guard_fails_closed_when_fresh_head_rpc_fails() -> None:
    class Rpc:
        async def block_number(self) -> int:
            raise RuntimeError("temporary rpc failure")

    calls: list[str] = []

    async def original(self, value: str) -> str:
        calls.append(value)
        return value

    guarded = frontier._entry_guard(original)
    plane = SimpleNamespace(rpc=Rpc(), _cursor=100, _latest_block=100, _caught_up=True)
    result = asyncio.run(guarded(plane, "should-not-run"))

    assert result is None
    assert calls == []
    assert plane._caught_up is False
    assert plane._roi_live_frontier_failures == 1


def test_live_frontier_installer_tolerates_status_only_compatibility_plane() -> None:
    class Plane:
        def status(self):
            return {}

    frontier.install_robinhood_live_frontier_verification_repair(Plane)
    payload = Plane().status()

    assert payload["live_frontier_verification"]["fresh_block_number_required_before_paper_entry"] is True
    assert payload["live_frontier_verification"]["strategy_thresholds_changed"] is False
