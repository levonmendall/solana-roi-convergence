from __future__ import annotations

import asyncio
import json
import time
from types import SimpleNamespace

import httpx
import pytest

from solana_roi import robinhood_drpc_environment as drpc
from solana_roi import robinhood_provider_failover as failover
from solana_roi import robinhood_provider_runtime_proof as proof


_ENV = (
    *drpc.DRPC_KEY_ENV_NAMES,
    "ROBINHOOD_RPC_ENDPOINTS_JSON",
    "ROBINHOOD_RPC_URL",
    "ROBINHOOD_WS_URL",
    "ROBINHOOD_BACKUP_RPC_URL",
    "ROBINHOOD_BACKUP_WS_URL",
    "ROBINHOOD_PROVIDER_PRIMARY",
    "ROBINHOOD_PROVIDER_FAILOVER_COOLDOWN_SECONDS",
    "ROBINHOOD_DRPC_METHOD_UNAVAILABLE_COOLDOWN_SECONDS",
)


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    for name in _ENV:
        monkeypatch.delenv(name, raising=False)
    failover.reset_for_tests()
    with proof._FAILURE_LOCK:
        proof._REQUEST_FAILURE_COUNTS.clear()
    yield
    failover.reset_for_tests()
    with proof._FAILURE_LOCK:
        proof._REQUEST_FAILURE_COUNTS.clear()


def _production_legacy_drpc(monkeypatch) -> None:
    monkeypatch.setenv("DRPC_API_KEY", "not-a-real-secret")
    monkeypatch.setenv(
        "ROBINHOOD_RPC_URL",
        "https://robinhood-mainnet.g.alchemy.com/v2/redacted",
    )
    monkeypatch.setenv(
        "ROBINHOOD_WS_URL",
        "wss://robinhood-mainnet.g.alchemy.com/v2/redacted",
    )
    monkeypatch.setenv("ROBINHOOD_PROVIDER_PRIMARY", "drpc")
    monkeypatch.setenv("ROBINHOOD_PROVIDER_FAILOVER_COOLDOWN_SECONDS", "30")
    assert drpc.configure_robinhood_drpc_backup() is True
    assert failover.active_name() == "backup"
    assert proof._provider_kind(failover.active_provider()) == "drpc"


def _capability_base(calls: list[tuple[str, str]]):
    async def base(rpc_self, method, params):
        calls.append((rpc_self.rpc_url, method))
        if method == "eth_chainId":
            return hex(failover.runtime.ROBINHOOD_CHAIN_ID)
        if method == "eth_blockNumber":
            return "0x12345"
        raise AssertionError(method)

    return base


def _http_jsonrpc_error(status: int, code: int) -> httpx.HTTPStatusError:
    request = httpx.Request("POST", "https://redacted.invalid")
    response = httpx.Response(
        status,
        request=request,
        json={"jsonrpc": "2.0", "id": 1, "error": {"code": code, "message": "SECRET provider body"}},
    )
    return httpx.HTTPStatusError("SECRET exception", request=request, response=response)


def test_preferred_drpc_proves_chain_and_read_before_first_authoritative_rpc(monkeypatch) -> None:
    _production_legacy_drpc(monkeypatch)
    calls: list[tuple[str, str]] = []
    monkeypatch.setattr(failover, "_ORIGINAL_RPC", _capability_base(calls))
    rpc = SimpleNamespace(rpc_url=failover.active_provider().http)

    assert asyncio.run(proof._verify_preferred_if_needed(rpc)) is True
    state = failover._PROVIDER_STATE["backup"]
    assert state["chain_verified"] is True
    assert state["read_capability_verified"] is True
    assert state["chain_verifications"] == 1
    assert state["read_capability_verifications"] == 1
    assert state["last_verified_block"] == int("12345", 16)
    assert failover.generation() == 0
    assert [method for _url, method in calls] == ["eth_chainId", "eth_blockNumber"]
    assert all("drpc.live" in url for url, _method in calls)


def test_recovered_drpc_reclaims_primary_only_after_read_capability(monkeypatch) -> None:
    _production_legacy_drpc(monkeypatch)
    with failover._LOCK:
        failover._ACTIVE_NAME = "primary"
        failover._GENERATION = 4
        state = failover._state_for_locked("backup")
        state["cooldown_until"] = 0.0
        state["chain_verified"] = False
        state["read_capability_verified"] = False

    calls: list[tuple[str, str]] = []
    monkeypatch.setattr(failover, "_ORIGINAL_RPC", _capability_base(calls))
    rpc = SimpleNamespace(rpc_url=failover._provider_by_name("primary").http)

    assert asyncio.run(proof._verify_preferred_if_needed(rpc)) is True
    assert failover.active_name() == "backup"
    assert failover.generation() == 5
    state = failover._PROVIDER_STATE["backup"]
    assert state["chain_verified"] is True
    assert state["read_capability_verified"] is True
    assert state["failbacks_to"] == 1
    assert state.get("capability_quarantine_reason") is None
    assert "drpc.live" in rpc.rpc_url


def test_wrong_chain_drpc_cannot_reclaim_from_healthy_alchemy(monkeypatch) -> None:
    _production_legacy_drpc(monkeypatch)
    with failover._LOCK:
        failover._ACTIVE_NAME = "primary"
        failover._GENERATION = 2
        state = failover._state_for_locked("backup")
        state["cooldown_until"] = 0.0
        state["chain_verified"] = False
        state["read_capability_verified"] = False

    async def base(rpc_self, method, params):
        return "0x1"

    monkeypatch.setattr(failover, "_ORIGINAL_RPC", base)
    primary = failover._provider_by_name("primary")
    rpc = SimpleNamespace(rpc_url=primary.http)

    assert asyncio.run(proof._verify_preferred_if_needed(rpc)) is False
    assert failover.active_name() == "primary"
    assert failover.generation() == 2
    state = failover._PROVIDER_STATE["backup"]
    assert state["chain_verified"] is False
    assert state["read_capability_verified"] is False
    assert state["cooldown_until"] > time.monotonic()
    assert state.get("capability_quarantine_reason") is None
    assert rpc.rpc_url == primary.http


def test_chain_only_provider_cannot_be_marked_healthy(monkeypatch) -> None:
    _production_legacy_drpc(monkeypatch)

    async def base(rpc_self, method, params):
        if method == "eth_chainId":
            return hex(failover.runtime.ROBINHOOD_CHAIN_ID)
        if method == "eth_blockNumber":
            raise TimeoutError("synthetic")
        raise AssertionError(method)

    monkeypatch.setattr(failover, "_ORIGINAL_RPC", base)
    rpc = SimpleNamespace(rpc_url=failover.active_provider().http)

    before = time.monotonic()
    assert asyncio.run(proof._verify_preferred_if_needed(rpc)) is False
    state = failover._PROVIDER_STATE["backup"]
    assert state["chain_verified"] is False
    assert state["read_capability_verified"] is False
    assert state["last_failure_type"] == "TimeoutError"
    assert state["cooldown_until"] > before
    assert state["cooldown_until"] < before + 60
    assert state.get("capability_quarantine_reason") is None


def test_drpc_method_not_found_capability_failure_gets_long_quarantine(monkeypatch, capsys) -> None:
    _production_legacy_drpc(monkeypatch)
    monkeypatch.setenv("ROBINHOOD_DRPC_METHOD_UNAVAILABLE_COOLDOWN_SECONDS", "900")

    async def base(rpc_self, method, params):
        if method == "eth_chainId":
            return hex(failover.runtime.ROBINHOOD_CHAIN_ID)
        if method == "eth_blockNumber":
            raise _http_jsonrpc_error(400, -32601)
        raise AssertionError(method)

    monkeypatch.setattr(failover, "_ORIGINAL_RPC", base)
    rpc = SimpleNamespace(rpc_url=failover.active_provider().http)
    before = time.monotonic()

    assert asyncio.run(proof._verify_preferred_if_needed(rpc)) is False
    state = failover._PROVIDER_STATE["backup"]
    assert state["chain_verified"] is False
    assert state["read_capability_verified"] is False
    assert state["last_failure_type"] == "HTTPStatusError"
    assert state["last_capability_jsonrpc_code"] == -32601
    assert state["capability_quarantine_reason"] == "jsonrpc_method_not_found"
    assert state["cooldown_until"] >= before + 899
    assert failover.active_name() == "primary"

    output = capsys.readouterr().out
    assert "ROBINHOOD_PROVIDER_CAPABILITY_QUARANTINED" in output
    assert "provider_kind=drpc" in output
    assert "reason=jsonrpc_method_not_found" in output
    assert "jsonrpc_code=-32601" in output
    assert "cooldown_seconds=900" in output
    assert "SECRET" not in output
    assert "not-a-real-secret" not in output
    assert "lb.drpc.live" not in output


def test_drpc_quarantine_self_expires_and_allows_recovery_probe(monkeypatch) -> None:
    _production_legacy_drpc(monkeypatch)
    with failover._LOCK:
        state = failover._state_for_locked("backup")
        state["cooldown_until"] = time.monotonic() + 900
        state["capability_quarantine_reason"] = "jsonrpc_method_not_found"
        state["last_capability_jsonrpc_code"] = -32601
        failover._ACTIVE_NAME = "primary"

    calls: list[tuple[str, str]] = []
    monkeypatch.setattr(failover, "_ORIGINAL_RPC", _capability_base(calls))
    primary = failover._provider_by_name("primary")
    rpc = SimpleNamespace(rpc_url=primary.http)

    assert asyncio.run(proof._verify_preferred_if_needed(rpc)) is False
    assert calls == []

    with failover._LOCK:
        state = failover._state_for_locked("backup")
        state["cooldown_until"] = 0.0

    assert asyncio.run(proof._verify_preferred_if_needed(rpc)) is True
    assert failover.active_name() == "backup"
    state = failover._PROVIDER_STATE["backup"]
    assert state["capability_quarantine_reason"] is None
    assert state["last_capability_jsonrpc_code"] is None
    assert state["read_capability_verified"] is True


def test_runtime_status_counts_traffic_without_endpoint_or_secret(monkeypatch) -> None:
    _production_legacy_drpc(monkeypatch)

    def original_mark_success(name: str, *, transport_kind: str) -> None:
        with failover._LOCK:
            state = failover._state_for_locked(name)
            state[f"{transport_kind}_failures"] = 0

    monkeypatch.setattr(proof, "_ORIGINAL_MARK_SUCCESS", original_mark_success)
    monkeypatch.setattr(
        proof,
        "_ORIGINAL_STATUS",
        lambda: {
            "version": failover.FAILOVER_VERSION,
            "provider_count": len(failover.providers()),
            "provider_names": [item.name for item in failover.providers()],
        },
    )
    with failover._LOCK:
        state = failover._state_for_locked("backup")
        state["chain_verified"] = True
        state["read_capability_verified"] = True

    proof._mark_success_with_telemetry("backup", transport_kind="http")
    proof._mark_success_with_telemetry("backup", transport_kind="ws")
    status = proof._status_with_runtime_proof()
    encoded = json.dumps(status, sort_keys=True)

    assert status["active_provider_semantic"] == "drpc"
    assert status["provider_traffic_observed"] is True
    assert status["provider_traffic"]["backup"]["provider_kind"] == "drpc"
    assert status["provider_traffic"]["backup"]["http_successes"] == 1
    assert status["provider_traffic"]["backup"]["ws_successes"] == 1
    assert status["provider_traffic"]["backup"]["read_capability_verified"] is True
    assert status["provider_traffic"]["backup"]["capability_quarantined"] is False
    assert status["drpc_method_unavailable_cooldown_seconds"] == 900.0
    assert "not-a-real-secret" not in encoded
    assert "lb.drpc.live" not in encoded
    assert "alchemy.com" not in encoded


def test_runtime_status_exposes_safe_drpc_quarantine_state(monkeypatch) -> None:
    _production_legacy_drpc(monkeypatch)
    monkeypatch.setattr(
        proof,
        "_ORIGINAL_STATUS",
        lambda: {
            "version": failover.FAILOVER_VERSION,
            "provider_count": len(failover.providers()),
            "provider_names": [item.name for item in failover.providers()],
        },
    )
    with failover._LOCK:
        state = failover._state_for_locked("backup")
        state["cooldown_until"] = time.monotonic() + 900
        state["capability_quarantine_reason"] = "jsonrpc_method_not_found"
        state["last_capability_jsonrpc_code"] = -32601
        failover._ACTIVE_NAME = "primary"

    status = proof._status_with_runtime_proof()
    encoded = json.dumps(status, sort_keys=True)
    drpc_state = status["provider_traffic"]["backup"]
    assert drpc_state["capability_quarantined"] is True
    assert drpc_state["capability_quarantine_reason"] == "jsonrpc_method_not_found"
    assert 0 < drpc_state["capability_quarantine_remaining_seconds"] <= 900
    assert drpc_state["last_capability_jsonrpc_code"] == -32601
    assert "not-a-real-secret" not in encoded
    assert "lb.drpc.live" not in encoded
    assert "SECRET" not in encoded


def test_runtime_request_failure_telemetry_is_secret_free(monkeypatch, capsys) -> None:
    _production_legacy_drpc(monkeypatch)
    proof._record_request_failure(failover.active_provider(), "eth_getLogs", "ReadTimeout")
    output = capsys.readouterr().out
    assert "ROBINHOOD_PROVIDER_REQUEST_FAILED" in output
    assert "provider_kind=drpc" in output
    assert "method=eth_getLogs" in output
    assert "error_type=ReadTimeout" in output
    assert "not-a-real-secret" not in output
    assert "lb.drpc.live" not in output


def test_runtime_proof_stays_inside_existing_robinhood_provider_finalizer() -> None:
    from pathlib import Path

    root = Path(__file__).parents[1] / "src" / "solana_roi"
    production = (root / "production.py").read_text(encoding="utf-8")
    finalizer = (root / "robinhood_production_provider_finalizer.py").read_text(encoding="utf-8")

    assert "install_robinhood_provider_runtime_proof()" not in production
    assert production.index("configure_robinhood_drpc_backup()") < production.index(
        "from .production_system import"
    )

    install_body = finalizer[finalizer.index("def install_robinhood_production_provider_finalizer("):]
    assert install_body.index("install_robinhood_provider_failover()") < install_body.index(
        "install_robinhood_provider_runtime_proof()"
    ) < install_body.index("_preserve_bounded_transport_aliases()")
    assert proof.DEFAULT_DRPC_METHOD_UNAVAILABLE_COOLDOWN_SECONDS == 900.0
