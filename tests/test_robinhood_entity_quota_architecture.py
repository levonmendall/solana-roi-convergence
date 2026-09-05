from __future__ import annotations

import asyncio
import sqlite3
import threading
import time
from types import SimpleNamespace

from solana_roi import continuation_market_recalibration as continuation
from solana_roi import robinhood_entity_quota_architecture as quota
from solana_roi import robinhood_entity_resolution_repair as entity_repair


ACTOR_A = "0x" + "1" * 40
ACTOR_B = "0x" + "2" * 40
ACTOR_C = "0x" + "3" * 40
DEPLOYER = "0x" + "4" * 40
FUNDER_A = "0x" + "a" * 40
FUNDER_B = "0x" + "b" * 40
FUNDER_C = "0x" + "c" * 40
TX_HASH = "0x" + "f" * 64


class Store:
    def __init__(self) -> None:
        self.db = sqlite3.connect(":memory:")
        self.db.row_factory = sqlite3.Row
        self._lock = threading.RLock()


class Response:
    status_code = 200

    def __init__(self, payload, *, credits: int = 99_980, rps: int = 4) -> None:
        self._payload = payload
        self.headers = {
            "x-credits-remaining": str(credits),
            "x-ratelimit-remaining": str(rps),
        }

    def raise_for_status(self) -> None:
        return None

    def json(self):
        return self._payload


class Client:
    def __init__(self, payload, *, credits: int = 99_980) -> None:
        self.payload = payload
        self.credits = credits
        self.calls = []

    async def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return Response(self.payload, credits=self.credits)


class Plane:
    def __init__(self, store: Store, payload, *, credits: int = 99_980) -> None:
        self.store = store
        self._entity_cache: dict[str, tuple[str, float]] = {}
        self._entity_resolution_failures = 0
        self.rpc = SimpleNamespace(client=Client(payload, credits=credits))


async def _base_anchor(plane: Plane, actor: str) -> str | None:
    return await entity_repair._entity_anchor(plane, actor)


def test_successful_entity_proof_is_durable_across_plane_instances(monkeypatch) -> None:
    monkeypatch.setenv("BLOCKSCOUT_API_KEY", "proapi_test_only")
    store = Store()
    payload = {
        "status": "1",
        "message": "OK",
        "result": [
            {
                "blockNumber": "123",
                "hash": TX_HASH,
                "from": FUNDER_A,
                "to": ACTOR_A,
                "value": "1000",
            }
        ],
    }
    first = Plane(store, payload)
    resolved = asyncio.run(quota._entity_anchor_fetch_quota(first, ACTOR_A))
    assert resolved == FUNDER_A
    assert len(first.rpc.client.calls) == 1

    with store._lock:
        row = store.db.execute(
            "SELECT funding_anchor,proof_kind,proof_block,proof_tx,resolver_version "
            "FROM robinhood_entity_proofs WHERE chain_id=? AND actor=?",
            (4663, ACTOR_A),
        ).fetchone()
    assert row is not None
    assert row["funding_anchor"] == FUNDER_A
    assert row["proof_kind"] == "earliest_inbound_native_funder"
    assert row["proof_block"] == 123
    assert row["proof_tx"] == TX_HASH
    assert row["resolver_version"] == quota.PROOF_VERSION

    second = Plane(store, {"status": "1", "message": "OK", "result": []})
    resolved_again = asyncio.run(quota._entity_anchor_fetch_quota(second, ACTOR_A))
    assert resolved_again == FUNDER_A
    assert second.rpc.client.calls == []
    stats = quota._quota_stats(second)
    assert stats["durable_cache_hits"] == 1
    assert stats["external_requests_avoided"] == 1


def test_provider_credit_headers_are_persisted_and_reported(monkeypatch) -> None:
    monkeypatch.setenv("BLOCKSCOUT_API_KEY", "proapi_test_only")
    store = Store()
    plane = Plane(store, {"status": "0", "message": "No transactions found", "result": []}, credits=77_777)
    resolved = asyncio.run(quota._entity_anchor_fetch_quota(plane, ACTOR_A))
    assert resolved == ACTOR_A
    stats = quota._quota_stats(plane)
    assert stats["provider_credits_remaining"] == 77_777
    assert stats["provider_ratelimit_remaining"] == 4
    usage = quota._usage_row(plane)
    assert usage["provider_requests"] == 1
    assert usage["assumed_credits"] == 20
    assert usage["provider_credits_remaining"] == 77_777


def test_noncritical_enrichment_yields_before_protected_reserve(monkeypatch) -> None:
    monkeypatch.setenv("BLOCKSCOUT_API_KEY", "proapi_test_only")
    monkeypatch.setenv("ROBINHOOD_BLOCKSCOUT_DAILY_CREDIT_BUDGET", "100")
    monkeypatch.setenv("ROBINHOOD_BLOCKSCOUT_CREDIT_RESERVE", "90")
    monkeypatch.setenv("ROBINHOOD_BLOCKSCOUT_ASSUMED_CREDITS_PER_REQUEST", "20")
    plane = Plane(Store(), {"status": "0", "message": "No transactions found", "result": []}, credits=80)

    token = quota._ENTITY_PRIORITY.set("noncritical")
    try:
        skipped = asyncio.run(quota._entity_anchor_fetch_quota(plane, ACTOR_A))
    finally:
        quota._ENTITY_PRIORITY.reset(token)
    assert skipped is None
    assert plane.rpc.client.calls == []
    assert quota._quota_stats(plane)["noncritical_reserve_skips"] == 1

    token = quota._ENTITY_PRIORITY.set("critical")
    try:
        resolved = asyncio.run(quota._entity_anchor_fetch_quota(plane, ACTOR_A))
    finally:
        quota._ENTITY_PRIORITY.reset(token)
    assert resolved == ACTOR_A
    assert len(plane.rpc.client.calls) == 1


class NoCallFlowPlane:
    async def _entity_anchor(self, actor: str) -> str | None:
        raise AssertionError(f"entity provider should not be called for {actor}")


def test_no_valid_buy_trigger_is_locally_pre_gated_without_entity_call() -> None:
    metrics = asyncio.run(quota._v5_flow_metrics_quota(NoCallFlowPlane(), [], deployer=DEPLOYER))
    assert metrics["entity_resolution_complete"] is True
    assert metrics["trigger_actor"] == ""
    assert metrics["entity_resolution_pre_gate"] == "no_valid_buy_trigger"


class ProgressiveFlowPlane:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []
        self.mapping = {
            ACTOR_A: FUNDER_A,
            ACTOR_B: FUNDER_B,
            ACTOR_C: FUNDER_C,
            DEPLOYER: DEPLOYER,
        }

    async def _entity_anchor(self, actor: str) -> str | None:
        self.calls.append((actor, quota._ENTITY_PRIORITY.get()))
        return self.mapping.get(actor)


def _buy(actor: str, *, age: float, quote: int, price: float) -> dict[str, object]:
    return {
        "observed_ts": time.time() - age,
        "side": "buy",
        "actor": actor,
        "quote_amount_wei": quote,
        "price_eth": price,
    }


def test_progressive_resolution_orders_trigger_and_deployer_before_nontriggers() -> None:
    plane = ProgressiveFlowPlane()
    swaps = [
        _buy(ACTOR_A, age=3.0, quote=20, price=1.00),
        _buy(ACTOR_B, age=2.0, quote=20, price=1.01),
        _buy(ACTOR_C, age=1.0, quote=20, price=1.02),
    ]
    metrics = asyncio.run(quota._v5_flow_metrics_quota(plane, swaps, deployer=DEPLOYER))
    assert plane.calls[:2] == [(ACTOR_C, "critical"), (DEPLOYER, "critical")]
    assert plane.calls[2:] == [(ACTOR_A, "noncritical"), (ACTOR_B, "noncritical")]
    assert metrics["entity_resolution_complete"] is True
    assert metrics["entity_resolution_partial"] is False
    assert metrics["independent_entities_60s"] == 3
    assert metrics["entity_resolution_order"] == "trigger_then_deployer_then_nontrigger_progressive"


def test_status_contract_proves_scope_and_thresholds_are_unchanged() -> None:
    class StatusPlane:
        def status(self):
            return {"entity_resolution": {}}

    StatusPlane.status = quota._status_with_quota(StatusPlane.status)
    payload = StatusPlane().status()
    architecture = payload["entity_quota_architecture"]
    assert architecture["universe_scope_reduced"] is False
    assert architecture["token_scope_reduced"] is False
    assert architecture["venue_scope_reduced"] is False
    assert architecture["strategy_thresholds_changed"] is False
    assert architecture["trigger_resolution_required"] is True
    assert architecture["deployer_resolution_required_when_present"] is True
    assert architecture["unresolved_nontrigger_flow_counts_toward_signal"] is False
    assert architecture["paper_only"] is True
    assert architecture["live_money_authority"] is False


def test_production_composition_keeps_continuation_authority_with_quota_substrate() -> None:
    from solana_roi.robinhood_chain_paper import RobinhoodChainPaperPlane

    assert bool(getattr(RobinhoodChainPaperPlane, "_roi_entity_quota_architecture_installed", False))
    assert continuation._ORIGINAL_RH_FLOW is quota._v5_flow_metrics_quota
    assert RobinhoodChainPaperPlane._v5_flow_metrics is continuation._rh_flow_without_sniper_cap
    assert entity_repair._entity_anchor_fetch is quota._entity_anchor_fetch_quota
