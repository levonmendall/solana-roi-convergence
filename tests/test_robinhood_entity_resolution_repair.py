from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace

from solana_roi.robinhood_entity_resolution_repair import (
    _entity_anchor,
    _stats,
    _v5_flow_metrics,
)


ACTOR_A = "0x1111111111111111111111111111111111111111"
ACTOR_B = "0x2222222222222222222222222222222222222222"
ACTOR_C = "0x3333333333333333333333333333333333333333"
ENTITY_A = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
ENTITY_C = "0xcccccccccccccccccccccccccccccccccccccccc"
DEPLOYER = "0xdddddddddddddddddddddddddddddddddddddddd"


class ProviderRateLimitError(RuntimeError):
    def __init__(self) -> None:
        super().__init__("rate limited")
        self.response = SimpleNamespace(status_code=429)


class RateLimitedResponse:
    def raise_for_status(self) -> None:
        raise ProviderRateLimitError()

    def json(self) -> dict[str, object]:
        return {}


class RateLimitedClient:
    def __init__(self) -> None:
        self.calls = 0

    async def get(self, *args: object, **kwargs: object) -> RateLimitedResponse:
        self.calls += 1
        return RateLimitedResponse()


class EntityDummy:
    def __init__(self) -> None:
        self._entity_cache: dict[str, tuple[str, float]] = {}
        self._entity_resolution_failures = 0
        self.rpc = SimpleNamespace(client=RateLimitedClient())


def test_failed_entity_lookup_is_negatively_cached_and_does_not_count_raw_actor() -> None:
    async def scenario() -> None:
        plane = EntityDummy()
        first = await _entity_anchor(plane, ACTOR_A)
        second = await _entity_anchor(plane, ACTOR_A)
        assert first is None
        assert second is None
        assert plane.rpc.client.calls == 1
        assert plane._entity_resolution_failures == 1
        stats = _stats(plane)
        assert stats["negative_cache_hits"] == 1
        assert stats["rate_limit_failures"] == 1
        assert ACTOR_A not in plane._entity_cache

    asyncio.run(scenario())


class FlowDummy:
    def __init__(self, mapping: dict[str, str | None]) -> None:
        self.mapping = mapping

    async def _entity_anchor(self, actor: str) -> str | None:
        return self.mapping.get(actor)


def _buy(actor: str, *, age: float, quote: int = 10, price: float = 1.0) -> dict[str, object]:
    return {
        "observed_ts": time.time() - age,
        "side": "buy",
        "actor": actor,
        "quote_amount_wei": quote,
        "price_eth": price,
    }


def test_unresolved_nontrigger_actor_is_excluded_without_zeroing_resolved_context() -> None:
    async def scenario() -> None:
        plane = FlowDummy(
            {
                ACTOR_A: ENTITY_A,
                ACTOR_B: None,
                ACTOR_C: ENTITY_C,
                DEPLOYER: DEPLOYER,
            }
        )
        swaps = [
            _buy(ACTOR_A, age=3.0, quote=10, price=1.00),
            _buy(ACTOR_B, age=2.0, quote=1000, price=1.01),
            _buy(ACTOR_C, age=1.0, quote=10, price=1.02),
        ]
        metrics = await _v5_flow_metrics(plane, swaps, deployer=DEPLOYER)
        assert metrics["entity_resolution_complete"] is True
        assert metrics["entity_resolution_partial"] is True
        assert metrics["unresolved_buy_actor_count"] == 1
        assert metrics["independent_entities_60s"] == 2
        assert metrics["buy_count_60s"] == 2
        assert metrics["raw_buy_count_60s"] == 3
        assert metrics["buy_quote_wei"] == 20
        assert metrics["unresolved_addresses_count_as_independent"] is False
        assert metrics["unresolved_buy_flow_counts_toward_signal"] is False

    asyncio.run(scenario())


def test_unresolved_trigger_still_fails_closed() -> None:
    async def scenario() -> None:
        plane = FlowDummy(
            {
                ACTOR_A: ENTITY_A,
                ACTOR_B: None,
                DEPLOYER: DEPLOYER,
            }
        )
        swaps = [
            _buy(ACTOR_A, age=2.0, quote=10, price=1.00),
            _buy(ACTOR_B, age=1.0, quote=10, price=1.01),
        ]
        metrics = await _v5_flow_metrics(plane, swaps, deployer=DEPLOYER)
        assert metrics["entity_resolution_complete"] is False
        assert metrics["entity_resolution_blocker"] == "trigger_entity_unresolved"
        assert metrics["independent_entities_60s"] == 0

    asyncio.run(scenario())


def test_unresolved_deployer_still_fails_closed() -> None:
    async def scenario() -> None:
        plane = FlowDummy(
            {
                ACTOR_A: ENTITY_A,
                DEPLOYER: None,
            }
        )
        swaps = [_buy(ACTOR_A, age=1.0, quote=10, price=1.00)]
        metrics = await _v5_flow_metrics(plane, swaps, deployer=DEPLOYER)
        assert metrics["entity_resolution_complete"] is False
        assert metrics["entity_resolution_blocker"] == "deployer_entity_unresolved"
        assert metrics["independent_entities_60s"] == 0

    asyncio.run(scenario())
