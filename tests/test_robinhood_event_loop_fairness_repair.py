from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace

from solana_roi import robinhood_chain_runtime as robinhood_runtime
from solana_roi.robinhood_chain_ingest import RobinhoodIngestMixin
from solana_roi.robinhood_chain_paper import RobinhoodChainPaperPlane
from solana_roi.robinhood_catchup_capacity_repair import (
    DEFAULT_CATCHUP_MAX_BLOCKS,
    _catchup_poll_seconds,
)
from solana_roi.robinhood_event_loop_fairness_repair import (
    REPAIR_VERSION,
    _status_with_event_loop_fairness,
    _wrap_ingest_method,
)


def test_production_composition_installs_cooperative_ingest_wrappers() -> None:
    assert RobinhoodChainPaperPlane is not None
    for name in ("_process_factory_log", "_process_v3_swap", "_process_v2_curve_log"):
        method = getattr(RobinhoodIngestMixin, name)
        assert bool(getattr(method, "_roi_event_loop_fairness", False))


def test_dense_catchup_processing_yields_to_health_like_coroutine() -> None:
    plane = SimpleNamespace(_roi_catchup_mode=True, _caught_up=False)
    heartbeat_ticks = 0
    done = asyncio.Event()

    async def synchronous_historical_handler(_self, value: int) -> int:
        # Models the synchronous SQLite transaction in historical swap handlers.
        time.sleep(0.003)
        return value

    wrapped = _wrap_ingest_method(synchronous_historical_handler, phase="dense_test")

    async def dense_worker() -> None:
        for index in range(40):
            assert await wrapped(plane, index) == index
        done.set()

    async def health_like_worker() -> None:
        nonlocal heartbeat_ticks
        while not done.is_set():
            heartbeat_ticks += 1
            await asyncio.sleep(0)

    async def scenario() -> None:
        await asyncio.gather(dense_worker(), health_like_worker())

    asyncio.run(scenario())

    # Without the cooperative checkpoint the dense worker can consume the event
    # loop until every synchronous handler finishes. The repair must schedule the
    # health-like coroutine throughout the batch, not only after it.
    assert heartbeat_ticks >= 20
    assert plane._roi_catchup_cooperative_yields_total == 40
    assert plane._roi_catchup_last_checkpoint_phase == "dense_test"


def test_fairness_repair_does_not_throttle_or_relax_catchup_policy() -> None:
    plane = SimpleNamespace(
        _roi_catchup_mode=True,
        _caught_up=False,
        _cursor=1000,
        _latest_block=2569,
    )
    wrapped = _status_with_event_loop_fairness(
        lambda _self: {
            "catchup_capacity": {
                "catchup_batch_limit_blocks": DEFAULT_CATCHUP_MAX_BLOCKS,
                "catchup_poll_seconds": _catchup_poll_seconds(),
                "live_lag_blocks": robinhood_runtime.LIVE_LAG_BLOCKS,
            }
        }
    )
    payload = wrapped(plane)
    fairness = payload["event_loop_fairness"]

    assert fairness["repair_version"] == REPAIR_VERSION
    assert fairness["cooperative_yield_after_each_processed_catchup_log"] is True
    assert fairness["catchup_batch_limit_changed"] is False
    assert fairness["catchup_poll_cadence_changed"] is False
    assert fairness["block_ranges_skipped"] is False
    assert fairness["cursor_advance_semantics_changed"] is False
    assert fairness["paper_decision_gate_changed"] is False
    assert fairness["paper_only"] is True
    assert fairness["live_money_authority"] is False
    assert payload["catchup_capacity"]["catchup_batch_limit_blocks"] == 800
    assert payload["catchup_capacity"]["live_lag_blocks"] == 2


def test_checkpoint_is_inert_once_robinhood_is_caught_up() -> None:
    plane = SimpleNamespace(_roi_catchup_mode=False, _caught_up=True)

    async def handler(_self) -> str:
        return "ok"

    wrapped = _wrap_ingest_method(handler, phase="live")
    assert asyncio.run(wrapped(plane)) == "ok"
    assert not hasattr(plane, "_roi_catchup_cooperative_yields_total")
