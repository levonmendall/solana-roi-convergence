from __future__ import annotations

from collections import deque

import pytest

from solana_roi.robinhood_chain_metrics import RobinhoodMetricsMixin


class _MetricsHarness(RobinhoodMetricsMixin):
    pass


def _buy(actor: str, observed_ts: float, price: float, *, block: int = 1, log: int = 0) -> dict[str, object]:
    return {
        "side": "buy",
        "actor": actor,
        "quote_amount_wei": 100,
        "price_eth": price,
        "observed_ts": observed_ts,
        "block_number": block,
        "log_index": log,
        "tx_hash": f"0x{block:04x}{log:04x}",
    }


def test_future_dated_flow_cannot_contaminate_earlier_decision() -> None:
    now = 1_000.0
    swaps = deque(
        [
            _buy("0x" + "1" * 40, now - 20, 1.00, block=1),
            _buy("0x" + "2" * 40, now - 10, 1.02, block=2),
            _buy("0x" + "3" * 40, now - 1, 1.04, block=3),
            _buy("0x" + "4" * 40, now + 1, 1.06, block=4),
        ]
    )

    metrics = _MetricsHarness()._recent_metrics(swaps, now_ts=now)

    assert metrics["buy_count_60s"] == 3
    assert metrics["independent_buyers_60s"] == 3
    assert metrics["trigger_actor"] == "0x" + "3" * 40


def test_cutoff_boundaries_are_event_time_exact() -> None:
    now = 1_000.0
    swaps = deque(
        [
            _buy("0x" + "1" * 40, now - 120, 0.90, block=1),
            _buy("0x" + "2" * 40, now - 60, 1.00, block=2),
            _buy("0x" + "3" * 40, now, 1.02, block=3),
            _buy("0x" + "4" * 40, now - 120.001, 0.80, block=4),
            _buy("0x" + "5" * 40, now + 0.001, 1.03, block=5),
        ]
    )

    metrics = _MetricsHarness()._recent_metrics(swaps, now_ts=now)

    assert metrics["buy_count_60s"] == 2
    assert metrics["buy_count_acceleration"] == 2.0
    assert metrics["trigger_actor"] == "0x" + "3" * 40


def test_out_of_order_and_late_arrival_use_event_time_not_ingestion_order() -> None:
    now = 1_000.0
    chronological = [
        _buy("0x" + "1" * 40, now - 50, 1.00, block=1),
        _buy("0x" + "2" * 40, now - 30, 1.02, block=2),
        _buy("0x" + "3" * 40, now - 10, 1.05, block=3),
    ]
    late_arrival_order = deque([chronological[2], chronological[0], chronological[1]])

    ordered = _MetricsHarness()._recent_metrics(deque(chronological), now_ts=now)
    shuffled = _MetricsHarness()._recent_metrics(late_arrival_order, now_ts=now)

    assert shuffled == ordered
    assert shuffled["price_change_60s"] == pytest.approx(0.05)
    assert shuffled["trigger_actor"] == "0x" + "3" * 40


def test_replay_of_same_event_set_is_deterministic() -> None:
    now = 1_000.0
    swaps = deque(
        [
            _buy("0x" + "1" * 40, now - 55, 1.00, block=1, log=2),
            _buy("0x" + "2" * 40, now - 55, 1.01, block=1, log=3),
            _buy("0x" + "3" * 40, now - 5, 1.04, block=2, log=1),
        ]
    )

    first = _MetricsHarness()._recent_metrics(swaps, now_ts=now)
    replay = _MetricsHarness()._recent_metrics(deque(reversed(swaps)), now_ts=now)

    assert replay == first
