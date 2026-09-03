from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

from solana_roi import certification_hotpath_repair as hotpath
from solana_roi import coverage_completeness_repair as coverage
from solana_roi import launch_coverage_bridge as bridge
from solana_roi import launch_ws_frontier_timing_repair as frontier
from solana_roi import production_capacity_repair as capacity
from solana_roi.launch_funding import LaunchFundingPolicy


class ConcurrentFundingRpc:
    def __init__(self, *, buy_at: datetime):
        self.buy_at = buy_at
        self.active = 0
        self.max_active = 0

    async def get_signatures_for_address(self, _wallet, *, before=None, limit=1000, hedge=False):
        assert before is None
        assert limit == 1000
        assert hedge is True
        return (
            [
                {
                    "signature": f"tx-{index}",
                    "slot": 990 - index,
                    "blockTime": int(self.buy_at.timestamp()) - 1 - index,
                    "err": None,
                }
                for index in range(8)
            ],
            "publicnode",
            1.0,
        )

    async def get_transaction(self, signature, *, hedge=False):
        assert hedge is True
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            await asyncio.sleep(0.01)
            index = int(str(signature).split("-")[-1])
            funder = "newest-funder" if index == 2 else ("older-funder" if index == 5 else None)
            instructions = []
            if funder is not None:
                instructions.append(
                    {
                        "program": "system",
                        "parsed": {
                            "type": "transfer",
                            "info": {
                                "source": funder,
                                "destination": "buyer-a",
                                "lamports": 250_000_000,
                            },
                        },
                    }
                )
            return (
                {
                    "blockTime": int(self.buy_at.timestamp()) - 1 - index,
                    "transaction": {"message": {"instructions": instructions}},
                    "meta": {"innerInstructions": []},
                },
                "publicnode",
                1.0,
            )
        finally:
            self.active -= 1


def test_funding_reads_overlap_but_newest_qualifying_source_still_wins():
    buy_at = datetime.now(timezone.utc)
    rpc = ConcurrentFundingRpc(buy_at=buy_at)
    collector = SimpleNamespace(rpc=rpc, policy=LaunchFundingPolicy())

    source, complete, reason = asyncio.run(
        hotpath._funding_source_result_bounded_parallel(
            collector,
            "buyer-a",
            buy_at,
            1000,
        )
    )

    assert complete is True
    assert reason == "latest_qualifying_source_found"
    assert source is not None
    assert source.funder == "newest-funder"
    assert rpc.max_active > 1
    assert rpc.max_active <= hotpath.FUNDING_TRANSACTION_CONCURRENCY


class FailingOlderFundingRpc(ConcurrentFundingRpc):
    async def get_transaction(self, signature, *, hedge=False):
        if signature == "tx-5":
            await asyncio.sleep(0.005)
            raise TimeoutError("older speculative row failed")
        return await super().get_transaction(signature, hedge=hedge)


def test_older_speculative_failure_cannot_override_newer_valid_source():
    buy_at = datetime.now(timezone.utc)
    rpc = FailingOlderFundingRpc(buy_at=buy_at)
    collector = SimpleNamespace(rpc=rpc, policy=LaunchFundingPolicy())

    source, complete, reason = asyncio.run(
        hotpath._funding_source_result_bounded_parallel(
            collector,
            "buyer-a",
            buy_at,
            1000,
        )
    )

    assert complete is True
    assert reason == "latest_qualifying_source_found"
    assert source is not None
    assert source.funder == "newest-funder"


def test_earlier_rpc_failure_remains_fail_closed_even_if_later_row_would_qualify():
    class FailingNewestFundingRpc(ConcurrentFundingRpc):
        async def get_transaction(self, signature, *, hedge=False):
            if signature == "tx-0":
                await asyncio.sleep(0.005)
                raise TimeoutError("newest row unavailable")
            return await super().get_transaction(signature, hedge=hedge)

    buy_at = datetime.now(timezone.utc)
    rpc = FailingNewestFundingRpc(buy_at=buy_at)
    collector = SimpleNamespace(rpc=rpc, policy=LaunchFundingPolicy())

    source, complete, reason = asyncio.run(
        hotpath._funding_source_result_bounded_parallel(
            collector,
            "buyer-a",
            buy_at,
            1000,
        )
    )

    assert source is None
    assert complete is False
    assert reason == "transaction_rpc:TimeoutError"


def test_only_marked_urgent_gap_pool_uses_original_bounded_hedge(monkeypatch):
    calls: list[tuple[str, bool]] = []

    async def original(_self, _method, _params, *, hedge=False):
        calls.append(("urgent", bool(hedge)))
        return 123, "secondary", 1.0

    async def routine(_self, _method, _params, *, hedge=False):
        calls.append(("routine", bool(hedge)))
        return 456, "primary", 2.0

    monkeypatch.setattr(capacity, "_ORIGINAL_RPC_CALL_WITH_META", original)
    monkeypatch.setattr(capacity, "_capacity_call_with_meta", routine)

    urgent_pool = SimpleNamespace(_roi_urgent_gap_recovery_pool=True)
    ordinary_pool = SimpleNamespace()

    urgent = asyncio.run(
        hotpath._capacity_call_with_urgent_recovery(
            urgent_pool,
            "getSignaturesForAddress",
            ["target", {}],
            hedge=True,
        )
    )
    ordinary = asyncio.run(
        hotpath._capacity_call_with_urgent_recovery(
            ordinary_pool,
            "getSignaturesForAddress",
            ["target", {}],
            hedge=True,
        )
    )

    assert urgent[1] == "secondary"
    assert ordinary[1] == "primary"
    assert calls == [("urgent", True), ("routine", True)]


def test_final_v7_hydration_hands_complete_window_to_launch_and_funding(monkeypatch):
    created_at = datetime(2026, 9, 3, 14, 0, tzinfo=timezone.utc)
    observed_at = datetime(2026, 9, 3, 14, 0, 1, tzinfo=timezone.utc)
    launch = SimpleNamespace()
    funding_collector = SimpleNamespace()
    raw = SimpleNamespace(launch=launch, funding=funding_collector)
    plane = SimpleNamespace(store=object())

    async def pre_repair(
        _self,
        *,
        mint,
        source,
        launch_signature,
        created_at,
    ):
        assert mint == "mint-a"
        assert source == "PUMP_FUN"
        assert launch_signature == "launch-sig"
        return 3, True, 3

    def seed(
        _self,
        *,
        mint,
        created_at,
        observed_at,
        complete,
    ):
        coverage._launch_contexts(launch)[mint] = {
            "created_at": created_at,
            "observed_at": observed_at,
            "complete": complete,
        }
        coverage._funding_contexts(funding_collector)[mint] = bool(complete)
        return True

    monkeypatch.setattr(hotpath, "_PRE_REPAIR_HYDRATE_MINT_LAUNCH_CONTEXT", pre_repair)
    monkeypatch.setattr(bridge, "_raw_collectors", lambda _self: raw)
    monkeypatch.setattr(coverage, "_queue_trigger_received_at", lambda _self, _signature: observed_at)
    monkeypatch.setattr(coverage, "_seed_runtime_collectors", seed)
    monkeypatch.setattr(frontier, "_frontier_row", lambda _store, _signature: {"launch_slot": 777})

    persisted, complete, candidate_count = asyncio.run(
        hotpath._hydrate_mint_launch_context_with_final_attestation(
            plane,
            mint="mint-a",
            source="PUMP_FUN",
            launch_signature="launch-sig",
            created_at=created_at,
        )
    )

    assert (persisted, complete, candidate_count) == (3, True, 3)
    launch_context = coverage._launch_contexts(launch)["mint-a"]
    assert launch_context["complete"] is True
    assert launch_context["launch_signature"] == "launch-sig"
    assert launch_context["launch_slot"] == 777
    assert coverage._funding_contexts(funding_collector)["mint-a"] is True
    assert getattr(plane, "_roi_launch_coverage_bridge_coverage_context_attested") == 1
    assert getattr(plane, "_roi_launch_coverage_bridge_coverage_context_complete") == 1


def test_signature_window_truncation_stays_fail_closed(monkeypatch):
    created_at = datetime(2026, 9, 3, 14, 0, tzinfo=timezone.utc)
    observed_at = datetime(2026, 9, 3, 14, 0, 1, tzinfo=timezone.utc)
    launch = SimpleNamespace()
    funding_collector = SimpleNamespace()
    raw = SimpleNamespace(launch=launch, funding=funding_collector)
    plane = SimpleNamespace(store=object())

    async def pre_repair(_self, **_kwargs):
        return 95, True, bridge.LAUNCH_CONTEXT_SIGNATURE_LIMIT - 1

    def seed(_self, *, mint, created_at, observed_at, complete):
        coverage._launch_contexts(launch)[mint] = {
            "created_at": created_at,
            "observed_at": observed_at,
            "complete": complete,
        }
        coverage._funding_contexts(funding_collector)[mint] = bool(complete)
        return True

    monkeypatch.setattr(hotpath, "_PRE_REPAIR_HYDRATE_MINT_LAUNCH_CONTEXT", pre_repair)
    monkeypatch.setattr(bridge, "_raw_collectors", lambda _self: raw)
    monkeypatch.setattr(coverage, "_queue_trigger_received_at", lambda _self, _signature: observed_at)
    monkeypatch.setattr(coverage, "_seed_runtime_collectors", seed)
    monkeypatch.setattr(frontier, "_frontier_row", lambda _store, _signature: {"launch_slot": 888})

    _persisted, complete, _candidate_count = asyncio.run(
        hotpath._hydrate_mint_launch_context_with_final_attestation(
            plane,
            mint="mint-b",
            source="PUMP_FUN",
            launch_signature="launch-sig-b",
            created_at=created_at,
        )
    )

    assert complete is False
    assert coverage._launch_contexts(launch)["mint-b"]["complete"] is False
    assert coverage._funding_contexts(funding_collector)["mint-b"] is False
