from __future__ import annotations

import asyncio
import gc
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from solana_roi import direct_solana as direct_solana_module
from solana_roi import launch_context_rpc_repair as repair
from solana_roi import launch_coverage_bridge as bridge


MINT = "9xQeWvG816bUx9EPfEZ1dmh7x9Yi4WBQPvWs7xQbX6L"
LAUNCH = "launch-signature"


class Journal:
    def __init__(self) -> None:
        self.finishes: list[tuple[str, str | None, bool]] = []
        self.hydrations: list[dict] = []

    def finish(self, signature: str, *, error: str | None = None, retry: bool = False) -> None:
        self.finishes.append((signature, error, retry))

    def record_hydration(self, **kwargs) -> None:
        self.hydrations.append(kwargs)


def _row(signature: str, at: datetime) -> dict:
    return {"signature": signature, "blockTime": int(at.timestamp()), "err": None}


def _assert_cancel_accounted(plane, *, child_count: int) -> None:
    assert getattr(plane, "_roi_launch_context_rpc_parent_cancellations", 0) == 1
    assert getattr(plane, "_roi_launch_context_rpc_active_child_tasks", 0) == 0
    assert getattr(plane, "_roi_launch_context_rpc_orphan_tasks_detected", 0) == 0
    assert getattr(plane, "_roi_launch_context_rpc_cleanup_failures", 0) == 0
    assert getattr(plane, "_roi_launch_context_rpc_cancellation_accounting_failures", 0) == 0
    assert getattr(plane, "_roi_launch_context_rpc_cancelled_launches_requeued", 0) == 1
    if child_count:
        assert getattr(plane, "_roi_launch_context_rpc_child_tasks_created", 0) == child_count
        assert getattr(plane, "_roi_launch_context_rpc_child_tasks_cancelled", 0) == child_count
        assert getattr(plane, "_roi_launch_context_rpc_child_tasks_drained", 0) == child_count
    assert plane.journal.finishes == [
        (
            LAUNCH,
            "CancelledError: launch context acquisition interrupted; retry required",
            True,
        )
    ]


def test_parent_cancellation_during_launch_window_requeues_without_children():
    class Rpc:
        async def get_signatures_for_address(self, *args, **kwargs):
            raise AssertionError("launch-window cancellation must occur before signature lookup")

    plane = SimpleNamespace(rpc=Rpc(), journal=Journal())

    async def scenario() -> None:
        task = asyncio.create_task(
            repair._hydrate_mint_launch_context_with_retry(
                plane,
                mint=MINT,
                source="PUMP_FUN",
                launch_signature=LAUNCH,
                created_at=datetime.now(timezone.utc),
            )
        )
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())
    _assert_cancel_accounted(plane, child_count=0)


def test_parent_cancellation_drains_primary_rpc_and_semaphore_waiter(monkeypatch):
    created_at = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(seconds=20)
    entered_rpc = asyncio.Event()
    blocker = asyncio.Event()

    class Rpc:
        async def get_signatures_for_address(self, mint, *, limit, hedge):
            return (
                [
                    _row("context-1", created_at + timedelta(seconds=2)),
                    _row("context-2", created_at + timedelta(seconds=3)),
                ],
                "primary",
                1.0,
            )

        async def get_transaction(self, signature, *, hedge):
            entered_rpc.set()
            await blocker.wait()
            raise AssertionError("blocked RPC should be cancelled")

    plane = SimpleNamespace(rpc=Rpc(), journal=Journal())
    monkeypatch.setattr(bridge, "LAUNCH_CONTEXT_CONCURRENCY", 1)

    async def scenario() -> list[dict]:
        loop = asyncio.get_running_loop()
        errors: list[dict] = []
        loop.set_exception_handler(lambda _loop, context: errors.append(context))
        task = asyncio.create_task(
            repair._hydrate_mint_launch_context_with_retry(
                plane,
                mint=MINT,
                source="PUMP_FUN",
                launch_signature=LAUNCH,
                created_at=created_at,
            )
        )
        await entered_rpc.wait()
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await asyncio.sleep(0)
        gc.collect()
        await asyncio.sleep(0)
        return errors

    errors = asyncio.run(scenario())
    _assert_cancel_accounted(plane, child_count=2)
    assert not [
        context
        for context in errors
        if "never retrieved" in str(context.get("message") or "").lower()
        or "_GatheringFuture" in repr(context)
    ]


def test_parent_cancellation_drains_secondary_provider_fallback():
    created_at = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(seconds=20)
    entered_secondary = asyncio.Event()
    blocker = asyncio.Event()
    primary = SimpleNamespace(name="primary")
    secondary = SimpleNamespace(name="secondary")

    class Rpc:
        async def get_signatures_for_address(self, mint, *, limit, hedge):
            return ([_row("context-1", created_at + timedelta(seconds=2))], "primary", 1.0)

        async def get_transaction(self, signature, *, hedge):
            return None, "primary", 1.0

        def _ordered(self, method):
            assert method == "getTransaction"
            return [primary, secondary]

        async def _call_endpoint(self, endpoint, method, params):
            assert endpoint is secondary
            entered_secondary.set()
            await blocker.wait()
            raise AssertionError("secondary fallback should be cancelled")

    plane = SimpleNamespace(rpc=Rpc(), journal=Journal())

    async def scenario() -> None:
        task = asyncio.create_task(
            repair._hydrate_mint_launch_context_with_retry(
                plane,
                mint=MINT,
                source="PUMP_FUN",
                launch_signature=LAUNCH,
                created_at=created_at,
            )
        )
        await entered_secondary.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())
    _assert_cancel_accounted(plane, child_count=1)


def test_parent_cancellation_during_rpc_retry_delay_is_drained():
    created_at = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(seconds=20)
    first_failure = asyncio.Event()

    class Rpc:
        async def get_signatures_for_address(self, mint, *, limit, hedge):
            return ([_row("context-1", created_at + timedelta(seconds=2))], "primary", 1.0)

        async def get_transaction(self, signature, *, hedge):
            first_failure.set()
            raise RuntimeError("transient rpc failure")

        def _ordered(self, method):
            return []

    plane = SimpleNamespace(rpc=Rpc(), journal=Journal())

    async def scenario() -> None:
        task = asyncio.create_task(
            repair._hydrate_mint_launch_context_with_retry(
                plane,
                mint=MINT,
                source="PUMP_FUN",
                launch_signature=LAUNCH,
                created_at=created_at,
            )
        )
        await first_failure.wait()
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())
    _assert_cancel_accounted(plane, child_count=1)
    assert getattr(plane, "_roi_launch_context_rpc_round_errors", 0) >= 1


def test_internal_deadline_drains_children_but_preserves_incomplete_return(monkeypatch):
    created_at = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(seconds=20)
    blocker = asyncio.Event()

    class Rpc:
        async def get_signatures_for_address(self, mint, *, limit, hedge):
            return ([_row("context-1", created_at + timedelta(seconds=2))], "primary", 1.0)

        async def get_transaction(self, signature, *, hedge):
            await blocker.wait()
            raise AssertionError("deadline should cancel blocked RPC")

    plane = SimpleNamespace(rpc=Rpc(), journal=Journal())
    monkeypatch.setattr(bridge, "LAUNCH_CONTEXT_DEADLINE_SECONDS", 0.001)

    result = asyncio.run(
        repair._hydrate_mint_launch_context_with_retry(
            plane,
            mint=MINT,
            source="PUMP_FUN",
            launch_signature=LAUNCH,
            created_at=created_at,
        )
    )

    assert result == (0, False, 1)
    assert getattr(plane, "_roi_launch_bridge_context_timeouts", 0) == 1
    assert getattr(plane, "_roi_launch_bridge_context_incomplete", 0) == 1
    assert getattr(plane, "_roi_launch_context_rpc_child_tasks_created", 0) == 1
    assert getattr(plane, "_roi_launch_context_rpc_child_tasks_cancelled", 0) == 1
    assert getattr(plane, "_roi_launch_context_rpc_child_tasks_drained", 0) == 1
    assert getattr(plane, "_roi_launch_context_rpc_active_child_tasks", 0) == 0
    assert getattr(plane, "_roi_launch_context_rpc_orphan_tasks_detected", 0) == 0
    assert plane.journal.finishes == []


def test_successful_hydration_keeps_window_and_persistence_semantics(monkeypatch):
    created_at = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(seconds=20)

    class Rpc:
        async def get_signatures_for_address(self, mint, *, limit, hedge):
            assert limit == bridge.LAUNCH_CONTEXT_SIGNATURE_LIMIT
            assert hedge is True
            return ([_row("context-1", created_at + timedelta(seconds=2))], "primary", 1.0)

        async def get_transaction(self, signature, *, hedge):
            return {"transaction": True}, "primary", 2.0

    class Plane:
        def __init__(self) -> None:
            self.rpc = Rpc()
            self.journal = Journal()
            self.persisted = []

        def _persist_context_swap(self, swap) -> None:
            self.persisted.append(swap)

    plane = Plane()
    monkeypatch.setattr(
        direct_solana_module,
        "normalize_standard_transaction",
        lambda *args, **kwargs: SimpleNamespace(token_mint=MINT),
    )

    result = asyncio.run(
        repair._hydrate_mint_launch_context_with_retry(
            plane,
            mint=MINT,
            source="PUMP_FUN",
            launch_signature=LAUNCH,
            created_at=created_at,
        )
    )

    assert result == (1, True, 1)
    assert len(plane.persisted) == 1
    assert len(plane.journal.hydrations) == 1
    assert plane.journal.hydrations[0]["candidate_context_prefilled"] is True
    assert getattr(plane, "_roi_launch_context_rpc_active_child_tasks", 0) == 0
    assert getattr(plane, "_roi_launch_context_rpc_orphan_tasks_detected", 0) == 0
    assert getattr(plane, "_roi_launch_context_rpc_parent_cancellations", 0) == 0


def test_status_exposes_cancellation_lifecycle_truth():
    plane = SimpleNamespace(
        _roi_launch_context_rpc_parent_cancellations=2,
        _roi_launch_context_rpc_child_tasks_created=5,
        _roi_launch_context_rpc_child_tasks_cancelled=3,
        _roi_launch_context_rpc_child_tasks_drained=5,
        _roi_launch_context_rpc_active_child_tasks=0,
        _roi_launch_context_rpc_orphan_tasks_detected=0,
        _roi_launch_context_rpc_cleanup_failures=0,
        _roi_launch_context_rpc_cancelled_launches_requeued=2,
        _roi_launch_context_rpc_cancellation_accounting_failures=0,
    )
    wrapped = repair._status_with_context_rpc(lambda _self: {"launch_coverage_bridge": {}})
    payload = wrapped(plane)["launch_coverage_bridge"]

    assert payload["context_parent_cancellation_fails_closed"] is True
    assert payload["context_cancelled_launch_requeued"] is True
    assert payload["context_parent_cancellations"] == 2
    assert payload["context_child_tasks_created"] == 5
    assert payload["context_child_tasks_cancelled"] == 3
    assert payload["context_child_tasks_drained"] == 5
    assert payload["context_active_child_tasks"] == 0
    assert payload["context_orphan_tasks_detected"] == 0
    assert payload["context_cleanup_failures"] == 0
    assert payload["context_cancelled_launches_requeued"] == 2
    assert payload["context_cancellation_accounting_failures"] == 0
