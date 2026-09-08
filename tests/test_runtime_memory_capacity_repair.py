from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from solana_roi import runtime_memory_capacity_repair as repair


class _Journal:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.rows = rows
        self.hydrated: list[str] = []

    def recent_source_signatures(self, source, *, start, end, exclude_signature, limit):
        del source, start, end, exclude_signature
        return list(self.rows[:limit])

    def record_hydration(self, **payload):
        self.hydrated.append(str(payload["signature"]))


class _Registry:
    def get(self, wallet: str):
        return object() if str(wallet).startswith("candidate-") else None


class _Service:
    registry = _Registry()


class _Plane:
    def __init__(self, rows: list[dict[str, object]], *, deadline: float = 2.0) -> None:
        self.journal = _Journal(rows)
        self.service = _Service()
        self.candidate_context_max_signatures = max(50, len(rows))
        self.candidate_context_deadline_seconds = deadline
        self.persisted: list[str] = []
        self.fetch_active = 0
        self.fetch_peak = 0
        self.fetch_delay = 0.0

    async def _pair_created_at(self, mint: str):
        del mint
        return datetime(2026, 1, 1, tzinfo=timezone.utc)

    async def _get_transaction_ready(self, signature: str, *, hedge: bool, attempts: int):
        del hedge, attempts
        self.fetch_active += 1
        self.fetch_peak = max(self.fetch_peak, self.fetch_active)
        try:
            if self.fetch_delay:
                await asyncio.sleep(self.fetch_delay)
            return {"signature": signature}, "test-rpc", 1.0
        finally:
            self.fetch_active -= 1

    def _persist_context_swap(self, swap) -> None:
        self.persisted.append(str(swap.signature))


def _candidate(signature: str = "candidate", *, wallet: str | None = None):
    return SimpleNamespace(
        source="ws:PUMPFUN:program",
        token_mint="mint-1",
        signature=signature,
        received_at=datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=30),
        wallet=wallet or f"candidate-{signature}",
        side="buy",
    )


def _rows(count: int) -> list[dict[str, object]]:
    received_at = datetime(2026, 1, 1, tzinfo=timezone.utc).isoformat()
    return [{"signature": f"sig-{index}", "received_at": received_at} for index in range(count)]


@pytest.fixture(autouse=True)
def _reset(monkeypatch):
    repair._reset_state_for_tests()
    monkeypatch.setattr(repair, "_memory_pressure_high", lambda: False)

    def normalize(result, *, signature, trigger_received_at, source_hint):
        del result, trigger_received_at, source_hint
        return SimpleNamespace(signature=signature, token_mint="mint-1")

    monkeypatch.setattr(repair.direct, "normalize_standard_transaction", normalize)


def test_large_prefill_creates_only_bounded_worker_tasks() -> None:
    async def run() -> tuple[bool, _Plane]:
        plane = _Plane(_rows(1000), deadline=5.0)
        result = await repair._bounded_prefill_launch_context(plane, _candidate())
        return result, plane

    result, plane = asyncio.run(run())
    state = repair.status()
    assert result is True
    assert len(plane.journal.hydrated) == 1000
    assert state["worker_tasks_created"] == repair.CONTEXT_WORKER_TASKS_PER_PREFILL
    assert state["active_worker_tasks"] == 0
    assert state["active_fetches"] == 0
    assert state["peak_worker_tasks"] <= repair.CONTEXT_WORKER_TASKS_PER_PREFILL


def test_concurrent_prefills_share_one_global_fetch_budget() -> None:
    async def run() -> tuple[bool, bool, _Plane]:
        plane = _Plane(_rows(120), deadline=3.0)
        plane.fetch_delay = 0.01
        first, second = await asyncio.gather(
            repair._bounded_prefill_launch_context(plane, _candidate("candidate-a")),
            repair._bounded_prefill_launch_context(plane, _candidate("candidate-b")),
        )
        return first, second, plane

    first, second, plane = asyncio.run(run())
    assert first is True
    assert second is True
    assert plane.fetch_peak <= repair.CONTEXT_FETCH_CONCURRENCY
    assert plane.fetch_peak > 1
    assert repair.status()["peak_fetches"] <= repair.CONTEXT_FETCH_CONCURRENCY


def test_outer_candidate_and_background_context_gates_remain_exactly_bounded() -> None:
    async def run() -> tuple[list[bool], _Plane]:
        plane = _Plane(_rows(48), deadline=3.0)
        plane.fetch_delay = 0.01
        jobs = [
            repair._bounded_prefill_launch_context(
                plane,
                _candidate(f"critical-{index}", wallet=f"candidate-{index}"),
            )
            for index in range(7)
        ]
        jobs += [
            repair._bounded_prefill_launch_context(
                plane,
                _candidate(f"background-{index}", wallet=f"background-{index}"),
            )
            for index in range(5)
        ]
        return list(await asyncio.gather(*jobs)), plane

    results, plane = asyncio.run(run())
    state = repair.status()
    assert all(results)
    assert state["peak_candidate_prefills"] == repair.LEGACY_CANDIDATE_CONTEXT_SLOTS == 3
    assert state["peak_background_prefills"] == repair.LEGACY_BACKGROUND_CONTEXT_SLOTS == 1
    assert state["active_candidate_prefills"] == 0
    assert state["active_background_prefills"] == 0
    assert plane.fetch_peak <= repair.CONTEXT_FETCH_CONCURRENCY
    assert state["legacy_outer_memory_gate_preserved"] is True


def test_timeout_cancels_and_drains_every_worker() -> None:
    async def run() -> tuple[bool, list[dict[str, object]]]:
        plane = _Plane(_rows(200), deadline=0.02)
        plane.fetch_delay = 1.0
        loop = asyncio.get_running_loop()
        unhandled: list[dict[str, object]] = []
        previous = loop.get_exception_handler()
        loop.set_exception_handler(lambda _loop, context: unhandled.append(dict(context)))
        try:
            result = await repair._bounded_prefill_launch_context(plane, _candidate())
            await asyncio.sleep(0)
            return result, unhandled
        finally:
            loop.set_exception_handler(previous)

    result, unhandled = asyncio.run(run())
    state = repair.status()
    assert result is False
    assert state["prefill_timeouts"] == 1
    assert state["active_worker_tasks"] == 0
    assert state["active_fetches"] == 0
    assert unhandled == []


def test_memory_pressure_defers_discretionary_context_before_rpc(monkeypatch) -> None:
    async def run() -> tuple[bool, _Plane]:
        plane = _Plane(_rows(100))
        result = await repair._bounded_prefill_launch_context(plane, _candidate())
        return result, plane

    monkeypatch.setattr(repair, "_memory_pressure_high", lambda: True)
    result, plane = asyncio.run(run())
    assert result is False
    assert plane.journal.hydrated == []
    assert repair.status()["memory_pressure_deferrals"] == 1


def test_cgroup_v2_memory_status_reports_raw_headroom_and_oom_events(tmp_path: Path) -> None:
    (tmp_path / "memory.current").write_text("1610612736\n", encoding="utf-8")
    (tmp_path / "memory.max").write_text("2147483648\n", encoding="utf-8")
    (tmp_path / "memory.events").write_text("low 0\nhigh 2\nmax 7\noom 3\noom_kill 2\n", encoding="utf-8")
    (tmp_path / "memory.stat").write_text(
        "anon 500000000\nfile 800000000\nkernel 100000000\nsock 12345\n",
        encoding="utf-8",
    )

    status = repair.cgroup_memory_status(tmp_path)
    assert status["available"] is True
    assert status["memory_current_bytes"] == 1610612736
    assert status["memory_max_bytes"] == 2147483648
    assert status["memory_headroom_bytes"] == 536870912
    assert status["memory_fraction"] == pytest.approx(0.75)
    assert status["events"]["oom_kill"] == 2
    assert status["stat"]["file"] == 800000000


def test_cgroup_parser_tolerates_unlimited_missing_and_malformed_values(tmp_path: Path) -> None:
    (tmp_path / "memory.current").write_text("not-an-int\n", encoding="utf-8")
    (tmp_path / "memory.max").write_text("max\n", encoding="utf-8")
    (tmp_path / "memory.events").write_text("oom nope\nmax 4\n", encoding="utf-8")

    status = repair.cgroup_memory_status(tmp_path)
    assert status["available"] is False
    assert status["memory_current_bytes"] is None
    assert status["memory_max_bytes"] is None
    assert status["memory_headroom_bytes"] is None
    assert status["memory_fraction"] is None
    assert status["events"] == {"max": 4}


def test_install_is_idempotent_and_preserves_paper_only_authority() -> None:
    repair.install_runtime_memory_capacity_repair()
    first = repair.direct.DirectSolanaIngestionPlane._prefill_launch_context
    repair.install_runtime_memory_capacity_repair()
    second = repair.direct.DirectSolanaIngestionPlane._prefill_launch_context
    status = repair.status()

    assert first is second
    assert getattr(second, "_roi_bounded_context_runtime", False) is True
    assert getattr(second, "_roi_memory_bounded", False) is True
    assert status["installed"] is True
    assert status["candidate_context_one_task_per_signature"] is False
    assert status["legacy_outer_memory_gate_preserved"] is True
    assert status["certification_thresholds_changed"] is False
    assert status["economic_thresholds_changed"] is False
    assert status["canonical_evidence_reset"] is False
    assert status["paper_only"] is True
    assert status["live_money_authority"] is False
    assert status["signing_available"] is False
    assert status["transaction_submission_available"] is False
