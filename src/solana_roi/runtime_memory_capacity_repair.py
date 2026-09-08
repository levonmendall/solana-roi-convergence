from __future__ import annotations

import asyncio
import threading
from pathlib import Path
from typing import Any

from . import direct_solana as direct


REPAIR_VERSION = "bounded-context-runtime-memory-v1"
CONTEXT_FETCH_CONCURRENCY = 24
CONTEXT_WORKER_TASKS_PER_PREFILL = 24
MEMORY_PRESSURE_DEFER_FRACTION = 0.80
MEMORY_PRESSURE_MIN_HEADROOM_BYTES = 256 * 1024 * 1024
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
CERTIFICATION_THRESHOLDS_CHANGED = False
ECONOMIC_THRESHOLDS_CHANGED = False
CANONICAL_EVIDENCE_RESET = False

_ORIGINAL_PREFILL_LAUNCH_CONTEXT = direct.DirectSolanaIngestionPlane._prefill_launch_context
_STATE_LOCK = threading.Lock()
_STATE: dict[str, Any] = {
    "installed": False,
    "prefill_attempts": 0,
    "prefill_completed": 0,
    "prefill_timeouts": 0,
    "prefill_cancelled": 0,
    "memory_pressure_deferrals": 0,
    "worker_tasks_created": 0,
    "active_worker_tasks": 0,
    "peak_worker_tasks": 0,
    "active_fetches": 0,
    "peak_fetches": 0,
}


def _state_inc(name: str, amount: int = 1) -> None:
    with _STATE_LOCK:
        _STATE[name] = int(_STATE.get(name, 0) or 0) + int(amount)


def _state_active(name: str, peak_name: str, delta: int) -> None:
    with _STATE_LOCK:
        value = max(0, int(_STATE.get(name, 0) or 0) + int(delta))
        _STATE[name] = value
        _STATE[peak_name] = max(int(_STATE.get(peak_name, 0) or 0), value)


def _read_scalar(path: Path) -> int | None:
    try:
        raw = path.read_text(encoding="utf-8").strip()
    except (OSError, UnicodeError):
        return None
    if not raw or raw == "max":
        return None
    try:
        return max(0, int(raw))
    except ValueError:
        return None


def _read_key_values(path: Path) -> dict[str, int]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError):
        return {}
    values: dict[str, int] = {}
    for line in lines:
        parts = line.split()
        if len(parts) != 2:
            continue
        try:
            values[str(parts[0])] = int(parts[1])
        except ValueError:
            continue
    return values


def cgroup_memory_status(root: Path | str = Path("/sys/fs/cgroup")) -> dict[str, Any]:
    """Read cgroup-v2 memory truth without allocating or mutating runtime state."""

    base = Path(root)
    current = _read_scalar(base / "memory.current")
    limit = _read_scalar(base / "memory.max")
    events = _read_key_values(base / "memory.events")
    memory_stat = _read_key_values(base / "memory.stat")
    headroom = max(0, limit - current) if current is not None and limit is not None else None
    fraction = (float(current) / float(limit)) if current is not None and limit not in (None, 0) else None
    return {
        "available": current is not None,
        "memory_current_bytes": current,
        "memory_max_bytes": limit,
        "memory_headroom_bytes": headroom,
        "memory_fraction": fraction,
        "events": {
            key: int(events.get(key, 0) or 0)
            for key in ("low", "high", "max", "oom", "oom_kill", "oom_group_kill")
            if key in events
        },
        "stat": {
            key: int(memory_stat.get(key, 0) or 0)
            for key in ("anon", "file", "kernel", "kernel_stack", "pagetables", "sock", "shmem", "file_mapped")
            if key in memory_stat
        },
        "read_only": True,
    }


def _memory_pressure_high() -> bool:
    memory = cgroup_memory_status()
    current = memory.get("memory_current_bytes")
    limit = memory.get("memory_max_bytes")
    fraction = memory.get("memory_fraction")
    headroom = memory.get("memory_headroom_bytes")
    if not isinstance(current, int) or not isinstance(limit, int) or limit <= 0:
        return False
    if isinstance(fraction, (int, float)) and float(fraction) >= MEMORY_PRESSURE_DEFER_FRACTION:
        return True
    return isinstance(headroom, int) and headroom <= MEMORY_PRESSURE_MIN_HEADROOM_BYTES


def _shared_fetch_gate(plane: Any) -> asyncio.Semaphore:
    """One transaction-fetch budget per ingestion plane/event loop, not per prefill."""

    loop = asyncio.get_running_loop()
    current_loop = getattr(plane, "_roi_context_fetch_gate_loop", None)
    gate = getattr(plane, "_roi_context_fetch_gate", None)
    if current_loop is not loop or not isinstance(gate, asyncio.Semaphore):
        gate = asyncio.Semaphore(CONTEXT_FETCH_CONCURRENCY)
        plane._roi_context_fetch_gate = gate
        plane._roi_context_fetch_gate_loop = loop
    return gate


async def _bounded_prefill_launch_context(self: Any, candidate: Any) -> bool:
    """Preserve launch-context semantics while bounding task and RPC fan-out.

    The legacy implementation allocated one asyncio Task for every signature (up to
    600) and created a fresh Semaphore(24) for every concurrent hydrator. With 12
    hydration workers that allowed thousands of live Tasks and hundreds of RPC calls
    to exist at once. This implementation keeps the existing signature window,
    deadline, normalization and persistence rules, but uses a fixed worker pool and a
    semaphore shared by the whole ingestion plane.
    """

    _state_inc("prefill_attempts")
    if _memory_pressure_high():
        _state_inc("memory_pressure_deferrals")
        return False

    parts = candidate.source.split(":")
    if len(parts) < 3:
        return False
    source = parts[1].upper()
    try:
        created_at = await self._pair_created_at(candidate.token_mint)
    except Exception:
        return False
    if created_at is None:
        return False
    launch_window_end = created_at + direct.timedelta(seconds=8.0)
    if candidate.received_at < launch_window_end:
        return False
    rows = self.journal.recent_source_signatures(
        source,
        start=created_at - direct.timedelta(seconds=1.0),
        end=launch_window_end,
        exclude_signature=candidate.signature,
        limit=self.candidate_context_max_signatures,
    )
    if not rows:
        return False

    iterator = iter(rows)
    fetch_gate = _shared_fetch_gate(self)

    async def worker() -> None:
        _state_active("active_worker_tasks", "peak_worker_tasks", 1)
        try:
            while True:
                try:
                    row = next(iterator)
                except StopIteration:
                    return
                signature = str(row["signature"])
                trigger = direct.datetime.fromisoformat(str(row["received_at"]))
                try:
                    async with fetch_gate:
                        _state_active("active_fetches", "peak_fetches", 1)
                        try:
                            result, provider, latency = await self._get_transaction_ready(
                                signature, hedge=False, attempts=3
                            )
                        finally:
                            _state_active("active_fetches", "peak_fetches", -1)
                    swap = direct.normalize_standard_transaction(
                        result,
                        signature=signature,
                        trigger_received_at=trigger,
                        source_hint=source,
                    )
                    if swap is not None and swap.token_mint == candidate.token_mint:
                        self._persist_context_swap(swap)
                        self.journal.record_hydration(
                            signature=signature,
                            source=source,
                            trigger_received_at=trigger,
                            hydrated_at=direct.utcnow(),
                            rpc_provider=provider,
                            rpc_latency_ms=latency,
                            normalized=True,
                            candidate_context_prefilled=True,
                        )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    continue
        finally:
            _state_active("active_worker_tasks", "peak_worker_tasks", -1)

    worker_count = min(len(rows), CONTEXT_WORKER_TASKS_PER_PREFILL)
    _state_inc("worker_tasks_created", worker_count)
    workers = [
        asyncio.create_task(worker(), name=f"direct-solana-context-worker-{index + 1}")
        for index in range(worker_count)
    ]
    timed_out = False
    cancelled = False
    try:
        await asyncio.wait_for(
            asyncio.gather(*workers),
            timeout=self.candidate_context_deadline_seconds,
        )
    except asyncio.TimeoutError:
        timed_out = True
        _state_inc("prefill_timeouts")
    except asyncio.CancelledError:
        cancelled = True
        _state_inc("prefill_cancelled")
    finally:
        for task in workers:
            if not task.done():
                task.cancel()
        if workers:
            await asyncio.gather(*workers, return_exceptions=True)

    if cancelled:
        raise asyncio.CancelledError
    if timed_out:
        return False
    _state_inc("prefill_completed")
    return True


setattr(_bounded_prefill_launch_context, "_roi_bounded_context_runtime", True)
# Preserve the pre-existing production guard contract used by architecture and
# legacy-entrypoint regressions. This repair is stricter than the older memory
# boundary, but downstream guard checks still use this marker as the invariant.
setattr(_bounded_prefill_launch_context, "_roi_memory_bounded", True)


def install_runtime_memory_capacity_repair() -> None:
    current = direct.DirectSolanaIngestionPlane._prefill_launch_context
    if not bool(getattr(current, "_roi_bounded_context_runtime", False)):
        direct.DirectSolanaIngestionPlane._prefill_launch_context = _bounded_prefill_launch_context  # type: ignore[assignment]
    with _STATE_LOCK:
        _STATE["installed"] = True


def status() -> dict[str, Any]:
    with _STATE_LOCK:
        state = dict(_STATE)
    return {
        **state,
        "repair_version": REPAIR_VERSION,
        "candidate_context_fetch_concurrency_global_per_plane": CONTEXT_FETCH_CONCURRENCY,
        "candidate_context_worker_tasks_per_prefill_max": CONTEXT_WORKER_TASKS_PER_PREFILL,
        "candidate_context_one_task_per_signature": False,
        "candidate_context_semaphore_scope": "ingestion_plane",
        "timeout_cancels_and_drains_workers": True,
        "memory_pressure_defer_fraction": MEMORY_PRESSURE_DEFER_FRACTION,
        "memory_pressure_min_headroom_bytes": MEMORY_PRESSURE_MIN_HEADROOM_BYTES,
        "cgroup_memory": cgroup_memory_status(),
        "certification_thresholds_changed": CERTIFICATION_THRESHOLDS_CHANGED,
        "economic_thresholds_changed": ECONOMIC_THRESHOLDS_CHANGED,
        "canonical_evidence_reset": CANONICAL_EVIDENCE_RESET,
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
        "signing_available": SIGNING_AVAILABLE,
        "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
    }


def _reset_state_for_tests() -> None:
    with _STATE_LOCK:
        for key in tuple(_STATE):
            _STATE[key] = False if key == "installed" else 0


__all__ = [
    "CONTEXT_FETCH_CONCURRENCY",
    "CONTEXT_WORKER_TASKS_PER_PREFILL",
    "MEMORY_PRESSURE_DEFER_FRACTION",
    "MEMORY_PRESSURE_MIN_HEADROOM_BYTES",
    "REPAIR_VERSION",
    "cgroup_memory_status",
    "install_runtime_memory_capacity_repair",
    "status",
]
