from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path
from typing import Any, Awaitable, Callable


OBSERVABILITY_VERSION = "sqlite-startup-phase-attribution-v1"
_CGROUP_ROOT = Path("/sys/fs/cgroup")
_INSTALLED = False


def _read_int(path: Path) -> int | None:
    try:
        raw = path.read_text(encoding="utf-8").strip()
        return int(raw) if raw and raw != "max" else None
    except (OSError, ValueError):
        return None


def _read_key_values(path: Path) -> dict[str, int]:
    values: dict[str, int] = {}
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            key, raw = line.split(maxsplit=1)
            values[key] = int(raw)
    except (OSError, ValueError):
        return values
    return values


def _proc_io() -> dict[str, int]:
    return _read_key_values(Path("/proc/self/io"))


def _thread_count() -> int | None:
    try:
        for line in Path("/proc/self/status").read_text(encoding="utf-8").splitlines():
            if line.startswith("Threads:"):
                return int(line.split(":", 1)[1].strip())
    except (OSError, ValueError):
        pass
    return None


def _db_path(store: Any | None) -> Path:
    if store is not None:
        raw = getattr(store, "path", None)
        if raw:
            return Path(raw)
    return Path(os.getenv("SOLANA_ROI_DB_PATH", "data/solana-roi.sqlite3"))


def _size(path: Path) -> int | None:
    try:
        return path.stat().st_size if path.exists() else 0
    except OSError:
        return None


def resource_snapshot(store: Any | None = None) -> dict[str, Any]:
    stat = _read_key_values(_CGROUP_ROOT / "memory.stat")
    file_bytes = int(stat.get("file", 0))
    dirty_bytes = int(stat.get("file_dirty", 0))
    writeback_bytes = int(stat.get("file_writeback", 0))
    db_path = _db_path(store)
    io = _proc_io()
    return {
        "memory_current_bytes": _read_int(_CGROUP_ROOT / "memory.current"),
        "memory_max_bytes": _read_int(_CGROUP_ROOT / "memory.max"),
        "anon_bytes": int(stat.get("anon", 0)),
        "file_cache_bytes": file_bytes,
        "clean_file_cache_bytes_estimate": max(0, file_bytes - dirty_bytes - writeback_bytes),
        "file_dirty_bytes": dirty_bytes,
        "file_writeback_bytes": writeback_bytes,
        "sqlite_db_bytes": _size(db_path),
        "sqlite_wal_bytes": _size(Path(f"{db_path}-wal")),
        "sqlite_shm_bytes": _size(Path(f"{db_path}-shm")),
        "proc_read_bytes": io.get("read_bytes"),
        "proc_write_bytes": io.get("write_bytes"),
        "proc_rchar": io.get("rchar"),
        "proc_wchar": io.get("wchar"),
        "proc_syscr": io.get("syscr"),
        "proc_syscw": io.get("syscw"),
        "proc_cancelled_write_bytes": io.get("cancelled_write_bytes"),
        "pids_current": _read_int(_CGROUP_ROOT / "pids.current"),
        "threads": _thread_count(),
    }


def _numeric_delta(before: dict[str, Any], after: dict[str, Any]) -> dict[str, int]:
    delta: dict[str, int] = {}
    for key, value in after.items():
        previous = before.get(key)
        if isinstance(value, int) and isinstance(previous, int):
            delta[key] = value - previous
    return delta


def emit_phase(
    phase: str,
    *,
    before: dict[str, Any],
    after: dict[str, Any],
    duration_ms: float,
    detail: dict[str, Any] | None = None,
) -> None:
    payload = {
        "version": OBSERVABILITY_VERSION,
        "phase": phase,
        "duration_ms": round(max(0.0, duration_ms), 3),
        "before": before,
        "after": after,
        "delta": _numeric_delta(before, after),
        "detail": dict(detail or {}),
        "paper_only": True,
        "live_money_authority": False,
        "resource_control_changed": False,
        "retention_changed": False,
    }
    print("ROI_SQLITE_PHASE " + json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str), flush=True)


def _sync_phase(name: str, original: Callable[..., Any]) -> Callable[..., Any]:
    if bool(getattr(original, "_roi_sqlite_phase_observed", False)):
        return original

    def wrapped(self: Any, *args: Any, **kwargs: Any) -> Any:
        store = getattr(self, "store", None)
        before = resource_snapshot(store)
        started = time.perf_counter()
        result: Any = None
        error: BaseException | None = None
        try:
            result = original(self, *args, **kwargs)
            return result
        except BaseException as exc:
            error = exc
            raise
        finally:
            detail: dict[str, Any] = {}
            if isinstance(result, tuple) and len(result) == 2 and all(isinstance(v, int) for v in result):
                detail["queue_rows"] = int(result[0])
                detail["metric_rows"] = int(result[1])
            elif isinstance(result, tuple) and len(result) == 3 and all(isinstance(v, int) for v in result):
                detail["checkpoint_busy"] = int(result[0])
                detail["checkpoint_log_frames"] = int(result[1])
                detail["checkpointed_frames"] = int(result[2])
            if error is not None:
                detail["error_type"] = type(error).__name__
            emit_phase(
                name,
                before=before,
                after=resource_snapshot(store),
                duration_ms=(time.perf_counter() - started) * 1000.0,
                detail=detail,
            )

    setattr(wrapped, "_roi_sqlite_phase_observed", True)
    return wrapped


def _async_worker_phase(name: str, original: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
    if bool(getattr(original, "_roi_sqlite_worker_observed", False)):
        return original

    async def wrapped(self: Any, *args: Any, **kwargs: Any) -> Any:
        store = getattr(self, "store", None)
        before = resource_snapshot(store)
        started = time.perf_counter()

        async def after_activation() -> None:
            await asyncio.sleep(0)
            emit_phase(
                f"worker:{name}:activated",
                before=before,
                after=resource_snapshot(store),
                duration_ms=(time.perf_counter() - started) * 1000.0,
            )

        marker = asyncio.create_task(after_activation(), name=f"roi-observe-{name}-activation")
        try:
            return await original(self, *args, **kwargs)
        finally:
            if not marker.done():
                marker.cancel()
            await asyncio.gather(marker, return_exceptions=True)

    try:
        wrapped.__dict__.update(getattr(original, "__dict__", {}))
    except Exception:
        pass
    setattr(wrapped, "_roi_sqlite_worker_observed", True)
    return wrapped


def _bootstrap_phase(original: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
    if bool(getattr(original, "_roi_sqlite_bootstrap_observed", False)):
        return original

    async def wrapped(stop: asyncio.Event) -> Any:
        before = resource_snapshot()
        started = time.perf_counter()
        result: Any = None
        error: BaseException | None = None
        try:
            result = await original(stop)
            return result
        except BaseException as exc:
            error = exc
            raise
        finally:
            detail = {"runtime_ready": result is not None}
            if error is not None:
                detail["error_type"] = type(error).__name__
            emit_phase(
                "runtime-bootstrap",
                before=before,
                after=resource_snapshot(getattr(result, "store", None) if result is not None else None),
                duration_ms=(time.perf_counter() - started) * 1000.0,
                detail=detail,
            )

    setattr(wrapped, "_roi_sqlite_bootstrap_observed", True)
    return wrapped


def install_sqlite_phase_observability() -> None:
    global _INSTALLED
    if _INSTALLED:
        return

    from . import continuity_storage_capacity_repair as storage
    from . import render_runtime_bootstrap_repair as bootstrap
    from .direct_solana import DirectSolanaIngestionPlane
    from .observation import ShadowPriceClock
    from .wallet_discovery import ContinuousWalletDiscovery
    from .webhook_queue import HeliusWebhookWorker

    storage._prune_operational_rows_once = _sync_phase(
        "direct-solana-storage-maintenance:prune", storage._prune_operational_rows_once
    )
    storage._checkpoint_wal = _sync_phase(
        "direct-solana-storage-maintenance:wal-checkpoint", storage._checkpoint_wal
    )
    bootstrap._build_runtime_until_ready = _bootstrap_phase(bootstrap._build_runtime_until_ready)

    DirectSolanaIngestionPlane.run = _async_worker_phase(
        "direct-solana-ingestion", DirectSolanaIngestionPlane.run
    )
    ContinuousWalletDiscovery.run = _async_worker_phase(
        "continuous-wallet-discovery", ContinuousWalletDiscovery.run
    )
    ShadowPriceClock.run = _async_worker_phase("shadow-price-clock", ShadowPriceClock.run)
    HeliusWebhookWorker.run = _async_worker_phase(
        "legacy-helius-webhook-worker", HeliusWebhookWorker.run
    )

    _INSTALLED = True


__all__ = [
    "OBSERVABILITY_VERSION",
    "emit_phase",
    "install_sqlite_phase_observability",
    "resource_snapshot",
]
