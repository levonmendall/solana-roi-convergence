from __future__ import annotations

import threading
import time
from typing import Any, Callable

from . import v51_counterfactual_extension as counterfactual
from .sqlite_phase_observability import _thread_io, emit_phase, resource_snapshot


OBSERVABILITY_VERSION = "robinhood-proof-price-io-v1"
PHASE_NAME = "robinhood-proof:counterfactual-price-resolution"
_INSTALLED = False
_ORIGINAL_PRICE_ROW: Callable[..., dict[str, Any] | None] | None = None
_ORIGINAL_RESOLVE: Callable[..., dict[str, int]] | None = None
_LOCAL = threading.local()


def _counter_delta(before: dict[str, int], after: dict[str, int], key: str) -> int:
    left = before.get(key)
    right = after.get(key)
    if isinstance(left, int) and isinstance(right, int):
        return max(0, right - left)
    return 0


def _plan_detail(row: Any) -> str:
    try:
        keys = row.keys()
    except Exception:
        keys = ()
    if "detail" in keys:
        try:
            return str(row["detail"])
        except Exception:
            pass
    try:
        return str(row[3])
    except Exception:
        return str(row)


def _query_plan(store: Any, *, bounded_after: bool) -> list[str]:
    where = [
        "release_commit=?",
        "market=?",
        "price_eth IS NOT NULL",
        "price_eth>0",
        "observed_at<=?",
    ]
    values: list[Any] = ["plan-release", "plan-market", "9999-12-31T23:59:59+00:00"]
    if bounded_after:
        where.append("observed_at>?")
        values.append("0001-01-01T00:00:00+00:00")
    sql = (
        "EXPLAIN QUERY PLAN SELECT price_eth,observed_at,tx_hash,log_index "
        "FROM robinhood_swaps WHERE "
        + " AND ".join(where)
        + " ORDER BY observed_at DESC,id DESC LIMIT 1"
    )
    try:
        with store._lock:
            rows = store.db.execute(sql, tuple(values)).fetchall()
    except Exception as exc:
        return [f"plan_unavailable:{type(exc).__name__}"]
    return [_plan_detail(row) for row in rows]


def _observed_price_row(
    store: Any,
    *,
    release_commit: str,
    market: str,
    before_or_at: str,
    after: str | None = None,
) -> dict[str, Any] | None:
    if _ORIGINAL_PRICE_ROW is None:
        raise RuntimeError("Robinhood proof price I/O observability is not installed")
    collector = getattr(_LOCAL, "collector", None)
    before_io = _thread_io()
    started = time.perf_counter()
    result: dict[str, Any] | None = None
    error: BaseException | None = None
    try:
        result = _ORIGINAL_PRICE_ROW(
            store,
            release_commit=release_commit,
            market=market,
            before_or_at=before_or_at,
            after=after,
        )
        return result
    except BaseException as exc:
        error = exc
        raise
    finally:
        if isinstance(collector, dict):
            after_io = _thread_io()
            kind = "bounded" if after is not None else "entry"
            collector["calls"] = int(collector.get("calls", 0) or 0) + 1
            collector[f"{kind}_calls"] = int(collector.get(f"{kind}_calls", 0) or 0) + 1
            if result is not None:
                collector["rows_found"] = int(collector.get("rows_found", 0) or 0) + 1
                collector[f"{kind}_rows_found"] = int(collector.get(f"{kind}_rows_found", 0) or 0) + 1
            duration_ms = max(0.0, (time.perf_counter() - started) * 1000.0)
            collector["lookup_duration_ms"] = float(collector.get("lookup_duration_ms", 0.0) or 0.0) + duration_ms
            collector[f"{kind}_duration_ms"] = float(collector.get(f"{kind}_duration_ms", 0.0) or 0.0) + duration_ms
            for key in ("rchar", "read_bytes", "syscr", "wchar", "write_bytes", "syscw"):
                delta = _counter_delta(before_io, after_io, key)
                collector[f"thread_{key}"] = int(collector.get(f"thread_{key}", 0) or 0) + delta
                collector[f"{kind}_thread_{key}"] = int(collector.get(f"{kind}_thread_{key}", 0) or 0) + delta
            if error is not None:
                collector["errors"] = int(collector.get("errors", 0) or 0) + 1
                collector["last_error_type"] = type(error).__name__


setattr(_observed_price_row, "_roi_robinhood_proof_price_io_observed", True)


def _observed_resolve(store: Any, *, limit: int = counterfactual.RESOLUTION_BATCH) -> dict[str, int]:
    if _ORIGINAL_RESOLVE is None:
        raise RuntimeError("Robinhood proof price I/O observability is not installed")
    previous = getattr(_LOCAL, "collector", None)
    collector: dict[str, Any] = {
        "calls": 0,
        "entry_calls": 0,
        "bounded_calls": 0,
        "rows_found": 0,
        "entry_rows_found": 0,
        "bounded_rows_found": 0,
        "lookup_duration_ms": 0.0,
        "entry_duration_ms": 0.0,
        "bounded_duration_ms": 0.0,
        "errors": 0,
    }
    _LOCAL.collector = collector
    before = resource_snapshot(store)
    started = time.perf_counter()
    result: dict[str, int] | None = None
    error: BaseException | None = None
    try:
        result = _ORIGINAL_RESOLVE(store, limit=limit)
        return result
    except BaseException as exc:
        error = exc
        raise
    finally:
        duration_ms = max(0.0, (time.perf_counter() - started) * 1000.0)
        detail: dict[str, Any] = {
            "observability_version": OBSERVABILITY_VERSION,
            "io_attribution": "thread_exact_per_price_lookup;phase_thread_context_for_resolution",
            "resolution_limit": int(limit),
            "price_lookup_calls": int(collector.get("calls", 0) or 0),
            "entry_lookup_calls": int(collector.get("entry_calls", 0) or 0),
            "bounded_exit_lookup_calls": int(collector.get("bounded_calls", 0) or 0),
            "price_rows_found": int(collector.get("rows_found", 0) or 0),
            "price_lookup_duration_ms_sum": round(float(collector.get("lookup_duration_ms", 0.0) or 0.0), 3),
            "entry_lookup_duration_ms_sum": round(float(collector.get("entry_duration_ms", 0.0) or 0.0), 3),
            "bounded_exit_lookup_duration_ms_sum": round(float(collector.get("bounded_duration_ms", 0.0) or 0.0), 3),
            "price_lookup_thread_rchar_bytes_sum": int(collector.get("thread_rchar", 0) or 0),
            "price_lookup_thread_read_bytes_sum": int(collector.get("thread_read_bytes", 0) or 0),
            "price_lookup_thread_syscr_sum": int(collector.get("thread_syscr", 0) or 0),
            "price_lookup_thread_wchar_bytes_sum": int(collector.get("thread_wchar", 0) or 0),
            "price_lookup_thread_write_bytes_sum": int(collector.get("thread_write_bytes", 0) or 0),
            "price_lookup_thread_syscw_sum": int(collector.get("thread_syscw", 0) or 0),
            "entry_query_plan": _query_plan(store, bounded_after=False),
            "bounded_exit_query_plan": _query_plan(store, bounded_after=True),
            "strategy_changed": False,
            "retention_changed": False,
            "coverage_changed": False,
            "paper_only": True,
            "live_money_authority": False,
        }
        if isinstance(result, dict):
            detail["examined"] = int(result.get("examined", 0) or 0)
            detail["resolved_market_return"] = int(result.get("resolved_market_return", 0) or 0)
            detail["resolved_no_observation"] = int(result.get("resolved_no_observation", 0) or 0)
        if error is not None:
            detail["error_type"] = type(error).__name__
        emit_phase(
            PHASE_NAME,
            before=before,
            after=resource_snapshot(store),
            duration_ms=duration_ms,
            detail=detail,
        )
        if previous is None:
            try:
                delattr(_LOCAL, "collector")
            except AttributeError:
                pass
        else:
            _LOCAL.collector = previous


setattr(_observed_resolve, "_roi_robinhood_proof_price_io_observed", True)


def install_robinhood_proof_price_io_observability() -> None:
    global _INSTALLED, _ORIGINAL_PRICE_ROW, _ORIGINAL_RESOLVE
    if _INSTALLED:
        return
    current_price = counterfactual._price_row
    current_resolve = counterfactual._resolve_robinhood_forward_market_returns
    if not bool(getattr(current_price, "_roi_robinhood_proof_price_io_observed", False)):
        _ORIGINAL_PRICE_ROW = current_price
        counterfactual._price_row = _observed_price_row
    if not bool(getattr(current_resolve, "_roi_robinhood_proof_price_io_observed", False)):
        _ORIGINAL_RESOLVE = current_resolve
        counterfactual._resolve_robinhood_forward_market_returns = _observed_resolve
    _INSTALLED = True


__all__ = [
    "OBSERVABILITY_VERSION",
    "PHASE_NAME",
    "install_robinhood_proof_price_io_observability",
]
