from __future__ import annotations

import asyncio
import contextvars
import math
import time
from collections import Counter
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import Any

from . import launch_coverage_bridge as bridge
from . import launch_ws_frontier_timing_repair as frontier
from .coverage_completeness_repair import _launch_contexts
from .direct_solana import DirectSolanaIngestionPlane
from .launch_funding import DexScreenerLaunchCollector
from .observation import LatencyCertificationPolicy, TimedRiskCollectors


SCOUT_REASONS = frozenset({"frozen_scout_processed_trigger", "frozen_scout_live_poll_trigger"})
CANDIDATE_END_TO_END_BUDGET_SECONDS = LatencyCertificationPolicy().max_p95_end_to_end_ms / 1000.0
CANDIDATE_RECORDING_RESERVE_SECONDS = 0.10
DIAGNOSTIC_HORIZON_SECONDS = 3600.0

_CURRENT_HYDRATION_REASON: contextvars.ContextVar[str] = contextvars.ContextVar(
    "roi_current_hydration_reason", default=""
)
_ORIGINAL_HYDRATE_ONE: Callable[..., Any] | None = None
_ORIGINAL_PREFILL: Callable[..., Any] | None = None
_ORIGINAL_TIMED_REFRESH: Callable[..., Any] | None = None
_ORIGINAL_LAUNCH_COLLECT: Callable[..., Any] | None = None
_ORIGINAL_DIRECT_STATUS: Callable[..., dict[str, Any]] | None = None

_DIAGNOSTIC_TABLE_SQL = (
    "CREATE TABLE IF NOT EXISTS launch_near_creation_diagnostics ("
    "id INTEGER PRIMARY KEY AUTOINCREMENT, token_mint TEXT NOT NULL, assessed_at TEXT NOT NULL, "
    "launch_signature TEXT, timing_proof TEXT NOT NULL, launch_lag_ms REAL, launch_near_creation INTEGER NOT NULL, "
    "launch_slot INTEGER, frontier_slot INTEGER, frontier_slot_delta INTEGER, frontier_age_ms REAL, "
    "frontier_block_time REAL, pair_created_at TEXT NOT NULL, recorded_at TEXT NOT NULL, "
    "UNIQUE(token_mint, assessed_at))"
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_dt(value: Any) -> datetime:
    parsed = datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * q
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def _increment(obj: Any, name: str, amount: int = 1) -> None:
    attr = f"_roi_candidate_hotpath_{name}"
    setattr(obj, attr, int(getattr(obj, attr, 0) or 0) + int(amount))


def _is_frozen_scout_buy(self: Any, candidate: Any) -> bool:
    if _CURRENT_HYDRATION_REASON.get() not in SCOUT_REASONS:
        return False
    if str(getattr(candidate, "side", "") or "").lower() != "buy":
        return False
    wallet = str(getattr(candidate, "wallet", "") or "")
    if not wallet or wallet not in set(getattr(self, "scout_wallets", ()) or ()):
        return False
    try:
        profile = self.service.registry.get(wallet)
    except Exception:
        return False
    if profile is None:
        return False
    historically_eligible = bool(getattr(profile, "historically_eligible", False))
    tier = str(getattr(getattr(profile, "tier", None), "value", getattr(profile, "tier", "")) or "").upper()
    return historically_eligible and tier in {"S", "A"}


def _attested_launch_context(self: Any, mint: str) -> bool:
    try:
        raw = bridge._raw_collectors(self)
        launch = getattr(raw, "launch", None)
        if launch is None:
            return False
        context = _launch_contexts(launch).get(mint)
        return bool(isinstance(context, dict) and context.get("complete"))
    except Exception:
        return False


async def _hydrate_with_reason(self: DirectSolanaIngestionPlane, row: dict[str, Any]) -> None:
    if _ORIGINAL_HYDRATE_ONE is None:
        raise RuntimeError("candidate certification hot path is not installed")
    token = _CURRENT_HYDRATION_REASON.set(str(row.get("reason") or ""))
    try:
        await _ORIGINAL_HYDRATE_ONE(self, row)
    finally:
        _CURRENT_HYDRATION_REASON.reset(token)


async def _candidate_prefill_from_attested_context(self: Any, candidate: Any) -> bool:
    if _ORIGINAL_PREFILL is None:
        raise RuntimeError("candidate certification hot path is not installed")
    if not _is_frozen_scout_buy(self, candidate):
        return bool(await _ORIGINAL_PREFILL(self, candidate))

    # The launch bridge has already reconstructed the mint-specific immutable
    # eight-second launch window. Reopening a source-wide 600-signature fanout here
    # is duplicate work on the latency-critical scout path. Reuse only a complete
    # attestation; otherwise continue fail-closed and let risk readiness remain
    # incomplete rather than spending the entire entry window manufacturing context.
    _increment(self, "scout_source_fanout_bypassed")
    complete = _attested_launch_context(self, str(getattr(candidate, "token_mint", "") or ""))
    if complete:
        _increment(self, "attested_launch_context_reused")
    else:
        _increment(self, "missing_attested_launch_context")
    return complete


async def _timed_refresh_with_candidate_budget(
    self: TimedRiskCollectors,
    mint: str,
    at: datetime,
    *,
    current_swap: Any = None,
) -> None:
    if _ORIGINAL_TIMED_REFRESH is None:
        raise RuntimeError("candidate certification hot path is not installed")

    coverage_refresh = getattr(self.inner, "refresh_coverage", None)
    candidate_refresh = getattr(self.inner, "refresh_candidate", None)
    eligible = bool(
        current_swap is not None
        and callable(coverage_refresh)
        and callable(candidate_refresh)
        and self._eligible_candidate(current_swap)
    )
    if not eligible:
        await _ORIGINAL_TIMED_REFRESH(self, mint, at, current_swap=current_swap)
        return

    started_at = self.now_fn()
    started_perf = self.perf_fn()
    trigger_observed_at = getattr(current_swap, "observed_at", at)
    trigger_received_at = getattr(current_swap, "received_at", at)
    ingestion_latency_ms = float(getattr(current_swap, "ingestion_latency_ms", 0.0) or 0.0)
    elapsed_before_collectors = max(0.0, (started_at - trigger_observed_at).total_seconds())
    available = max(
        0.0,
        CANDIDATE_END_TO_END_BUDGET_SECONDS
        - CANDIDATE_RECORDING_RESERVE_SECONDS
        - elapsed_before_collectors,
    )
    timed_out = False
    unexpected_error: str | None = None

    if available > 0.0:
        try:
            # Launch/funding and the four dynamic dimensions are independent risk
            # dimensions. Overlap them, but preserve their existing collectors,
            # evidence rules, thresholds and fail-closed results.
            await asyncio.wait_for(
                asyncio.gather(
                    coverage_refresh(mint, at, current_swap=current_swap),
                    candidate_refresh(mint, at, current_swap=current_swap),
                ),
                timeout=available,
            )
        except asyncio.TimeoutError:
            timed_out = True
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            unexpected_error = f"{type(exc).__name__}:{str(exc)[:300]}"
    else:
        timed_out = True

    completed_at = self.now_fn()
    elapsed_ms = max(0.0, (self.perf_fn() - started_perf) * 1000.0)
    readiness_raw = self.risk.readiness(mint, as_of=completed_at)
    readiness = dict(readiness_raw) if isinstance(readiness_raw, dict) else {"complete": False, "fresh": False}
    if timed_out:
        readiness["candidate_certification_budget_exhausted"] = True
        readiness["candidate_end_to_end_budget_seconds"] = CANDIDATE_END_TO_END_BUDGET_SECONDS
        _increment(self, "budget_exhausted")
    if unexpected_error is not None:
        readiness["candidate_hotpath_error"] = unexpected_error
        _increment(self, "unexpected_errors")

    self.store.record_risk_refresh(
        token_mint=mint,
        trigger_observed_at=trigger_observed_at.isoformat(),
        trigger_received_at=trigger_received_at.isoformat(),
        started_at=started_at.isoformat(),
        completed_at=completed_at.isoformat(),
        elapsed_ms=elapsed_ms,
        ingestion_latency_ms=ingestion_latency_ms,
        end_to_end_ms=max(0.0, (completed_at - trigger_observed_at).total_seconds() * 1000.0),
        complete=bool(readiness.get("complete")) and not timed_out and unexpected_error is None,
        fresh=bool(readiness.get("fresh")) and not timed_out and unexpected_error is None,
        readiness=readiness,
    )
    _increment(self, "measurements_recorded")


def _ensure_diagnostic_schema(store: Any) -> None:
    with store._lock, store.db:
        store.db.execute(_DIAGNOSTIC_TABLE_SQL)
        store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_launch_near_creation_diag_recorded "
            "ON launch_near_creation_diagnostics(recorded_at)"
        )


def _record_launch_diagnostic(self: DexScreenerLaunchCollector, mint: str, at: datetime) -> None:
    store = self.store
    _ensure_diagnostic_schema(store)
    with store._lock:
        coverage = store.db.execute(
            "SELECT pair_created_at, assessed_at, launch_lag_ms, launch_near_creation "
            "FROM program_coverage_observations WHERE token_mint=? AND assessed_at=? LIMIT 1",
            (mint, at.isoformat()),
        ).fetchone()
    if coverage is None:
        return

    context = _launch_contexts(self).get(mint)
    signature = str(context.get("launch_signature") or "") if isinstance(context, dict) else ""
    frontier_row = frontier._frontier_row(store, signature) if signature else None
    try:
        launch_slot = int(frontier_row.get("launch_slot") or 0) if isinstance(frontier_row, dict) else 0
        frontier_slot = int(frontier_row.get("frontier_slot") or 0) if isinstance(frontier_row, dict) else 0
    except (TypeError, ValueError):
        launch_slot = frontier_slot = 0
    slot_delta = frontier_slot - launch_slot if launch_slot > 0 and frontier_slot > 0 else None
    try:
        frontier_age_ms = float(frontier_row.get("frontier_age_ms")) if isinstance(frontier_row, dict) and frontier_row.get("frontier_age_ms") is not None else None
    except (TypeError, ValueError):
        frontier_age_ms = None
    try:
        frontier_block_time = float(frontier_row.get("frontier_block_time")) if isinstance(frontier_row, dict) and frontier_row.get("frontier_block_time") is not None else None
    except (TypeError, ValueError):
        frontier_block_time = None
    timing_proof = str(getattr(self, "_roi_last_launch_timing_proof", "unknown") or "unknown")

    with store._lock, store.db:
        store.db.execute(
            "INSERT OR REPLACE INTO launch_near_creation_diagnostics("
            "token_mint, assessed_at, launch_signature, timing_proof, launch_lag_ms, launch_near_creation, "
            "launch_slot, frontier_slot, frontier_slot_delta, frontier_age_ms, frontier_block_time, "
            "pair_created_at, recorded_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                mint,
                str(coverage["assessed_at"]),
                signature or None,
                timing_proof,
                float(coverage["launch_lag_ms"]) if coverage["launch_lag_ms"] is not None else None,
                1 if bool(coverage["launch_near_creation"]) else 0,
                launch_slot or None,
                frontier_slot or None,
                slot_delta,
                frontier_age_ms,
                frontier_block_time,
                str(coverage["pair_created_at"]),
                _utcnow().isoformat(),
            ),
        )


async def _launch_collect_with_diagnostics(self: DexScreenerLaunchCollector, mint: str, at: datetime) -> bool:
    if _ORIGINAL_LAUNCH_COLLECT is None:
        raise RuntimeError("candidate certification hot path is not installed")
    result = bool(await _ORIGINAL_LAUNCH_COLLECT(self, mint, at))
    try:
        _record_launch_diagnostic(self, mint, at)
    except Exception:
        # Diagnostics never authorize evidence and must not alter the gate result.
        pass
    return result


def _diagnostic_status(store: Any) -> dict[str, Any]:
    _ensure_diagnostic_schema(store)
    cutoff = (_utcnow() - timedelta(seconds=DIAGNOSTIC_HORIZON_SECONDS)).isoformat()
    with store._lock:
        rows = store.db.execute(
            "SELECT token_mint, assessed_at, timing_proof, launch_lag_ms, launch_near_creation, "
            "launch_slot, frontier_slot, frontier_slot_delta, frontier_age_ms "
            "FROM launch_near_creation_diagnostics WHERE recorded_at>=? "
            "ORDER BY id DESC LIMIT 100",
            (cutoff,),
        ).fetchall()
    items = [dict(row) for row in rows]
    lags = [float(row["launch_lag_ms"]) for row in items if row.get("launch_lag_ms") is not None]
    slot_deltas = [float(row["frontier_slot_delta"]) for row in items if row.get("frontier_slot_delta") is not None]
    ages = [float(row["frontier_age_ms"]) for row in items if row.get("frontier_age_ms") is not None]
    proofs = Counter(str(row.get("timing_proof") or "unknown") for row in items)
    return {
        "diagnostic_only": True,
        "gate_semantics_unchanged": True,
        "threshold_seconds_unchanged": 3.0,
        "horizon_seconds": DIAGNOSTIC_HORIZON_SECONDS,
        "sample_count": len(items),
        "near_creation_count": sum(1 for row in items if bool(row.get("launch_near_creation"))),
        "timing_proof_counts": dict(proofs),
        "p50_launch_lag_ms": _percentile(lags, 0.50),
        "p95_launch_lag_ms": _percentile(lags, 0.95),
        "max_launch_lag_ms": max(lags) if lags else None,
        "p50_frontier_slot_delta": _percentile(slot_deltas, 0.50),
        "p95_frontier_slot_delta": _percentile(slot_deltas, 0.95),
        "max_frontier_slot_delta": max(slot_deltas) if slot_deltas else None,
        "p95_frontier_age_ms": _percentile(ages, 0.95),
        "recent": items[:10],
    }


def _direct_status_with_candidate_hotpath(self: DirectSolanaIngestionPlane) -> dict[str, Any]:
    if _ORIGINAL_DIRECT_STATUS is None:
        raise RuntimeError("candidate certification hot path is not installed")
    payload = _ORIGINAL_DIRECT_STATUS(self)
    collectors = getattr(getattr(self, "service", None), "collectors", None)
    payload["candidate_certification_hotpath"] = {
        "installed": True,
        "frozen_scout_source_wide_prefill_bypassed": True,
        "mint_specific_attested_launch_context_required_for_reuse": True,
        "risk_dimensions_refreshed_concurrently": True,
        "candidate_end_to_end_budget_seconds": CANDIDATE_END_TO_END_BUDGET_SECONDS,
        "candidate_latency_threshold_unchanged": True,
        "incomplete_or_timeout_attempts_remain_in_denominator": True,
        "scout_source_fanout_bypassed_session": int(getattr(self, "_roi_candidate_hotpath_scout_source_fanout_bypassed", 0) or 0),
        "attested_launch_context_reused_session": int(getattr(self, "_roi_candidate_hotpath_attested_launch_context_reused", 0) or 0),
        "missing_attested_launch_context_session": int(getattr(self, "_roi_candidate_hotpath_missing_attested_launch_context", 0) or 0),
        "risk_measurements_recorded_session": int(getattr(collectors, "_roi_candidate_hotpath_measurements_recorded", 0) or 0),
        "risk_budget_exhausted_session": int(getattr(collectors, "_roi_candidate_hotpath_budget_exhausted", 0) or 0),
        "risk_unexpected_errors_session": int(getattr(collectors, "_roi_candidate_hotpath_unexpected_errors", 0) or 0),
        "entry_window_seconds_unchanged": 20.0,
        "paper_only_authority_unchanged": True,
        "signing_or_submission_available": False,
    }
    try:
        payload["launch_near_creation_diagnostics"] = _diagnostic_status(self.store)
    except Exception as exc:
        payload["launch_near_creation_diagnostics"] = {
            "diagnostic_only": True,
            "gate_semantics_unchanged": True,
            "error_type": type(exc).__name__,
        }
    policy = payload.setdefault("provider_runtime_policy", {})
    if isinstance(policy, dict):
        policy.update(
            {
                "candidate_source_wide_prefill_removed_from_frozen_scout_hotpath": True,
                "candidate_risk_refresh_parallelized": True,
                "candidate_latency_threshold_unchanged": True,
                "launch_near_creation_threshold_unchanged": True,
                "paper_only_authority_unchanged": True,
                "signing_or_submission_available": False,
            }
        )
    return payload


def install_candidate_certification_hotpath_repair() -> None:
    global _ORIGINAL_HYDRATE_ONE, _ORIGINAL_PREFILL, _ORIGINAL_TIMED_REFRESH
    global _ORIGINAL_LAUNCH_COLLECT, _ORIGINAL_DIRECT_STATUS

    current_hydrate = DirectSolanaIngestionPlane._hydrate_one
    if not bool(getattr(current_hydrate, "_roi_candidate_certification_hotpath", False)):
        _ORIGINAL_HYDRATE_ONE = current_hydrate
        try:
            _hydrate_with_reason.__dict__.update(getattr(current_hydrate, "__dict__", {}))
        except Exception:
            pass
        setattr(_hydrate_with_reason, "_roi_candidate_certification_hotpath", True)
        DirectSolanaIngestionPlane._hydrate_one = _hydrate_with_reason  # type: ignore[method-assign]

    current_prefill = DirectSolanaIngestionPlane._prefill_launch_context
    if not bool(getattr(current_prefill, "_roi_candidate_certification_hotpath", False)):
        _ORIGINAL_PREFILL = current_prefill
        try:
            _candidate_prefill_from_attested_context.__dict__.update(getattr(current_prefill, "__dict__", {}))
        except Exception:
            pass
        setattr(_candidate_prefill_from_attested_context, "_roi_candidate_certification_hotpath", True)
        DirectSolanaIngestionPlane._prefill_launch_context = _candidate_prefill_from_attested_context  # type: ignore[method-assign]

    current_refresh = TimedRiskCollectors.refresh
    if not bool(getattr(current_refresh, "_roi_candidate_certification_hotpath", False)):
        _ORIGINAL_TIMED_REFRESH = current_refresh
        try:
            _timed_refresh_with_candidate_budget.__dict__.update(getattr(current_refresh, "__dict__", {}))
        except Exception:
            pass
        setattr(_timed_refresh_with_candidate_budget, "_roi_candidate_certification_hotpath", True)
        TimedRiskCollectors.refresh = _timed_refresh_with_candidate_budget  # type: ignore[method-assign]

    current_launch_collect = DexScreenerLaunchCollector.collect
    if not bool(getattr(current_launch_collect, "_roi_candidate_certification_hotpath", False)):
        _ORIGINAL_LAUNCH_COLLECT = current_launch_collect
        try:
            _launch_collect_with_diagnostics.__dict__.update(getattr(current_launch_collect, "__dict__", {}))
        except Exception:
            pass
        setattr(_launch_collect_with_diagnostics, "_roi_candidate_certification_hotpath", True)
        DexScreenerLaunchCollector.collect = _launch_collect_with_diagnostics  # type: ignore[method-assign]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_candidate_certification_hotpath", False)):
        _ORIGINAL_DIRECT_STATUS = current_status
        try:
            _direct_status_with_candidate_hotpath.__dict__.update(getattr(current_status, "__dict__", {}))
        except Exception:
            pass
        setattr(_direct_status_with_candidate_hotpath, "_roi_candidate_certification_hotpath", True)
        DirectSolanaIngestionPlane.status = _direct_status_with_candidate_hotpath  # type: ignore[method-assign]


__all__ = [
    "CANDIDATE_END_TO_END_BUDGET_SECONDS",
    "CANDIDATE_RECORDING_RESERVE_SECONDS",
    "SCOUT_REASONS",
    "_candidate_prefill_from_attested_context",
    "_diagnostic_status",
    "_timed_refresh_with_candidate_budget",
    "install_candidate_certification_hotpath_repair",
]
