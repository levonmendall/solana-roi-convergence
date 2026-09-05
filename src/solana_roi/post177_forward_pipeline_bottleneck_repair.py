from __future__ import annotations

import asyncio
import math
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from functools import wraps
from typing import Any, Callable

from . import candidate_fomo_runtime_repair as candidate_fomo
from . import candidate_hydration_work_conserving_repair as hydration_flex
from . import candidate_rpc_priority_repair as candidate_rpc
from . import continuation_market_recalibration as continuation
from . import economic_signal_continuation_repair as economic
from . import ephemeral_candidate_retention as ephemeral
from . import post104_production_architecture_repair as post104
from . import robinhood_forward_only_runtime_repair as forward
from . import robinhood_live_frontier_verification_repair as frontier
from . import semantic_candidate_attribution_architecture as semantic
from . import unified_strategy_status as unified_status
from . import venue_native_candidate_graph_repair as venue
from .direct_solana import DirectSolanaIngestionPlane


REPAIR_VERSION = "post177-forward-pipeline-bottleneck-v1"
ROBINHOOD_HEAD_OBSERVER_INTERVAL_SECONDS = 0.75
ROBINHOOD_HEAD_OBSERVER_MAX_GAP_SECONDS = 5.0
FOMO_DELIVERY_LOOKBACK_SECONDS = 300.0
CANDIDATE_OPERATIONAL_RETENTION_SECONDS = float(economic.CANDIDATE_OPERATIONAL_TIMEOUT_SECONDS)
IMMEDIATE_COPY_WINDOW_SECONDS = float(economic.IMMEDIATE_COPY_SECONDS)
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False

_ORIGINAL_ROBINHOOD_RUN: Callable[..., Any] | None = None
_ORIGINAL_ROBINHOOD_V2: Callable[..., Any] | None = None
_ORIGINAL_ROBINHOOD_V3: Callable[..., Any] | None = None
_ORIGINAL_ROBINHOOD_STATUS: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_DIRECT_STATUS: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_UNIFIED_STATUS: Callable[..., dict[str, Any]] | None = None
_INSTALLED = False


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def _parse_dt(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        dt = value
    else:
        try:
            dt = datetime.fromisoformat(str(value))
        except Exception:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _inc(obj: Any, name: str, amount: int = 1) -> None:
    attr = f"_roi_post177_{name}"
    setattr(obj, attr, int(getattr(obj, attr, 0) or 0) + int(amount))


def _observer_generation(self: Any) -> int:
    return int(getattr(self, "_roi_post177_head_observer_generation", 0) or 0)


def _mark_observer_gap(self: Any, reason: str) -> None:
    setattr(self, "_roi_post177_head_observer_generation", _observer_generation(self) + 1)
    setattr(self, "_roi_post177_head_observer_continuity_ok", False)
    setattr(self, "_roi_post177_head_observer_last_gap_reason", reason)
    _inc(self, "head_observer_gaps")


async def _observe_robinhood_head(self: Any, stop: asyncio.Event) -> None:
    """Observe the chain head independently from heavier market processing.

    A continuously observed head may build a processing backlog without becoming a
    historical replay. A real observer interruption increments a generation; the
    decision epoch then re-anchors and never treats the unobserved interval as current.
    """

    while not stop.is_set():
        started = time.monotonic()
        prior_success = float(getattr(self, "_roi_post177_head_observer_last_success_monotonic", 0.0) or 0.0)
        try:
            latest = int(await self.rpc.block_number())
            now = time.monotonic()
            if prior_success > 0.0 and now - prior_success > ROBINHOOD_HEAD_OBSERVER_MAX_GAP_SECONDS:
                _mark_observer_gap(self, "observer_heartbeat_gap")
            setattr(self, "_roi_post177_head_observed_block", latest)
            setattr(self, "_roi_post177_head_observer_last_success_monotonic", now)
            setattr(self, "_roi_post177_head_observer_last_success_at", _iso(_utcnow()))
            setattr(self, "_roi_post177_head_observer_last_error_type", None)
            setattr(self, "_roi_post177_head_observer_continuity_ok", True)
            self._latest_block = latest
            _inc(self, "head_observer_successes")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _mark_observer_gap(self, "observer_rpc_failure")
            setattr(self, "_roi_post177_head_observer_last_error_type", type(exc).__name__)
            _inc(self, "head_observer_failures")

        elapsed = max(0.0, time.monotonic() - started)
        delay = max(0.05, ROBINHOOD_HEAD_OBSERVER_INTERVAL_SECONDS - elapsed)
        try:
            await asyncio.wait_for(stop.wait(), timeout=delay)
        except asyncio.TimeoutError:
            pass


def _observer_head_fresh(self: Any) -> bool:
    last = float(getattr(self, "_roi_post177_head_observer_last_success_monotonic", 0.0) or 0.0)
    return bool(
        last > 0.0
        and time.monotonic() - last <= ROBINHOOD_HEAD_OBSERVER_MAX_GAP_SECONDS
        and getattr(self, "_roi_post177_head_observer_continuity_ok", False)
    )


async def _current_observed_head(self: Any) -> int:
    if _observer_head_fresh(self):
        observed = getattr(self, "_roi_post177_head_observed_block", None)
        if observed is not None:
            return int(observed)
    latest = int(await self.rpc.block_number())
    self._latest_block = latest
    setattr(self, "_roi_post177_head_observed_block", latest)
    setattr(self, "_roi_post177_head_observer_last_success_monotonic", time.monotonic())
    setattr(self, "_roi_post177_head_observer_last_success_at", _iso(_utcnow()))
    setattr(self, "_roi_post177_head_observer_continuity_ok", True)
    return latest


def _pending_v3(self: Any) -> dict[str, tuple[Any, int]]:
    value = getattr(self, "_roi_post177_pending_v3", None)
    if not isinstance(value, dict):
        value = {}
        setattr(self, "_roi_post177_pending_v3", value)
    return value


def _pending_v2(self: Any) -> dict[str, Any]:
    value = getattr(self, "_roi_post177_pending_v2", None)
    if not isinstance(value, dict):
        value = {}
        setattr(self, "_roi_post177_pending_v2", value)
    return value


def _clear_pending_markets(self: Any) -> None:
    _pending_v3(self).clear()
    _pending_v2(self).clear()


def _start_observed_epoch(self: Any, *, latest: int, reason: str) -> None:
    frontier._start_epoch(self, anchor_block=int(latest), reason=reason)
    setattr(self, "_roi_post177_epoch_observer_generation", _observer_generation(self))
    _clear_pending_markets(self)


def _schedule_rwa_refresh(self: Any) -> None:
    refresh = getattr(self, "_refresh_rwa_registry", None)
    if not callable(refresh):
        return
    current = getattr(self, "_roi_post177_rwa_refresh_task", None)
    if isinstance(current, asyncio.Task) and not current.done():
        return
    try:
        task = asyncio.create_task(refresh(), name="robinhood-rwa-registry-refresh")
    except RuntimeError:
        return
    setattr(self, "_roi_post177_rwa_refresh_task", task)
    _inc(self, "rwa_refresh_scheduled")

    def done(completed: asyncio.Task[Any]) -> None:
        try:
            completed.result()
            _inc(self, "rwa_refresh_completed")
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            setattr(self, "_roi_post177_rwa_refresh_last_error_type", type(exc).__name__)
            _inc(self, "rwa_refresh_errors")

    task.add_done_callback(done)


async def _advance_observed_forward_epoch(self: Any) -> None:
    """Process a continuously observed Robinhood backlog without historical catch-up.

    Processing is bounded to the existing 64-block acquisition chunk. A large
    *processing* lag is drained prospectively when the independent observer remained
    continuous. A real observer generation change re-anchors at the current head and
    discards pre-anchor flow, preserving the no-retrospective-entry rule.
    """

    frontier._inc(self, "polls")
    _schedule_rwa_refresh(self)

    if not bool(getattr(self, "_roi_forward_only_chain_id_verified", False)):
        chain_id = await self.rpc.chain_id()
        if chain_id != forward.runtime.ROBINHOOD_CHAIN_ID:
            raise RuntimeError(f"wrong Robinhood chain id: {chain_id}")
        setattr(self, "_roi_forward_only_chain_id_verified", True)

    latest = await _current_observed_head(self)
    self._latest_block = latest

    if not frontier._live_epoch_active(self):
        await forward._sync_bounded_metadata(
            self,
            latest=latest,
            previous_live_cursor=None,
            reason="startup_current_frontier_metadata",
        )
        _start_observed_epoch(self, latest=latest, reason="forward_only_observed_head")
        return

    live_cursor = frontier._live_cursor(self)
    assert live_cursor is not None

    if latest < live_cursor:
        await forward._sync_bounded_metadata(
            self,
            latest=latest,
            previous_live_cursor=None,
            reason="chain_head_regressed_reanchor",
        )
        _start_observed_epoch(self, latest=latest, reason="chain_head_regressed")
        frontier._inc(self, "epoch_resets")
        return

    epoch_generation = int(
        getattr(self, "_roi_post177_epoch_observer_generation", _observer_generation(self))
    )
    if _observer_generation(self) != epoch_generation:
        await forward._sync_bounded_metadata(
            self,
            latest=latest,
            previous_live_cursor=live_cursor,
            reason="observer_continuity_gap_bounded_metadata_only",
        )
        _start_observed_epoch(self, latest=latest, reason="live_observation_gap_reanchored_forward_only")
        frontier._inc(self, "epoch_resets")
        _inc(self, "true_observation_reanchors")
        return

    gap = max(0, latest - live_cursor)
    if gap == 0:
        ready = _observer_head_fresh(self)
        setattr(self, "_roi_live_epoch_ready", ready)
        if ready:
            setattr(self, "_roi_live_epoch_last_success_at", frontier._utcnow())
        return

    # Drain only a bounded contiguous slice. Unlike the prior implementation, a
    # processing backlog >64 blocks is not itself classified as a data outage.
    from_block = live_cursor + 1
    to_block = min(latest, live_cursor + int(frontier.MAX_LIVE_FRONTIER_GAP_BLOCKS))
    observed_at = frontier._utcnow()

    await frontier._sync_factory_state(self, from_block=from_block, to_block=to_block)
    market_logs = await frontier._fetch_market_logs(self, from_block=from_block, to_block=to_block)

    touched_v3: dict[str, tuple[Any, int]] = {}
    touched_v2: dict[str, Any] = {}
    setattr(self, "_roi_live_epoch_suppress_entries", True)
    try:
        for kind, market, log in market_logs:
            block = int(str(log.get("blockNumber") or "0x0"), 16)
            if kind == "v3":
                await self._process_v3_swap(market, log, live=True, observed_at=observed_at)
                previous = touched_v3.get(market.pool)
                if previous is None or block > previous[1]:
                    touched_v3[market.pool] = (market, block)
            else:
                await self._process_v2_curve_log(market, log, live=True, observed_at=observed_at)
                touched_v2[market.curve] = market
    finally:
        setattr(self, "_roi_live_epoch_suppress_entries", False)

    _pending_v3(self).update(touched_v3)
    _pending_v2(self).update(touched_v2)
    setattr(self, "_roi_live_epoch_cursor", to_block)
    setattr(self, "_roi_live_epoch_factory_verified_through", to_block)
    setattr(self, "_roi_live_epoch_last_success_at", frontier._utcnow())
    setattr(self, "_roi_live_epoch_last_error_type", None)
    frontier._inc(self, "ranges_completed")

    latest_observed = int(getattr(self, "_roi_post177_head_observed_block", latest) or latest)
    remaining = max(0, latest_observed - to_block)
    ready = bool(_observer_head_fresh(self) and remaining <= forward.runtime.LIVE_LAG_BLOCKS)
    setattr(self, "_roi_live_epoch_ready", ready)
    setattr(
        self,
        "_roi_live_epoch_last_range",
        {
            "from_block": from_block,
            "to_block": to_block,
            "market_logs": len(market_logs),
            "forward_only": True,
            "observer_continuity_proven": True,
            "processing_backlog_remaining_blocks": remaining,
        },
    )
    if gap > frontier.MAX_LIVE_FRONTIER_GAP_BLOCKS:
        _inc(self, "processing_backlog_chunks")

    # Do not consume pending market evaluations until a fresh chain-head check proves
    # the processing cursor is decision-current. This preserves signals accumulated
    # in earlier backlog chunks rather than losing them when the final chunk is empty.
    if not ready or not await frontier._fresh_head_ready(self):
        return

    pending_v3 = list(_pending_v3(self).values())
    pending_v2 = list(_pending_v2(self).values())
    _clear_pending_markets(self)
    for market, block in pending_v3:
        await self._maybe_open_v3(market, current_block=block)
    for market in pending_v2:
        await self._maybe_open_v2(market)


async def _run_with_head_observer(self: Any, stop: asyncio.Event) -> None:
    if _ORIGINAL_ROBINHOOD_RUN is None:
        raise RuntimeError("post-177 Robinhood run repair is not installed")
    observer = asyncio.create_task(_observe_robinhood_head(self, stop), name="robinhood-head-observer")
    try:
        await _ORIGINAL_ROBINHOOD_RUN(self, stop)
    finally:
        observer.cancel()
        await asyncio.gather(observer, return_exceptions=True)
        task = getattr(self, "_roi_post177_rwa_refresh_task", None)
        if isinstance(task, asyncio.Task) and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


setattr(_run_with_head_observer, "_roi_post177_forward_pipeline", True)


def _forward_transport_compat_guard(original: Callable[..., Any]) -> Callable[..., Any]:
    """Satisfy legacy inner `_caught_up` checks only inside a verified forward decision.

    The global historical flag remains false. This shim exists solely because older
    entity wrappers captured the pre-forward-only readiness predicate by value.
    """

    @wraps(original)
    async def wrapped(self: Any, *args: Any, **kwargs: Any) -> Any:
        ready = bool(frontier._paper_transport_ready(self))
        prior = bool(getattr(self, "_caught_up", False))
        if ready and not prior:
            self._caught_up = True
            _inc(self, "legacy_caught_up_shims")
        try:
            return await original(self, *args, **kwargs)
        finally:
            if ready and not prior:
                self._caught_up = False

    setattr(wrapped, "_roi_post177_forward_transport_compat", True)
    return wrapped


def _robinhood_status_with_forward_semantics(self: Any) -> dict[str, Any]:
    if _ORIGINAL_ROBINHOOD_STATUS is None:
        raise RuntimeError("post-177 Robinhood status repair is not installed")
    payload = _ORIGINAL_ROBINHOOD_STATUS(self)
    ready = bool(frontier._paper_transport_ready(self))
    live_cursor = frontier._live_cursor(self)
    observed_head = getattr(self, "_roi_post177_head_observed_block", None)
    processing_lag = (
        max(0, int(observed_head) - int(live_cursor))
        if observed_head is not None and live_cursor is not None
        else None
    )

    # Preserve the old key only as a compatibility alias. It no longer describes a
    # historical catch-up requirement and must never be read as one.
    payload["caught_up_for_paper_decisions"] = ready
    payload["paper_decision_transport_ready"] = ready
    payload["forward_frontier_ready"] = ready
    payload["caught_up_for_paper_decisions_semantics"] = "deprecated_alias_for_forward_frontier_ready"
    payload["historical_cursor_has_paper_decision_authority"] = False
    payload["forward_frontier_readiness"] = {
        "repair_version": REPAIR_VERSION,
        "ready": ready,
        "observed_head_block": observed_head,
        "processing_cursor_block": live_cursor,
        "processing_lag_blocks": processing_lag,
        "observer_continuity_ok": bool(getattr(self, "_roi_post177_head_observer_continuity_ok", False)),
        "observer_generation": _observer_generation(self),
        "observer_last_success_at": getattr(self, "_roi_post177_head_observer_last_success_at", None),
        "observer_last_error_type": getattr(self, "_roi_post177_head_observer_last_error_type", None),
        "observer_last_gap_reason": getattr(self, "_roi_post177_head_observer_last_gap_reason", None),
        "observer_gap_count_session": int(getattr(self, "_roi_post177_head_observer_gaps", 0) or 0),
        "processing_backlog_chunks_session": int(getattr(self, "_roi_post177_processing_backlog_chunks", 0) or 0),
        "true_observation_reanchors_session": int(getattr(self, "_roi_post177_true_observation_reanchors", 0) or 0),
        "legacy_caught_up_shims_session": int(getattr(self, "_roi_post177_legacy_caught_up_shims", 0) or 0),
        "historical_catchup_required": False,
        "historical_cursor_is_archival_only": True,
        "large_processing_lag_is_observation_outage": False,
        "real_observer_gap_reanchors_forward_only": True,
        "paper_only": True,
        "live_money_authority": False,
    }
    live = payload.get("live_frontier_verification")
    if isinstance(live, dict):
        live["live_epoch_ready"] = ready
        live["historical_cursor_is_archival_only"] = True
        live["historical_backfill_can_authorize_entries"] = False
        live["processing_backlog_is_not_historical_catchup"] = True
        live["observer_continuity_required"] = True
    catchup = payload.get("catchup_capacity")
    if isinstance(catchup, dict):
        catchup["deprecated_historical_metrics_only"] = True
        catchup["historical_catchup_required_for_paper_decisions"] = False
        catchup["paper_entries_allowed_during_catchup"] = False
    funnel = payload.get("decision_funnel")
    if isinstance(funnel, dict):
        funnel["legacy_not_caught_up_count"] = int(funnel.get("not_caught_up") or 0)
        funnel["historical_catchup_is_not_rejection_authority"] = True
    rwa = payload.get("rwa_filter")
    if isinstance(rwa, dict):
        rwa["background_refresh_scheduled_session"] = int(getattr(self, "_roi_post177_rwa_refresh_scheduled", 0) or 0)
        rwa["background_refresh_completed_session"] = int(getattr(self, "_roi_post177_rwa_refresh_completed", 0) or 0)
        rwa["background_refresh_errors_session"] = int(getattr(self, "_roi_post177_rwa_refresh_errors", 0) or 0)
        rwa["background_refresh_last_error_type"] = getattr(self, "_roi_post177_rwa_refresh_last_error_type", None)
    return payload


setattr(_robinhood_status_with_forward_semantics, "_roi_post177_forward_pipeline", True)


async def _prewarm_durable_opportunity_immediately(plane: Any, swap: Any, key: str) -> None:
    """Start six-dimension risk work when the opportunity becomes durable.

    This has no entry authority. It only removes the previous artificial 20-second
    delay before collecting evidence needed by both immediate and continuation lanes.
    """

    async with venue._prewarm_sem(plane):
        collectors = getattr(getattr(plane, "service", None), "collectors", None)
        inner = getattr(collectors, "inner", None)
        coverage = getattr(inner, "refresh_coverage", None)
        candidate = getattr(inner, "refresh_candidate", None)
        try:
            now = _utcnow()

            async def run() -> None:
                if callable(coverage):
                    await coverage(swap.token_mint, now, current_swap=swap)
                if callable(candidate):
                    await candidate(swap.token_mint, now, current_swap=swap)

            await asyncio.wait_for(run(), timeout=venue.PREWARM_TIMEOUT_SECONDS)
            semantic._persist_risk_readthrough(plane, swap, as_of=_utcnow())
            venue._inc(plane, "prewarm_completed")
            _inc(plane, "immediate_risk_prewarm_completed")
        except asyncio.TimeoutError:
            venue._inc(plane, "prewarm_timeouts")
        except asyncio.CancelledError:
            raise
        except Exception:
            venue._inc(plane, "prewarm_errors")
        finally:
            venue._prewarm_last(plane)[key] = time.monotonic()


setattr(_prewarm_durable_opportunity_immediately, "_roi_post177_forward_pipeline", True)


def _event_time(row: dict[str, Any]) -> datetime | None:
    return _parse_dt(row.get("observed_at")) or _parse_dt(row.get("received_at"))


def _fomo_scan_rows_event_time(
    rows: list[dict[str, Any]], *, now: datetime
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Apply unchanged FOMO thresholds on event time, not delivery latency."""

    long_cutoff = now - timedelta(seconds=continuation.FOMO_LONG_WINDOW_SECONDS)
    short_cutoff = now - timedelta(seconds=continuation.FOMO_SHORT_WINDOW_SECONDS)
    grouped: dict[str, list[dict[str, Any]]] = {}
    source_counts: Counter[str] = Counter()
    delayed = 0
    accepted_rows = 0
    for row in rows:
        event_at = _event_time(row)
        if event_at is None or event_at < long_cutoff:
            continue
        token = str(row.get("token_mint") or "")
        if not token:
            continue
        grouped.setdefault(token, []).append(row)
        source_counts[str(row.get("source") or "unknown")] += 1
        accepted_rows += 1
        received = _parse_dt(row.get("received_at"))
        if received is not None and (received - event_at).total_seconds() > continuation.FOMO_SHORT_WINDOW_SECONDS:
            delayed += 1

    diagnostics: dict[str, Any] = {
        "version": "fomo-event-time-delivery-overlap-v1",
        "rows_scanned": len(rows),
        "rows_in_strategy_event_window": accepted_rows,
        "tokens_grouped": len(grouped),
        "tokens_with_buy": 0,
        "tokens_with_short_buy": 0,
        "rejected_min_buys": 0,
        "rejected_min_independent_buyers": 0,
        "rejected_net_buy_flow": 0,
        "rejected_acceleration": 0,
        "pre_fomo_candidates": 0,
        "active_fomo_candidates": 0,
        "candidates_before_cap": 0,
        "candidates_emitted": 0,
        "source_row_counts": dict(source_counts),
        "scanner_consuming_normalized_swaps": bool(rows),
        "event_time_authoritative": True,
        "delivery_latency_changes_strategy_window": False,
        "delayed_arrivals_over_short_window": delayed,
        "delivery_lookback_seconds": FOMO_DELIVERY_LOOKBACK_SECONDS,
        "strategy_window_seconds": continuation.FOMO_LONG_WINDOW_SECONDS,
    }
    candidates: list[dict[str, Any]] = []

    for token, raw_items in grouped.items():
        items = sorted(raw_items, key=lambda row: _event_time(row) or datetime.min.replace(tzinfo=timezone.utc))
        buys = [row for row in items if str(row.get("side") or "").lower() == "buy"]
        sells = [row for row in items if str(row.get("side") or "").lower() == "sell"]
        if buys:
            diagnostics["tokens_with_buy"] += 1
        short_buys = [row for row in buys if (_event_time(row) or long_cutoff) >= short_cutoff]
        if short_buys:
            diagnostics["tokens_with_short_buy"] += 1
        if not buys or not short_buys:
            continue

        buy_sol = sum(candidate_fomo._finite_nonnegative(row.get("native_amount_sol")) for row in buys)
        sell_sol = sum(candidate_fomo._finite_nonnegative(row.get("native_amount_sol")) for row in sells)
        buyers = len({str(row.get("wallet") or "") for row in buys if str(row.get("wallet") or "")})
        acceleration = (
            (len(short_buys) / max(continuation.FOMO_SHORT_WINDOW_SECONDS, 1.0))
            / (len(buys) / continuation.FOMO_LONG_WINDOW_SECONDS)
        )
        if len(buys) < 3:
            diagnostics["rejected_min_buys"] += 1
            continue
        if buyers < 2:
            diagnostics["rejected_min_independent_buyers"] += 1
            continue
        if buy_sol <= sell_sol:
            diagnostics["rejected_net_buy_flow"] += 1
            continue
        if acceleration < 0.9:
            diagnostics["rejected_acceleration"] += 1
            continue

        latest = buys[-1]
        score = acceleration + buyers / 2.0 + buy_sol / max(sell_sol, 0.01)
        state = "active_fomo" if len(short_buys) >= 2 and buyers >= 3 and acceleration >= 1.25 else "pre_fomo"
        diagnostics[f"{state}_candidates"] += 1
        candidates.append(
            {
                "token": token,
                "rows": items,
                "latest": latest,
                "buyers": buyers,
                "buy_sol": buy_sol,
                "sell_sol": sell_sol,
                "acceleration": acceleration,
                "score": score,
                "state": state,
            }
        )

    candidates.sort(key=lambda item: float(item["score"]), reverse=True)
    diagnostics["candidates_before_cap"] = len(candidates)
    emitted = candidates[: continuation.FOMO_MAX_CANDIDATES_PER_SCAN]
    diagnostics["candidates_emitted"] = len(emitted)
    return emitted, diagnostics


def _fomo_flow_candidates_with_event_time_overlap(adapter: Any) -> list[dict[str, Any]]:
    now = _utcnow()
    delivery_start = now - timedelta(seconds=FOMO_DELIVERY_LOOKBACK_SECONDS)
    event_start = now - timedelta(seconds=continuation.FOMO_LONG_WINDOW_SECONDS)
    continuation._continuation_schema(adapter)
    with adapter.store._lock:
        raw = adapter.store.db.execute(
            "SELECT id,signature,wallet,token_mint,side,token_amount,native_amount_sol,"
            "reference_price_sol,observed_at,received_at,source FROM normalized_swaps "
            "WHERE received_at>=? OR observed_at>=? ORDER BY id DESC LIMIT ?",
            (delivery_start.isoformat(), event_start.isoformat(), continuation.FOMO_MAX_ROWS_PER_SCAN),
        ).fetchall()
    rows = [dict(row) for row in reversed(raw)]
    candidates, diagnostics = _fomo_scan_rows_event_time(rows, now=now)
    latest_received = max((str(row.get("received_at") or "") for row in rows), default="")
    latest_observed = max((str(row.get("observed_at") or "") for row in rows), default="")
    durable_cursor = max((int(row.get("id") or 0) for row in rows), default=0)
    diagnostics.update(
        {
            "scanner_last_scan_at": now.isoformat(),
            "scanner_latest_normalized_swap_received_at": latest_received,
            "scanner_latest_normalized_swap_observed_at": latest_observed,
            "scanner_window_seconds": continuation.FOMO_LONG_WINDOW_SECONDS,
            "scanner_row_cap": continuation.FOMO_MAX_ROWS_PER_SCAN,
            "scanner_candidate_cap": continuation.FOMO_MAX_CANDIDATES_PER_SCAN,
            "durable_cursor_id": durable_cursor,
            "cursor_mode": "durable_high_water_plus_event_time_overlap",
        }
    )
    candidate_fomo._write_fomo_runtime(adapter, {f"diag:{key}": value for key, value in diagnostics.items()})
    setattr(adapter, "_roi_fomo_scanner_last_diagnostics", diagnostics)
    return candidates


setattr(_fomo_flow_candidates_with_event_time_overlap, "_roi_post177_forward_pipeline", True)


def _direct_status_with_post177(self: Any) -> dict[str, Any]:
    if _ORIGINAL_DIRECT_STATUS is None:
        raise RuntimeError("post-177 DirectSolana status repair is not installed")
    payload = _ORIGINAL_DIRECT_STATUS(self)
    retention = payload.get("ephemeral_candidate_retention")
    if isinstance(retention, dict):
        retention.update(
            {
                "entry_window_seconds": IMMEDIATE_COPY_WINDOW_SECONDS,
                "immediate_copy_window_seconds": IMMEDIATE_COPY_WINDOW_SECONDS,
                "operational_hydration_retention_seconds": CANDIDATE_OPERATIONAL_RETENTION_SECONDS,
                "twenty_seconds_is_hydration_prune_boundary": False,
                "expired_candidate_hydration_work_pruned": False,
                "hydration_work_pruned_after_operational_timeout": True,
                "continuation_context_preserved_after_immediate_window": True,
                "repair_version": REPAIR_VERSION,
            }
        )
    post = payload.get("post104_architecture_repair")
    if isinstance(post, dict):
        post.update(
            {
                "candidate_entry_window_seconds_unchanged": IMMEDIATE_COPY_WINDOW_SECONDS,
                "candidate_context_operational_timeout_seconds": CANDIDATE_OPERATIONAL_RETENTION_SECONDS,
                "candidate_context_20s_hard_cutoff_active": False,
                "continuation_context_collection_after_20s": True,
            }
        )
    graph = payload.get("venue_native_candidate_graph_repair")
    if isinstance(graph, dict):
        graph.update(
            {
                "continuation_prewarm_starts_after_immediate_window": False,
                "risk_prewarm_start_policy": "immediate_after_durable_opportunity",
                "risk_prewarm_entry_authority": False,
                "post177_repair_version": REPAIR_VERSION,
            }
        )
    policy = payload.setdefault("provider_runtime_policy", {})
    if isinstance(policy, dict):
        policy.update(
            {
                "candidate_hydration_retention_uses_operational_timeout": True,
                "candidate_20s_is_immediate_copy_context_only": True,
                "durable_opportunity_risk_prewarm_immediate": True,
                "fomo_delivery_window_separate_from_strategy_event_window": True,
                "strategy_thresholds_changed": False,
                "certification_thresholds_changed": False,
                "paper_only_authority_unchanged": True,
                "signing_or_submission_available": False,
            }
        )
    payload["post177_forward_pipeline_bottleneck_repair"] = {
        "installed": True,
        "version": REPAIR_VERSION,
        "candidate_operational_retention_seconds": CANDIDATE_OPERATIONAL_RETENTION_SECONDS,
        "immediate_copy_window_seconds": IMMEDIATE_COPY_WINDOW_SECONDS,
        "risk_prewarm_immediate_after_durable_opportunity": True,
        "fomo_event_time_authoritative": True,
        "fomo_delivery_lookback_seconds": FOMO_DELIVERY_LOOKBACK_SECONDS,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }
    return payload


setattr(_direct_status_with_post177, "_roi_post177_forward_pipeline", True)


def _replace_legacy_robinhood_blocker(value: Any) -> Any:
    if isinstance(value, str):
        return (
            "robinhood_forward_frontier_not_ready"
            if value == "robinhood_not_caught_up_for_paper_decisions"
            else value
        )
    if isinstance(value, list):
        return [_replace_legacy_robinhood_blocker(item) for item in value]
    if isinstance(value, dict):
        return {key: _replace_legacy_robinhood_blocker(item) for key, item in value.items()}
    return value


def _unified_status_with_forward_readiness(
    base_status: dict[str, Any], runtime: Any, robinhood_status: dict[str, Any]
) -> dict[str, Any]:
    if _ORIGINAL_UNIFIED_STATUS is None:
        raise RuntimeError("post-177 unified status repair is not installed")
    payload = _ORIGINAL_UNIFIED_STATUS(base_status, runtime, robinhood_status)
    payload = _replace_legacy_robinhood_blocker(payload)
    robinhood = payload.get("robinhood") if isinstance(payload.get("robinhood"), dict) else {}
    robinhood["readiness_semantics"] = {
        "historical_catchup_required": False,
        "forward_frontier_authoritative": True,
        "paper_decision_transport_ready": bool(robinhood_status.get("paper_decision_transport_ready")),
        "legacy_caught_up_key_is_compatibility_alias": True,
        "repair_version": REPAIR_VERSION,
    }
    payload["robinhood"] = robinhood
    overall = payload.get("overall") if isinstance(payload.get("overall"), dict) else {}
    overall["post177_forward_pipeline_bottleneck_repair"] = REPAIR_VERSION
    payload["overall"] = overall
    return payload


setattr(_unified_status_with_forward_readiness, "_roi_post177_forward_pipeline", True)


def install_post177_forward_pipeline_bottleneck_repair(plane_cls: type[Any]) -> None:
    """Install the coordinated post-PR177 bottleneck repair before workers start."""

    global _INSTALLED, _ORIGINAL_ROBINHOOD_RUN, _ORIGINAL_ROBINHOOD_V2, _ORIGINAL_ROBINHOOD_V3
    global _ORIGINAL_ROBINHOOD_STATUS, _ORIGINAL_DIRECT_STATUS, _ORIGINAL_UNIFIED_STATUS
    if _INSTALLED:
        return

    # v5.1: 20 seconds is the immediate-copy context, not a deletion deadline.
    ephemeral.ENTRY_WINDOW_SECONDS = CANDIDATE_OPERATIONAL_RETENTION_SECONDS
    post104.CANDIDATE_ENTRY_WINDOW_SECONDS = CANDIDATE_OPERATIONAL_RETENTION_SECONDS

    # Keep the already-designed work-conserving hydration/RPC priority layers in the
    # final composition after every later wrapper has been installed.
    hydration_flex.install_candidate_hydration_work_conserving_repair()
    candidate_rpc.install_candidate_rpc_priority_repair()

    # Durable opportunities should begin collecting risk evidence immediately. The
    # original prewarm remains zero-authority and all exact quote/mechanical gates stay.
    venue._prewarm_after_immediate_window = _prewarm_durable_opportunity_immediately  # type: ignore[assignment]

    # FOMO reads a bounded delivery overlap but applies unchanged thresholds on chain
    # event time. A slow normalizer can no longer shrink the strategy's time window.
    candidate_fomo._fomo_scan_rows = _fomo_scan_rows_event_time  # type: ignore[assignment]
    candidate_fomo._fomo_flow_candidates_with_diagnostics = _fomo_flow_candidates_with_event_time_overlap  # type: ignore[assignment]
    continuation._fomo_flow_candidates = _fomo_flow_candidates_with_event_time_overlap  # type: ignore[assignment]

    # Replace only the live-frontier function looked up dynamically by the installed
    # poll wrapper. Historical `_cursor` remains untouched and archival.
    frontier._advance_live_epoch = _advance_observed_forward_epoch  # type: ignore[assignment]

    current_run = plane_cls.run
    if not bool(getattr(current_run, "_roi_post177_forward_pipeline", False)):
        _ORIGINAL_ROBINHOOD_RUN = current_run
        plane_cls.run = _run_with_head_observer  # type: ignore[method-assign]

    current_v2 = plane_cls._maybe_open_v2
    if not bool(getattr(current_v2, "_roi_post177_forward_transport_compat", False)):
        _ORIGINAL_ROBINHOOD_V2 = current_v2
        plane_cls._maybe_open_v2 = _forward_transport_compat_guard(current_v2)  # type: ignore[method-assign]
    current_v3 = plane_cls._maybe_open_v3
    if not bool(getattr(current_v3, "_roi_post177_forward_transport_compat", False)):
        _ORIGINAL_ROBINHOOD_V3 = current_v3
        plane_cls._maybe_open_v3 = _forward_transport_compat_guard(current_v3)  # type: ignore[method-assign]

    current_status = plane_cls.status
    if not bool(getattr(current_status, "_roi_post177_forward_pipeline", False)):
        _ORIGINAL_ROBINHOOD_STATUS = current_status
        plane_cls.status = _robinhood_status_with_forward_semantics  # type: ignore[method-assign]

    current_direct_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_direct_status, "_roi_post177_forward_pipeline", False)):
        _ORIGINAL_DIRECT_STATUS = current_direct_status
        try:
            _direct_status_with_post177.__dict__.update(getattr(current_direct_status, "__dict__", {}))
        except Exception:
            pass
        DirectSolanaIngestionPlane.status = _direct_status_with_post177  # type: ignore[method-assign]

    current_unified = unified_status.build_unified_strategy_status
    if not bool(getattr(current_unified, "_roi_post177_forward_pipeline", False)):
        _ORIGINAL_UNIFIED_STATUS = current_unified
        unified_status.build_unified_strategy_status = _unified_status_with_forward_readiness

    setattr(plane_cls, "_roi_post177_forward_pipeline_bottleneck_repair_installed", True)
    setattr(plane_cls, "_roi_post177_forward_pipeline_bottleneck_repair_version", REPAIR_VERSION)
    _INSTALLED = True


__all__ = [
    "REPAIR_VERSION",
    "CANDIDATE_OPERATIONAL_RETENTION_SECONDS",
    "FOMO_DELIVERY_LOOKBACK_SECONDS",
    "_advance_observed_forward_epoch",
    "_fomo_scan_rows_event_time",
    "_forward_transport_compat_guard",
    "install_post177_forward_pipeline_bottleneck_repair",
]
