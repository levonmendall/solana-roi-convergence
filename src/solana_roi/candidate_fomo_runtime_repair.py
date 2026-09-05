from __future__ import annotations

import math
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from . import continuation_market_recalibration as continuation
from . import economic_signal_continuation_repair as economic
from . import scout_candidate_continuity_repair as scout
from . import venue_native_candidate_graph_repair as venue
from .direct_solana import DirectSolanaIngestionPlane
from .profit_first_entity_final_research import FinalProfitFirstResearchAdapter


REPAIR_VERSION = "candidate-fomo-runtime-observability-v1"
PUMP_ENDPOINT_RECOVERY_VERSION = "pump-economic-endpoint-fallback-v1"
FOMO_SCANNER_DIAGNOSTICS_VERSION = "fomo-normalized-swap-scanner-diagnostics-v1"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False

_FILTERED_NON_CANDIDATE_REASONS = frozenset(
    {
        "supported_swap_source_missing",
        "economic_token_movement_missing",
    }
)
_GRAPH_FALLBACK_ERRORS = frozenset(
    {
        "semantic_graph_actor_legs_missing",
        "semantic_native_wsol_direction_ambiguous",
    }
)

_ORIGINAL_GRAPH_SWAP_FACTS: Callable[..., Any] | None = None
_ORIGINAL_DIRECT_STATUS: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_FOMO_OPEN: Callable[..., Any] | None = None
_ORIGINAL_FINAL_STATUS: Callable[..., dict[str, Any]] | None = None
_INSTALLED = False


def _inc(obj: Any, name: str, amount: int = 1) -> None:
    attr = f"_roi_candidate_fomo_repair_{name}"
    setattr(obj, attr, int(getattr(obj, attr, 0) or 0) + int(amount))


def _pump_graph_with_economic_endpoint(
    result: dict[str, Any], *, wallet: str, source: str
) -> tuple[tuple[str, str, float, float] | None, str | None]:
    """Recover one proven Pump.fun actor movement without weakening venue proof.

    The existing venue graph remains first authority. This fallback is reached only
    when Pump.fun was already proven to execute in the transaction, but incomplete
    token-account ownership metadata prevented the graph from tying the unique
    economic endpoint to the exact configured scout. PR #167's venue-agnostic
    economic-movement proof supplies that missing actor fact. Ambiguous or unpriced
    movement remains fail-closed.
    """
    if _ORIGINAL_GRAPH_SWAP_FACTS is None:
        raise RuntimeError("candidate/FOMO runtime repair is not installed")

    facts, error = _ORIGINAL_GRAPH_SWAP_FACTS(result, wallet=wallet, source=source)
    if facts is not None:
        return facts, error
    if str(source) != "PUMP_FUN" or str(error or "") not in _GRAPH_FALLBACK_ERRORS:
        return None, error

    plane = scout._SCOUT_HYDRATION_PLANE.get()
    if plane is not None:
        _inc(plane, "pump_endpoint_fallback_attempts")

    movement, movement_error = economic._economic_movement(result, wallet)
    if movement is None:
        if plane is not None:
            _inc(plane, "pump_endpoint_fallback_fail_closed")
        return None, error or movement_error

    native_amount = movement.get("native_amount_sol")
    try:
        token_amount = float(movement.get("token_amount") or 0.0)
        native = float(native_amount) if native_amount is not None else 0.0
    except (TypeError, ValueError):
        token_amount = native = 0.0
    side = str(movement.get("side") or "")
    token_mint = str(movement.get("token_mint") or "")

    if (
        side not in {"buy", "sell"}
        or not token_mint
        or not math.isfinite(token_amount)
        or not math.isfinite(native)
        or token_amount <= 0.0
        or native <= 0.0
    ):
        if plane is not None:
            _inc(plane, "pump_endpoint_fallback_fail_closed")
        return None, error or movement_error or "economic_endpoint_unpriced"

    if plane is not None:
        _inc(plane, "pump_endpoint_fallback_resolved")
    return (side, token_mint, token_amount, native), None


setattr(_pump_graph_with_economic_endpoint, "_roi_candidate_fomo_runtime_repair", True)


def _reclassify_candidate_telemetry(payload: dict[str, Any], obj: Any) -> None:
    status = payload.get("scout_candidate_continuity_repair")
    if not isinstance(status, dict):
        return

    raw_reasons = dict(status.get("candidate_normalization_failure_reasons") or {})
    filtered = {
        reason: int(count or 0)
        for reason, count in raw_reasons.items()
        if reason in _FILTERED_NON_CANDIDATE_REASONS
    }
    true_failures = {
        reason: int(count or 0)
        for reason, count in raw_reasons.items()
        if reason not in _FILTERED_NON_CANDIDATE_REASONS
    }
    raw_failed = int(status.get("candidate_normalization_failed_session") or 0)
    filtered_count = sum(filtered.values())
    true_failure_count = sum(true_failures.values())

    status.update(
        {
            "telemetry_semantics_version": REPAIR_VERSION,
            "candidate_normalization_failed_session_raw": raw_failed,
            "candidate_normalization_failure_reasons_raw": raw_reasons,
            "filtered_non_candidate_transactions_session": filtered_count,
            "filtered_non_candidate_reasons": filtered,
            "candidate_normalization_failed_session": true_failure_count,
            "candidate_normalization_failure_reasons": true_failures,
            "supported_swap_source_missing_is_decoder_failure": False,
            "economic_token_movement_missing_is_decoder_failure": False,
            "candidate_failure_denominator_excludes_non_candidate_scout_activity": True,
            "pump_endpoint_recovery_version": PUMP_ENDPOINT_RECOVERY_VERSION,
            "pump_endpoint_fallback_attempts_session": int(
                getattr(obj, "_roi_candidate_fomo_repair_pump_endpoint_fallback_attempts", 0) or 0
            ),
            "pump_endpoint_fallback_resolved_session": int(
                getattr(obj, "_roi_candidate_fomo_repair_pump_endpoint_fallback_resolved", 0) or 0
            ),
            "pump_endpoint_fallback_fail_closed_session": int(
                getattr(obj, "_roi_candidate_fomo_repair_pump_endpoint_fallback_fail_closed", 0) or 0
            ),
        }
    )


def _direct_status_with_candidate_semantics(self: Any) -> dict[str, Any]:
    if _ORIGINAL_DIRECT_STATUS is None:
        raise RuntimeError("candidate telemetry status repair is not installed")
    payload = _ORIGINAL_DIRECT_STATUS(self)
    _reclassify_candidate_telemetry(payload, self)
    return payload


setattr(_direct_status_with_candidate_semantics, "_roi_candidate_fomo_runtime_repair", True)


def _finite_nonnegative(value: Any) -> float:
    try:
        number = float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0
    return max(0.0, number) if math.isfinite(number) else 0.0


def _fomo_scan_rows(
    rows: list[dict[str, Any]], *, now: datetime
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Apply the existing independent-FOMO thresholds while exposing every stage."""
    short_cutoff = (now - timedelta(seconds=continuation.FOMO_SHORT_WINDOW_SECONDS)).isoformat()
    grouped: dict[str, list[dict[str, Any]]] = {}
    source_counts: Counter[str] = Counter()
    for row in rows:
        token = str(row.get("token_mint") or "")
        if not token:
            continue
        grouped.setdefault(token, []).append(row)
        source_counts[str(row.get("source") or "unknown")] += 1

    diagnostics: dict[str, Any] = {
        "version": FOMO_SCANNER_DIAGNOSTICS_VERSION,
        "rows_scanned": len(rows),
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
    }
    candidates: list[dict[str, Any]] = []

    for token, items in grouped.items():
        buys = [row for row in items if str(row.get("side") or "").lower() == "buy"]
        sells = [row for row in items if str(row.get("side") or "").lower() == "sell"]
        if buys:
            diagnostics["tokens_with_buy"] += 1
        short_buys = [
            row
            for row in buys
            if str(row.get("received_at") or "") >= short_cutoff
        ]
        if short_buys:
            diagnostics["tokens_with_short_buy"] += 1
        if not buys or not short_buys:
            continue

        buy_sol = sum(_finite_nonnegative(row.get("native_amount_sol")) for row in buys)
        sell_sol = sum(_finite_nonnegative(row.get("native_amount_sol")) for row in sells)
        buyers = len(
            {
                str(row.get("wallet") or "")
                for row in buys
                if str(row.get("wallet") or "")
            }
        )
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
        state = (
            "active_fomo"
            if len(short_buys) >= 2 and buyers >= 3 and acceleration >= 1.25
            else "pre_fomo"
        )
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


def _write_fomo_runtime(adapter: Any, values: dict[str, Any]) -> None:
    import json

    continuation._continuation_schema(adapter)
    now = datetime.now(timezone.utc).isoformat()
    with adapter.store._lock, adapter.store.db:
        for key, value in values.items():
            if isinstance(value, (dict, list, tuple, bool)):
                text = json.dumps(value, sort_keys=True, separators=(",", ":"))
            elif value is None:
                text = ""
            else:
                text = str(value)
            adapter.store.db.execute(
                "INSERT OR REPLACE INTO independent_fomo_runtime(key,value,updated_at) VALUES (?,?,?)",
                (str(key), text, now),
            )


def _fomo_flow_candidates_with_diagnostics(adapter: Any) -> list[dict[str, Any]]:
    now = datetime.now(timezone.utc)
    start = (now - timedelta(seconds=continuation.FOMO_LONG_WINDOW_SECONDS)).isoformat()
    continuation._continuation_schema(adapter)
    with adapter.store._lock:
        raw = adapter.store.db.execute(
            "SELECT signature,wallet,token_mint,side,token_amount,native_amount_sol,"
            "reference_price_sol,observed_at,received_at,source "
            "FROM normalized_swaps WHERE received_at>=? ORDER BY received_at DESC,id DESC LIMIT ?",
            (start, continuation.FOMO_MAX_ROWS_PER_SCAN),
        ).fetchall()
    rows = [dict(row) for row in reversed(raw)]
    candidates, diagnostics = _fomo_scan_rows(rows, now=now)
    latest_received_at = max(
        (str(row.get("received_at") or "") for row in rows),
        default="",
    )
    diagnostics.update(
        {
            "scanner_last_scan_at": now.isoformat(),
            "scanner_latest_normalized_swap_received_at": latest_received_at,
            "scanner_window_seconds": continuation.FOMO_LONG_WINDOW_SECONDS,
            "scanner_row_cap": continuation.FOMO_MAX_ROWS_PER_SCAN,
            "scanner_candidate_cap": continuation.FOMO_MAX_CANDIDATES_PER_SCAN,
        }
    )
    _write_fomo_runtime(adapter, {f"diag:{key}": value for key, value in diagnostics.items()})
    setattr(adapter, "_roi_fomo_scanner_last_diagnostics", diagnostics)
    return candidates


async def _open_independent_fomo_with_diagnostics(adapter: Any, candidate: dict[str, Any]) -> bool:
    if _ORIGINAL_FOMO_OPEN is None:
        raise RuntimeError("independent FOMO open diagnostics are not installed")
    _inc(adapter, "fomo_open_attempts")
    opened = bool(await _ORIGINAL_FOMO_OPEN(adapter, candidate))
    if opened:
        _inc(adapter, "fomo_open_successes")
        result = "opened"
    else:
        _inc(adapter, "fomo_open_downstream_rejections")
        result = "rejected_downstream_risk_execution_capacity_or_duplicate"
    _write_fomo_runtime(
        adapter,
        {
            "diag:open_attempts_session": int(getattr(adapter, "_roi_candidate_fomo_repair_fomo_open_attempts", 0) or 0),
            "diag:open_successes_session": int(getattr(adapter, "_roi_candidate_fomo_repair_fomo_open_successes", 0) or 0),
            "diag:open_downstream_rejections_session": int(
                getattr(adapter, "_roi_candidate_fomo_repair_fomo_open_downstream_rejections", 0) or 0
            ),
            "diag:last_open_result": result,
            "diag:last_open_token": str(candidate.get("token") or ""),
        },
    )
    return opened


def _read_fomo_runtime(adapter: Any) -> dict[str, Any]:
    import json

    continuation._continuation_schema(adapter)
    with adapter.store._lock:
        rows = adapter.store.db.execute(
            "SELECT key,value,updated_at FROM independent_fomo_runtime WHERE key LIKE 'diag:%'"
        ).fetchall()
    output: dict[str, Any] = {}
    for raw in rows:
        row = dict(raw)
        key = str(row.get("key") or "")
        value = str(row.get("value") or "")
        if key.startswith("diag:"):
            key = key[5:]
        parsed: Any = value
        if value in {"true", "false"}:
            parsed = value == "true"
        else:
            try:
                parsed = json.loads(value)
            except Exception:
                try:
                    parsed = int(value)
                except (TypeError, ValueError):
                    try:
                        parsed = float(value)
                    except (TypeError, ValueError):
                        parsed = value
        output[key] = parsed
        output["updated_at"] = str(row.get("updated_at") or "")
    return output


def _final_status_with_fomo_scanner(self: Any) -> dict[str, Any]:
    if _ORIGINAL_FINAL_STATUS is None:
        raise RuntimeError("FOMO scanner status repair is not installed")
    payload = _ORIGINAL_FINAL_STATUS(self)
    try:
        diagnostics = _read_fomo_runtime(self)
        error = None
    except Exception as exc:
        diagnostics = {}
        error = f"{type(exc).__name__}: FOMO scanner diagnostics unavailable"

    section = payload.setdefault("continuation_market_recalibration", {})
    if isinstance(section, dict):
        section["fomo_scanner_diagnostics"] = {
            "version": FOMO_SCANNER_DIAGNOSTICS_VERSION,
            **diagnostics,
            "last_error": error,
            "zero_candidates_is_distinguishable_from_scanner_inactivity": True,
            "thresholds_changed": False,
            "paper_only": PAPER_ONLY,
            "live_money_authority": LIVE_MONEY_AUTHORITY,
            "signing_available": SIGNING_AVAILABLE,
            "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
        }
    return payload


setattr(_final_status_with_fomo_scanner, "_roi_candidate_fomo_runtime_repair", True)


def install_candidate_fomo_runtime_repair() -> None:
    """Install narrow post-PR167 candidate/FOMO repairs before runtime workers start."""
    global _ORIGINAL_GRAPH_SWAP_FACTS, _ORIGINAL_DIRECT_STATUS, _ORIGINAL_FOMO_OPEN
    global _ORIGINAL_FINAL_STATUS, _INSTALLED

    if _INSTALLED:
        return

    continuation.install_continuation_market_recalibration()

    _ORIGINAL_GRAPH_SWAP_FACTS = venue._graph_swap_facts
    venue._graph_swap_facts = _pump_graph_with_economic_endpoint  # type: ignore[assignment]

    _ORIGINAL_DIRECT_STATUS = DirectSolanaIngestionPlane.status
    DirectSolanaIngestionPlane.status = _direct_status_with_candidate_semantics  # type: ignore[method-assign]

    continuation._fomo_flow_candidates = _fomo_flow_candidates_with_diagnostics  # type: ignore[assignment]
    _ORIGINAL_FOMO_OPEN = continuation._open_independent_fomo
    continuation._open_independent_fomo = _open_independent_fomo_with_diagnostics  # type: ignore[assignment]

    _ORIGINAL_FINAL_STATUS = FinalProfitFirstResearchAdapter.status
    FinalProfitFirstResearchAdapter.status = _final_status_with_fomo_scanner  # type: ignore[method-assign]

    _INSTALLED = True


__all__ = [
    "REPAIR_VERSION",
    "FOMO_SCANNER_DIAGNOSTICS_VERSION",
    "_fomo_scan_rows",
    "_pump_graph_with_economic_endpoint",
    "_reclassify_candidate_telemetry",
    "install_candidate_fomo_runtime_repair",
]
