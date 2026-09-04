from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from .direct_solana import DirectSolanaIngestionPlane
from .ingestion import NormalizedSwap
from .wallet_discovery import MANIPULATION_BLOCKERS, SIDE_WALLET_BLOCKERS


SCOUT_REASONS = frozenset(
    {"frozen_scout_processed_trigger", "frozen_scout_live_poll_trigger"}
)
HANDOFF_TASK_LIMIT = 16
ENTRY_WINDOW_SECONDS = 20.0
MAX_CHASE_FRACTION = 0.15
SEEN_SIGNATURE_LIMIT = 4096

_ORIGINAL_HYDRATE_ONE: Callable[..., Any] | None = None
_ORIGINAL_DIRECT_STATUS: Callable[..., dict[str, Any]] | None = None


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _inc(obj: Any, name: str, amount: int = 1) -> None:
    attr = f"_roi_candidate_v4_handoff_{name}"
    setattr(obj, attr, int(getattr(obj, attr, 0) or 0) + int(amount))


def _set_last(obj: Any, *, blocker: str | None = None, error_type: str | None = None) -> None:
    setattr(obj, "_roi_candidate_v4_handoff_last_blocker", blocker)
    setattr(obj, "_roi_candidate_v4_handoff_last_error_type", error_type)


def _tasks(obj: Any) -> set[asyncio.Task[Any]]:
    current = getattr(obj, "_roi_candidate_v4_handoff_tasks", None)
    if not isinstance(current, set):
        current = set()
        setattr(obj, "_roi_candidate_v4_handoff_tasks", current)
    return current


def _seen(obj: Any) -> dict[str, None]:
    current = getattr(obj, "_roi_candidate_v4_handoff_seen", None)
    if not isinstance(current, dict):
        current = {}
        setattr(obj, "_roi_candidate_v4_handoff_seen", current)
    return current


def _claim_signature(obj: Any, signature: str) -> bool:
    seen = _seen(obj)
    if signature in seen:
        return False
    seen[signature] = None
    while len(seen) > SEEN_SIGNATURE_LIMIT:
        seen.pop(next(iter(seen)))
    return True


def _attached_discovery(obj: Any) -> Any | None:
    return getattr(obj, "_roi_candidate_v4_wallet_discovery", None)


def attach_candidate_v4_wallet_discovery(
    direct: DirectSolanaIngestionPlane,
    discovery: Any,
) -> None:
    """Attach the already-built wallet/V4 research plane to the direct candidate plane.

    This is dependency wiring only. It grants no promotion, paper-entry, signing or
    transaction-submission authority.
    """

    setattr(direct, "_roi_candidate_v4_wallet_discovery", discovery)
    setattr(direct, "_roi_candidate_v4_runtime_attached", True)


def _queue_status(obj: Any, signature: str) -> str:
    try:
        with obj.store._lock:
            row = obj.store.db.execute(
                "SELECT status FROM direct_solana_hydration_queue WHERE signature=?",
                (signature,),
            ).fetchone()
        return str(row["status"] or "") if row is not None else ""
    except Exception:
        return ""


def _load_normalized_swap(obj: Any, signature: str) -> NormalizedSwap | None:
    try:
        with obj.store._lock:
            row = obj.store.db.execute(
                "SELECT signature,slot,observed_at,received_at,wallet,token_mint,side,token_amount,"
                "native_amount_sol,reference_price_sol,source FROM normalized_swaps "
                "WHERE signature=? ORDER BY id DESC LIMIT 1",
                (signature,),
            ).fetchone()
        if row is None:
            return None
        return NormalizedSwap(
            signature=str(row["signature"]),
            slot=int(row["slot"]),
            observed_at=datetime.fromisoformat(str(row["observed_at"])),
            received_at=datetime.fromisoformat(str(row["received_at"])),
            wallet=str(row["wallet"]),
            token_mint=str(row["token_mint"]),
            side=str(row["side"]),
            token_amount=float(row["token_amount"]),
            native_amount_sol=float(row["native_amount_sol"]),
            reference_price_sol=float(row["reference_price_sol"]),
            source=str(row["source"]),
        )
    except Exception:
        return None


def _matching_existing_quote(obj: Any, swap: NormalizedSwap) -> dict[str, Any] | None:
    """Reuse a quote the primary ingestion service already captured for this touch.

    The legacy quote ledger predates source-signature correlation, so match only a
    same-token, post-trigger quote with the exact scout reference price (within a
    tiny floating-point tolerance). If that proof is not present, take a new
    explicit evidence-only probe rather than assuming equivalence.
    """

    try:
        with obj.store._lock:
            rows = obj.store.db.execute(
                "SELECT token_mint,stage,effective_price_sol,scout_reference_price_sol,drift_fraction,"
                "received_at,chain_to_quote_ms,usable,reason FROM execution_quote_observations "
                "WHERE token_mint=? AND received_at>=? ORDER BY id DESC LIMIT 20",
                (swap.token_mint, swap.received_at.isoformat()),
            ).fetchall()
    except Exception:
        return None
    tolerance = max(1e-15, abs(swap.reference_price_sol) * 1e-9)
    for raw in rows:
        row = dict(raw)
        try:
            if abs(float(row["scout_reference_price_sol"]) - swap.reference_price_sol) > tolerance:
                continue
        except (TypeError, ValueError):
            continue
        row["usable"] = bool(row.get("usable"))
        return row
    return None


async def _canonical_quote(obj: Any, swap: NormalizedSwap) -> Any | None:
    existing = _matching_existing_quote(obj, swap)
    if existing is not None:
        _inc(obj, "quote_reused")
        return existing

    handoff = getattr(getattr(obj, "service", None), "quote_handoff", None)
    observe = getattr(handoff, "observe", None)
    if not callable(observe):
        _inc(obj, "quote_unavailable")
        return None
    _inc(obj, "quote_attempted")
    try:
        return await observe(
            token_mint=swap.token_mint,
            stage="v4_forward_probe",
            fraction_of_full_position=1.0,
            scout_reference_price_sol=swap.reference_price_sol,
            trigger_observed_at=swap.observed_at,
        )
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        _inc(obj, "quote_errors")
        _set_last(obj, blocker="canonical_quote_exception", error_type=type(exc).__name__)
        return None


def _quote_field(quote: Any, name: str, default: Any = None) -> Any:
    if isinstance(quote, dict):
        return quote.get(name, default)
    return getattr(quote, name, default)


def _quote_usable(quote: Any) -> bool:
    if quote is None:
        return False
    try:
        return bool(
            _quote_field(quote, "usable", False)
            and float(_quote_field(quote, "drift_fraction", 1.0)) <= MAX_CHASE_FRACTION
            and float(_quote_field(quote, "chain_to_quote_ms", ENTRY_WINDOW_SECONDS * 1000.0 + 1.0))
            <= ENTRY_WINDOW_SECONDS * 1000.0
        )
    except (TypeError, ValueError):
        return False


def _risk_flags(discovery: Any, swap: NormalizedSwap, snapshot: Any, at: datetime) -> tuple[bool, bool]:
    blockers = set(getattr(snapshot, "blockers", ()) or ())
    manipulation = bool(blockers & MANIPULATION_BLOCKERS)
    side_wallet = bool(blockers & SIDE_WALLET_BLOCKERS)
    try:
        component = discovery.entity_resolver.component(swap.wallet, as_of=at)
        side_wallet = side_wallet or len(component) > 1
    except Exception:
        side_wallet = True
    return manipulation, side_wallet


def _insert_forward_observation(
    obj: Any,
    *,
    swap: NormalizedSwap,
    quote: Any | None,
    risk_complete: bool,
    manipulation_flag: bool,
    side_wallet_flag: bool,
    copyable: bool,
) -> bool:
    now = _utcnow()
    quote_price = _quote_field(quote, "effective_price_sol") if quote is not None else None
    chase = _quote_field(quote, "drift_fraction") if quote is not None else None
    quote_lag_ms = _quote_field(quote, "chain_to_quote_ms") if quote is not None else None
    try:
        observation_lag_ms = float(quote_lag_ms) if quote_lag_ms is not None else max(
            swap.ingestion_latency_ms,
            max(0.0, (now - swap.observed_at).total_seconds() * 1000.0),
        )
    except (TypeError, ValueError):
        observation_lag_ms = max(0.0, (now - swap.observed_at).total_seconds() * 1000.0)

    with obj.store._lock, obj.store.db:
        cursor = obj.store.db.execute(
            "INSERT OR IGNORE INTO wallet_discovery_forward_observations("
            "signature,wallet,token_mint,side,token_amount,observed_at,received_at,wallet_price_sol,"
            "copyable_price_sol,chase_fraction,copyable,observation_lag_ms,risk_complete,manipulation_flag,"
            "side_wallet_flag,source) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                swap.signature,
                swap.wallet,
                swap.token_mint,
                swap.side,
                float(swap.token_amount),
                swap.observed_at.isoformat(),
                swap.received_at.isoformat(),
                float(swap.reference_price_sol),
                float(quote_price) if quote_price is not None else None,
                float(chase) if chase is not None else None,
                1 if copyable else 0,
                float(observation_lag_ms),
                1 if risk_complete else 0,
                1 if manipulation_flag else 0,
                1 if side_wallet_flag else 0,
                "direct-candidate-v4:" + swap.source,
            ),
        )
    if cursor.rowcount != 1:
        _inc(obj, "forward_duplicate")
        return False

    _inc(obj, "forward_inserted")
    obj.store.append(
        "candidate_risk_quote_v4_handoff",
        now.isoformat(),
        {
            "signature": swap.signature,
            "token_mint": swap.token_mint,
            "wallet": swap.wallet,
            "side": swap.side,
            "risk_complete": risk_complete,
            "canonical_quote_observed": quote is not None,
            "canonical_quote_usable": _quote_usable(quote),
            "copyable": copyable,
            "historical_promotion_authority": False,
            "paper_entry_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        },
    )
    return True


def _schedule_v4(discovery: Any, signature: str) -> None:
    from .profit_first_entity_final_research import _adapter

    _adapter(discovery).schedule(signature)


async def _process_candidate_handoff(obj: Any, signature: str) -> None:
    _inc(obj, "attempted")
    discovery = _attached_discovery(obj)
    if discovery is None:
        _inc(obj, "runtime_unattached")
        _set_last(obj, blocker="wallet_v4_runtime_not_attached")
        return

    swap = _load_normalized_swap(obj, signature)
    if swap is None:
        _inc(obj, "normalized_swap_missing")
        _set_last(obj, blocker="normalized_swap_missing_after_complete_hydration")
        return

    side = swap.side.lower()
    if side not in {"buy", "sell"}:
        _inc(obj, "non_trade_side")
        return

    if side == "sell":
        inserted = _insert_forward_observation(
            obj,
            swap=swap,
            quote=None,
            risk_complete=True,
            manipulation_flag=False,
            side_wallet_flag=False,
            copyable=False,
        )
        if inserted:
            _inc(obj, "sell_forward_inserted")
            try:
                _schedule_v4(discovery, signature)
                _inc(obj, "v4_scheduled")
            except Exception as exc:
                _inc(obj, "v4_schedule_errors")
                _set_last(obj, blocker="v4_sell_schedule_failed", error_type=type(exc).__name__)
        return

    at = _utcnow()
    risk = getattr(getattr(obj, "service", None), "risk_provider", None)
    readiness_fn = getattr(risk, "readiness", None)
    snapshot_fn = getattr(risk, "snapshot", None)
    if not callable(readiness_fn) or not callable(snapshot_fn):
        _inc(obj, "risk_unavailable")
        _set_last(obj, blocker="risk_provider_unavailable")
        return

    try:
        readiness = readiness_fn(swap.token_mint, as_of=at)
    except Exception as exc:
        _inc(obj, "risk_errors")
        _set_last(obj, blocker="risk_readiness_error", error_type=type(exc).__name__)
        return
    complete_fresh = bool(
        isinstance(readiness, dict)
        and readiness.get("complete")
        and readiness.get("fresh")
    )
    if not complete_fresh:
        _inc(obj, "risk_incomplete")
        _set_last(obj, blocker="six_dimension_risk_incomplete_or_stale")
        _insert_forward_observation(
            obj,
            swap=swap,
            quote=None,
            risk_complete=False,
            manipulation_flag=True,
            side_wallet_flag=True,
            copyable=False,
        )
        return

    try:
        profile = getattr(getattr(obj, "service", None), "registry", None)
        profile = profile.get(swap.wallet) if profile is not None else None
        fallback_entity = getattr(profile, "entity_id", None) if profile is not None else None
        resolver = getattr(getattr(obj, "service", None), "entity_resolver", None)
        entity_id = (
            resolver.entity_id_for(swap.wallet, fallback_entity_id=fallback_entity, as_of=at)
            if resolver is not None
            else fallback_entity
        )
        snapshot = await snapshot_fn(
            swap.token_mint,
            at,
            scout_wallet=swap.wallet,
            scout_entity_id=entity_id,
        )
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        snapshot = None
        _inc(obj, "risk_errors")
        _set_last(obj, blocker="risk_snapshot_error", error_type=type(exc).__name__)
    if snapshot is None:
        _inc(obj, "risk_snapshot_missing")
        _set_last(obj, blocker="six_dimension_risk_snapshot_missing")
        _insert_forward_observation(
            obj,
            swap=swap,
            quote=None,
            risk_complete=False,
            manipulation_flag=True,
            side_wallet_flag=True,
            copyable=False,
        )
        return

    _inc(obj, "risk_complete")
    manipulation_flag, side_wallet_flag = _risk_flags(discovery, swap, snapshot, at)
    quote = await _canonical_quote(obj, swap)
    usable = _quote_usable(quote)
    if quote is None:
        _inc(obj, "quote_missing")
        _set_last(obj, blocker="canonical_quote_or_unsigned_simulation_unavailable")
    elif usable:
        _inc(obj, "quote_usable")
        _set_last(obj, blocker=None, error_type=None)
    else:
        _inc(obj, "quote_unusable")
        _set_last(obj, blocker="canonical_quote_unusable_or_outside_entry_window")

    inserted = _insert_forward_observation(
        obj,
        swap=swap,
        quote=quote,
        risk_complete=True,
        manipulation_flag=manipulation_flag,
        side_wallet_flag=side_wallet_flag,
        copyable=usable,
    )
    if not inserted:
        return
    try:
        _schedule_v4(discovery, signature)
        _inc(obj, "v4_scheduled")
    except Exception as exc:
        _inc(obj, "v4_schedule_errors")
        _set_last(obj, blocker="v4_schedule_failed", error_type=type(exc).__name__)


def _schedule_candidate_handoff(obj: Any, signature: str) -> None:
    if not signature or not _claim_signature(obj, signature):
        _inc(obj, "duplicate_schedule")
        return
    tasks = _tasks(obj)
    for task in list(tasks):
        if task.done():
            tasks.discard(task)
    if len(tasks) >= HANDOFF_TASK_LIMIT:
        _inc(obj, "backpressure_drops")
        _set_last(obj, blocker="candidate_v4_handoff_task_bound_reached")
        return

    task = asyncio.create_task(
        _process_candidate_handoff(obj, signature),
        name=f"candidate-risk-quote-v4:{signature[:10]}",
    )
    tasks.add(task)

    def done(completed: asyncio.Task[Any]) -> None:
        tasks.discard(completed)
        try:
            exc = completed.exception()
        except asyncio.CancelledError:
            _inc(obj, "tasks_cancelled")
            return
        except BaseException:
            _inc(obj, "task_errors")
            return
        if exc is not None:
            _inc(obj, "task_errors")
            _set_last(obj, blocker="candidate_v4_handoff_task_failed", error_type=type(exc).__name__)

    task.add_done_callback(done)


async def _hydrate_then_candidate_v4(
    self: DirectSolanaIngestionPlane,
    row: dict[str, Any],
) -> None:
    if _ORIGINAL_HYDRATE_ONE is None:
        raise RuntimeError("candidate risk/quote/v4 handoff is not installed")
    await _ORIGINAL_HYDRATE_ONE(self, row)

    reason = str(row.get("reason") or "")
    if reason not in SCOUT_REASONS:
        return
    signature = str(row.get("signature") or "")
    if not signature:
        return
    if _queue_status(self, signature) != "complete":
        _inc(self, "nonterminal_or_failed_hydration")
        return
    _schedule_candidate_handoff(self, signature)


setattr(_hydrate_then_candidate_v4, "_roi_candidate_risk_quote_v4_handoff", True)


def _status_with_candidate_v4(self: DirectSolanaIngestionPlane) -> dict[str, Any]:
    if _ORIGINAL_DIRECT_STATUS is None:
        raise RuntimeError("candidate risk/quote/v4 status is not installed")
    payload = _ORIGINAL_DIRECT_STATUS(self)
    tasks = _tasks(self)
    payload["candidate_risk_quote_v4_handoff"] = {
        "installed": True,
        "runtime_attached": _attached_discovery(self) is not None,
        "source_scope": sorted(SCOUT_REASONS),
        "historical_or_gap_backfill_allowed": False,
        "post_hydration_only": True,
        "requires_complete_fresh_six_dimension_risk_before_quote": True,
        "canonical_quote_path": "amount-specific-jupiter-order-plus-unsigned-mainnet-simulation",
        "v4_forward_source": "direct-candidate-v4",
        "task_limit": HANDOFF_TASK_LIMIT,
        "pending_tasks": sum(1 for task in tasks if not task.done()),
        "attempted": int(getattr(self, "_roi_candidate_v4_handoff_attempted", 0) or 0),
        "risk_complete": int(getattr(self, "_roi_candidate_v4_handoff_risk_complete", 0) or 0),
        "risk_incomplete": int(getattr(self, "_roi_candidate_v4_handoff_risk_incomplete", 0) or 0),
        "quote_attempted": int(getattr(self, "_roi_candidate_v4_handoff_quote_attempted", 0) or 0),
        "quote_reused": int(getattr(self, "_roi_candidate_v4_handoff_quote_reused", 0) or 0),
        "quote_usable": int(getattr(self, "_roi_candidate_v4_handoff_quote_usable", 0) or 0),
        "quote_unusable": int(getattr(self, "_roi_candidate_v4_handoff_quote_unusable", 0) or 0),
        "quote_missing": int(getattr(self, "_roi_candidate_v4_handoff_quote_missing", 0) or 0),
        "forward_inserted": int(getattr(self, "_roi_candidate_v4_handoff_forward_inserted", 0) or 0),
        "forward_duplicate": int(getattr(self, "_roi_candidate_v4_handoff_forward_duplicate", 0) or 0),
        "v4_scheduled": int(getattr(self, "_roi_candidate_v4_handoff_v4_scheduled", 0) or 0),
        "sell_forward_inserted": int(getattr(self, "_roi_candidate_v4_handoff_sell_forward_inserted", 0) or 0),
        "backpressure_drops": int(getattr(self, "_roi_candidate_v4_handoff_backpressure_drops", 0) or 0),
        "last_blocker": getattr(self, "_roi_candidate_v4_handoff_last_blocker", None),
        "last_error_type": getattr(self, "_roi_candidate_v4_handoff_last_error_type", None),
        "candidate_processing_target_seconds_unchanged": 5.0,
        "strategy_entry_ceiling_seconds_unchanged": ENTRY_WINDOW_SECONDS,
        "max_chase_fraction_unchanged": MAX_CHASE_FRACTION,
        "paper_entry_authority": False,
        "historical_promotion_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
        "live_money_authority": False,
    }
    return payload


setattr(_status_with_candidate_v4, "_roi_candidate_risk_quote_v4_handoff", True)


def install_candidate_risk_quote_v4_handoff() -> None:
    global _ORIGINAL_HYDRATE_ONE, _ORIGINAL_DIRECT_STATUS

    current_hydrate = DirectSolanaIngestionPlane._hydrate_one
    if not bool(getattr(current_hydrate, "_roi_candidate_risk_quote_v4_handoff", False)):
        _ORIGINAL_HYDRATE_ONE = current_hydrate
        try:
            _hydrate_then_candidate_v4.__dict__.update(getattr(current_hydrate, "__dict__", {}))
        except Exception:
            pass
        setattr(_hydrate_then_candidate_v4, "_roi_candidate_risk_quote_v4_handoff", True)
        DirectSolanaIngestionPlane._hydrate_one = _hydrate_then_candidate_v4  # type: ignore[method-assign]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_candidate_risk_quote_v4_handoff", False)):
        _ORIGINAL_DIRECT_STATUS = current_status
        try:
            _status_with_candidate_v4.__dict__.update(getattr(current_status, "__dict__", {}))
        except Exception:
            pass
        setattr(_status_with_candidate_v4, "_roi_candidate_risk_quote_v4_handoff", True)
        DirectSolanaIngestionPlane.status = _status_with_candidate_v4  # type: ignore[method-assign]


__all__ = [
    "ENTRY_WINDOW_SECONDS",
    "HANDOFF_TASK_LIMIT",
    "SCOUT_REASONS",
    "_process_candidate_handoff",
    "attach_candidate_v4_wallet_discovery",
    "install_candidate_risk_quote_v4_handoff",
]
