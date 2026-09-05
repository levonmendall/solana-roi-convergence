from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from typing import Any, Callable

from . import continuity_exact_durable_signature_repair as exact
from . import final_production_proof_readiness_repair as proof
from . import live_poll_redundancy as live_poll
from . import raw_receipt_dispatch_repair as raw_dispatch
from . import scout_candidate_continuity_repair as scout
from .direct_solana import DirectSolanaIngestionPlane


REPAIR_VERSION = "post182-production-proof-wiring-v1"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
PUMP_SOURCES = frozenset({"PUMP_FUN", "PUMP_AMM"})

_ORIGINAL_SCOUT_NORMALIZER: Callable[..., Any] | None = None
_ORIGINAL_HANDLER: Callable[..., Any] | None = None
_ORIGINAL_STATUS: Callable[..., dict[str, Any]] | None = None
_INSTALLED = False


def _inc(obj: Any, name: str, amount: int = 1) -> None:
    attr = f"_roi_post182_wiring_{name}"
    setattr(obj, attr, int(getattr(obj, attr, 0) or 0) + int(amount))


def _normalizer_with_current_context_probe(
    result: Any,
    *,
    signature: str,
    trigger_received_at: datetime,
    wallet: str,
    source_hint: str | None = None,
) -> Any:
    """Schedule proof from the *actual* scout economic normalizer boundary.

    PR #182 wrapped ``direct_transaction.normalize_standard_transaction``. The
    production scout path no longer ends there: economic-signal continuation wraps
    ``scout._normalize_tracked_wallet`` and persists the durable unpriced movement
    before returning ``economic_movement_price_unresolved``. Bind the proof here so
    every newly persisted unpriced scout buy can exercise current venue/risk/quote
    evidence without creating retrospective entry authority.
    """
    if _ORIGINAL_SCOUT_NORMALIZER is None:
        raise RuntimeError("post-182 scout normalizer wiring is not installed")

    normalized = _ORIGINAL_SCOUT_NORMALIZER(
        result,
        signature=signature,
        trigger_received_at=trigger_received_at,
        wallet=wallet,
        source_hint=source_hint,
    )
    try:
        swap, error = normalized
    except (TypeError, ValueError):
        return normalized

    if swap is not None or source_hint is not None:
        return normalized

    plane = scout._SCOUT_HYDRATION_PLANE.get()
    if plane is None:
        return normalized

    # Only a durable economic observation can start the zero-allocation proof.
    # This excludes ordinary parser failures and never fabricates a candidate.
    row = proof._economic_unpriced_buy(plane, signature)
    if row is None:
        return normalized

    _inc(plane, "unpriced_probe_candidates_seen")
    proof._schedule_probe(plane, signature)
    _inc(plane, "probe_schedule_calls")
    setattr(plane, "_roi_post182_wiring_last_probe_signature", str(signature))
    setattr(plane, "_roi_post182_wiring_last_probe_error", str(error or ""))
    return normalized


setattr(_normalizer_with_current_context_probe, "_roi_post182_production_proof_wiring", True)


def _mapped_real_pump_receipt(
    provider: str,
    subscription_targets: dict[int, Any],
    message: dict[str, Any],
) -> dict[str, Any] | None:
    """Return exact receipt identity only for a mapped real PUMP WebSocket row."""
    if str(provider) == str(live_poll.POLL_PROVIDER_NAME):
        return None
    if not isinstance(message, dict) or message.get("method") != "logsNotification":
        return None
    params = message.get("params")
    if not isinstance(params, dict):
        return None
    try:
        subscription = int(params["subscription"])
        result = params["result"]
        slot = int(result["context"]["slot"])
        value = result["value"]
        signature = str(value["signature"])
    except (KeyError, TypeError, ValueError):
        return None
    target = subscription_targets.get(subscription)
    if target is None or not signature or slot <= 0 or not isinstance(value, dict):
        return None
    source = str(getattr(target, "source_hint", "") or "")
    if source not in PUMP_SOURCES:
        return None
    return {"signature": signature, "slot": slot, "source_key": source}


def _publish_verified_ws_frontier(
    plane: Any,
    receipt: dict[str, Any],
    *,
    received_at: datetime,
) -> bool:
    """Publish only after the exact signature/source is already durable in SQLite.

    A real WebSocket notification supplies transport provenance. SQLite supplies
    durability. Both are required. A live-poll or history row can never enter this
    helper because ``_mapped_real_pump_receipt`` rejects it before the handler runs.
    """
    try:
        with plane.store._lock:
            durable = plane.store.db.execute(
                "SELECT slot,received_at FROM direct_solana_recent_receipts "
                "WHERE signature=? AND source_key=? LIMIT 1",
                (str(receipt["signature"]), str(receipt["source_key"])),
            ).fetchone()
    except Exception:
        durable = None
    if durable is None:
        _inc(plane, "ws_durable_not_yet_committed")
        return False

    try:
        durable_slot = int(durable["slot"])
    except (KeyError, TypeError, ValueError):
        _inc(plane, "ws_durable_invalid")
        return False
    if durable_slot != int(receipt["slot"]):
        _inc(plane, "ws_durable_slot_mismatch")
        return False

    journal = getattr(plane, "journal", None)
    if journal is None:
        _inc(plane, "ws_journal_missing")
        return False
    frontiers = exact._journal_frontiers(journal)
    committed = time.monotonic()
    row = {
        "signature": str(receipt["signature"]),
        "slot": durable_slot,
        "source_key": str(receipt["source_key"]),
        "received_at": received_at.isoformat(),
        "committed_monotonic": committed,
        "durable": True,
        "transport": "websocket",
        "final_handler_verified": True,
        "repair_version": REPAIR_VERSION,
    }
    current = frontiers.get(str(receipt["source_key"]))
    if not isinstance(current, dict) or (
        int(row["slot"]), float(row["committed_monotonic"])
    ) >= (
        int(current.get("slot", 0) or 0),
        float(current.get("committed_monotonic", 0.0) or 0.0),
    ):
        frontiers[str(receipt["source_key"])] = row
        setattr(
            journal,
            "_roi_exact_durable_ws_frontier_updates",
            int(getattr(journal, "_roi_exact_durable_ws_frontier_updates", 0) or 0) + 1,
        )
        _inc(plane, "ws_frontier_published")
        setattr(plane, "_roi_post182_wiring_last_frontier_source", str(receipt["source_key"]))
        setattr(plane, "_roi_post182_wiring_last_frontier_signature", str(receipt["signature"]))
        return True
    return False


async def _final_handler_with_verified_ws_frontier(
    self: Any,
    provider: str,
    subscription_targets: dict[int, Any],
    message: dict[str, Any],
) -> None:
    if _ORIGINAL_HANDLER is None:
        raise RuntimeError("post-182 final WebSocket handler wiring is not installed")

    receipt = _mapped_real_pump_receipt(provider, subscription_targets, message)
    existing = raw_dispatch._RECEIPT_WALL_TIME.get()
    bound_at = existing if isinstance(existing, datetime) else datetime.now(timezone.utc)
    token = None
    if receipt is not None:
        _inc(self, "real_pump_ws_seen")
        if not isinstance(existing, datetime):
            token = raw_dispatch._RECEIPT_WALL_TIME.set(bound_at)
            _inc(self, "ws_context_bound")
        else:
            _inc(self, "ws_context_preserved")

    try:
        await _ORIGINAL_HANDLER(self, provider, subscription_targets, message)
        if receipt is not None:
            _inc(self, "ws_durable_publish_attempts")
            _publish_verified_ws_frontier(self, receipt, received_at=bound_at)
    finally:
        if token is not None:
            raw_dispatch._RECEIPT_WALL_TIME.reset(token)


setattr(_final_handler_with_verified_ws_frontier, "_roi_post182_production_proof_wiring", True)


def _status_with_wiring(self: Any) -> dict[str, Any]:
    if _ORIGINAL_STATUS is None:
        raise RuntimeError("post-182 proof wiring status is not installed")
    payload = _ORIGINAL_STATUS(self)
    proof_status = payload.get("production_proof_readiness")
    if isinstance(proof_status, dict):
        proof_status["runtime_wiring"] = {
            "version": REPAIR_VERSION,
            "actual_scout_economic_normalizer_attached": bool(
                getattr(scout._normalize_tracked_wallet, "_roi_post182_production_proof_wiring", False)
            ),
            "final_websocket_handler_attached": bool(
                getattr(DirectSolanaIngestionPlane._handle_notification, "_roi_post182_production_proof_wiring", False)
            ),
            "unpriced_probe_candidates_seen_session": int(
                getattr(self, "_roi_post182_wiring_unpriced_probe_candidates_seen", 0) or 0
            ),
            "probe_schedule_calls_session": int(getattr(self, "_roi_post182_wiring_probe_schedule_calls", 0) or 0),
            "real_pump_ws_seen_session": int(getattr(self, "_roi_post182_wiring_real_pump_ws_seen", 0) or 0),
            "ws_context_bound_session": int(getattr(self, "_roi_post182_wiring_ws_context_bound", 0) or 0),
            "ws_context_preserved_session": int(getattr(self, "_roi_post182_wiring_ws_context_preserved", 0) or 0),
            "ws_durable_publish_attempts_session": int(
                getattr(self, "_roi_post182_wiring_ws_durable_publish_attempts", 0) or 0
            ),
            "ws_frontier_published_session": int(getattr(self, "_roi_post182_wiring_ws_frontier_published", 0) or 0),
            "ws_durable_not_yet_committed_session": int(
                getattr(self, "_roi_post182_wiring_ws_durable_not_yet_committed", 0) or 0
            ),
            "live_poll_can_publish_exact_websocket_frontier": False,
            "historical_rows_can_publish_exact_websocket_frontier": False,
            "paper_only": True,
            "live_money_authority": False,
        }
    return payload


setattr(_status_with_wiring, "_roi_post182_production_proof_wiring", True)


def install_post182_production_proof_wiring_repair() -> None:
    """Attach proof/frontier behavior to the final production call sites."""
    global _INSTALLED, _ORIGINAL_SCOUT_NORMALIZER, _ORIGINAL_HANDLER, _ORIGINAL_STATUS
    if _INSTALLED:
        return

    current_normalizer = scout._normalize_tracked_wallet
    if not bool(getattr(current_normalizer, "_roi_post182_production_proof_wiring", False)):
        _ORIGINAL_SCOUT_NORMALIZER = current_normalizer
        try:
            _normalizer_with_current_context_probe.__dict__.update(getattr(current_normalizer, "__dict__", {}))
        except Exception:
            pass
        setattr(_normalizer_with_current_context_probe, "_roi_post182_production_proof_wiring", True)
        scout._normalize_tracked_wallet = _normalizer_with_current_context_probe  # type: ignore[assignment]

    current_handler = DirectSolanaIngestionPlane._handle_notification
    if not bool(getattr(current_handler, "_roi_post182_production_proof_wiring", False)):
        _ORIGINAL_HANDLER = current_handler
        try:
            _final_handler_with_verified_ws_frontier.__dict__.update(getattr(current_handler, "__dict__", {}))
        except Exception:
            pass
        setattr(_final_handler_with_verified_ws_frontier, "_roi_post182_production_proof_wiring", True)
        DirectSolanaIngestionPlane._handle_notification = _final_handler_with_verified_ws_frontier  # type: ignore[method-assign]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_post182_production_proof_wiring", False)):
        _ORIGINAL_STATUS = current_status
        try:
            _status_with_wiring.__dict__.update(getattr(current_status, "__dict__", {}))
        except Exception:
            pass
        setattr(_status_with_wiring, "_roi_post182_production_proof_wiring", True)
        DirectSolanaIngestionPlane.status = _status_with_wiring  # type: ignore[method-assign]

    _INSTALLED = True


__all__ = [
    "REPAIR_VERSION",
    "_final_handler_with_verified_ws_frontier",
    "_mapped_real_pump_receipt",
    "_normalizer_with_current_context_probe",
    "_publish_verified_ws_frontier",
    "install_post182_production_proof_wiring_repair",
]
