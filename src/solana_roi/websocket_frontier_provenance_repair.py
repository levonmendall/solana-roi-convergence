from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import datetime
from typing import Any

from . import live_poll_redundancy as live_poll
from . import raw_receipt_dispatch_repair as raw_dispatch
from .direct_solana import DirectSolanaIngestionPlane


REPAIR_VERSION = "websocket-frontier-provenance-offloop-v2-cooperative-yield"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False

_ORIGINAL_HANDLER: Callable[..., Any] | None = None
_ORIGINAL_STATUS: Callable[..., dict[str, Any]] | None = None


def _increment(self: Any, name: str, amount: int = 1) -> None:
    attr = f"_roi_ws_frontier_provenance_{name}"
    setattr(self, attr, int(getattr(self, attr, 0) or 0) + int(amount))


def _is_real_websocket_notification(
    provider: str,
    subscription_targets: dict[int, Any],
    message: dict[str, Any],
) -> bool:
    """Accept only a mapped live logsNotification from a real WebSocket provider."""

    if str(provider) == str(live_poll.POLL_PROVIDER_NAME):
        return False
    if not isinstance(message, dict) or message.get("method") != "logsNotification":
        return False
    params = message.get("params")
    if not isinstance(params, dict):
        return False
    try:
        subscription = int(params["subscription"])
        result = params["result"]
        slot = int(result["context"]["slot"])
        value = result["value"]
        signature = str(value["signature"])
    except (KeyError, TypeError, ValueError):
        return False
    return bool(
        subscription in subscription_targets
        and slot > 0
        and signature
        and isinstance(value, dict)
    )


async def _handle_with_websocket_provenance(
    self: Any,
    provider: str,
    subscription_targets: dict[int, Any],
    message: dict[str, Any],
) -> None:
    """Carry socket-read provenance and own the final cooperative-yield boundary.

    PR #136 moved the selective durable receipt section into ``asyncio.to_thread``.
    The exact durable frontier deliberately publishes only while the raw WebSocket
    receipt ContextVar is bound, so an off-loop handler reached outside the older
    raw-dispatch worker could persist every row while publishing zero recovery
    frontiers. Bind that provenance at the final handler boundary for mapped real
    WebSocket notifications only. Python copies ContextVars into ``to_thread``.

    If an outer raw-dispatch worker already supplied an earlier socket-read time,
    preserve it exactly rather than replacing it with a later handler timestamp.
    Regardless of branch, yield once after the final handler completes so dense
    notification bursts cannot monopolize the Uvicorn loop. This replaces the
    former production-root fairness monkeypatch with behavior owned by the final
    WebSocket frontier handler itself.
    """

    if _ORIGINAL_HANDLER is None:
        raise RuntimeError("WebSocket frontier provenance repair is not installed")

    try:
        existing = raw_dispatch._RECEIPT_WALL_TIME.get()
        if isinstance(existing, datetime):
            _increment(self, "existing_context_preserved")
            await _ORIGINAL_HANDLER(self, provider, subscription_targets, message)
            return

        if not _is_real_websocket_notification(provider, subscription_targets, message):
            _increment(self, "non_websocket_unbound")
            await _ORIGINAL_HANDLER(self, provider, subscription_targets, message)
            return

        received_at = raw_dispatch._ORIGINAL_UTCNOW()
        token = raw_dispatch._RECEIPT_WALL_TIME.set(received_at)
        try:
            _increment(self, "contexts_bound")
            setattr(self, "_roi_ws_frontier_provenance_last_received_at", received_at.isoformat())
            setattr(self, "_roi_ws_frontier_provenance_last_provider", str(provider))
            await _ORIGINAL_HANDLER(self, provider, subscription_targets, message)
        finally:
            raw_dispatch._RECEIPT_WALL_TIME.reset(token)
    finally:
        await asyncio.sleep(0)


setattr(_handle_with_websocket_provenance, "_roi_ws_frontier_provenance_offloop", True)
setattr(_handle_with_websocket_provenance, "_roi_cooperative_yield", True)


def _status_with_websocket_provenance(self: Any) -> dict[str, Any]:
    if _ORIGINAL_STATUS is None:
        raise RuntimeError("WebSocket frontier provenance status is not installed")
    payload = _ORIGINAL_STATUS(self)
    payload["websocket_frontier_provenance"] = {
        "installed": True,
        "version": REPAIR_VERSION,
        "real_websocket_only": True,
        "live_poll_can_publish_websocket_frontier": False,
        "to_thread_context_propagation_explicit": True,
        "existing_socket_read_timestamp_preserved": True,
        "cooperative_yield_owned_by_final_handler": True,
        "contexts_bound_session": int(getattr(self, "_roi_ws_frontier_provenance_contexts_bound", 0) or 0),
        "existing_context_preserved_session": int(
            getattr(self, "_roi_ws_frontier_provenance_existing_context_preserved", 0) or 0
        ),
        "non_websocket_unbound_session": int(
            getattr(self, "_roi_ws_frontier_provenance_non_websocket_unbound", 0) or 0
        ),
        "last_received_at": getattr(self, "_roi_ws_frontier_provenance_last_received_at", None),
        "last_provider": getattr(self, "_roi_ws_frontier_provenance_last_provider", None),
        "recoverability_lease_seconds_unchanged": 12.0,
        "hard_recovery_bound_unchanged": "3x1000",
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
        "signing_available": SIGNING_AVAILABLE,
        "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
    }
    policy = payload.setdefault("provider_runtime_policy", {})
    if isinstance(policy, dict):
        policy.update(
            {
                "offloop_websocket_receipt_context_explicit": True,
                "live_poll_cannot_publish_websocket_frontier": True,
                "exact_durable_frontier_requires_real_websocket_provenance": True,
                "final_notification_handler_cooperative_yield": True,
                "continuity_lease_unchanged": True,
                "recovery_bound_unchanged": True,
                "paper_only_authority_unchanged": True,
                "signing_or_submission_available": False,
            }
        )
    return payload


setattr(_status_with_websocket_provenance, "_roi_ws_frontier_provenance_offloop", True)


def install_websocket_frontier_provenance_repair() -> None:
    """Install after the final selective handler and exact/scout frontier wrappers."""

    global _ORIGINAL_HANDLER, _ORIGINAL_STATUS

    current_handler = DirectSolanaIngestionPlane._handle_notification
    if not bool(getattr(current_handler, "_roi_ws_frontier_provenance_offloop", False)):
        # The repair is intentionally narrow. If the final production composition no
        # longer uses the PR #136 off-loop selective handler, do not invent a second
        # receipt path; the older raw-dispatch worker already owns the ContextVar.
        if bool(getattr(current_handler, "_roi_raw_receipt_sqlite_offloop", False)):
            _ORIGINAL_HANDLER = current_handler
            try:
                _handle_with_websocket_provenance.__dict__.update(
                    getattr(current_handler, "__dict__", {})
                )
            except Exception:
                pass
            setattr(_handle_with_websocket_provenance, "_roi_ws_frontier_provenance_offloop", True)
            setattr(_handle_with_websocket_provenance, "_roi_cooperative_yield", True)
            DirectSolanaIngestionPlane._handle_notification = _handle_with_websocket_provenance  # type: ignore[method-assign]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_ws_frontier_provenance_offloop", False)):
        _ORIGINAL_STATUS = current_status
        try:
            _status_with_websocket_provenance.__dict__.update(getattr(current_status, "__dict__", {}))
        except Exception:
            pass
        setattr(_status_with_websocket_provenance, "_roi_ws_frontier_provenance_offloop", True)
        DirectSolanaIngestionPlane.status = _status_with_websocket_provenance  # type: ignore[method-assign]


__all__ = [
    "REPAIR_VERSION",
    "_handle_with_websocket_provenance",
    "_is_real_websocket_notification",
    "install_websocket_frontier_provenance_repair",
]
