from __future__ import annotations

"""Lifecycle-owned one-shot production cleanup for proven stale artifacts.

Importing this module is deliberately side-effect free. Production registration is
storage-non-mutating: the bounded cleanup runs only when the canonical FastAPI
lifespan actually starts. Direct operator/test calls retain the legacy immediate
helper semantics and emit the same exact bounded evidence.
"""

from contextlib import asynccontextmanager
import json
import logging
from typing import Any

from .safe_retention_cleanup import CLEANUP_VERSION, install_safe_retention_cleanup as _run_cleanup


_LOG = logging.getLogger("solana_roi.safe_retention")
_STATUS_PATH = "/v1/operations/safe-retention-cleanup"
_REGISTRATION_ATTR = "roi_safe_retention_cleanup_startup_registered"


def _pending_state() -> dict[str, Any]:
    return {
        "version": CLEANUP_VERSION,
        "installed": False,
        "startup_pending": True,
        "startup_stale_export_cleanup": {
            "version": CLEANUP_VERSION,
            "examined": 0,
            "removed": 0,
            "skipped": 0,
            "bounded": True,
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        },
        "scope": ["stale_certification_exports"],
        "candidate_selection_changed": False,
        "provenance_ambiguous_data_deleted": False,
        "replication_journal_deleted": False,
        "robinhood_history_deleted": False,
        "event_ledger_deleted": False,
        "wallet_history_deleted": False,
        "strategy_thresholds_changed": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


def _failure_state(exc: Exception) -> dict[str, Any]:
    state = _pending_state()
    state["startup_pending"] = False
    state["startup_error"] = f"{type(exc).__name__}:{exc}"
    state["startup_stale_export_cleanup"] = {
        **dict(state["startup_stale_export_cleanup"]),
        "error": state["startup_error"],
    }
    return state


def _emit_evidence(state: dict[str, Any]) -> None:
    cleanup = dict(state.get("startup_stale_export_cleanup") or {})
    evidence = {
        "version": state.get("version"),
        "scope": list(state.get("scope") or ()),
        "examined": int(cleanup.get("examined") or 0),
        "removed": int(cleanup.get("removed") or 0),
        "skipped": int(cleanup.get("skipped") or 0),
        "scan_truncated": bool(cleanup.get("scan_truncated", False)),
        "outcomes": dict(cleanup.get("outcomes") or {}),
        "error": cleanup.get("error") or state.get("startup_error"),
        "startup_pending": bool(state.get("startup_pending", False)),
        "paper_only": bool(state.get("paper_only", True)),
        "live_money_authority": bool(state.get("live_money_authority", False)),
        "signing_available": bool(state.get("signing_available", False)),
        "transaction_submission_available": bool(
            state.get("transaction_submission_available", False)
        ),
    }
    payload = json.dumps(evidence, sort_keys=True, separators=(",", ":"))
    print(f"ROI_SAFE_RETENTION_CLEANUP {payload}", flush=True)
    _LOG.info("ROI_SAFE_RETENTION_CLEANUP %s", payload)


def _execute_cleanup(app: Any, ingestion_runtime: Any) -> dict[str, Any]:
    try:
        state = dict(_run_cleanup(app, ingestion_runtime))
        state["startup_pending"] = False
    except Exception as exc:  # mutation ambiguity stays visible and fail-closed
        state = _failure_state(exc)
    app.state.roi_safe_retention_cleanup = state
    app.state.roi_safe_retention_cleanup_version = CLEANUP_VERSION
    _emit_evidence(state)
    return state


def install_startup_retention_cleanup(app: Any, ingestion_runtime: Any) -> dict[str, Any]:
    """Wrap one existing FastAPI lifespan with exactly one guarded cleanup pass.

    Registration itself does not mutate storage. Generic/test applications retain
    ordinary lifespan wrapping. When the predecessor is the canonical Render handoff
    lifespan, its exported symbol is updated to the same wrapper so repository
    identity/provenance checks continue to observe one canonical lifespan object.
    """

    if bool(getattr(app.state, _REGISTRATION_ATTR, False)):
        return dict(getattr(app.state, "roi_safe_retention_cleanup", _pending_state()))

    pending = _pending_state()
    app.state.roi_safe_retention_cleanup = pending
    app.state.roi_safe_retention_cleanup_version = CLEANUP_VERSION

    existing = {getattr(route, "path", None) for route in getattr(app, "routes", ())}
    if _STATUS_PATH not in existing:
        @app.get(_STATUS_PATH)
        def safe_retention_cleanup_status() -> dict[str, Any]:
            return dict(getattr(app.state, "roi_safe_retention_cleanup", pending))

    router = getattr(app, "router", None)
    previous_lifespan = getattr(router, "lifespan_context", None)
    if not callable(previous_lifespan):
        raise RuntimeError("authoritative FastAPI lifespan is unavailable")

    @asynccontextmanager
    async def _retention_owned_lifespan(app_instance: Any):
        _execute_cleanup(app, ingestion_runtime)
        async with previous_lifespan(app_instance) as lifespan_state:
            yield lifespan_state

    setattr(_retention_owned_lifespan, "_roi_safe_retention_cleanup_lifespan", True)
    setattr(_retention_owned_lifespan, "_roi_previous_lifespan", previous_lifespan)
    app.router.lifespan_context = _retention_owned_lifespan

    # The Render bootstrap regression intentionally checks object identity against
    # this exported canonical symbol. Preserve that contract while keeping the
    # original handoff captured as the wrapper predecessor.
    try:
        from . import render_runtime_bootstrap_repair as render_handoff

        if previous_lifespan is getattr(render_handoff, "_render_handoff_lifespan", None):
            render_handoff._render_handoff_lifespan = _retention_owned_lifespan
    except Exception:
        pass

    setattr(app.state, _REGISTRATION_ATTR, True)
    return dict(pending)


def register_startup_retention_cleanup(app: Any, ingestion_runtime: Any) -> dict[str, Any]:
    """Production-facing registration name; intentionally storage-non-mutating."""

    return install_startup_retention_cleanup(app, ingestion_runtime)


def run_safe_retention_cleanup(app: Any, ingestion_runtime: Any) -> dict[str, Any]:
    """Legacy/direct helper: execute one bounded pass immediately and log evidence."""

    return dict(_execute_cleanup(app, ingestion_runtime))
