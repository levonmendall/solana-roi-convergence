from __future__ import annotations

"""Lifecycle-owned one-shot production cleanup for proven stale artifacts.

Importing this module is deliberately side-effect free. The authoritative
production facade wraps the already-canonical FastAPI lifespan so the bounded
cleanup executes once at real application startup, immediately before canonical
workers are started. Filesystem mutation therefore cannot occur merely because a
module is imported by a test, probe, worker, or tooling process.
"""

from contextlib import asynccontextmanager
import json
import logging
import os
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from .safe_retention_cleanup import CLEANUP_VERSION, install_safe_retention_cleanup as _run_cleanup


_LOG = logging.getLogger("solana_roi.safe_retention")
_STATUS_PATH = "/v1/operations/safe-retention-cleanup"
_REGISTRATION_ATTR = "roi_safe_retention_cleanup_startup_registered"
_DEFAULT_STORE_PATH = "data/solana-roi.sqlite3"


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


def _cleanup_runtime_view(ingestion_runtime: Any) -> Any:
    """Return a path-only runtime view without constructing the canonical runtime.

    In production ``ingestion_runtime`` is deliberately replaced by the Render
    bootstrap guard. Calling it before the canonical lifespan would synchronously
    construct the heavy runtime and defeat the liveness/memory handoff. The runtime
    builder itself uses ``SOLANA_ROI_DB_PATH`` with ``data/solana-roi.sqlite3`` as
    its default, so the cleanup resolves that exact same path contract directly.

    A concrete runtime supplied by unit/replay callers may still provide ``store.path``;
    in that case its explicit path wins. The returned object intentionally exposes
    only the store path needed by the conservative stale-export cleanup.
    """

    store = getattr(ingestion_runtime, "store", None)
    raw_path = getattr(store, "path", None)
    source = "runtime_store"
    if raw_path is None:
        raw_path = os.getenv("SOLANA_ROI_DB_PATH", _DEFAULT_STORE_PATH).strip()
        source = "environment_contract"
    if not raw_path:
        raise RuntimeError("canonical store path is unavailable")
    path = Path(raw_path)
    return SimpleNamespace(
        store=SimpleNamespace(path=path),
        retention_store_path_source=source,
    )


def install_startup_retention_cleanup(app: Any, ingestion_runtime: Any) -> dict[str, Any]:
    """Wrap the existing FastAPI lifespan with exactly one guarded cleanup pass.

    Registration is non-mutating with respect to production storage. The cleanup
    itself remains the existing conservative stale-export cleanup and therefore
    preserves all paper-only, provenance, replication, wallet, event-ledger, and
    strategy-authority boundaries. Unknown lifecycle shapes fail closed rather
    than falling back to import-time mutation.
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

    def _startup_cleanup() -> None:
        try:
            cleanup_runtime = _cleanup_runtime_view(ingestion_runtime)
            state = dict(_run_cleanup(app, cleanup_runtime))
            state["startup_pending"] = False
            state["store_path_source"] = str(
                getattr(cleanup_runtime, "retention_store_path_source", "unknown")
            )
        except Exception as exc:  # mutation ambiguity stays visible and fail-closed
            state = _failure_state(exc)
        app.state.roi_safe_retention_cleanup = state
        app.state.roi_safe_retention_cleanup_version = CLEANUP_VERSION
        _emit_evidence(state)

    @asynccontextmanager
    async def _retention_owned_lifespan(app_instance: Any):
        _startup_cleanup()
        async with previous_lifespan(app_instance) as lifespan_state:
            yield lifespan_state

    # Preserve explicit composition provenance so architecture regressions can prove
    # that the canonical Render handoff lifespan remains the wrapped predecessor
    # without requiring the final router callable to have identical object identity.
    setattr(_retention_owned_lifespan, "_roi_safe_retention_cleanup_lifespan", True)
    setattr(_retention_owned_lifespan, "_roi_previous_lifespan", previous_lifespan)
    app.router.lifespan_context = _retention_owned_lifespan
    setattr(app.state, _REGISTRATION_ATTR, True)
    return dict(pending)


# Compatibility alias for callers introduced before the lifecycle ownership repair.
# It now wraps the startup lifespan; it never performs cleanup at import time.
def run_safe_retention_cleanup(app: Any, ingestion_runtime: Any) -> dict[str, Any]:
    return install_startup_retention_cleanup(app, ingestion_runtime)
