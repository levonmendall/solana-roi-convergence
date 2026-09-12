from __future__ import annotations

"""Quiesce logical-bootstrap SQLite cache before the next page can begin.

Production telemetry proved that bounded logical-bootstrap pages can each succeed while
clean SQLite-backed page cache accumulates faster than post-response background cleanup
can evict it.  On a 2 GiB cgroup, that clean cache can combine with a large but
reclaimable anonymous-heap baseline and push the unchanged 94% raw-memory guard back
into fail-closed 503s after only a handful of pages.

This repair changes no pagination, historical identity, certification threshold, or
trading authority.  It keeps the post-response cleanup, but also performs one best-
effort DB/WAL/SHM cache quiesce synchronously inside the already-offloop page worker
after the SQLite reader has closed and before the materialized payload is returned to
the ASGI route.  The next certifier request therefore cannot race ahead of the prior
page's read-cache eviction.  Failed manifest/page opens receive the same cleanup.
"""

import sqlite3
from pathlib import Path
from typing import Any, Callable

from fastapi import HTTPException

from . import certification_logical_bootstrap as logical

REPAIR_VERSION = "logical-bootstrap-page-cache-quiesce-v1"

PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
STRATEGY_THRESHOLDS_CHANGED = False
CERTIFICATION_THRESHOLDS_CHANGED = False
CONTINUITY_SEMANTICS_CHANGED = False
RAW_CRITICAL_FRACTION_CHANGED = False

_INSTALLED = False
_ORIGINAL_PINNED_READER: Callable[..., sqlite3.Connection] | None = None
_ORIGINAL_MANIFEST: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_PAGE: Callable[..., dict[str, Any]] | None = None


def _source_path(store: Any) -> Path:
    return Path(getattr(store, "path", ""))


def _lease_aware_pinned_reader(store: Any) -> sqlite3.Connection:
    """Open the canonical bounded reader while preserving bootstrap WAL ownership."""

    source_path = _source_path(store)
    if not source_path.is_file():
        raise HTTPException(status_code=503, detail="canonical certification source unavailable")

    from . import durable_bootstrap_memory_repair as durable_memory

    lease_state = getattr(store, "_roi_certification_bootstrap_autocheckpoint_lease", None)
    lease_active = isinstance(lease_state, dict) and bool(lease_state.get("active"))
    try:
        durable_memory._guard_raw_cgroup(
            source_path,
            allow_wal_checkpoint=not lease_active,
        )
    except MemoryError as exc:
        raise HTTPException(
            status_code=503,
            detail="certification logical bootstrap deferred: raw cgroup memory pressure",
        ) from exc

    reader = sqlite3.connect(
        f"file:{source_path.resolve()}?mode=ro&cache=private",
        uri=True,
        timeout=5.0,
    )
    reader.execute("PRAGMA query_only=ON")
    reader.execute("PRAGMA busy_timeout=5000")
    reader.execute(f"PRAGMA cache_size=-{logical.READER_CACHE_KIB}")
    reader.execute("PRAGMA mmap_size=0")
    reader.execute("PRAGMA temp_store=FILE")
    return reader


setattr(_lease_aware_pinned_reader, "_roi_durable_bootstrap_memory_bounded", True)
setattr(_lease_aware_pinned_reader, "_roi_bootstrap_lease_wal_aware", True)


def _cleanup_source(store: Any) -> bool:
    """Use the runtime's current sidecar-aware cleanup hook, if available."""

    path = _source_path(store)
    try:
        return bool(logical.split._drop_file_cache(path))
    except (OSError, RuntimeError):
        # Cache eviction is best-effort only.  The unchanged raw-cgroup guard remains
        # authoritative and will fail closed before the next read if pressure persists.
        return False


def _manifest_with_failure_cleanup(store: Any) -> dict[str, Any]:
    if _ORIGINAL_MANIFEST is None:
        raise RuntimeError("logical bootstrap cache repair manifest wrapper is unbound")
    try:
        return _ORIGINAL_MANIFEST(store)
    finally:
        # The original manifest already releases cache after a successful open.  This
        # second idempotent release is what also covers a guard/open failure before its
        # local reader-finally block is entered.
        _cleanup_source(store)


def _page_with_pre_response_cache_quiesce(store: Any, **kwargs: Any) -> dict[str, Any]:
    if _ORIGINAL_PAGE is None:
        raise RuntimeError("logical bootstrap cache repair page wrapper is unbound")
    try:
        return _ORIGINAL_PAGE(store, **kwargs)
    finally:
        # _ORIGINAL_PAGE closes its SQLite reader before control returns here.  The
        # response rows are already materialized Python values, so evicting DB/WAL/SHM
        # read cache cannot alter the response or its historical identity.  Because the
        # whole page callable already runs in Starlette's bounded worker pool, this
        # cleanup cannot block the ASGI event loop.
        _cleanup_source(store)


setattr(_manifest_with_failure_cleanup, "_roi_bootstrap_failure_cache_cleanup", True)
setattr(_page_with_pre_response_cache_quiesce, "_roi_pre_response_cache_quiesce", True)


def configure_logical_bootstrap_page_cache_repair() -> None:
    """Install one idempotent production-only logical-bootstrap cache repair."""

    global _INSTALLED, _ORIGINAL_PINNED_READER, _ORIGINAL_MANIFEST, _ORIGINAL_PAGE
    if _INSTALLED:
        return

    if not bool(getattr(logical._pinned_reader, "_roi_bootstrap_lease_wal_aware", False)):
        _ORIGINAL_PINNED_READER = logical._pinned_reader
        logical._pinned_reader = _lease_aware_pinned_reader  # type: ignore[assignment]

    if not bool(getattr(logical._manifest, "_roi_bootstrap_failure_cache_cleanup", False)):
        _ORIGINAL_MANIFEST = logical._manifest
        logical._manifest = _manifest_with_failure_cleanup  # type: ignore[assignment]

    if not bool(getattr(logical._page, "_roi_pre_response_cache_quiesce", False)):
        _ORIGINAL_PAGE = logical._page
        logical._page = _page_with_pre_response_cache_quiesce  # type: ignore[assignment]

    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "repair_version": REPAIR_VERSION,
        "installed": _INSTALLED,
        "pre_response_sqlite_cache_quiesce": bool(
            getattr(logical._page, "_roi_pre_response_cache_quiesce", False)
        ),
        "failed_open_cache_cleanup": bool(
            getattr(logical._manifest, "_roi_bootstrap_failure_cache_cleanup", False)
        ),
        "bootstrap_lease_wal_aware": bool(
            getattr(logical._pinned_reader, "_roi_bootstrap_lease_wal_aware", False)
        ),
        "post_response_cleanup_preserved": True,
        "raw_critical_fraction_changed": RAW_CRITICAL_FRACTION_CHANGED,
        "strategy_thresholds_changed": STRATEGY_THRESHOLDS_CHANGED,
        "certification_thresholds_changed": CERTIFICATION_THRESHOLDS_CHANGED,
        "continuity_semantics_changed": CONTINUITY_SEMANTICS_CHANGED,
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
        "signing_available": SIGNING_AVAILABLE,
        "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
    }


__all__ = [
    "REPAIR_VERSION",
    "configure_logical_bootstrap_page_cache_repair",
    "status",
]
