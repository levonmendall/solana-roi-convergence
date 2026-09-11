from __future__ import annotations

"""Temporarily suppress SQLite writer auto-checkpoints during logical bootstrap.

The authoritative logical bootstrap is a bounded read-only scan, but SQLite's default
writer-side ``wal_autocheckpoint`` threshold is about 1000 pages. On the production
4 KiB database that repeatedly copied a ~4 MiB WAL back into the 1.7 GiB main file
while the bootstrap reader was trying to evict clean cache. This lease changes only
physical checkpoint cadence while bootstrap requests are active. It restores the
writer's exact original setting on completion or inactivity and pauses fail-closed if
the WAL reaches a bounded maintenance ceiling.
"""

import os
import sqlite3
import threading
import weakref
from pathlib import Path
from typing import Any

from fastapi import HTTPException

LEASE_VERSION = "certification-bootstrap-autocheckpoint-lease-v1"
DEFAULT_IDLE_SECONDS = 45.0
MIN_IDLE_SECONDS = 35.0
MAX_IDLE_SECONDS = 120.0
DEFAULT_MAX_WAL_BYTES = 64 * 1024 * 1024
MIN_MAX_WAL_BYTES = 32 * 1024 * 1024
MAX_MAX_WAL_BYTES = 256 * 1024 * 1024
STATE_ATTR = "_roi_certification_bootstrap_autocheckpoint_lease"

PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
STRATEGY_THRESHOLDS_CHANGED = False
CERTIFICATION_THRESHOLDS_CHANGED = False
CANONICAL_EVIDENCE_RESET = False

_INSTALLED = False
_ORIGINAL_MANIFEST: Any = None
_ORIGINAL_PAGE: Any = None


def _idle_seconds() -> float:
    try:
        return max(
            MIN_IDLE_SECONDS,
            min(
                MAX_IDLE_SECONDS,
                float(
                    os.getenv(
                        "SOLANA_ROI_CERTIFICATION_BOOTSTRAP_AUTOCHECKPOINT_LEASE_SECONDS",
                        str(DEFAULT_IDLE_SECONDS),
                    )
                ),
            ),
        )
    except ValueError:
        return DEFAULT_IDLE_SECONDS


def _max_wal_bytes() -> int:
    try:
        return max(
            MIN_MAX_WAL_BYTES,
            min(
                MAX_MAX_WAL_BYTES,
                int(
                    os.getenv(
                        "SOLANA_ROI_CERTIFICATION_BOOTSTRAP_MAX_WAL_BYTES",
                        str(DEFAULT_MAX_WAL_BYTES),
                    )
                ),
            ),
        )
    except ValueError:
        return DEFAULT_MAX_WAL_BYTES


def _wal_path(store: Any) -> Path:
    return Path(str(Path(getattr(store, "path", ""))) + "-wal")


def _wal_size_bytes(store: Any) -> int:
    try:
        return int(_wal_path(store).stat().st_size)
    except OSError:
        return 0


def _state(store: Any) -> dict[str, Any] | None:
    value = getattr(store, STATE_ATTR, None)
    return value if isinstance(value, dict) else None


def _read_autocheckpoint_locked(store: Any) -> int:
    row = store.db.execute("PRAGMA wal_autocheckpoint").fetchone()
    if row is None:
        raise RuntimeError("authoritative SQLite wal_autocheckpoint unavailable")
    return max(0, int(row[0]))


def _set_autocheckpoint_locked(store: Any, pages: int) -> None:
    store.db.execute(f"PRAGMA wal_autocheckpoint={max(0, int(pages))}")


def _cancel_timer(state: dict[str, Any]) -> None:
    timer = state.get("timer")
    if timer is not None:
        try:
            timer.cancel()
        except Exception:
            pass
    state["timer"] = None


def _restore_locked(store: Any, state: dict[str, Any], *, reason: str, cancel_timer: bool = True) -> bool:
    if cancel_timer:
        _cancel_timer(state)
    if not bool(state.get("active")):
        return False
    original = max(0, int(state.get("original_pages") or 0))
    try:
        _set_autocheckpoint_locked(store, original)
    except (sqlite3.Error, AttributeError, RuntimeError):
        # Store teardown may race the inactivity callback. There is no live writer to
        # restore once its SQLite connection is already closed.
        state["active"] = False
        state["restore_reason"] = f"{reason}:store_unavailable"
        return False
    state["active"] = False
    state["restore_reason"] = reason
    state["generation"] = int(state.get("generation") or 0) + 1
    print(
        "ROI_BOOTSTRAP_AUTOCHECKPOINT_LEASE "
        f"event=restore reason={reason} original_pages={original} "
        f"wal_bytes={_wal_size_bytes(store)}",
        flush=True,
    )
    return True


def _expire(store_ref: "weakref.ReferenceType[Any]", generation: int) -> None:
    store = store_ref()
    if store is None:
        return
    lock = getattr(store, "_lock", None)
    if lock is None:
        return
    try:
        with lock:
            state = _state(store)
            if state is None or not bool(state.get("active")):
                return
            if int(state.get("generation") or -1) != int(generation):
                return
            _restore_locked(store, state, reason="idle_timeout", cancel_timer=False)
    except Exception:
        # Expiry is a safety restoration best effort during possible store teardown.
        return


def _sync_and_release(path: Path) -> None:
    sync = getattr(os, "fdatasync", None) or getattr(os, "fsync", None)
    fadvise = getattr(os, "posix_fadvise", None)
    advice = getattr(os, "POSIX_FADV_DONTNEED", None)
    for candidate in (path, Path(str(path) + "-wal"), Path(str(path) + "-shm")):
        try:
            fd = os.open(candidate, os.O_RDONLY)
        except OSError:
            continue
        try:
            if sync is not None:
                try:
                    sync(fd)
                except OSError:
                    pass
            if fadvise is not None and advice is not None:
                try:
                    fadvise(fd, 0, 0, advice)
                except OSError:
                    pass
        finally:
            os.close(fd)


def _maintenance_checkpoint_locked(store: Any) -> tuple[int | None, int | None, int | None, str | None]:
    try:
        row = store.db.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()
        if row is None or len(row) < 3:
            return None, None, None, None
        return int(row[0]), int(row[1]), int(row[2]), None
    except (sqlite3.Error, AttributeError) as exc:
        return None, None, None, f"{type(exc).__name__}:{exc}"


def refresh(store: Any) -> dict[str, Any]:
    """Acquire/refresh the bounded lease before any bootstrap memory guard runs."""

    lock = getattr(store, "_lock", None)
    path = Path(getattr(store, "path", ""))
    if lock is None or not path.is_file() or not hasattr(store, "db"):
        raise HTTPException(status_code=503, detail="certification bootstrap checkpoint lease unavailable")

    maintenance: tuple[int | None, int | None, int | None, str | None] | None = None
    maintenance_wal = 0
    with lock:
        state = _state(store)
        if state is None:
            state = {
                "active": False,
                "generation": 0,
                "original_pages": None,
                "timer": None,
                "table_names": (),
                "restore_reason": None,
            }
            setattr(store, STATE_ATTR, state)

        maintenance_wal = _wal_size_bytes(store)
        if maintenance_wal >= _max_wal_bytes():
            if bool(state.get("active")):
                _restore_locked(store, state, reason="wal_bound", cancel_timer=True)
            maintenance = _maintenance_checkpoint_locked(store)
        else:
            if not bool(state.get("active")):
                original = _read_autocheckpoint_locked(store)
                _set_autocheckpoint_locked(store, 0)
                state["original_pages"] = original
                state["active"] = True
                state["restore_reason"] = None
                print(
                    "ROI_BOOTSTRAP_AUTOCHECKPOINT_LEASE "
                    f"event=acquire original_pages={original} idle_seconds={_idle_seconds():.3f} "
                    f"wal_bound_bytes={_max_wal_bytes()} wal_bytes={maintenance_wal}",
                    flush=True,
                )

            _cancel_timer(state)
            state["generation"] = int(state.get("generation") or 0) + 1
            generation = int(state["generation"])
            timer = threading.Timer(_idle_seconds(), _expire, args=(weakref.ref(store), generation))
            timer.daemon = True
            state["timer"] = timer
            timer.start()
            return dict(state)

    assert maintenance is not None
    _sync_and_release(path)
    busy, log_frames, checkpointed_frames, error = maintenance
    print(
        "ROI_BOOTSTRAP_AUTOCHECKPOINT_LEASE "
        "event=wal_maintenance_pause "
        f"wal_bytes={maintenance_wal} wal_bound_bytes={_max_wal_bytes()} "
        f"busy={busy if busy is not None else 'unknown'} "
        f"log_frames={log_frames if log_frames is not None else 'unknown'} "
        f"checkpointed_frames={checkpointed_frames if checkpointed_frames is not None else 'unknown'} "
        f"error={(error or 'none').replace(chr(10), ' ')[:160]}",
        flush=True,
    )
    raise HTTPException(
        status_code=503,
        detail="certification logical bootstrap paused: bounded WAL checkpoint maintenance",
    )


def set_manifest_tables(store: Any, manifest: dict[str, Any]) -> None:
    tables = manifest.get("tables")
    names = tuple(
        str(item.get("name") or "")
        for item in tables
        if isinstance(item, dict) and str(item.get("name") or "")
    ) if isinstance(tables, list) else ()
    lock = getattr(store, "_lock", None)
    if lock is None:
        return
    with lock:
        state = _state(store)
        if state is not None:
            state["table_names"] = names


def finish_if_complete(store: Any, payload: dict[str, Any]) -> bool:
    if not bool(payload.get("done")):
        return False
    table = str(payload.get("table") or "")
    lock = getattr(store, "_lock", None)
    if lock is None:
        return False
    with lock:
        state = _state(store)
        if state is None:
            return False
        names = tuple(state.get("table_names") or ())
        if not names or table != str(names[-1]):
            return False
        return _restore_locked(store, state, reason="bootstrap_complete", cancel_timer=True)


def finish(store: Any, *, reason: str = "explicit") -> bool:
    lock = getattr(store, "_lock", None)
    if lock is None:
        return False
    with lock:
        state = _state(store)
        if state is None:
            return False
        return _restore_locked(store, state, reason=reason, cancel_timer=True)


def _manifest_with_lease(store: Any) -> dict[str, Any]:
    assert _ORIGINAL_MANIFEST is not None
    refresh(store)
    payload = _ORIGINAL_MANIFEST(store)
    set_manifest_tables(store, payload)
    return payload


def _page_with_lease(store: Any, **kwargs: Any) -> dict[str, Any]:
    assert _ORIGINAL_PAGE is not None
    refresh(store)
    payload = _ORIGINAL_PAGE(store, **kwargs)
    finish_if_complete(store, payload)
    return payload


def install_certification_bootstrap_autocheckpoint_lease(app: Any | None = None) -> None:
    global _INSTALLED, _ORIGINAL_MANIFEST, _ORIGINAL_PAGE
    if _INSTALLED:
        if app is not None:
            app.state.roi_certification_bootstrap_autocheckpoint_lease = True
            app.state.roi_certification_bootstrap_autocheckpoint_lease_version = LEASE_VERSION
        return

    from . import certification_logical_bootstrap as logical

    _ORIGINAL_MANIFEST = logical._manifest
    _ORIGINAL_PAGE = logical._page
    logical._manifest = _manifest_with_lease  # type: ignore[assignment]
    logical._page = _page_with_lease  # type: ignore[assignment]
    _INSTALLED = True
    if app is not None:
        app.state.roi_certification_bootstrap_autocheckpoint_lease = True
        app.state.roi_certification_bootstrap_autocheckpoint_lease_version = LEASE_VERSION


def status() -> dict[str, Any]:
    return {
        "lease_version": LEASE_VERSION,
        "installed": _INSTALLED,
        "idle_seconds": _idle_seconds(),
        "max_wal_bytes": _max_wal_bytes(),
        "original_autocheckpoint_restored": True,
        "wal_bound_fail_closed": True,
        "strategy_thresholds_changed": STRATEGY_THRESHOLDS_CHANGED,
        "certification_thresholds_changed": CERTIFICATION_THRESHOLDS_CHANGED,
        "canonical_evidence_reset": CANONICAL_EVIDENCE_RESET,
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
        "signing_available": SIGNING_AVAILABLE,
        "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
    }


__all__ = [
    "LEASE_VERSION",
    "finish",
    "finish_if_complete",
    "install_certification_bootstrap_autocheckpoint_lease",
    "refresh",
    "set_manifest_tables",
    "status",
]
