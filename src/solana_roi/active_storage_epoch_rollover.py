from __future__ import annotations

import json
import os
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .active_storage import ActiveStorage, ActiveStorageBudget
from .storage_file_retention import assert_persistent_file_registered
from .storage_shadow_migration import build_shadow_database
from .storage_transition import load_verified_checkpoint

ROLLOVER_MARKER_SUFFIX = ".rollover-requested"
SEALED_EPOCH_DIR = "active-epochs"
ROLLOVER_MARKER_DATASET = "active_rollover_request"
SEALED_EPOCH_DATASET = "sealed_active_epoch"


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")


def _family(path: Path) -> tuple[Path, Path, Path]:
    return path, Path(str(path) + "-wal"), Path(str(path) + "-shm")


def _fsync_dir(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def _truncate_wal(path: Path) -> None:
    if not path.is_file():
        raise RuntimeError(f"active epoch rollover blocked: database missing:{path}")
    connection = sqlite3.connect(path, timeout=30.0)
    try:
        connection.execute("PRAGMA busy_timeout=30000")
        row = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        if row is None or int(row[0]) != 0:
            raise RuntimeError(f"active epoch rollover blocked: WAL checkpoint busy:{row}")
    finally:
        connection.close()
    wal = Path(str(path) + "-wal")
    if wal.exists() and wal.stat().st_size != 0:
        raise RuntimeError(
            f"active epoch rollover blocked: WAL remained nonempty:{wal.stat().st_size}"
        )


def rollover_marker(path: Path | str) -> Path:
    candidate = Path(path)
    return Path(str(candidate) + ROLLOVER_MARKER_SUFFIX)


def request_rollover(path: Path | str, *, reason: str, sizes: dict[str, int] | None = None) -> Path:
    """Persist a tiny restart-safe request without touching SQLite contents."""
    assert_persistent_file_registered(ROLLOVER_MARKER_DATASET)
    candidate = Path(path)
    marker = rollover_marker(candidate)
    payload = {
        "requested_at": datetime.now(timezone.utc).isoformat(),
        "reason": str(reason),
        "database_path": str(candidate),
        "sizes": dict(sizes or {}),
        "retention_dataset": ROLLOVER_MARKER_DATASET,
        "paper_only": True,
        "live_money_authority": False,
    }
    marker.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    _fsync_dir(marker.parent)
    return marker


def _remove_zero_sidecars(path: Path) -> None:
    _, wal, shm = _family(path)
    if wal.exists() and wal.stat().st_size:
        raise RuntimeError(
            f"active epoch rollover blocked: cannot remove nonempty WAL:{wal}:{wal.stat().st_size}"
        )
    for sidecar in (wal, shm):
        if sidecar.exists():
            sidecar.unlink()


def rollover_active_epoch_if_needed(
    path: Path | str,
    *,
    release_sha: str,
    budget: ActiveStorageBudget | None = None,
) -> dict[str, Any]:
    """Rebuild an oversized active DB from verified current truth at quiescent startup.

    The source remains untouched until a complete, semantically equivalent,
    bounded replacement has been built and verified. The previous main file is
    then hard-linked into a physically isolated sealed-epoch directory before an
    atomic main-file replacement. No old bytes are deleted by this operation.

    This is deliberately a startup-only operation: callers must invoke it before
    opening the long-lived runtime store or starting workers.
    """
    assert_persistent_file_registered(ROLLOVER_MARKER_DATASET)
    assert_persistent_file_registered(SEALED_EPOCH_DATASET)
    active = Path(path)
    configured = budget or ActiveStorageBudget()
    marker = rollover_marker(active)
    sizes = ActiveStorage(active, budget=configured).storage_bytes()
    requested = marker.exists() or sizes["main"] >= configured.warning_bytes
    if not requested:
        return {
            "status": "not_required",
            "path": str(active),
            "main_bytes": sizes["main"],
            "wal_bytes": sizes["wal"],
            "warning_bytes": configured.warning_bytes,
            "hard_bytes": configured.hard_bytes,
            "paper_only": True,
            "live_money_authority": False,
        }
    if not active.is_file():
        raise RuntimeError(f"active epoch rollover blocked: requested database missing:{active}")

    # Fold the source WAL into the current main before any archival link is made.
    # With no runtime store/workers open, this is a bounded quiescent checkpoint.
    _truncate_wal(active)
    before = ActiveStorage(active, budget=configured).storage_bytes()

    token = uuid.uuid4().hex
    successor = active.with_name(f".{active.name}.epoch-next-{token}")
    report = build_shadow_database(
        legacy_path=active,
        active_path=successor,
        release_sha=release_sha,
        replace_existing=True,
    )
    if not report.equivalent:
        raise RuntimeError(
            "active epoch rollover blocked: successor semantic equivalence failed:"
            + ",".join(report.mismatched_sections)
        )
    if report.active_size_bytes >= configured.warning_bytes:
        raise RuntimeError(
            "active epoch rollover blocked: bounded successor lacks warning headroom:"
            f"{report.active_size_bytes}>={configured.warning_bytes}"
        )

    successor_storage = ActiveStorage(successor, budget=configured)
    successor_storage.assert_positive_schema()
    successor_storage.enforce_hard_budget()
    _truncate_wal(successor)
    load_verified_checkpoint(successor, expected_release_sha=release_sha)

    # Stamp a real active epoch identity after migration verification. This does
    # not alter portfolio/strategy/certification truth or the checkpoint payload.
    epoch_id = f"active-{_utc_stamp()}-{token[:12]}"
    with successor_storage.connect() as connection:
        connection.execute(
            "UPDATE storage_epoch_state SET epoch_id=?,status='OPEN',updated_at=? WHERE singleton_key=1",
            (epoch_id, datetime.now(timezone.utc).isoformat()),
        )
        connection.commit()
    _truncate_wal(successor)

    # The old source is retained without copying its multi-GB body: a hard link
    # preserves the exact inode on the same persistent filesystem. It is outside
    # normal runtime/certification paths and can be classified/purged later.
    sealed_dir = active.parent / SEALED_EPOCH_DIR / f"sealed-{_utc_stamp()}-{token[:12]}"
    sealed_dir.mkdir(parents=True, exist_ok=False)
    sealed_main = sealed_dir / active.name
    try:
        os.link(active, sealed_main)
    except OSError as exc:
        raise RuntimeError(
            f"active epoch rollover blocked: unable to preserve sealed source by hard link:{exc.errno}"
        ) from exc
    _fsync_dir(sealed_dir)

    # Both source and successor are fully checkpointed. Sidecars can therefore be
    # removed without discarding committed truth. The main-file replacement is a
    # single same-filesystem rename; if it never occurs, the old active main still
    # exists at the canonical path and remains restartable.
    _remove_zero_sidecars(active)
    _remove_zero_sidecars(successor)
    os.replace(successor, active)
    _fsync_dir(active.parent)

    # Re-open only the compact canonical successor and prove its checkpoint after
    # the path swap. The sealed source remains physically present but unopened.
    post_storage = ActiveStorage(active, budget=configured)
    post_storage.assert_positive_schema()
    post_storage.enforce_hard_budget()
    checkpoint = load_verified_checkpoint(active, expected_release_sha=release_sha)
    after = post_storage.storage_bytes()
    if marker.exists():
        marker.unlink()
        _fsync_dir(marker.parent)

    payload = {
        "status": "rolled_over",
        "path": str(active),
        "release_sha": release_sha,
        "epoch_id": epoch_id,
        "source_main_bytes": before["main"],
        "source_wal_bytes": before["wal"],
        "successor_main_bytes": after["main"],
        "successor_wal_bytes": after["wal"],
        "warning_bytes": configured.warning_bytes,
        "hard_bytes": configured.hard_bytes,
        "sealed_source_path": str(sealed_main),
        "sealed_source_retention_dataset": SEALED_EPOCH_DATASET,
        "sealed_source_deleted": False,
        "checkpoint_id": checkpoint.get("checkpoint_id"),
        "semantic_hash": report.semantic_hash,
        "equivalent": True,
        "mismatched_sections": list(report.mismatched_sections),
        "copied_rows": dict(report.copied_rows),
        "history_copied_wholesale": False,
        "legacy_or_prior_epoch_opened_after_swap": False,
        "paper_only": True,
        "live_money_authority": False,
    }
    print("ROI_ACTIVE_STORAGE_EPOCH_ROLLOVER " + json.dumps(payload, sort_keys=True), flush=True)
    return payload


__all__ = [
    "ROLLOVER_MARKER_SUFFIX",
    "ROLLOVER_MARKER_DATASET",
    "SEALED_EPOCH_DIR",
    "SEALED_EPOCH_DATASET",
    "request_rollover",
    "rollover_active_epoch_if_needed",
    "rollover_marker",
]
