from __future__ import annotations

import json
import os
import shutil
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
MAX_UNRECLAIMED_SEALED_EPOCHS = 2


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


def _remove_successor_family(path: Path) -> None:
    """Remove only an uninstalled temporary successor family after a failed build."""
    for candidate in (Path(str(path) + "-wal"), Path(str(path) + "-shm"), path):
        if candidate.exists():
            candidate.unlink()


def sealed_epoch_physical_inventory(active_path: Path | str) -> dict[str, Any]:
    """Count retained physical inodes once, even if a path is hard-linked."""
    active = Path(active_path)
    root = active.parent / SEALED_EPOCH_DIR
    paths = tuple(sorted(root.glob(f"sealed-*/{active.name}"))) if root.is_dir() else ()
    identities: set[tuple[int, int]] = set()
    allocated = 0
    logical = 0
    blockers: list[str] = []
    for path in paths:
        if path.is_symlink():
            blockers.append(f"sealed_epoch_symlink:{path}")
            continue
        try:
            stat = path.stat()
        except OSError as exc:
            blockers.append(f"sealed_epoch_unreadable:{path}:{type(exc).__name__}")
            continue
        identity = (int(stat.st_dev), int(stat.st_ino))
        if identity in identities:
            continue
        identities.add(identity)
        logical += int(stat.st_size)
        allocated += int(getattr(stat, "st_blocks", 0)) * 512
    return {
        "path_count": len(paths),
        "physical_inode_count": len(identities),
        "logical_bytes": logical,
        "allocated_bytes": allocated,
        "blockers": blockers,
    }


def _enforce_sealed_epoch_bound(active_path: Path) -> dict[str, Any]:
    inventory = sealed_epoch_physical_inventory(active_path)
    if inventory["blockers"]:
        raise RuntimeError(
            "active epoch rollover blocked: sealed epoch inventory uncertain:"
            + ",".join(inventory["blockers"])
        )
    if int(inventory["physical_inode_count"]) >= MAX_UNRECLAIMED_SEALED_EPOCHS:
        raise RuntimeError(
            "active epoch rollover blocked: unreclaimed sealed epoch bound reached:"
            f"count={inventory['physical_inode_count']}:"
            f"max={MAX_UNRECLAIMED_SEALED_EPOCHS}:"
            f"allocated_bytes={inventory['allocated_bytes']}"
        )
    return inventory


def _disk_usage_payload(path: Path, *, prefix: str) -> dict[str, int]:
    usage = shutil.disk_usage(path.parent)
    return {
        f"{prefix}_disk_total_bytes": int(usage.total),
        f"{prefix}_disk_used_bytes": int(usage.used),
        f"{prefix}_disk_free_bytes": int(usage.free),
    }


def _rollover_headroom(path: Path, budget: ActiveStorageBudget) -> dict[str, int]:
    """Fail closed unless a bounded shadow build has safe temporary disk headroom.

    The migration uses WAL mode and can temporarily hold both the bounded database
    body and a similarly sized WAL before checkpointing. The source epoch is never
    deleted to make room. Requiring two hard-boundary bodies therefore prevents a
    near-full disk from being consumed by a half-built successor.
    """
    usage = shutil.disk_usage(path.parent)
    required = max(1, int(budget.hard_bytes)) * 2
    free = int(usage.free)
    if free < required:
        raise RuntimeError(
            "active epoch rollover blocked: insufficient temporary disk headroom:"
            f"free={free}:required={required}:hard_bytes={int(budget.hard_bytes)}"
        )
    return {
        # These values are sampled before the successor exists. Prefix them so
        # operators cannot mistake the admission measurement for post-rollover
        # free space after another predecessor has been retained.
        "prebuild_disk_total_bytes": int(usage.total),
        "prebuild_disk_used_bytes": int(usage.used),
        "prebuild_disk_free_bytes": free,
        "required_rollover_free_bytes": required,
    }


def _source_certification_release_commit(path: Path) -> str:
    """Resolve the latest persisted certification release from the quiescent source.

    A new deployment cannot require the old source database to already contain a
    certification epoch for the new release SHA. Migration therefore reads the
    exact persisted source frontier and never inserts or rewrites a release epoch.
    """
    uri = f"file:{path.resolve()}?mode=ro&cache=private"
    connection = sqlite3.connect(uri, uri=True, timeout=30.0)
    try:
        connection.execute("PRAGMA query_only=ON")
        table = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='certification_release_epochs'"
        ).fetchone()
        if table is None:
            raise RuntimeError(
                "active epoch rollover blocked: source certification release epoch table missing"
            )
        row = connection.execute(
            "SELECT release_commit FROM certification_release_epochs "
            "WHERE TRIM(COALESCE(release_commit,''))<>'' "
            "ORDER BY started_at DESC, release_commit DESC LIMIT 1"
        ).fetchone()
    finally:
        connection.close()
    if row is None or not str(row[0] or "").strip():
        raise RuntimeError(
            "active epoch rollover blocked: source certification release frontier missing"
        )
    return str(row[0]).strip()


def _build_shadow_database_for_rollover(
    *,
    source: Path,
    successor: Path,
    target_release_sha: str,
):
    """Build against source certification truth while stamping the target checkpoint.

    `release_commit_from_env()` is used by current-state extraction. Bind only the
    quiescent migration call to the source release so extraction selects the row
    that actually exists in the source. The shadow checkpoint still receives the
    target release SHA explicitly. Restore the deployment environment before any
    runtime worker can start.
    """
    source_release_commit = _source_certification_release_commit(source)
    prior_override = os.environ.get("SOLANA_ROI_RELEASE_COMMIT")
    os.environ["SOLANA_ROI_RELEASE_COMMIT"] = source_release_commit
    try:
        return build_shadow_database(
            legacy_path=source,
            active_path=successor,
            release_sha=target_release_sha,
            replace_existing=True,
        )
    finally:
        if prior_override is None:
            os.environ.pop("SOLANA_ROI_RELEASE_COMMIT", None)
        else:
            os.environ["SOLANA_ROI_RELEASE_COMMIT"] = prior_override


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

    # Never turn a failed/disabled reclamation path into unbounded successor
    # generation. Two physical predecessors allow one rollback boundary plus one
    # operator-approved reclamation window; further rollover fails closed.
    sealed_inventory = _enforce_sealed_epoch_bound(active)

    # Refuse the build before checkpointing or creating any temporary successor
    # when the persistent filesystem cannot safely hold the bounded replacement.
    headroom = _rollover_headroom(active, configured)

    # Fold the source WAL into the current main before any archival link is made.
    # With no runtime store/workers open, this is a bounded quiescent checkpoint.
    _truncate_wal(active)
    before = ActiveStorage(active, budget=configured).storage_bytes()

    token = uuid.uuid4().hex
    successor = active.with_name(f".{active.name}.epoch-next-{token}")
    sealed_main: Path | None = None
    swapped = False
    try:
        report = _build_shadow_database_for_rollover(
            source=active,
            successor=successor,
            target_release_sha=release_sha,
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
        swapped = True
        _fsync_dir(active.parent)
    except Exception:
        # A failed build or pre-swap verification must not consume the remaining
        # persistent disk with an orphaned temporary database/WAL. This never
        # removes the canonical active source or the sealed rollback hard link.
        _remove_successor_family(successor)
        # If the swap did not occur, the just-created sealed path is only a
        # second name for the still-canonical active inode. Remove that exact
        # alias so an interrupted pre-swap attempt cannot consume the bounded
        # predecessor allowance forever. Never unlink a distinct inode here.
        if not swapped and sealed_main is not None and sealed_main.exists():
            try:
                active_stat = active.stat()
                sealed_stat = sealed_main.stat()
                if (active_stat.st_dev, active_stat.st_ino) == (
                    sealed_stat.st_dev,
                    sealed_stat.st_ino,
                ):
                    sealed_main.unlink()
                    _fsync_dir(sealed_main.parent)
            except OSError:
                pass
        raise

    # Re-open only the compact canonical successor and prove its checkpoint after
    # the path swap. The sealed source remains physically present but unopened.
    post_storage = ActiveStorage(active, budget=configured)
    post_storage.assert_positive_schema()
    post_storage.enforce_hard_budget()
    checkpoint = load_verified_checkpoint(active, expected_release_sha=release_sha)
    after = post_storage.storage_bytes()
    postrollover_disk = _disk_usage_payload(active, prefix="postrollover")
    sealed_inventory_after = sealed_epoch_physical_inventory(active)
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
        **headroom,
        **postrollover_disk,
        "disk_measurement_timing": {
            "prebuild": "before_successor_construction",
            "postrollover": "after_swap_and_checkpoint_verification",
        },
        "sealed_source_path": str(sealed_main),
        "sealed_source_retention_dataset": SEALED_EPOCH_DATASET,
        "sealed_source_deleted": False,
        "sealed_epoch_inventory_before": sealed_inventory,
        "sealed_epoch_inventory_after": sealed_inventory_after,
        "sealed_epoch_bound": MAX_UNRECLAIMED_SEALED_EPOCHS,
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
    "sealed_epoch_physical_inventory",
]
