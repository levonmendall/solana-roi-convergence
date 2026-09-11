from __future__ import annotations

"""Cross-process ownership for the production persistent SQLite disk.

Render may briefly overlap outgoing and incoming processes during a blue/green handoff.
SQLite transaction locks are insufficient for one-shot VACUUM/cleanup, so every
cleanup-capable process takes a process-lifetime advisory flock before constructing
runtime state.

Two durable same-release proofs are intentionally distinct:

* maintenance readiness: a disabled deployment acquired the exclusive disk lease and
  completed the read-only cleanup dependency/protection probe before heavy bootstrap;
* full-runtime establishment: the same release later reached canonical full runtime.

Destructive maintenance may rely on the first proof when the history being cleaned is
itself what prevents full runtime. This still requires a *prior disabled deployment of
the exact same release* and never weakens exclusive disk ownership.
"""

import asyncio
import fcntl
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

LOCK_FILENAME = ".solana-roi-runtime-owner.lock"
ESTABLISHED_FILENAME = ".solana-roi-runtime-owner-established.json"
MAINTENANCE_ESTABLISHED_FILENAME = ".solana-roi-maintenance-established.json"
LOCK_PROTOCOL_VERSION = "runtime-disk-ownership-v1"
MAINTENANCE_PROTOCOL_VERSION = "runtime-disk-maintenance-v1"
LOCK_TIMEOUT_ENV = "SOLANA_ROI_RUNTIME_DISK_LOCK_TIMEOUT_SECONDS"
DEFAULT_LOCK_TIMEOUT_SECONDS = 180.0
POLL_SECONDS = 0.25


class DiskOwnershipError(RuntimeError):
    pass


def current_release_commit() -> str | None:
    value = (
        os.getenv("RENDER_GIT_COMMIT", "").strip()
        or os.getenv("RENDER_GIT_COMMIT_SHA", "").strip()
    )
    return value or None


@dataclass
class RuntimeDiskLease:
    database_path: Path
    lock_path: Path
    handle: Any
    acquired_at: str
    waited_seconds: float
    release_commit: str | None
    pid: int

    def status(self) -> dict[str, Any]:
        return {
            "owned": self.handle is not None,
            "lock_path": str(self.lock_path),
            "database_path": str(self.database_path),
            "acquired_at": self.acquired_at,
            "waited_seconds": round(float(self.waited_seconds), 3),
            "release_commit": self.release_commit,
            "pid": int(self.pid),
            "protocol_version": LOCK_PROTOCOL_VERSION,
            "cross_process_exclusive": True,
        }

    def release(self) -> None:
        handle = self.handle
        if handle is None:
            return
        self.handle = None
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()


def _lock_path(database_path: Path) -> Path:
    return Path(database_path).parent / LOCK_FILENAME


def _established_path(database_path: Path) -> Path:
    return Path(database_path).parent / ESTABLISHED_FILENAME


def _maintenance_path(database_path: Path) -> Path:
    return Path(database_path).parent / MAINTENANCE_ESTABLISHED_FILENAME


def _timeout_from_environment() -> float:
    raw = os.getenv(LOCK_TIMEOUT_ENV, "").strip()
    if not raw:
        return DEFAULT_LOCK_TIMEOUT_SECONDS
    try:
        value = float(raw)
    except ValueError as exc:
        raise DiskOwnershipError("runtime disk lock timeout is not numeric") from exc
    if value <= 0:
        raise DiskOwnershipError("runtime disk lock timeout must be positive")
    return value


def _owner_payload(database_path: Path, acquired_at: str) -> dict[str, Any]:
    return {
        "protocol_version": LOCK_PROTOCOL_VERSION,
        "pid": os.getpid(),
        "release_commit": current_release_commit(),
        "database_path": str(database_path),
        "acquired_at": acquired_at,
    }


def _write_owner_record(handle: Any, database_path: Path, acquired_at: str) -> None:
    raw = json.dumps(
        _owner_payload(database_path, acquired_at),
        sort_keys=True,
        separators=(",", ":"),
    ) + "\n"
    handle.seek(0)
    handle.truncate(0)
    handle.write(raw)
    handle.flush()
    os.fsync(handle.fileno())


def _read_marker(path: Path, *, label: str) -> dict[str, Any] | None:
    if not path.exists():
        return None
    if path.is_symlink() or not path.is_file():
        raise DiskOwnershipError(f"{label} marker is not a regular file")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DiskOwnershipError(f"{label} marker is unreadable") from exc
    if not isinstance(value, dict):
        raise DiskOwnershipError(f"{label} marker is invalid")
    return value


def read_establishment(database_path: Path) -> dict[str, Any] | None:
    return _read_marker(_established_path(Path(database_path)), label="runtime disk establishment")


def read_maintenance_establishment(database_path: Path) -> dict[str, Any] | None:
    return _read_marker(_maintenance_path(Path(database_path)), label="maintenance establishment")


def same_release_established(database_path: Path) -> bool:
    release = current_release_commit()
    if not release:
        return False
    marker = read_establishment(database_path)
    return bool(
        marker
        and marker.get("protocol_version") == LOCK_PROTOCOL_VERSION
        and marker.get("release_commit") == release
        and marker.get("database_path") == str(Path(database_path))
    )


def same_release_maintenance_established(database_path: Path) -> bool:
    release = current_release_commit()
    if not release:
        return False
    marker = read_maintenance_establishment(database_path)
    return bool(
        marker
        and marker.get("protocol_version") == MAINTENANCE_PROTOCOL_VERSION
        and marker.get("release_commit") == release
        and marker.get("database_path") == str(Path(database_path))
        and marker.get("cleanup_disabled_during_establishment") is True
        and marker.get("dependency_probe_status") == "ok"
    )


def _write_marker(path: Path, payload: dict[str, Any]) -> dict[str, Any]:
    if path.exists() and path.is_symlink():
        raise DiskOwnershipError("establishment marker must not be a symlink")
    temporary = path.with_name(path.name + f".tmp-{os.getpid()}")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    directory_fd = os.open(str(path.parent), os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)
    return payload


def _validate_lease(database_path: Path, lease: RuntimeDiskLease) -> tuple[Path, str]:
    database_path = Path(database_path)
    release = current_release_commit()
    if not release:
        raise DiskOwnershipError("release commit unavailable; cannot establish cleanup lease protocol")
    if lease.handle is None:
        raise DiskOwnershipError("runtime disk lease is not held")
    if lease.database_path != database_path:
        raise DiskOwnershipError("runtime disk lease/database mismatch")
    return database_path, release


def mark_same_release_established(database_path: Path, lease: RuntimeDiskLease) -> dict[str, Any]:
    database_path, release = _validate_lease(database_path, lease)
    payload = {
        "protocol_version": LOCK_PROTOCOL_VERSION,
        "release_commit": release,
        "database_path": str(database_path),
        "established_at": datetime.now(timezone.utc).isoformat(),
        "established_pid": os.getpid(),
        "lease_acquired_at": lease.acquired_at,
        "full_runtime_proven": True,
    }
    return _write_marker(_established_path(database_path), payload)


def mark_same_release_maintenance_established(
    database_path: Path,
    lease: RuntimeDiskLease,
    *,
    dependency_probe: dict[str, Any],
) -> dict[str, Any]:
    database_path, release = _validate_lease(database_path, lease)
    if str(dependency_probe.get("status") or "") != "ok":
        raise DiskOwnershipError("maintenance readiness requires a successful dependency probe")
    if not bool(dependency_probe.get("read_only")):
        raise DiskOwnershipError("maintenance dependency proof must be read-only")
    if str(dependency_probe.get("database_path") or "") != str(database_path):
        raise DiskOwnershipError("maintenance dependency probe/database mismatch")
    payload = {
        "protocol_version": MAINTENANCE_PROTOCOL_VERSION,
        "release_commit": release,
        "database_path": str(database_path),
        "established_at": datetime.now(timezone.utc).isoformat(),
        "established_pid": os.getpid(),
        "lease_acquired_at": lease.acquired_at,
        "cleanup_disabled_during_establishment": True,
        "dependency_probe_status": "ok",
        "dependency_probe_version": dependency_probe.get("probe_version"),
        "dependency_probe_table": dependency_probe.get("table"),
        "dependency_probe_schema_dependents": [
            item.get("name") for item in dependency_probe.get("schema_dependents", [])
            if isinstance(item, dict)
        ],
        "full_runtime_required_for_maintenance": False,
    }
    return _write_marker(_maintenance_path(database_path), payload)


async def acquire_runtime_disk_lease(
    database_path: Path,
    *,
    stop: asyncio.Event | None = None,
    timeout_seconds: float | None = None,
) -> RuntimeDiskLease:
    database_path = Path(database_path)
    parent = database_path.parent
    if not parent.exists() or not parent.is_dir():
        raise DiskOwnershipError(f"database directory unavailable: {parent}")

    lock_path = _lock_path(database_path)
    if lock_path.exists() and lock_path.is_symlink():
        raise DiskOwnershipError("runtime disk ownership lock must not be a symlink")

    timeout = _timeout_from_environment() if timeout_seconds is None else float(timeout_seconds)
    if timeout <= 0:
        raise DiskOwnershipError("runtime disk lock timeout must be positive")

    handle = lock_path.open("a+", encoding="utf-8")
    started = time.monotonic()
    try:
        while True:
            if stop is not None and stop.is_set():
                raise asyncio.CancelledError
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                waited = time.monotonic() - started
                if waited >= timeout:
                    raise DiskOwnershipError(
                        f"timed out after {waited:.1f}s waiting for exclusive runtime disk ownership"
                    )
                await asyncio.sleep(min(POLL_SECONDS, max(0.01, timeout - waited)))

        acquired_at = datetime.now(timezone.utc).isoformat()
        _write_owner_record(handle, database_path, acquired_at)
        waited = time.monotonic() - started
        return RuntimeDiskLease(
            database_path=database_path,
            lock_path=lock_path,
            handle=handle,
            acquired_at=acquired_at,
            waited_seconds=waited,
            release_commit=current_release_commit(),
            pid=os.getpid(),
        )
    except BaseException:
        handle.close()
        raise


__all__ = [
    "DEFAULT_LOCK_TIMEOUT_SECONDS",
    "DiskOwnershipError",
    "ESTABLISHED_FILENAME",
    "LOCK_FILENAME",
    "LOCK_PROTOCOL_VERSION",
    "LOCK_TIMEOUT_ENV",
    "MAINTENANCE_ESTABLISHED_FILENAME",
    "MAINTENANCE_PROTOCOL_VERSION",
    "RuntimeDiskLease",
    "acquire_runtime_disk_lease",
    "current_release_commit",
    "mark_same_release_established",
    "mark_same_release_maintenance_established",
    "read_establishment",
    "read_maintenance_establishment",
    "same_release_established",
    "same_release_maintenance_established",
]
