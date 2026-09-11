from __future__ import annotations

"""Cross-process ownership for the production persistent SQLite disk.

Render may briefly overlap the outgoing and incoming service processes during a
blue/green handoff. SQLite's normal locks protect individual transactions, but a
one-shot cleanup/VACUUM needs a stronger invariant: no other production runtime may
own the persistent database while destructive maintenance is running.

Every cleanup-capable release acquires this advisory flock before constructing the
canonical runtime and holds it until shutdown. The first deployment establishes the
lease with cleanup disabled. A later cleanup deployment of the *same release SHA*
can therefore become HTTP live, wait for the outgoing process to terminate and
release the lease, acquire exclusive disk ownership, run cleanup, and only then
start database-writing workers.
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
LOCK_PROTOCOL_VERSION = "runtime-disk-ownership-v1"
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
    return database_path.parent / LOCK_FILENAME


def _established_path(database_path: Path) -> Path:
    return database_path.parent / ESTABLISHED_FILENAME


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


def read_establishment(database_path: Path) -> dict[str, Any] | None:
    path = _established_path(Path(database_path))
    if not path.exists():
        return None
    if path.is_symlink() or not path.is_file():
        raise DiskOwnershipError("runtime disk establishment marker is not a regular file")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DiskOwnershipError("runtime disk establishment marker is unreadable") from exc
    if not isinstance(value, dict):
        raise DiskOwnershipError("runtime disk establishment marker is invalid")
    return value


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


def mark_same_release_established(database_path: Path, lease: RuntimeDiskLease) -> dict[str, Any]:
    database_path = Path(database_path)
    release = current_release_commit()
    if not release:
        raise DiskOwnershipError("release commit unavailable; cannot establish cleanup lease protocol")
    if lease.handle is None:
        raise DiskOwnershipError("runtime disk lease is not held")
    if lease.database_path != database_path:
        raise DiskOwnershipError("runtime disk lease/database mismatch")

    path = _established_path(database_path)
    if path.exists() and path.is_symlink():
        raise DiskOwnershipError("runtime disk establishment marker must not be a symlink")
    payload = {
        "protocol_version": LOCK_PROTOCOL_VERSION,
        "release_commit": release,
        "database_path": str(database_path),
        "established_at": datetime.now(timezone.utc).isoformat(),
        "established_pid": os.getpid(),
        "lease_acquired_at": lease.acquired_at,
    }
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
    "RuntimeDiskLease",
    "acquire_runtime_disk_lease",
    "current_release_commit",
    "mark_same_release_established",
    "read_establishment",
    "same_release_established",
]
