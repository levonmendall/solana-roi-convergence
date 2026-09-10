from __future__ import annotations

"""Certifier-owned incremental SQLite replica client.

A full authoritative snapshot is used only when the certifier has no valid replica
or when exact replication identity/schema continuity is lost. Normal cycles fetch
bounded logical deltas and apply them transactionally to the certifier-owned replica.
The pristine replica is cloned locally for the disposable certification child so
production-composition initialization can never contaminate replication state.
"""

import fcntl
import json
import os
import sqlite3
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

from .certification_chunk_transfer import download_snapshot_chunked
from .certification_incremental_replication import (
    CHANGE_TABLE,
    META_TABLE,
    REPLICATION_VERSION,
    TRIGGER_PREFIX,
    _current_watermark,
)


CLIENT_VERSION = "certification-incremental-replica-client-v3-trigger-safe-replay"
DEFAULT_DELTA_TIMEOUT_SECONDS = 30.0
DEFAULT_COPY_CHUNK_BYTES = 8 * 1024 * 1024
FICLONE = 0x40049409

PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False


class ReplicaBootstrapRequired(RuntimeError):
    pass


def _replica_path() -> Path:
    configured = os.getenv("SOLANA_ROI_CERTIFIER_REPLICA_PATH", "").strip()
    path = Path(configured) if configured else Path(tempfile.gettempdir()) / "solana-roi-certifier-replica.sqlite3"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _state_path(replica: Path) -> Path:
    return replica.with_suffix(replica.suffix + ".state.json")


def _atomic_state(path: Path, payload: dict[str, Any]) -> None:
    fd, raw = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    os.close(fd)
    tmp = Path(raw)
    try:
        tmp.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")), encoding="utf-8")
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def _read_state(replica: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(_state_path(replica).read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _remove_replica(replica: Path) -> None:
    for path in (
        replica,
        replica.with_name(replica.name + "-wal"),
        replica.with_name(replica.name + "-shm"),
        replica.with_name(replica.name + "-journal"),
        _state_path(replica),
    ):
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass


def _validate_sqlite(path: Path, expected_bytes: int | None = None) -> int:
    actual = int(path.stat().st_size)
    if expected_bytes is not None and actual != int(expected_bytes):
        raise RuntimeError(f"certifier replica byte-count mismatch:{actual}:{expected_bytes}")
    with path.open("rb") as handle:
        header = handle.read(100)
    if len(header) < 100 or not header.startswith(b"SQLite format 3\x00"):
        raise RuntimeError("certifier replica SQLite header invalid")
    page_size_raw = int.from_bytes(header[16:18], "big")
    page_size = 65536 if page_size_raw == 1 else page_size_raw
    pages = int.from_bytes(header[28:32], "big")
    if page_size < 512 or page_size > 65536 or page_size & (page_size - 1) or pages <= 0:
        raise RuntimeError("certifier replica SQLite geometry invalid")
    if page_size * pages != actual:
        raise RuntimeError(f"certifier replica SQLite geometry mismatch:{actual}:{page_size * pages}")
    uri = f"file:{path.resolve()}?mode=ro&immutable=1"
    connection = sqlite3.connect(uri, uri=True, timeout=5.0)
    try:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("SELECT COUNT(*) FROM sqlite_master").fetchone()
    finally:
        connection.close()
    return actual


def _read_source_replication_identity(path: Path) -> dict[str, Any]:
    connection = sqlite3.connect(path)
    try:
        tables = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name IN (?,?)",
                (META_TABLE, CHANGE_TABLE),
            ).fetchall()
        }
        if {META_TABLE, CHANGE_TABLE} - tables:
            raise RuntimeError("authoritative bootstrap lacks incremental replication metadata")
        meta = {
            str(row[0]): str(row[1])
            for row in connection.execute(f'SELECT key,value FROM "{META_TABLE}"').fetchall()
        }
        epoch = str(meta.get("epoch") or "")
        fingerprint = str(meta.get("schema_fingerprint") or "")
        version = str(meta.get("replication_version") or "")
        # A bootstrap snapshot can legitimately contain an already-pruned change
        # journal. sqlite_sequence is the canonical monotonic acknowledgement
        # frontier and must be used instead of MAX(id), which can fall back to zero.
        watermark = _current_watermark(connection)
        if not epoch or not fingerprint or version != REPLICATION_VERSION:
            raise RuntimeError("authoritative bootstrap replication identity invalid")

        connection.execute("PRAGMA journal_mode=DELETE")
        triggers = connection.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE ?",
            (TRIGGER_PREFIX + "%",),
        ).fetchall()
        for row in triggers:
            name = str(row[0]).replace('"', '""')
            connection.execute(f'DROP TRIGGER IF EXISTS "{name}"')
        connection.execute(f'DELETE FROM "{CHANGE_TABLE}"')
        connection.commit()
        return {
            "replication_version": version,
            "epoch": epoch,
            "schema_fingerprint": fingerprint,
            "watermark": watermark,
        }
    finally:
        connection.close()


def _bootstrap(replica: Path, *, base: str, token: str, expected_release: str) -> dict[str, Any]:
    fd, raw = tempfile.mkstemp(prefix=".certifier-replica-bootstrap-", suffix=".sqlite3", dir=str(replica.parent))
    os.close(fd)
    tmp = Path(raw)
    try:
        release, expected_bytes = download_snapshot_chunked(tmp, base=base, token=token, expected_release=expected_release)
        if release != expected_release:
            raise RuntimeError(f"certifier replica bootstrap release mismatch:{release}:{expected_release}")
        actual_bytes = _validate_sqlite(tmp, expected_bytes)
        identity = _read_source_replication_identity(tmp)
        os.replace(tmp, replica)
        state = {
            "client_version": CLIENT_VERSION,
            "release_commit": expected_release,
            "replication_version": identity["replication_version"],
            "epoch": identity["epoch"],
            "schema_fingerprint": identity["schema_fingerprint"],
            "watermark": int(identity["watermark"]),
            "bootstrap_bytes": actual_bytes,
            "last_transport": "full_snapshot_bootstrap",
        }
        _atomic_state(_state_path(replica), state)
        return {**state, "bootstrapped": True, "delta_applied": False, "delta_change_count": 0}
    finally:
        tmp.unlink(missing_ok=True)


def _delta_timeout() -> float:
    try:
        return max(5.0, float(os.getenv("SOLANA_ROI_CERTIFIER_DELTA_TIMEOUT_SECONDS", str(DEFAULT_DELTA_TIMEOUT_SECONDS))))
    except ValueError:
        return DEFAULT_DELTA_TIMEOUT_SECONDS


def _fetch_delta(*, base: str, token: str, expected_release: str, state: dict[str, Any]) -> dict[str, Any]:
    query = urllib.parse.urlencode(
        {
            "from_watermark": int(state["watermark"]),
            "epoch": str(state["epoch"]),
            "schema_fingerprint": str(state["schema_fingerprint"]),
        }
    )
    request = urllib.request.Request(
        f"{base}/v1/operations/certification-db-delta?{query}",
        headers={
            "Accept": "application/json",
            "X-Certification-Token": token,
            "User-Agent": "solana-roi-isolated-certifier/3",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=_delta_timeout()) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        if int(exc.code) == 409:
            raise ReplicaBootstrapRequired("authoritative incremental replica requested bootstrap") from exc
        raise RuntimeError(f"authoritative certification delta HTTP failure:{exc.code}") from exc
    except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"authoritative certification delta failed:{type(exc).__name__}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("authoritative certification delta returned non-object")
    if str(payload.get("release_commit") or "") != expected_release:
        raise RuntimeError("authoritative certification delta release mismatch")
    if str(payload.get("replication_version") or "") != REPLICATION_VERSION:
        raise ReplicaBootstrapRequired("authoritative certification replication version changed")
    if str(payload.get("epoch") or "") != str(state["epoch"]):
        raise ReplicaBootstrapRequired("authoritative certification replication epoch changed")
    if str(payload.get("schema_fingerprint") or "") != str(state["schema_fingerprint"]):
        raise ReplicaBootstrapRequired("authoritative certification schema fingerprint changed")
    if int(payload.get("from_watermark") or -1) != int(state["watermark"]):
        raise RuntimeError("authoritative certification delta starting watermark mismatch")
    return payload


def _user_trigger_definitions(connection: sqlite3.Connection) -> list[tuple[str, str]]:
    """Capture canonical user triggers so replay cannot fire source side effects twice."""
    rows = connection.execute(
        "SELECT name,sql FROM sqlite_master WHERE type='trigger' AND sql IS NOT NULL "
        "AND name NOT LIKE ? ORDER BY name",
        (TRIGGER_PREFIX + "%",),
    ).fetchall()
    return [(str(name), str(sql)) for name, sql in rows if str(sql or "").strip()]


def _drop_user_triggers(connection: sqlite3.Connection, triggers: list[tuple[str, str]]) -> None:
    for name, _sql in triggers:
        quoted = name.replace('"', '""')
        connection.execute(f'DROP TRIGGER IF EXISTS "{quoted}"')


def _restore_user_triggers(connection: sqlite3.Connection, triggers: list[tuple[str, str]]) -> None:
    for _name, sql in triggers:
        connection.execute(sql)


def _apply_delta(replica: Path, state: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    to_watermark = int(payload.get("to_watermark") or 0)
    if to_watermark < int(state["watermark"]):
        raise ReplicaBootstrapRequired("authoritative certification watermark regressed")
    changes = payload.get("changes")
    if not isinstance(changes, list):
        raise RuntimeError("authoritative certification delta changes invalid")

    connection = sqlite3.connect(replica, timeout=10.0)
    try:
        connection.execute("PRAGMA foreign_keys=OFF")
        connection.execute("PRAGMA busy_timeout=10000")
        connection.execute("BEGIN IMMEDIATE")
        user_triggers = _user_trigger_definitions(connection)
        # Source-side triggers have already executed in the authoritative commit and
        # their resulting rows are represented by the source journal. Firing those
        # triggers again during replica replay can create duplicate/random/timestamped
        # side effects. DDL is transactional in SQLite, so temporarily remove and
        # restore user triggers inside the same transaction as the exact row replay.
        _drop_user_triggers(connection, user_triggers)
        for change in changes:
            if not isinstance(change, dict):
                raise RuntimeError("authoritative certification delta row invalid")
            sql = str(change.get("sql") or "")
            if not sql or ";" in sql.rstrip(";"):
                raise RuntimeError("authoritative certification delta SQL invalid")
            connection.execute(sql)
        _restore_user_triggers(connection, user_triggers)
        connection.commit()
    except BaseException:
        try:
            connection.rollback()
        except sqlite3.Error:
            pass
        raise
    finally:
        connection.close()

    next_state = dict(state)
    next_state["client_version"] = CLIENT_VERSION
    next_state["watermark"] = to_watermark
    next_state["last_transport"] = "incremental_delta"
    _atomic_state(_state_path(replica), next_state)
    return {
        **next_state,
        "bootstrapped": False,
        "delta_applied": True,
        "delta_change_count": len(changes),
        "source_change_count": int(payload.get("source_change_count") or 0),
        "delta_payload_bytes": int(payload.get("payload_bytes") or 0),
    }


def synchronize_replica(*, base: str, token: str, expected_release: str) -> tuple[Path, dict[str, Any]]:
    if not base or not token:
        raise RuntimeError("incremental certification replica source is not configured")
    replica = _replica_path()
    state = _read_state(replica)
    if (
        state is None
        or not replica.is_file()
        or str(state.get("release_commit") or "") != expected_release
        or str(state.get("replication_version") or "") != REPLICATION_VERSION
    ):
        _remove_replica(replica)
        return replica, _bootstrap(replica, base=base, token=token, expected_release=expected_release)

    try:
        _validate_sqlite(replica)
    except BaseException:
        _remove_replica(replica)
        return replica, _bootstrap(replica, base=base, token=token, expected_release=expected_release)

    try:
        delta = _fetch_delta(base=base, token=token, expected_release=expected_release, state=state)
    except ReplicaBootstrapRequired:
        _remove_replica(replica)
        return replica, _bootstrap(replica, base=base, token=token, expected_release=expected_release)
    return replica, _apply_delta(replica, state, delta)


def _drop_cache(fd: int, offset: int, length: int) -> None:
    fadvise = getattr(os, "posix_fadvise", None)
    advice = getattr(os, "POSIX_FADV_DONTNEED", None)
    if fadvise is None or advice is None or length <= 0:
        return
    try:
        fadvise(fd, offset, length, advice)
    except OSError:
        pass


def clone_replica_for_cycle(replica: Path, destination: Path) -> str:
    """Create a disposable local child image without touching authoritative cgroup."""
    src_fd = os.open(replica, os.O_RDONLY)
    dst_fd = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        try:
            fcntl.ioctl(dst_fd, FICLONE, src_fd)
            os.fsync(dst_fd)
            return "local_reflink"
        except OSError:
            os.ftruncate(dst_fd, 0)
            os.lseek(src_fd, 0, os.SEEK_SET)
            os.lseek(dst_fd, 0, os.SEEK_SET)

        copied = 0
        while True:
            block = os.read(src_fd, DEFAULT_COPY_CHUNK_BYTES)
            if not block:
                break
            view = memoryview(block)
            while view:
                written = os.write(dst_fd, view)
                view = view[written:]
            start = copied
            copied += len(block)
            if copied % (64 * 1024 * 1024) < DEFAULT_COPY_CHUNK_BYTES:
                try:
                    os.fdatasync(dst_fd)
                except OSError:
                    pass
            _drop_cache(src_fd, start, len(block))
            _drop_cache(dst_fd, start, len(block))
        os.fsync(dst_fd)
        return "local_progressive_copy"
    finally:
        os.close(dst_fd)
        os.close(src_fd)


def status() -> dict[str, Any]:
    replica = _replica_path()
    state = _read_state(replica)
    return {
        "client_version": CLIENT_VERSION,
        "replica_path_configured": bool(os.getenv("SOLANA_ROI_CERTIFIER_REPLICA_PATH", "").strip()),
        "replica_available": replica.is_file(),
        "replica_release_commit": state.get("release_commit") if state else None,
        "replica_epoch": state.get("epoch") if state else None,
        "replica_schema_fingerprint": state.get("schema_fingerprint") if state else None,
        "replica_watermark": state.get("watermark") if state else None,
        "normal_cycle_transport": "bounded_incremental_delta",
        "full_snapshot_role": "bootstrap_recovery_reconciliation_only",
        "child_uses_disposable_local_clone": True,
        "authoritative_full_snapshot_per_cycle": False,
        "trigger_safe_replay": True,
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
        "signing_available": SIGNING_AVAILABLE,
        "transaction_submission_available": TRANSACTION_SUBMISSION_AVAILABLE,
    }


__all__ = ["CLIENT_VERSION", "ReplicaBootstrapRequired", "clone_replica_for_cycle", "status", "synchronize_replica"]
