from __future__ import annotations

"""Client for the bounded logical certification bootstrap transport.

The logical bootstrap is resumable across transient authoritative transport and
resource-pressure pauses. Every committed page is checkpointed beside a durable
partial SQLite replica. Release/schema/epoch identity changes are the only reasons
that invalidate that partial replica.
"""

import base64
import hashlib
import json
import logging
import os
import sqlite3
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

from .certification_incremental_replication import REPLICATION_VERSION
from .certification_logical_bootstrap import BOOTSTRAP_VERSION


CLIENT_VERSION = "certification-logical-bootstrap-client-v5-adaptive-memory-bound"
DEFAULT_PAGE_PAUSE_SECONDS = 0.01
DEFAULT_TRANSIENT_RETRY_ATTEMPTS = 4
DEFAULT_TRANSIENT_RETRY_SECONDS = 2.0
DEFAULT_ADAPTIVE_PAGE_ATTEMPTS = 6
DEFAULT_MIN_PAGE_ROWS = 16
DEFAULT_PAGE_RECOVERY_SUCCESSES = 8
_LOGGER = logging.getLogger(__name__)


class LogicalBootstrapRestartRequired(RuntimeError):
    """Authoritative identity changed, so the partial logical replica is invalid."""


class LogicalBootstrapPause(RuntimeError):
    """Transient transport/resource pressure paused bootstrap; preserve progress."""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


def _page_pause_seconds() -> float:
    try:
        return max(
            0.0,
            min(
                1.0,
                float(
                    os.getenv(
                        "SOLANA_ROI_CERTIFIER_LOGICAL_BOOTSTRAP_PAGE_PAUSE_SECONDS",
                        str(DEFAULT_PAGE_PAUSE_SECONDS),
                    )
                ),
            ),
        )
    except ValueError:
        return DEFAULT_PAGE_PAUSE_SECONDS


def _transient_retry_attempts() -> int:
    try:
        return max(
            1,
            min(
                12,
                int(
                    os.getenv(
                        "SOLANA_ROI_CERTIFIER_LOGICAL_BOOTSTRAP_TRANSIENT_RETRY_ATTEMPTS",
                        str(DEFAULT_TRANSIENT_RETRY_ATTEMPTS),
                    )
                ),
            ),
        )
    except ValueError:
        return DEFAULT_TRANSIENT_RETRY_ATTEMPTS


def _transient_retry_seconds() -> float:
    try:
        return max(
            0.25,
            min(
                15.0,
                float(
                    os.getenv(
                        "SOLANA_ROI_CERTIFIER_LOGICAL_BOOTSTRAP_TRANSIENT_RETRY_SECONDS",
                        str(DEFAULT_TRANSIENT_RETRY_SECONDS),
                    )
                ),
            ),
        )
    except ValueError:
        return DEFAULT_TRANSIENT_RETRY_SECONDS


def _adaptive_page_attempts() -> int:
    try:
        return max(
            2,
            min(
                12,
                int(
                    os.getenv(
                        "SOLANA_ROI_CERTIFIER_LOGICAL_BOOTSTRAP_ADAPTIVE_PAGE_ATTEMPTS",
                        str(DEFAULT_ADAPTIVE_PAGE_ATTEMPTS),
                    )
                ),
            ),
        )
    except ValueError:
        return DEFAULT_ADAPTIVE_PAGE_ATTEMPTS


def _minimum_page_rows() -> int:
    try:
        return max(
            1,
            min(
                250,
                int(
                    os.getenv(
                        "SOLANA_ROI_CERTIFIER_LOGICAL_BOOTSTRAP_MIN_PAGE_ROWS",
                        str(DEFAULT_MIN_PAGE_ROWS),
                    )
                ),
            ),
        )
    except ValueError:
        return DEFAULT_MIN_PAGE_ROWS


def _page_recovery_successes() -> int:
    try:
        return max(
            2,
            min(
                64,
                int(
                    os.getenv(
                        "SOLANA_ROI_CERTIFIER_LOGICAL_BOOTSTRAP_PAGE_RECOVERY_SUCCESSES",
                        str(DEFAULT_PAGE_RECOVERY_SUCCESSES),
                    )
                ),
            ),
        )
    except ValueError:
        return DEFAULT_PAGE_RECOVERY_SUCCESSES


def _open_json(request: urllib.request.Request, *, timeout: float = 30.0) -> dict[str, Any]:
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        if int(exc.code) == 409:
            raise LogicalBootstrapRestartRequired("authoritative logical bootstrap identity changed") from exc
        if int(exc.code) in {502, 503, 504}:
            raise LogicalBootstrapPause(
                f"certification logical bootstrap HTTP pause:{exc.code}",
                status_code=int(exc.code),
            ) from exc
        raise RuntimeError(f"certification logical bootstrap HTTP failure:{exc.code}") from exc
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise LogicalBootstrapPause(
            f"certification logical bootstrap transport paused:{type(exc).__name__}"
        ) from exc
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"certification logical bootstrap transport invalid:{type(exc).__name__}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("certification logical bootstrap returned non-object")
    return payload


def _open_json_resumable(request: urllib.request.Request, *, timeout: float = 30.0) -> dict[str, Any]:
    last_pause: LogicalBootstrapPause | None = None
    attempts = _transient_retry_attempts()
    for attempt in range(attempts):
        try:
            return _open_json(request, timeout=timeout)
        except LogicalBootstrapPause as exc:
            last_pause = exc
            if attempt + 1 >= attempts:
                break
            time.sleep(min(15.0, _transient_retry_seconds() * (attempt + 1)))
    assert last_pause is not None
    raise last_pause


def _request(url: str, token: str) -> urllib.request.Request:
    return urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "X-Certification-Token": token,
            "User-Agent": "solana-roi-isolated-certifier-logical-bootstrap/5",
        },
    )


def _decode_value(value: Any) -> Any:
    if isinstance(value, dict) and set(value) == {"__sqlite_blob_b64__"}:
        try:
            return base64.b64decode(str(value["__sqlite_blob_b64__"]), validate=True)
        except (ValueError, TypeError) as exc:
            raise RuntimeError("certification logical bootstrap invalid blob") from exc
    if value is None or isinstance(value, (str, int, float)):
        return value
    raise RuntimeError("certification logical bootstrap invalid SQLite value")


def _qident(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _validate_manifest(payload: dict[str, Any], expected_release: str) -> tuple[str, str, int]:
    if str(payload.get("bootstrap_version") or "") != BOOTSTRAP_VERSION:
        raise RuntimeError("certification logical bootstrap version mismatch")
    if str(payload.get("replication_version") or "") != REPLICATION_VERSION:
        raise RuntimeError("certification logical bootstrap replication version mismatch")
    if str(payload.get("release_commit") or "") != expected_release:
        raise RuntimeError("certification logical bootstrap release mismatch")
    epoch = str(payload.get("epoch") or "")
    fingerprint = str(payload.get("schema_fingerprint") or "")
    if len(epoch) < 8 or len(fingerprint) < 32:
        raise RuntimeError("certification logical bootstrap identity invalid")
    try:
        watermark = int(payload.get("start_watermark"))
    except (TypeError, ValueError) as exc:
        raise RuntimeError("certification logical bootstrap watermark invalid") from exc
    if watermark < 0:
        raise RuntimeError("certification logical bootstrap watermark invalid")
    return epoch, fingerprint, watermark


def _restore_sqlite_metadata(
    connection: sqlite3.Connection,
    *,
    manifest: dict[str, Any],
    table_names: set[str],
) -> None:
    """Restore metadata that logical row copies do not reconstruct exactly."""
    sequences = manifest.get("sqlite_sequence", [])
    pragmas = manifest.get("pragmas", {})
    if not isinstance(sequences, list) or not isinstance(pragmas, dict):
        raise RuntimeError("certification logical bootstrap SQLite metadata invalid")

    if sequences:
        sequence_exists = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='sqlite_sequence'"
        ).fetchone()
        if sequence_exists is None:
            raise RuntimeError("certification logical bootstrap sqlite_sequence missing")
        connection.execute("DELETE FROM sqlite_sequence")
        seen: set[str] = set()
        for item in sequences:
            if not isinstance(item, dict):
                raise RuntimeError("certification logical bootstrap sequence row invalid")
            name = str(item.get("name") or "")
            seq = item.get("seq")
            if name not in table_names or name in seen or not isinstance(seq, int) or isinstance(seq, bool):
                raise RuntimeError("certification logical bootstrap sequence frontier invalid")
            connection.execute("INSERT INTO sqlite_sequence(name,seq) VALUES (?,?)", (name, int(seq)))
            seen.add(name)

    user_version = pragmas.get("user_version", 0)
    application_id = pragmas.get("application_id", 0)
    if (
        not isinstance(user_version, int)
        or isinstance(user_version, bool)
        or not isinstance(application_id, int)
        or isinstance(application_id, bool)
    ):
        raise RuntimeError("certification logical bootstrap SQLite pragma metadata invalid")
    if not (-2147483648 <= user_version <= 2147483647) or not (-2147483648 <= application_id <= 2147483647):
        raise RuntimeError("certification logical bootstrap SQLite pragma metadata out of range")
    connection.execute(f"PRAGMA user_version={int(user_version)}")
    connection.execute(f"PRAGMA application_id={int(application_id)}")


def _resume_paths(destination: Path, *, base: str, expected_release: str) -> tuple[Path, Path]:
    key = hashlib.sha256(f"{base}|{expected_release}".encode()).hexdigest()[:20]
    partial = destination.parent / f".certifier-logical-bootstrap-{key}.sqlite3"
    return partial, partial.with_suffix(partial.suffix + ".state.json")


def _atomic_state(path: Path, payload: dict[str, Any]) -> None:
    fd, raw = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    os.close(fd)
    tmp = Path(raw)
    try:
        tmp.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")), encoding="utf-8")
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def _read_state(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _discard_resume(partial: Path, state_path: Path) -> None:
    for path in (
        partial,
        partial.with_name(partial.name + "-wal"),
        partial.with_name(partial.name + "-shm"),
        partial.with_name(partial.name + "-journal"),
        state_path,
    ):
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass


def _checkpoint_matches(
    state: dict[str, Any] | None,
    *,
    expected_release: str,
    epoch: str,
    fingerprint: str,
    start_watermark: int,
) -> bool:
    if not isinstance(state, dict):
        return False
    try:
        saved_watermark = int(state.get("start_watermark") if state.get("start_watermark") is not None else -1)
    except (TypeError, ValueError):
        return False
    return (
        str(state.get("client_version") or "") == CLIENT_VERSION
        and str(state.get("bootstrap_version") or "") == BOOTSTRAP_VERSION
        and str(state.get("replication_version") or "") == REPLICATION_VERSION
        and str(state.get("release_commit") or "") == expected_release
        and str(state.get("epoch") or "") == epoch
        and str(state.get("schema_fingerprint") or "") == fingerprint
        and saved_watermark == start_watermark
    )


def _base_checkpoint(
    *,
    expected_release: str,
    epoch: str,
    fingerprint: str,
    start_watermark: int,
) -> dict[str, Any]:
    return {
        "client_version": CLIENT_VERSION,
        "bootstrap_version": BOOTSTRAP_VERSION,
        "replication_version": REPLICATION_VERSION,
        "release_commit": expected_release,
        "epoch": epoch,
        "schema_fingerprint": fingerprint,
        "start_watermark": int(start_watermark),
        "next_table_index": 0,
        "current_table": None,
        "cursor": None,
        "bootstrap_rows": 0,
        "bootstrap_payload_bytes": 0,
        "bootstrap_tables": 0,
        "page_rows": None,
        "page_success_streak": 0,
        "resource_pressure_pauses": 0,
    }


def _schema_object_exists(connection: sqlite3.Connection, kind: str, name: str) -> bool:
    row = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type=? AND name=?",
        (kind, name),
    ).fetchone()
    return row is not None


def _manifest_page_rows(manifest: dict[str, Any]) -> tuple[int, int]:
    try:
        default_rows = max(1, int(manifest.get("page_default_rows") or 250))
    except (TypeError, ValueError):
        default_rows = 250
    try:
        max_rows = max(1, int(manifest.get("page_max_rows") or default_rows))
    except (TypeError, ValueError):
        max_rows = default_rows
    default_rows = min(default_rows, max_rows)
    return default_rows, min(default_rows, _minimum_page_rows())


def _next_recovered_page_rows(current: int, target: int) -> int:
    """Grow conservatively after sustained success; never jump back to the target."""
    if current >= target:
        return target
    return min(target, max(current + 1, (current * 5 + 3) // 4))


def logical_bootstrap(
    destination: Path,
    *,
    base: str,
    token: str,
    expected_release: str,
) -> dict[str, Any]:
    if not base or not token:
        raise RuntimeError("logical certification bootstrap source is not configured")
    manifest = _open_json_resumable(
        _request(f"{base}/v1/operations/certification-db-logical-bootstrap", token),
        timeout=30.0,
    )
    epoch, fingerprint, start_watermark = _validate_manifest(manifest, expected_release)
    tables = manifest.get("tables")
    post_schema = manifest.get("post_schema")
    if not isinstance(tables, list) or not isinstance(post_schema, list):
        raise RuntimeError("certification logical bootstrap schema manifest invalid")

    destination.unlink(missing_ok=True)
    partial, state_path = _resume_paths(destination, base=base, expected_release=expected_release)
    checkpoint = _read_state(state_path)
    if not partial.is_file() or not _checkpoint_matches(
        checkpoint,
        expected_release=expected_release,
        epoch=epoch,
        fingerprint=fingerprint,
        start_watermark=start_watermark,
    ):
        _discard_resume(partial, state_path)
        checkpoint = _base_checkpoint(
            expected_release=expected_release,
            epoch=epoch,
            fingerprint=fingerprint,
            start_watermark=start_watermark,
        )
        connection = sqlite3.connect(partial, timeout=30.0)
        connection.close()
        _atomic_state(state_path, checkpoint)
    assert checkpoint is not None

    target_page_rows, min_page_rows = _manifest_page_rows(manifest)
    try:
        saved_page_rows = int(checkpoint.get("page_rows"))
    except (TypeError, ValueError):
        saved_page_rows = target_page_rows
    page_rows = min(target_page_rows, max(min_page_rows, saved_page_rows))
    checkpoint["page_rows"] = page_rows
    checkpoint["page_success_streak"] = max(0, int(checkpoint.get("page_success_streak") or 0))
    checkpoint["resource_pressure_pauses"] = max(0, int(checkpoint.get("resource_pressure_pauses") or 0))
    _atomic_state(state_path, checkpoint)

    connection = sqlite3.connect(partial, timeout=30.0)
    table_names: set[str] = set()
    try:
        connection.execute("PRAGMA foreign_keys=OFF")
        connection.execute("PRAGMA journal_mode=DELETE")
        connection.execute("PRAGMA synchronous=OFF")
        connection.execute("PRAGMA cache_size=-4096")
        connection.execute("PRAGMA mmap_size=0")

        next_table_index = int(checkpoint.get("next_table_index") or 0)
        if next_table_index < 0 or next_table_index > len(tables):
            raise RuntimeError("certification logical bootstrap checkpoint table index invalid")

        for table_index, table in enumerate(tables):
            if not isinstance(table, dict):
                raise RuntimeError("certification logical bootstrap table manifest invalid")
            name = str(table.get("name") or "")
            create_sql = str(table.get("create_sql") or "")
            columns = table.get("columns")
            without_rowid = bool(table.get("without_rowid"))
            if not name or name in table_names or not create_sql or not isinstance(columns, list) or not columns:
                raise RuntimeError("certification logical bootstrap table manifest incomplete")
            table_names.add(name)
            if table_index < next_table_index:
                continue

            column_names = [str(column) for column in columns]
            current_table = checkpoint.get("current_table")
            cursor = checkpoint.get("cursor")
            if table_index == next_table_index and current_table == name:
                exists = connection.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
                ).fetchone()
                if exists is None:
                    raise RuntimeError("certification logical bootstrap checkpoint table missing")
                cursor = str(cursor) if isinstance(cursor, str) and cursor else None
            else:
                if current_table not in (None, ""):
                    raise RuntimeError("certification logical bootstrap checkpoint table mismatch")
                connection.execute(create_sql)
                connection.commit()
                checkpoint["current_table"] = name
                checkpoint["cursor"] = None
                checkpoint["bootstrap_tables"] = int(checkpoint.get("bootstrap_tables") or 0) + 1
                _atomic_state(state_path, checkpoint)
                cursor = None

            while True:
                payload: dict[str, Any] | None = None
                last_pause: LogicalBootstrapPause | None = None
                for attempt in range(_adaptive_page_attempts()):
                    query: dict[str, Any] = {
                        "table": name,
                        "epoch": epoch,
                        "schema_fingerprint": fingerprint,
                        "limit": page_rows,
                    }
                    if cursor:
                        query["cursor"] = cursor
                    try:
                        payload = _open_json(
                            _request(
                                f"{base}/v1/operations/certification-db-logical-bootstrap-page?{urllib.parse.urlencode(query)}",
                                token,
                            ),
                            timeout=30.0,
                        )
                        break
                    except LogicalBootstrapPause as exc:
                        last_pause = exc
                        prior_rows = page_rows
                        if exc.status_code == 503:
                            page_rows = max(min_page_rows, page_rows // 2)
                            checkpoint["page_rows"] = page_rows
                            checkpoint["page_success_streak"] = 0
                            checkpoint["resource_pressure_pauses"] = int(
                                checkpoint.get("resource_pressure_pauses") or 0
                            ) + 1
                            _atomic_state(state_path, checkpoint)
                            _LOGGER.warning(
                                "SOLANA_ROI_CERTIFIER_LOGICAL_BOOTSTRAP_BACKOFF table=%s same_cursor=true status=503 prior_rows=%s next_rows=%s attempt=%s",
                                name,
                                prior_rows,
                                page_rows,
                                attempt + 1,
                            )
                        if attempt + 1 >= _adaptive_page_attempts():
                            raise
                        multiplier = (2 ** min(attempt, 4)) if exc.status_code == 503 else (attempt + 1)
                        time.sleep(min(30.0, _transient_retry_seconds() * multiplier))
                if payload is None:
                    assert last_pause is not None
                    raise last_pause

                if str(payload.get("release_commit") or "") != expected_release:
                    raise LogicalBootstrapRestartRequired("certification logical bootstrap release changed")
                if str(payload.get("epoch") or "") != epoch or str(payload.get("schema_fingerprint") or "") != fingerprint:
                    raise LogicalBootstrapRestartRequired("certification logical bootstrap identity changed")
                if str(payload.get("table") or "") != name:
                    raise RuntimeError("certification logical bootstrap table response mismatch")
                if [str(column) for column in payload.get("columns", [])] != column_names:
                    raise LogicalBootstrapRestartRequired("certification logical bootstrap column layout changed")
                rows = payload.get("rows")
                if not isinstance(rows, list):
                    raise RuntimeError("certification logical bootstrap rows invalid")
                quoted_columns = [_qident(column) for column in column_names]
                insert_columns = quoted_columns if without_rowid else [_qident("rowid"), *quoted_columns]
                placeholders = ",".join("?" for _ in insert_columns)
                insert_sql = (
                    f"INSERT OR REPLACE INTO {_qident(name)}("
                    + ",".join(insert_columns)
                    + f") VALUES ({placeholders})"
                )
                decoded_rows: list[tuple[Any, ...]] = []
                for record in rows:
                    if not isinstance(record, dict) or not isinstance(record.get("values"), list):
                        raise RuntimeError("certification logical bootstrap row invalid")
                    values = tuple(_decode_value(value) for value in record["values"])
                    if len(values) != len(column_names):
                        raise RuntimeError("certification logical bootstrap row width mismatch")
                    if without_rowid:
                        decoded_rows.append(values)
                    else:
                        rowid = record.get("rowid")
                        if not isinstance(rowid, int) or isinstance(rowid, bool):
                            raise RuntimeError("certification logical bootstrap rowid invalid")
                        decoded_rows.append((rowid, *values))
                if decoded_rows:
                    connection.executemany(insert_sql, decoded_rows)
                connection.commit()

                checkpoint["bootstrap_rows"] = int(checkpoint.get("bootstrap_rows") or 0) + len(decoded_rows)
                checkpoint["bootstrap_payload_bytes"] = int(checkpoint.get("bootstrap_payload_bytes") or 0) + int(
                    payload.get("payload_bytes") or 0
                )
                success_streak = int(checkpoint.get("page_success_streak") or 0) + 1
                if success_streak >= _page_recovery_successes() and page_rows < target_page_rows:
                    prior_rows = page_rows
                    page_rows = _next_recovered_page_rows(page_rows, target_page_rows)
                    success_streak = 0
                    _LOGGER.info(
                        "SOLANA_ROI_CERTIFIER_LOGICAL_BOOTSTRAP_RECOVERY table=%s prior_rows=%s next_rows=%s",
                        name,
                        prior_rows,
                        page_rows,
                    )
                checkpoint["page_rows"] = page_rows
                checkpoint["page_success_streak"] = success_streak

                done = bool(payload.get("done"))
                next_cursor = payload.get("next_cursor")
                if done:
                    if next_cursor not in (None, ""):
                        raise RuntimeError("certification logical bootstrap terminal cursor invalid")
                    checkpoint["next_table_index"] = table_index + 1
                    checkpoint["current_table"] = None
                    checkpoint["cursor"] = None
                    _atomic_state(state_path, checkpoint)
                    next_table_index = table_index + 1
                    break
                if not isinstance(next_cursor, str) or not next_cursor or next_cursor == cursor:
                    raise RuntimeError("certification logical bootstrap cursor did not advance")
                cursor = next_cursor
                checkpoint["cursor"] = cursor
                _atomic_state(state_path, checkpoint)
                pause = _page_pause_seconds()
                if pause > 0:
                    time.sleep(pause)

        if int(checkpoint.get("next_table_index") or 0) != len(tables):
            raise RuntimeError("certification logical bootstrap checkpoint did not reach final table")

        _restore_sqlite_metadata(connection, manifest=manifest, table_names=table_names)
        connection.commit()

        order = {"index": 0, "view": 1, "trigger": 2}
        ordered_schema = sorted(
            (item for item in post_schema if isinstance(item, dict)),
            key=lambda item: (order.get(str(item.get("type") or ""), 99), str(item.get("name") or "")),
        )
        for item in ordered_schema:
            kind = str(item.get("type") or "")
            name = str(item.get("name") or "")
            sql = str(item.get("sql") or "").strip()
            if sql and name and not _schema_object_exists(connection, kind, name):
                connection.execute(sql)
        connection.commit()
        connection.execute("PRAGMA synchronous=NORMAL")
        connection.execute("SELECT COUNT(*) FROM sqlite_master").fetchone()
    except LogicalBootstrapPause:
        try:
            connection.rollback()
        except sqlite3.Error:
            pass
        connection.close()
        raise
    except LogicalBootstrapRestartRequired:
        try:
            connection.rollback()
        except sqlite3.Error:
            pass
        connection.close()
        _discard_resume(partial, state_path)
        raise
    except BaseException:
        try:
            connection.rollback()
        except sqlite3.Error:
            pass
        connection.close()
        _discard_resume(partial, state_path)
        raise
    else:
        connection.close()

    os.replace(partial, destination)
    state_path.unlink(missing_ok=True)
    return {
        "client_version": CLIENT_VERSION,
        "bootstrap_version": BOOTSTRAP_VERSION,
        "replication_version": REPLICATION_VERSION,
        "release_commit": expected_release,
        "epoch": epoch,
        "schema_fingerprint": fingerprint,
        "watermark": start_watermark,
        "bootstrap_rows": int(checkpoint.get("bootstrap_rows") or 0),
        "bootstrap_payload_bytes": int(checkpoint.get("bootstrap_payload_bytes") or 0),
        "bootstrap_tables": int(checkpoint.get("bootstrap_tables") or 0),
        "sqlite_sequence_preserved": True,
        "sqlite_pragma_metadata_preserved": True,
        "page_pause_seconds": _page_pause_seconds(),
        "adaptive_page_rows": int(checkpoint.get("page_rows") or target_page_rows),
        "minimum_page_rows": min_page_rows,
        "resource_pressure_pauses": int(checkpoint.get("resource_pressure_pauses") or 0),
        "durable_page_resume": True,
        "resource_pressure_503_resumable": True,
        "resource_pressure_503_adaptive": True,
        "cursor_advances_only_after_commit": True,
        "transient_transport_resumable": True,
        "sqlite_mmap_disabled": True,
        "last_transport": "bounded_logical_bootstrap",
        "paper_only": True,
        "live_money_authority": False,
    }


__all__ = [
    "CLIENT_VERSION",
    "LogicalBootstrapPause",
    "LogicalBootstrapRestartRequired",
    "_open_json",
    "_resume_paths",
    "logical_bootstrap",
]
