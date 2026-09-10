from __future__ import annotations

"""Client for the bounded logical certification bootstrap transport."""

import base64
import hashlib
import json
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


CLIENT_VERSION = "certification-logical-bootstrap-client-v4-durable-page-resume"
CHECKPOINT_VERSION = "certification-logical-bootstrap-checkpoint-v1"
DEFAULT_PAGE_PAUSE_SECONDS = 0.01


class LogicalBootstrapRestartRequired(RuntimeError):
    """The authoritative bootstrap identity changed and partial state is invalid."""


class LogicalBootstrapTransportError(RuntimeError):
    """A transient bootstrap transport/resource failure; preserve committed progress."""


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


def _open_json(request: urllib.request.Request, *, timeout: float = 30.0) -> dict[str, Any]:
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        code = int(exc.code)
        if code == 409:
            raise LogicalBootstrapRestartRequired(
                "authoritative logical bootstrap identity changed"
            ) from exc
        if code in {408, 429, 500, 502, 503, 504}:
            raise LogicalBootstrapTransportError(
                f"certification logical bootstrap HTTP failure:{code}"
            ) from exc
        raise RuntimeError(f"certification logical bootstrap HTTP failure:{code}") from exc
    except (urllib.error.URLError, TimeoutError, OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise LogicalBootstrapTransportError(
            f"certification logical bootstrap transport failed:{type(exc).__name__}"
        ) from exc
    if not isinstance(payload, dict):
        raise RuntimeError("certification logical bootstrap returned non-object")
    return payload


def _request(url: str, token: str) -> urllib.request.Request:
    return urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "X-Certification-Token": token,
            "User-Agent": "solana-roi-isolated-certifier-logical-bootstrap/4",
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
        raise LogicalBootstrapRestartRequired("certification logical bootstrap version mismatch")
    if str(payload.get("replication_version") or "") != REPLICATION_VERSION:
        raise LogicalBootstrapRestartRequired(
            "certification logical bootstrap replication version mismatch"
        )
    if str(payload.get("release_commit") or "") != expected_release:
        raise LogicalBootstrapRestartRequired("certification logical bootstrap release mismatch")
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
    if not (-2147483648 <= user_version <= 2147483647) or not (
        -2147483648 <= application_id <= 2147483647
    ):
        raise RuntimeError("certification logical bootstrap SQLite pragma metadata out of range")
    connection.execute(f"PRAGMA user_version={int(user_version)}")
    connection.execute(f"PRAGMA application_id={int(application_id)}")


def _work_paths(destination: Path, *, base: str, expected_release: str) -> tuple[Path, Path]:
    """Return stable same-filesystem work paths shared across retry cycles."""
    key = hashlib.sha256(f"{base}|{expected_release}".encode()).hexdigest()[:24]
    work = destination.parent / f".roi-certifier-logical-bootstrap-{key}.sqlite3"
    return work, work.with_suffix(work.suffix + ".state.json")


def _atomic_checkpoint(path: Path, payload: dict[str, Any]) -> None:
    fd, raw = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    tmp = Path(raw)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def _read_checkpoint(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _discard_partial(work: Path, checkpoint: Path) -> None:
    for path in (
        work,
        work.with_name(work.name + "-wal"),
        work.with_name(work.name + "-shm"),
        work.with_name(work.name + "-journal"),
        checkpoint,
    ):
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass


def _normalize_tables(tables: Any) -> list[dict[str, Any]]:
    if not isinstance(tables, list):
        raise RuntimeError("certification logical bootstrap schema manifest invalid")
    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for table in tables:
        if not isinstance(table, dict):
            raise RuntimeError("certification logical bootstrap table manifest invalid")
        name = str(table.get("name") or "")
        create_sql = str(table.get("create_sql") or "")
        columns = table.get("columns")
        if (
            not name
            or name in seen
            or not create_sql
            or not isinstance(columns, list)
            or not columns
        ):
            raise RuntimeError("certification logical bootstrap table manifest incomplete")
        seen.add(name)
        normalized.append(
            {
                "name": name,
                "create_sql": create_sql,
                "columns": [str(column) for column in columns],
                "without_rowid": bool(table.get("without_rowid")),
            }
        )
    return normalized


def _new_checkpoint(
    *,
    expected_release: str,
    epoch: str,
    fingerprint: str,
    start_watermark: int,
) -> dict[str, Any]:
    return {
        "checkpoint_version": CHECKPOINT_VERSION,
        "client_version": CLIENT_VERSION,
        "bootstrap_version": BOOTSTRAP_VERSION,
        "replication_version": REPLICATION_VERSION,
        "release_commit": expected_release,
        "epoch": epoch,
        "schema_fingerprint": fingerprint,
        "start_watermark": int(start_watermark),
        "phase": "rows",
        "table_index": 0,
        "cursor": None,
        "post_schema_index": 0,
        "bootstrap_rows": 0,
        "bootstrap_payload_bytes": 0,
    }


def _checkpoint_matches(
    state: dict[str, Any] | None,
    *,
    expected_release: str,
    epoch: str,
    fingerprint: str,
    start_watermark: int,
    table_count: int,
) -> bool:
    if not isinstance(state, dict):
        return False
    if (
        str(state.get("checkpoint_version") or "") != CHECKPOINT_VERSION
        or str(state.get("bootstrap_version") or "") != BOOTSTRAP_VERSION
        or str(state.get("replication_version") or "") != REPLICATION_VERSION
        or str(state.get("release_commit") or "") != expected_release
        or str(state.get("epoch") or "") != epoch
        or str(state.get("schema_fingerprint") or "") != fingerprint
    ):
        return False
    try:
        watermark = int(state.get("start_watermark"))
        table_index = int(state.get("table_index"))
        post_schema_index = int(state.get("post_schema_index", 0))
        rows = int(state.get("bootstrap_rows", 0))
        payload_bytes = int(state.get("bootstrap_payload_bytes", 0))
    except (TypeError, ValueError):
        return False
    if watermark != int(start_watermark):
        return False
    if not (0 <= table_index <= table_count) or post_schema_index < 0 or rows < 0 or payload_bytes < 0:
        return False
    if state.get("phase") not in {"rows", "post_schema"}:
        return False
    cursor = state.get("cursor")
    if cursor is not None and (not isinstance(cursor, str) or not cursor):
        return False
    return True


def _schema_object_exists(connection: sqlite3.Connection, item: dict[str, Any]) -> bool:
    name = str(item.get("name") or "")
    kind = str(item.get("type") or "")
    if not name or kind not in {"index", "view", "trigger"}:
        return False
    row = connection.execute(
        "SELECT type FROM sqlite_master WHERE name=? LIMIT 1", (name,)
    ).fetchone()
    return row is not None and str(row[0]) == kind


def logical_bootstrap(
    destination: Path,
    *,
    base: str,
    token: str,
    expected_release: str,
) -> dict[str, Any]:
    if not base or not token:
        raise RuntimeError("logical certification bootstrap source is not configured")

    manifest = _open_json(
        _request(f"{base}/v1/operations/certification-db-logical-bootstrap", token),
        timeout=30.0,
    )
    epoch, fingerprint, start_watermark = _validate_manifest(manifest, expected_release)
    tables = _normalize_tables(manifest.get("tables"))
    post_schema = manifest.get("post_schema")
    if not isinstance(post_schema, list):
        raise RuntimeError("certification logical bootstrap schema manifest invalid")

    work, checkpoint_path = _work_paths(
        destination, base=base, expected_release=expected_release
    )
    checkpoint = _read_checkpoint(checkpoint_path)
    if not work.is_file() or not _checkpoint_matches(
        checkpoint,
        expected_release=expected_release,
        epoch=epoch,
        fingerprint=fingerprint,
        start_watermark=start_watermark,
        table_count=len(tables),
    ):
        _discard_partial(work, checkpoint_path)
        connection = sqlite3.connect(work, timeout=30.0)
        try:
            connection.execute("PRAGMA foreign_keys=OFF")
            connection.execute("PRAGMA journal_mode=DELETE")
            connection.execute("PRAGMA synchronous=OFF")
            connection.execute("PRAGMA cache_size=-8192")
        finally:
            connection.close()
        checkpoint = _new_checkpoint(
            expected_release=expected_release,
            epoch=epoch,
            fingerprint=fingerprint,
            start_watermark=start_watermark,
        )
        _atomic_checkpoint(checkpoint_path, checkpoint)

    assert checkpoint is not None
    connection = sqlite3.connect(work, timeout=30.0)
    table_names = {str(table["name"]) for table in tables}
    try:
        connection.execute("PRAGMA foreign_keys=OFF")
        connection.execute("PRAGMA journal_mode=DELETE")
        connection.execute("PRAGMA synchronous=OFF")
        connection.execute("PRAGMA cache_size=-8192")

        table_index = int(checkpoint.get("table_index", 0))
        if str(checkpoint.get("phase")) == "rows":
            for index in range(table_index, len(tables)):
                table = tables[index]
                name = str(table["name"])
                create_sql = str(table["create_sql"])
                column_names = list(table["columns"])
                without_rowid = bool(table["without_rowid"])
                cursor = (
                    checkpoint.get("cursor")
                    if index == int(checkpoint.get("table_index", 0))
                    else None
                )

                exists = connection.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                    (name,),
                ).fetchone()
                if exists is None:
                    if cursor:
                        raise RuntimeError(
                            "certification logical bootstrap checkpoint table missing"
                        )
                    connection.execute(create_sql)
                    connection.commit()

                while True:
                    query: dict[str, Any] = {
                        "table": name,
                        "epoch": epoch,
                        "schema_fingerprint": fingerprint,
                        "limit": int(manifest.get("page_default_rows") or 250),
                    }
                    if cursor:
                        query["cursor"] = cursor
                    payload = _open_json(
                        _request(
                            f"{base}/v1/operations/certification-db-logical-bootstrap-page?"
                            f"{urllib.parse.urlencode(query)}",
                            token,
                        ),
                        timeout=30.0,
                    )
                    if str(payload.get("release_commit") or "") != expected_release:
                        raise LogicalBootstrapRestartRequired(
                            "certification logical bootstrap release changed"
                        )
                    if (
                        str(payload.get("epoch") or "") != epoch
                        or str(payload.get("schema_fingerprint") or "") != fingerprint
                    ):
                        raise LogicalBootstrapRestartRequired(
                            "certification logical bootstrap identity changed"
                        )
                    if str(payload.get("table") or "") != name:
                        raise RuntimeError(
                            "certification logical bootstrap table response mismatch"
                        )
                    if [str(column) for column in payload.get("columns", [])] != column_names:
                        raise LogicalBootstrapRestartRequired(
                            "certification logical bootstrap column layout changed"
                        )
                    rows = payload.get("rows")
                    if not isinstance(rows, list):
                        raise RuntimeError("certification logical bootstrap rows invalid")

                    quoted_columns = [_qident(column) for column in column_names]
                    insert_columns = (
                        quoted_columns
                        if without_rowid
                        else [_qident("rowid"), *quoted_columns]
                    )
                    placeholders = ",".join("?" for _ in insert_columns)
                    insert_sql = (
                        f"INSERT OR REPLACE INTO {_qident(name)}("
                        + ",".join(insert_columns)
                        + f") VALUES ({placeholders})"
                    )
                    decoded_rows: list[tuple[Any, ...]] = []
                    for record in rows:
                        if not isinstance(record, dict) or not isinstance(
                            record.get("values"), list
                        ):
                            raise RuntimeError(
                                "certification logical bootstrap row invalid"
                            )
                        values = tuple(_decode_value(value) for value in record["values"])
                        if len(values) != len(column_names):
                            raise RuntimeError(
                                "certification logical bootstrap row width mismatch"
                            )
                        if without_rowid:
                            decoded_rows.append(values)
                        else:
                            rowid = record.get("rowid")
                            if not isinstance(rowid, int) or isinstance(rowid, bool):
                                raise RuntimeError(
                                    "certification logical bootstrap rowid invalid"
                                )
                            decoded_rows.append((rowid, *values))

                    if decoded_rows:
                        connection.executemany(insert_sql, decoded_rows)
                    connection.commit()

                    checkpoint["bootstrap_rows"] = int(
                        checkpoint.get("bootstrap_rows", 0)
                    ) + len(decoded_rows)
                    checkpoint["bootstrap_payload_bytes"] = int(
                        checkpoint.get("bootstrap_payload_bytes", 0)
                    ) + int(payload.get("payload_bytes") or 0)

                    done = bool(payload.get("done"))
                    next_cursor = payload.get("next_cursor")
                    if done:
                        if next_cursor not in (None, ""):
                            raise RuntimeError(
                                "certification logical bootstrap terminal cursor invalid"
                            )
                        checkpoint["table_index"] = index + 1
                        checkpoint["cursor"] = None
                        _atomic_checkpoint(checkpoint_path, checkpoint)
                        break

                    if (
                        not isinstance(next_cursor, str)
                        or not next_cursor
                        or next_cursor == cursor
                    ):
                        raise RuntimeError(
                            "certification logical bootstrap cursor did not advance"
                        )
                    cursor = next_cursor
                    checkpoint["table_index"] = index
                    checkpoint["cursor"] = cursor
                    _atomic_checkpoint(checkpoint_path, checkpoint)

                    pause = _page_pause_seconds()
                    if pause > 0:
                        time.sleep(pause)

            _restore_sqlite_metadata(
                connection, manifest=manifest, table_names=table_names
            )
            connection.commit()
            checkpoint["phase"] = "post_schema"
            checkpoint["table_index"] = len(tables)
            checkpoint["cursor"] = None
            checkpoint["post_schema_index"] = 0
            _atomic_checkpoint(checkpoint_path, checkpoint)

        order = {"index": 0, "view": 1, "trigger": 2}
        ordered_schema = sorted(
            (item for item in post_schema if isinstance(item, dict)),
            key=lambda item: (
                order.get(str(item.get("type") or ""), 99),
                str(item.get("name") or ""),
            ),
        )
        start_schema = int(checkpoint.get("post_schema_index", 0))
        for schema_index in range(start_schema, len(ordered_schema)):
            item = ordered_schema[schema_index]
            sql = str(item.get("sql") or "").strip()
            if sql and not _schema_object_exists(connection, item):
                connection.execute(sql)
                connection.commit()
            checkpoint["post_schema_index"] = schema_index + 1
            _atomic_checkpoint(checkpoint_path, checkpoint)

        connection.execute("PRAGMA synchronous=NORMAL")
        connection.execute("SELECT COUNT(*) FROM sqlite_master").fetchone()
    except LogicalBootstrapTransportError:
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
        _discard_partial(work, checkpoint_path)
        raise
    except BaseException:
        try:
            connection.rollback()
        except sqlite3.Error:
            pass
        connection.close()
        _discard_partial(work, checkpoint_path)
        raise
    else:
        connection.close()

    destination.unlink(missing_ok=True)
    os.replace(work, destination)
    checkpoint_path.unlink(missing_ok=True)

    return {
        "client_version": CLIENT_VERSION,
        "bootstrap_version": BOOTSTRAP_VERSION,
        "replication_version": REPLICATION_VERSION,
        "release_commit": expected_release,
        "epoch": epoch,
        "schema_fingerprint": fingerprint,
        "watermark": start_watermark,
        "bootstrap_rows": int(checkpoint.get("bootstrap_rows", 0)),
        "bootstrap_payload_bytes": int(
            checkpoint.get("bootstrap_payload_bytes", 0)
        ),
        "bootstrap_tables": len(tables),
        "sqlite_sequence_preserved": True,
        "sqlite_pragma_metadata_preserved": True,
        "page_pause_seconds": _page_pause_seconds(),
        "resumable_page_checkpoint": True,
        "last_transport": "bounded_logical_bootstrap",
        "paper_only": True,
        "live_money_authority": False,
    }


__all__ = [
    "CHECKPOINT_VERSION",
    "CLIENT_VERSION",
    "LogicalBootstrapRestartRequired",
    "LogicalBootstrapTransportError",
    "logical_bootstrap",
]
