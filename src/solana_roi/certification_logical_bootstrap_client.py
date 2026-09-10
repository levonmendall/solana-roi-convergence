from __future__ import annotations

"""Client for the bounded logical certification bootstrap transport."""

import base64
import json
import sqlite3
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

from .certification_incremental_replication import REPLICATION_VERSION
from .certification_logical_bootstrap import BOOTSTRAP_VERSION


CLIENT_VERSION = "certification-logical-bootstrap-client-v1"


class LogicalBootstrapRestartRequired(RuntimeError):
    pass


def _open_json(request: urllib.request.Request, *, timeout: float = 30.0) -> dict[str, Any]:
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        if int(exc.code) == 409:
            raise LogicalBootstrapRestartRequired("authoritative logical bootstrap identity changed") from exc
        raise RuntimeError(f"certification logical bootstrap HTTP failure:{exc.code}") from exc
    except (urllib.error.URLError, TimeoutError, OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"certification logical bootstrap transport failed:{type(exc).__name__}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("certification logical bootstrap returned non-object")
    return payload


def _request(url: str, token: str) -> urllib.request.Request:
    return urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "X-Certification-Token": token,
            "User-Agent": "solana-roi-isolated-certifier-logical-bootstrap/1",
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
    tables = manifest.get("tables")
    post_schema = manifest.get("post_schema")
    if not isinstance(tables, list) or not isinstance(post_schema, list):
        raise RuntimeError("certification logical bootstrap schema manifest invalid")

    destination.unlink(missing_ok=True)
    connection = sqlite3.connect(destination, timeout=30.0)
    total_rows = 0
    total_payload_bytes = 0
    table_count = 0
    try:
        connection.execute("PRAGMA foreign_keys=OFF")
        connection.execute("PRAGMA journal_mode=DELETE")
        connection.execute("PRAGMA synchronous=OFF")
        connection.execute("PRAGMA cache_size=-8192")
        for table in tables:
            if not isinstance(table, dict):
                raise RuntimeError("certification logical bootstrap table manifest invalid")
            name = str(table.get("name") or "")
            create_sql = str(table.get("create_sql") or "")
            columns = table.get("columns")
            without_rowid = bool(table.get("without_rowid"))
            if not name or not create_sql or not isinstance(columns, list) or not columns:
                raise RuntimeError("certification logical bootstrap table manifest incomplete")
            column_names = [str(column) for column in columns]
            connection.execute(create_sql)
            connection.commit()
            table_count += 1

            cursor: str | None = None
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
                        f"{base}/v1/operations/certification-db-logical-bootstrap-page?{urllib.parse.urlencode(query)}",
                        token,
                    ),
                    timeout=30.0,
                )
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
                if without_rowid:
                    insert_columns = quoted_columns
                else:
                    insert_columns = [_qident("rowid"), *quoted_columns]
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
                        if not isinstance(rowid, int):
                            raise RuntimeError("certification logical bootstrap rowid invalid")
                        decoded_rows.append((rowid, *values))
                if decoded_rows:
                    connection.executemany(insert_sql, decoded_rows)
                connection.commit()
                total_rows += len(decoded_rows)
                total_payload_bytes += int(payload.get("payload_bytes") or 0)
                done = bool(payload.get("done"))
                next_cursor = payload.get("next_cursor")
                if done:
                    if next_cursor not in (None, ""):
                        raise RuntimeError("certification logical bootstrap terminal cursor invalid")
                    break
                if not isinstance(next_cursor, str) or not next_cursor or next_cursor == cursor:
                    raise RuntimeError("certification logical bootstrap cursor did not advance")
                cursor = next_cursor

        # Data loads occur before user triggers exist, so source-side effects are not
        # re-fired. Explicit user indexes/views/triggers are recreated only after all
        # canonical rows have been copied.
        order = {"index": 0, "view": 1, "trigger": 2}
        ordered_schema = sorted(
            (item for item in post_schema if isinstance(item, dict)),
            key=lambda item: (order.get(str(item.get("type") or ""), 99), str(item.get("name") or "")),
        )
        for item in ordered_schema:
            sql = str(item.get("sql") or "").strip()
            if sql:
                connection.execute(sql)
        connection.commit()
        connection.execute("PRAGMA synchronous=NORMAL")
        connection.execute("SELECT COUNT(*) FROM sqlite_master").fetchone()
    except BaseException:
        try:
            connection.rollback()
        except sqlite3.Error:
            pass
        connection.close()
        destination.unlink(missing_ok=True)
        raise
    else:
        connection.close()

    return {
        "client_version": CLIENT_VERSION,
        "bootstrap_version": BOOTSTRAP_VERSION,
        "replication_version": REPLICATION_VERSION,
        "release_commit": expected_release,
        "epoch": epoch,
        "schema_fingerprint": fingerprint,
        "watermark": start_watermark,
        "bootstrap_rows": total_rows,
        "bootstrap_payload_bytes": total_payload_bytes,
        "bootstrap_tables": table_count,
        "last_transport": "bounded_logical_bootstrap",
        "paper_only": True,
        "live_money_authority": False,
    }


__all__ = ["CLIENT_VERSION", "LogicalBootstrapRestartRequired", "logical_bootstrap"]
