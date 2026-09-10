from __future__ import annotations

"""Bounded logical bootstrap transport for the isolated certification replica.

The authoritative service never materializes a second full SQLite file. Instead it
exposes authenticated schema metadata and deterministic keyset-paginated table rows.
The replication journal is established before the scan begins; mutations that race
with the scan are reconciled by the normal delta protocol after the certifier builds
its local replica.
"""

import base64
import json
import os
import sqlite3
from pathlib import Path
from typing import Any, Callable

from fastapi import Header, HTTPException, Query

from . import certification_incremental_replication as replication
from . import certification_service_split as split

BOOTSTRAP_VERSION = "certification-logical-bootstrap-v2-sequence-frontier"
DEFAULT_PAGE_ROWS = 250
MAX_PAGE_ROWS = 500
DEFAULT_PAGE_BYTES = 4 * 1024 * 1024
MAX_PAGE_BYTES = 16 * 1024 * 1024

PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False


def _page_bytes() -> int:
    try:
        return min(
            MAX_PAGE_BYTES,
            max(256 * 1024, int(os.getenv("SOLANA_ROI_CERTIFICATION_LOGICAL_BOOTSTRAP_PAGE_BYTES", str(DEFAULT_PAGE_BYTES)))),
        )
    except ValueError:
        return DEFAULT_PAGE_BYTES


def _encode_value(value: Any) -> Any:
    if isinstance(value, bytes):
        return {"__sqlite_blob_b64__": base64.b64encode(value).decode("ascii")}
    return value


def _encode_cursor(values: list[Any]) -> str:
    raw = json.dumps([_encode_value(value) for value in values], separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _decode_cursor(value: str | None) -> list[Any]:
    if not value:
        return []
    try:
        padding = "=" * (-len(value) % 4)
        raw = base64.urlsafe_b64decode((value + padding).encode("ascii"))
        decoded = json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=400, detail="invalid certification logical bootstrap cursor") from exc
    if not isinstance(decoded, list):
        raise HTTPException(status_code=400, detail="invalid certification logical bootstrap cursor")
    result: list[Any] = []
    for item in decoded:
        if isinstance(item, dict) and set(item) == {"__sqlite_blob_b64__"}:
            try:
                result.append(base64.b64decode(str(item["__sqlite_blob_b64__"]), validate=True))
            except (ValueError, TypeError) as exc:
                raise HTTPException(status_code=400, detail="invalid certification logical bootstrap cursor blob") from exc
        elif item is None or isinstance(item, (str, int, float)):
            result.append(item)
        else:
            raise HTTPException(status_code=400, detail="invalid certification logical bootstrap cursor value")
    return result


def _runtime_store(runtime_provider: Callable[[], Any]) -> Any:
    try:
        runtime = runtime_provider()
    except Exception as exc:
        raise HTTPException(status_code=503, detail="canonical certification runtime unavailable") from exc
    store = getattr(runtime, "store", None)
    if store is None:
        raise HTTPException(status_code=503, detail="canonical certification store unavailable")
    return store


def _pinned_reader(store: Any) -> sqlite3.Connection:
    source_path = Path(getattr(store, "path", ""))
    if not source_path.is_file():
        raise HTTPException(status_code=503, detail="canonical certification source unavailable")
    reader = sqlite3.connect(f"file:{source_path.resolve()}?mode=ro", uri=True, timeout=5.0)
    reader.execute("PRAGMA query_only=ON")
    reader.execute("PRAGMA busy_timeout=5000")
    reader.execute("PRAGMA cache_size=-4096")
    reader.execute("BEGIN")
    return reader


def _validate_identity(
    reader: sqlite3.Connection,
    epoch: str,
    fingerprint: str,
    *,
    verify_fingerprint: bool = True,
) -> dict[str, str]:
    meta = replication._meta(reader)
    if str(meta.get("epoch") or "") != epoch or str(meta.get("schema_fingerprint") or "") != fingerprint:
        raise HTTPException(status_code=409, detail="certification_logical_bootstrap_restart_required:identity_changed")
    configured = int(meta.get("configured_schema_version", "-1"))
    observed = int(reader.execute("PRAGMA schema_version").fetchone()[0])
    if configured != observed:
        raise HTTPException(status_code=409, detail="certification_logical_bootstrap_restart_required:schema_changed")
    # Manifest construction performs the full fingerprint walk. Page requests only
    # need the durable identity plus SQLite's schema cookie: every DDL operation that
    # can alter tables/indexes/views/triggers changes schema_version. Avoiding a full
    # sqlite_master/table_xinfo walk on every historical page keeps page work bounded
    # without weakening fail-closed schema-change detection.
    if verify_fingerprint and replication._schema_fingerprint(reader) != fingerprint:
        raise HTTPException(status_code=409, detail="certification_logical_bootstrap_restart_required:schema_changed")
    return meta


def _page_table(reader: sqlite3.Connection, table_name: str) -> dict[str, Any] | None:
    """Resolve one ordinary table without rescanning metadata for every table."""

    if table_name.startswith("sqlite_") or table_name in {replication.CHANGE_TABLE, replication.META_TABLE}:
        return None
    row = reader.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    if row is None:
        return None
    sql = str(row[0] or "")
    if not sql or "VIRTUAL TABLE" in sql.upper():
        return None
    return {
        "name": table_name,
        "sql": sql,
        "without_rowid": "WITHOUT ROWID" in sql.upper(),
    }


def _manifest(store: Any) -> dict[str, Any]:
    identity = replication.prepare_bootstrap(store)
    reader = _pinned_reader(store)
    source_path = Path(getattr(store, "path", ""))
    try:
        meta = _validate_identity(reader, str(identity["epoch"]), str(identity["schema_fingerprint"]))
        tables = replication._ordinary_tables(reader)
        table_names = {str(table["name"]) for table in tables}
        schema_objects = replication._schema_objects(reader)
        schema_table_names = {name for kind, name, _table, _sql in schema_objects if kind == "table"}
        unsupported = sorted(schema_table_names - table_names)
        if unsupported:
            raise HTTPException(
                status_code=409,
                detail="certification_logical_bootstrap_unsupported_table_kind:" + ",".join(unsupported[:8]),
            )
        manifest_tables: list[dict[str, Any]] = []
        for table in tables:
            name = str(table["name"])
            columns = replication._columns(reader, name)
            visible = [column for column in columns if int(column["hidden"]) == 0]
            primary = replication._primary_columns(visible)
            if bool(table["without_rowid"]) and not primary:
                raise HTTPException(status_code=409, detail=f"certification_logical_bootstrap_unstable_table:{name}")
            manifest_tables.append(
                {
                    "name": name,
                    "create_sql": str(table["sql"]),
                    "without_rowid": bool(table["without_rowid"]),
                    "columns": [str(column["name"]) for column in visible],
                    "primary_key_columns": [str(column["name"]) for column in primary],
                }
            )
        post_schema = [
            {"type": kind, "name": name, "table": table_name, "sql": sql}
            for kind, name, table_name, sql in schema_objects
            if kind in {"index", "view", "trigger"} and str(sql or "").strip()
        ]
        sequence_rows: list[dict[str, Any]] = []
        sequence_exists = reader.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='sqlite_sequence'"
        ).fetchone()
        if sequence_exists is not None:
            sequence_rows = [
                {"name": str(name), "seq": int(seq)}
                for name, seq in reader.execute("SELECT name,seq FROM sqlite_sequence ORDER BY name").fetchall()
                if str(name) in table_names and seq is not None
            ]
        start_watermark = replication._current_watermark(reader)
        return {
            "bootstrap_version": BOOTSTRAP_VERSION,
            "replication_version": replication.REPLICATION_VERSION,
            "release_commit": split._release_commit(),
            "epoch": str(identity["epoch"]),
            "schema_fingerprint": str(identity["schema_fingerprint"]),
            "schema_version": int(meta["configured_schema_version"]),
            "start_watermark": int(start_watermark),
            "tables": manifest_tables,
            "post_schema": post_schema,
            "sqlite_sequence": sequence_rows,
            "pragmas": {
                "user_version": int(reader.execute("PRAGMA user_version").fetchone()[0]),
                "application_id": int(reader.execute("PRAGMA application_id").fetchone()[0]),
            },
            "page_default_rows": DEFAULT_PAGE_ROWS,
            "page_max_rows": MAX_PAGE_ROWS,
            "page_max_bytes": _page_bytes(),
            "full_snapshot_required": False,
            "bootstrap_transport": "bounded_logical_keyset_pages_plus_journal_reconciliation",
            "paper_only": True,
            "live_money_authority": False,
            "signing_available": False,
            "transaction_submission_available": False,
        }
    finally:
        reader.close()
        split._drop_file_cache(source_path)


def _stream_page_records(
    raw_cursor: Any,
    *,
    table_name: str,
    bounded_limit: int,
    max_bytes: int,
    row_to_record: Callable[[Any], dict[str, Any]],
    cursor_for: Callable[[dict[str, Any]], str],
) -> tuple[list[dict[str, Any]], int, bool, str | None]:
    """Materialize at most the bounded response plus one transient SQLite row.

    The transport enforces the byte limit as each buffered row is encoded. A row
    rejected by the byte cap is intentionally not represented in ``next_cursor``;
    the next keyset request starts after the last included row and therefore reads
    that row again exactly.
    """

    response_rows: list[dict[str, Any]] = []
    payload_bytes = 0
    truncated_by_bytes = False
    exhausted = False

    while len(response_rows) < bounded_limit:
        row = raw_cursor.fetchone()
        if row is None:
            exhausted = True
            break
        record = row_to_record(row)
        size = len(json.dumps(record, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
        if response_rows and payload_bytes + size > max_bytes:
            truncated_by_bytes = True
            break
        if not response_rows and size > max_bytes:
            raise HTTPException(
                status_code=409,
                detail=f"certification_logical_bootstrap_row_exceeds_page_bound:{table_name}",
            )
        response_rows.append(record)
        payload_bytes += size

    has_more_by_rows = False
    if not truncated_by_bytes and not exhausted and len(response_rows) >= bounded_limit:
        has_more_by_rows = raw_cursor.fetchone() is not None
        exhausted = not has_more_by_rows

    done = exhausted and not truncated_by_bytes
    next_cursor = cursor_for(response_rows[-1]) if response_rows and not done else None
    return response_rows, payload_bytes, done, next_cursor


def _json_value_lower_bound(value: Any) -> int:
    """Cheap allocation-free lower bound for encoded JSON bytes of a SQLite value."""

    if value is None:
        return 4
    if isinstance(value, bytes):
        # Blob transport uses base64 plus a wrapper, so raw bytes are a strict lower bound.
        return len(value)
    if isinstance(value, str):
        # UTF-8 uses at least one byte per code point and JSON adds quoting/escaping.
        return len(value)
    if isinstance(value, bool):
        return 4
    if isinstance(value, int):
        return len(str(value))
    if isinstance(value, float):
        return 1
    return 0


def _fetch_bounded_raw_rows(raw_cursor: Any, *, bounded_limit: int, max_bytes: int) -> list[tuple[Any, ...]]:
    """Copy a small raw batch, then let the SQLite snapshot close before JSON work.

    At most ``bounded_limit + 1`` rows are retained. For wide rows, the raw-value
    lower bound stops collection after the first row that proves the eventual JSON
    page must cross ``max_bytes``. Keeping that one overflow row is intentional: the
    normal byte-bound materializer will reject or defer it exactly, preserving cursor
    semantics without holding the SQLite read transaction during encoding.
    """

    rows: list[tuple[Any, ...]] = []
    raw_lower_bound = 0
    while len(rows) < bounded_limit + 1:
        row = raw_cursor.fetchone()
        if row is None:
            break
        copied = tuple(row)
        rows.append(copied)
        raw_lower_bound += sum(_json_value_lower_bound(value) for value in copied)
        if raw_lower_bound > max_bytes:
            break
    return rows


class _BufferedRawCursor:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self._rows = iter(rows)

    def fetchone(self) -> tuple[Any, ...] | None:
        try:
            return next(self._rows)
        except StopIteration:
            return None


def _page(
    store: Any,
    *,
    table_name: str,
    epoch: str,
    fingerprint: str,
    cursor: str | None,
    limit: int,
) -> dict[str, Any]:
    reader = _pinned_reader(store)
    source_path = Path(getattr(store, "path", ""))
    names: list[str]
    row_to_record: Callable[[Any], dict[str, Any]]
    cursor_for: Callable[[dict[str, Any]], str]
    raw_rows: list[tuple[Any, ...]]
    max_bytes = _page_bytes()
    try:
        # The manifest already performed the expensive full schema fingerprint walk.
        # Each page still fails closed on epoch/fingerprint identity and schema_version,
        # but does not rescan every schema object and every table's columns.
        _validate_identity(reader, epoch, fingerprint, verify_fingerprint=False)
        table = _page_table(reader, table_name)
        if table is None:
            raise HTTPException(status_code=404, detail="certification logical bootstrap table not found")
        columns = [column for column in replication._columns(reader, table_name) if int(column["hidden"]) == 0]
        names = [str(column["name"]) for column in columns]
        if not names:
            raise HTTPException(status_code=409, detail=f"certification_logical_bootstrap_empty_table_schema:{table_name}")
        bounded_limit = min(MAX_PAGE_ROWS, max(1, int(limit)))
        decoded_cursor = _decode_cursor(cursor)
        quoted_table = replication._qident(table_name)
        projection = ",".join(replication._qident(name) for name in names)
        params: list[Any] = []

        if bool(table["without_rowid"]):
            primary = replication._primary_columns(columns)
            pk_names = [str(column["name"]) for column in primary]
            if not pk_names:
                raise HTTPException(status_code=409, detail=f"certification_logical_bootstrap_unstable_table:{table_name}")
            order = ",".join(replication._qident(name) for name in pk_names)
            where = ""
            if decoded_cursor:
                if len(decoded_cursor) != len(pk_names):
                    raise HTTPException(status_code=400, detail="invalid certification logical bootstrap composite cursor")
                tuple_names = "(" + ",".join(replication._qident(name) for name in pk_names) + ")"
                tuple_values = "(" + ",".join("?" for _ in pk_names) + ")"
                where = f" WHERE {tuple_names} > {tuple_values}"
                params.extend(decoded_cursor)
            query = f"SELECT {projection} FROM {quoted_table}{where} ORDER BY {order} LIMIT ?"
            params.append(bounded_limit + 1)
            raw_cursor = reader.execute(query, params)

            def row_to_record(row: Any) -> dict[str, Any]:
                return {"values": [_encode_value(value) for value in row]}

            primary_indexes = [names.index(name) for name in pk_names]

            def cursor_for(record: dict[str, Any]) -> str:
                values = record["values"]
                raw_values: list[Any] = []
                for index in primary_indexes:
                    value = values[index]
                    if isinstance(value, dict) and "__sqlite_blob_b64__" in value:
                        raw_values.append(base64.b64decode(str(value["__sqlite_blob_b64__"])))
                    else:
                        raw_values.append(value)
                return _encode_cursor(raw_values)
        else:
            last_rowid = 0
            if decoded_cursor:
                if len(decoded_cursor) != 1 or not isinstance(decoded_cursor[0], int):
                    raise HTTPException(status_code=400, detail="invalid certification logical bootstrap rowid cursor")
                last_rowid = int(decoded_cursor[0])
            query = f"SELECT rowid,{projection} FROM {quoted_table} WHERE rowid>? ORDER BY rowid LIMIT ?"
            raw_cursor = reader.execute(query, (last_rowid, bounded_limit + 1))

            def row_to_record(row: Any) -> dict[str, Any]:
                return {"rowid": int(row[0]), "values": [_encode_value(value) for value in row[1:]]}

            def cursor_for(record: dict[str, Any]) -> str:
                return _encode_cursor([int(record["rowid"])])

        raw_rows = _fetch_bounded_raw_rows(
            raw_cursor,
            bounded_limit=bounded_limit,
            max_bytes=max_bytes,
        )
    finally:
        # Release the WAL read snapshot and advise away source pages before base64/
        # JSON materialization. Concurrent canonical mutations remain exact through
        # the start-watermark replication journal and post-bootstrap delta replay.
        reader.close()
        split._drop_file_cache(source_path)

    response_rows, payload_bytes, done, next_cursor = _stream_page_records(
        _BufferedRawCursor(raw_rows),
        table_name=table_name,
        bounded_limit=bounded_limit,
        max_bytes=max_bytes,
        row_to_record=row_to_record,
        cursor_for=cursor_for,
    )
    return {
        "bootstrap_version": BOOTSTRAP_VERSION,
        "release_commit": split._release_commit(),
        "epoch": epoch,
        "schema_fingerprint": fingerprint,
        "table": table_name,
        "columns": names,
        "rows": response_rows,
        "row_count": len(response_rows),
        "payload_bytes": payload_bytes,
        "next_cursor": next_cursor,
        "done": done,
        "paper_only": True,
        "live_money_authority": False,
    }


def install_certification_logical_bootstrap(app: Any, runtime_provider: Callable[[], Any]) -> None:
    manifest_path = "/v1/operations/certification-db-logical-bootstrap"
    page_path = "/v1/operations/certification-db-logical-bootstrap-page"
    existing = {getattr(route, "path", None) for route in app.routes}
    if manifest_path not in existing:
        @app.get(manifest_path)
        def certification_db_logical_bootstrap(
            x_certification_token: str | None = Header(default=None, alias="X-Certification-Token"),
        ) -> dict[str, Any]:
            replication._require_shared_token(x_certification_token)
            return _manifest(_runtime_store(runtime_provider))
    if page_path not in existing:
        @app.get(page_path)
        def certification_db_logical_bootstrap_page(
            table: str = Query(min_length=1, max_length=256),
            epoch: str = Query(min_length=8, max_length=128),
            schema_fingerprint: str = Query(min_length=32, max_length=128),
            cursor: str | None = Query(default=None, max_length=4096),
            limit: int = Query(default=DEFAULT_PAGE_ROWS, ge=1, le=MAX_PAGE_ROWS),
            x_certification_token: str | None = Header(default=None, alias="X-Certification-Token"),
        ) -> dict[str, Any]:
            replication._require_shared_token(x_certification_token)
            return _page(
                _runtime_store(runtime_provider),
                table_name=table,
                epoch=epoch,
                fingerprint=schema_fingerprint,
                cursor=cursor,
                limit=limit,
            )
    app.state.roi_certification_logical_bootstrap = True
    app.state.roi_certification_logical_bootstrap_version = BOOTSTRAP_VERSION


__all__ = ["BOOTSTRAP_VERSION", "install_certification_logical_bootstrap"]
