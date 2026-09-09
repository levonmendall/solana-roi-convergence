from __future__ import annotations

"""Incremental, exact-state replication for the isolated certification service.

The authoritative runtime keeps the canonical SQLite database. This module adds a
small operational change journal backed by SQLite triggers. The journal records only
the stable key of rows that changed, never a second copy of the row payload. A
certifier asks for all committed changes after its verified high-watermark; the
runtime resolves those keys against one pinned read transaction and returns a bounded
set of deterministic SQL mutations representing the exact logical table state at the
new high-watermark.

Full SQLite snapshots remain the bootstrap/recovery path. They are not the normal
certification heartbeat.
"""

import hashlib
import hmac
import os
import secrets
import sqlite3
from pathlib import Path
from typing import Any, Callable

from fastapi import Header, HTTPException, Query

from . import certification_service_split as split


REPLICATION_VERSION = "certification-incremental-replica-v1"
CHANGE_TABLE = "certification_replication_changes"
META_TABLE = "certification_replication_meta"
TRIGGER_PREFIX = "roi_cert_rep_"
DEFAULT_MAX_DELTA_ROWS = 50_000
DEFAULT_MAX_DELTA_BYTES = 16 * 1024 * 1024

PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
STRATEGY_THRESHOLDS_CHANGED = False
CERTIFICATION_THRESHOLDS_CHANGED = False
CONTINUITY_SEMANTICS_CHANGED = False


def _env_int(name: str, default: int, minimum: int) -> int:
    try:
        return max(minimum, int(os.getenv(name, str(default))))
    except ValueError:
        return default


def _max_delta_rows() -> int:
    return _env_int("SOLANA_ROI_CERTIFICATION_DELTA_MAX_ROWS", DEFAULT_MAX_DELTA_ROWS, 100)


def _max_delta_bytes() -> int:
    return _env_int("SOLANA_ROI_CERTIFICATION_DELTA_MAX_BYTES", DEFAULT_MAX_DELTA_BYTES, 1024 * 1024)


def _qident(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _qliteral(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _table_rows(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    """Return ordinary user tables only; virtual/shadow/internal tables are excluded."""
    rows: list[dict[str, Any]] = []
    try:
        listed = connection.execute("PRAGMA table_list").fetchall()
        for row in listed:
            schema, name, kind, _ncol, without_rowid, _strict = row
            name = str(name)
            if str(schema) != "main" or str(kind) != "table":
                continue
            if name.startswith("sqlite_") or name in {CHANGE_TABLE, META_TABLE}:
                continue
            sql_row = connection.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
                (name,),
            ).fetchone()
            sql = str(sql_row[0] or "") if sql_row is not None else ""
            if not sql:
                continue
            rows.append({"name": name, "sql": sql, "without_rowid": bool(without_rowid)})
    except sqlite3.DatabaseError:
        listed = connection.execute(
            "SELECT name, sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ).fetchall()
        for name, sql in listed:
            name = str(name)
            if name in {CHANGE_TABLE, META_TABLE} or not sql:
                continue
            upper = str(sql).upper()
            if "VIRTUAL TABLE" in upper:
                continue
            rows.append({"name": name, "sql": str(sql), "without_rowid": "WITHOUT ROWID" in upper})
    rows.sort(key=lambda row: row["name"])
    return rows


def _columns(connection: sqlite3.Connection, table: str) -> list[dict[str, Any]]:
    rows = connection.execute(f"PRAGMA table_info({_qliteral(table)})").fetchall()
    return [
        {
            "cid": int(row[0]),
            "name": str(row[1]),
            "type": str(row[2] or ""),
            "notnull": int(row[3]),
            "default": row[4],
            "pk": int(row[5]),
        }
        for row in rows
    ]


def _schema_fingerprint(connection: sqlite3.Connection) -> str:
    payload: list[str] = []
    for table in _table_rows(connection):
        payload.append(f"TABLE:{table['name']}:{table['sql']}")
        for column in _columns(connection, str(table["name"])):
            payload.append(
                "COLUMN:"
                + ":".join(
                    [
                        str(table["name"]),
                        str(column["cid"]),
                        str(column["name"]),
                        str(column["type"]),
                        str(column["notnull"]),
                        str(column["default"]),
                        str(column["pk"]),
                    ]
                )
            )
    return hashlib.sha256("\n".join(payload).encode("utf-8")).hexdigest()


def _meta(connection: sqlite3.Connection) -> dict[str, str]:
    rows = connection.execute(f"SELECT key, value FROM {_qident(META_TABLE)}").fetchall()
    return {str(row[0]): str(row[1]) for row in rows}


def _set_meta(connection: sqlite3.Connection, key: str, value: str) -> None:
    connection.execute(
        f"INSERT INTO {_qident(META_TABLE)}(key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )


def _drop_tracking_triggers(connection: sqlite3.Connection) -> None:
    rows = connection.execute(
        "SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE ?",
        (TRIGGER_PREFIX + "%",),
    ).fetchall()
    for row in rows:
        connection.execute(f"DROP TRIGGER IF EXISTS {_qident(str(row[0]))}")


def _key_expression(table: dict[str, Any], columns: list[dict[str, Any]], alias: str) -> str:
    if not bool(table["without_rowid"]):
        return f"'rowid IS ' || quote({alias}.rowid)"
    primary = sorted(
        (column for column in columns if int(column["pk"]) > 0),
        key=lambda row: int(row["pk"]),
    )
    if not primary:
        raise RuntimeError(f"WITHOUT ROWID certification table lacks primary key:{table['name']}")
    parts: list[str] = []
    for index, column in enumerate(primary):
        prefix = "" if index == 0 else " AND "
        parts.append(
            _qliteral(prefix + _qident(str(column["name"])) + " IS ")
            + f" || quote({alias}.{_qident(str(column['name']))})"
        )
    return " || ".join(parts)


def _install_table_triggers(connection: sqlite3.Connection, table: dict[str, Any]) -> None:
    table_name = str(table["name"])
    columns = _columns(connection, table_name)
    if not columns:
        raise RuntimeError(f"certification replication table has no columns:{table_name}")
    digest = hashlib.sha256(table_name.encode("utf-8")).hexdigest()[:16]
    new_key = _key_expression(table, columns, "NEW")
    old_key = _key_expression(table, columns, "OLD")
    quoted_table = _qident(table_name)
    table_literal = _qliteral(table_name)
    changed_at = "strftime('%Y-%m-%dT%H:%M:%fZ','now')"

    connection.execute(
        f"CREATE TRIGGER IF NOT EXISTS {_qident(TRIGGER_PREFIX + digest + '_ai')} AFTER INSERT ON {quoted_table} BEGIN "
        f"INSERT INTO {_qident(CHANGE_TABLE)}(table_name,key_sql,operation,changed_at) "
        f"VALUES ({table_literal},{new_key},'upsert',{changed_at}); END"
    )
    connection.execute(
        f"CREATE TRIGGER IF NOT EXISTS {_qident(TRIGGER_PREFIX + digest + '_ad')} AFTER DELETE ON {quoted_table} BEGIN "
        f"INSERT INTO {_qident(CHANGE_TABLE)}(table_name,key_sql,operation,changed_at) "
        f"VALUES ({table_literal},{old_key},'delete',{changed_at}); END"
    )
    connection.execute(
        f"CREATE TRIGGER IF NOT EXISTS {_qident(TRIGGER_PREFIX + digest + '_au')} AFTER UPDATE ON {quoted_table} BEGIN "
        f"INSERT INTO {_qident(CHANGE_TABLE)}(table_name,key_sql,operation,changed_at) VALUES ({table_literal},{old_key},'delete',{changed_at}); "
        f"INSERT INTO {_qident(CHANGE_TABLE)}(table_name,key_sql,operation,changed_at) VALUES ({table_literal},{new_key},'upsert',{changed_at}); END"
    )


def _ensure_tracking_locked(connection: sqlite3.Connection) -> tuple[dict[str, str], bool]:
    connection.execute(
        f"CREATE TABLE IF NOT EXISTS {_qident(CHANGE_TABLE)}("
        "id INTEGER PRIMARY KEY AUTOINCREMENT,table_name TEXT NOT NULL,key_sql TEXT NOT NULL,"
        "operation TEXT NOT NULL CHECK(operation IN ('upsert','delete')),changed_at TEXT NOT NULL)"
    )
    connection.execute(
        f"CREATE INDEX IF NOT EXISTS ix_certification_replication_changes_id ON {_qident(CHANGE_TABLE)}(id)"
    )
    connection.execute(
        f"CREATE TABLE IF NOT EXISTS {_qident(META_TABLE)}(key TEXT PRIMARY KEY,value TEXT NOT NULL)"
    )
    fingerprint = _schema_fingerprint(connection)
    meta = _meta(connection)
    changed = not meta or meta.get("schema_fingerprint") != fingerprint
    if changed:
        _drop_tracking_triggers(connection)
        connection.execute(f"DELETE FROM {_qident(CHANGE_TABLE)}")
        _set_meta(connection, "epoch", secrets.token_urlsafe(24))
        _set_meta(connection, "schema_fingerprint", fingerprint)
        _set_meta(connection, "replication_version", REPLICATION_VERSION)
    for table in _table_rows(connection):
        _install_table_triggers(connection, table)
    return _meta(connection), changed


def prepare_bootstrap(store: Any) -> dict[str, Any]:
    """Refresh schema tracking before a full bootstrap snapshot is materialized."""
    with store._lock, store.db:
        meta, changed = _ensure_tracking_locked(store.db)
        watermark = int(
            store.db.execute(f"SELECT COALESCE(MAX(id),0) FROM {_qident(CHANGE_TABLE)}").fetchone()[0]
        )
    return {
        "replication_version": REPLICATION_VERSION,
        "epoch": meta["epoch"],
        "schema_fingerprint": meta["schema_fingerprint"],
        "watermark": watermark,
        "schema_reconfigured": changed,
        "paper_only": True,
        "live_money_authority": False,
    }


def _sql_value(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bytes):
        return "X'" + value.hex() + "'"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if value != value:
            return "NULL"
        if value == float("inf"):
            return "9e999"
        if value == float("-inf"):
            return "-9e999"
        return repr(value)
    return _qliteral(str(value))


def _upsert_sql(connection: sqlite3.Connection, table: dict[str, Any], key_sql: str) -> str | None:
    name = str(table["name"])
    columns = _columns(connection, name)
    names = [str(column["name"]) for column in columns]
    if bool(table["without_rowid"]):
        row = connection.execute(
            "SELECT " + ",".join(_qident(column) for column in names) + f" FROM {_qident(name)} WHERE {key_sql} LIMIT 1"
        ).fetchone()
        if row is None:
            return None
        values = list(row)
        insert_columns = names
    else:
        row = connection.execute(
            "SELECT rowid," + ",".join(_qident(column) for column in names) + f" FROM {_qident(name)} WHERE {key_sql} LIMIT 1"
        ).fetchone()
        if row is None:
            return None
        values = list(row)
        insert_columns = ["rowid", *names]
    return (
        f"INSERT OR REPLACE INTO {_qident(name)}("
        + ",".join(_qident(column) for column in insert_columns)
        + ") VALUES ("
        + ",".join(_sql_value(value) for value in values)
        + ")"
    )


def _require_shared_token(value: str | None) -> None:
    expected = split._shared_token()
    if not expected:
        raise HTTPException(status_code=503, detail="certification replication authentication is not configured")
    if not hmac.compare_digest(value or "", expected):
        raise HTTPException(status_code=401, detail="invalid certification replication authorization")


def _delta_payload(store: Any, *, from_watermark: int, epoch: str, schema_fingerprint: str) -> dict[str, Any]:
    with store._lock, store.db:
        meta, reconfigured = _ensure_tracking_locked(store.db)
    if reconfigured:
        raise HTTPException(status_code=409, detail="certification_replica_bootstrap_required:schema_changed")
    if epoch != meta.get("epoch") or schema_fingerprint != meta.get("schema_fingerprint"):
        raise HTTPException(status_code=409, detail="certification_replica_bootstrap_required:identity_mismatch")

    source_path = Path(getattr(store, "path", ""))
    if not source_path.is_file():
        raise HTTPException(status_code=503, detail="canonical certification source unavailable")
    uri = f"file:{source_path.resolve()}?mode=ro"
    reader = sqlite3.connect(uri, uri=True, timeout=5.0)
    try:
        reader.execute("PRAGMA query_only=ON")
        reader.execute("PRAGMA busy_timeout=5000")
        reader.execute("BEGIN")
        current_fingerprint = _schema_fingerprint(reader)
        if current_fingerprint != schema_fingerprint:
            raise HTTPException(status_code=409, detail="certification_replica_bootstrap_required:schema_changed")
        to_watermark = int(reader.execute(f"SELECT COALESCE(MAX(id),0) FROM {_qident(CHANGE_TABLE)}").fetchone()[0])
        if from_watermark > to_watermark:
            raise HTTPException(status_code=409, detail="certification_replica_bootstrap_required:watermark_ahead")
        count = int(
            reader.execute(
                f"SELECT COUNT(*) FROM {_qident(CHANGE_TABLE)} WHERE id>? AND id<=?",
                (from_watermark, to_watermark),
            ).fetchone()[0]
        )
        if count > _max_delta_rows():
            raise HTTPException(status_code=409, detail="certification_replica_bootstrap_required:delta_too_large")
        rows = reader.execute(
            f"SELECT table_name,key_sql,operation FROM {_qident(CHANGE_TABLE)} WHERE id>? AND id<=? ORDER BY id",
            (from_watermark, to_watermark),
        ).fetchall()
        coalesced: dict[tuple[str, str], str] = {}
        for table_name, key_sql, operation in rows:
            coalesced[(str(table_name), str(key_sql))] = str(operation)

        tables = {str(row["name"]): row for row in _table_rows(reader)}
        changes: list[dict[str, str]] = []
        payload_bytes = 0
        for (table_name, key_sql), operation in coalesced.items():
            table = tables.get(table_name)
            if table is None:
                raise HTTPException(status_code=409, detail="certification_replica_bootstrap_required:table_missing")
            if operation == "delete":
                sql = f"DELETE FROM {_qident(table_name)} WHERE {key_sql}"
            else:
                sql = _upsert_sql(reader, table, key_sql)
                if sql is None:
                    sql = f"DELETE FROM {_qident(table_name)} WHERE {key_sql}"
            payload_bytes += len(sql.encode("utf-8"))
            if payload_bytes > _max_delta_bytes():
                raise HTTPException(status_code=409, detail="certification_replica_bootstrap_required:delta_bytes_exceeded")
            changes.append({"table": table_name, "sql": sql})
    finally:
        reader.close()

    if from_watermark > 0:
        with store._lock, store.db:
            store.db.execute(f"DELETE FROM {_qident(CHANGE_TABLE)} WHERE id<=?", (from_watermark,))

    return {
        "replication_version": REPLICATION_VERSION,
        "release_commit": split._release_commit(),
        "epoch": epoch,
        "schema_fingerprint": schema_fingerprint,
        "from_watermark": int(from_watermark),
        "to_watermark": int(to_watermark),
        "source_change_count": int(count),
        "coalesced_change_count": len(changes),
        "payload_bytes": int(payload_bytes),
        "changes": changes,
        "full_snapshot_required": False,
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }


def install_certification_incremental_replication(app: Any, runtime_provider: Callable[[], Any]) -> None:
    runtime = runtime_provider()
    store = getattr(runtime, "store", None)
    if store is None:
        raise RuntimeError("certification replication canonical store unavailable")
    prepare_bootstrap(store)

    path = "/v1/operations/certification-db-delta"
    status_path = "/v1/operations/certification-db-replication"
    existing = {getattr(route, "path", None) for route in app.routes}

    if path not in existing:
        @app.get(path)
        def certification_db_delta(
            from_watermark: int = Query(ge=0),
            epoch: str = Query(min_length=8, max_length=128),
            schema_fingerprint: str = Query(min_length=32, max_length=128),
            x_certification_token: str | None = Header(default=None, alias="X-Certification-Token"),
        ) -> dict[str, Any]:
            _require_shared_token(x_certification_token)
            runtime = runtime_provider()
            store = getattr(runtime, "store", None)
            if store is None:
                raise HTTPException(status_code=503, detail="canonical certification store unavailable")
            return _delta_payload(
                store,
                from_watermark=from_watermark,
                epoch=epoch,
                schema_fingerprint=schema_fingerprint,
            )

    if status_path not in existing:
        @app.get(status_path)
        def certification_db_replication_status() -> dict[str, Any]:
            runtime = runtime_provider()
            store = getattr(runtime, "store", None)
            if store is None:
                return {
                    "replication_version": REPLICATION_VERSION,
                    "ready": False,
                    "reason": "canonical_store_unavailable",
                    "paper_only": True,
                    "live_money_authority": False,
                }
            state = prepare_bootstrap(store)
            return {
                **state,
                "ready": True,
                "full_snapshot_normal_cycle": False,
                "full_snapshot_role": "bootstrap_recovery_reconciliation_only",
                "max_delta_rows": _max_delta_rows(),
                "max_delta_bytes": _max_delta_bytes(),
                "strategy_thresholds_changed": False,
                "certification_thresholds_changed": False,
                "continuity_semantics_changed": False,
                "signing_available": False,
                "transaction_submission_available": False,
            }

    app.state.roi_certification_incremental_replication = True
    app.state.roi_certification_incremental_replication_version = REPLICATION_VERSION


__all__ = ["REPLICATION_VERSION", "install_certification_incremental_replication", "prepare_bootstrap"]
