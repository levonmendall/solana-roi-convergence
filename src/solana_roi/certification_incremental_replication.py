from __future__ import annotations

"""Bounded exact-state replication for the isolated certification service.

The authoritative process remains the sole owner of the canonical SQLite database.
A compact transactional journal records only stable row identities that change.  The
certifier owns a replica and asks for all mutations after its durable watermark.
Each response resolves those identities from one pinned read transaction, giving the
certifier one exact point-in-time logical database state.

Full SQLite copies are retained only for bootstrap/recovery/reconciliation.  Normal
certification cycles use bounded deltas.
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

REPLICATION_VERSION = "certification-incremental-replica-v2-monotonic-watermark"
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


def _columns(connection: sqlite3.Connection, table: str) -> list[dict[str, Any]]:
    try:
        rows = connection.execute(f"PRAGMA table_xinfo({_qliteral(table)})").fetchall()
    except sqlite3.DatabaseError:
        rows = connection.execute(f"PRAGMA table_info({_qliteral(table)})").fetchall()
    return [
        {
            "cid": int(row[0]),
            "name": str(row[1]),
            "type": str(row[2] or ""),
            "notnull": int(row[3]),
            "default": row[4],
            "pk": int(row[5]),
            "hidden": int(row[6]) if len(row) > 6 else 0,
        }
        for row in rows
    ]


def _ordinary_tables(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    tables: list[dict[str, Any]] = []
    try:
        listed = connection.execute("PRAGMA table_list").fetchall()
        for schema, name, kind, _ncol, without_rowid, _strict in listed:
            name = str(name)
            if str(schema) != "main" or str(kind) != "table":
                continue
            if name.startswith("sqlite_") or name in {CHANGE_TABLE, META_TABLE}:
                continue
            row = connection.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (name,)
            ).fetchone()
            sql = str(row[0] or "") if row is not None else ""
            if sql:
                tables.append({"name": name, "sql": sql, "without_rowid": bool(without_rowid)})
    except sqlite3.DatabaseError:
        listed = connection.execute(
            "SELECT name,sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ).fetchall()
        for name, sql in listed:
            name, text = str(name), str(sql or "")
            if name in {CHANGE_TABLE, META_TABLE} or not text or "VIRTUAL TABLE" in text.upper():
                continue
            tables.append({"name": name, "sql": text, "without_rowid": "WITHOUT ROWID" in text.upper()})
    tables.sort(key=lambda row: str(row["name"]))
    return tables


def _schema_fingerprint(connection: sqlite3.Connection) -> str:
    payload: list[str] = []
    for table in _ordinary_tables(connection):
        name = str(table["name"])
        payload.append(f"TABLE:{name}:{table['sql']}")
        for column in _columns(connection, name):
            payload.append(
                "COLUMN:"
                + ":".join(
                    (
                        name,
                        str(column["cid"]),
                        str(column["name"]),
                        str(column["type"]),
                        str(column["notnull"]),
                        str(column["default"]),
                        str(column["pk"]),
                        str(column["hidden"]),
                    )
                )
            )
    return hashlib.sha256("\n".join(payload).encode()).hexdigest()


def _meta(connection: sqlite3.Connection) -> dict[str, str]:
    return {
        str(row[0]): str(row[1])
        for row in connection.execute(f"SELECT key,value FROM {_qident(META_TABLE)}").fetchall()
    }


def _set_meta(connection: sqlite3.Connection, key: str, value: str) -> None:
    connection.execute(
        f"INSERT INTO {_qident(META_TABLE)}(key,value) VALUES (?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )


def _current_watermark(connection: sqlite3.Connection) -> int:
    row = connection.execute("SELECT seq FROM sqlite_sequence WHERE name=?", (CHANGE_TABLE,)).fetchone()
    return max(0, int(row[0])) if row is not None and row[0] is not None else 0


def _primary_columns(columns: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        (column for column in columns if int(column["pk"]) > 0),
        key=lambda column: int(column["pk"]),
    )


def _key_expression(table: dict[str, Any], columns: list[dict[str, Any]], alias: str) -> str:
    primary = _primary_columns(columns)
    if primary:
        pieces: list[str] = []
        for index, column in enumerate(primary):
            prefix = "" if index == 0 else " AND "
            label = prefix + _qident(str(column["name"])) + " IS "
            pieces.append(_qliteral(label) + f" || quote({alias}.{_qident(str(column['name']))})")
        return " || ".join(pieces)
    if bool(table["without_rowid"]):
        raise RuntimeError(f"WITHOUT ROWID table lacks stable primary key:{table['name']}")
    return f"'rowid IS ' || quote({alias}.rowid)"


def _drop_tracking_triggers(connection: sqlite3.Connection) -> None:
    rows = connection.execute(
        "SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE ?", (TRIGGER_PREFIX + "%",)
    ).fetchall()
    for row in rows:
        connection.execute(f"DROP TRIGGER IF EXISTS {_qident(str(row[0]))}")


def _install_table_triggers(connection: sqlite3.Connection, table: dict[str, Any]) -> None:
    name = str(table["name"])
    columns = _columns(connection, name)
    if not columns:
        raise RuntimeError(f"certification replication table has no columns:{name}")
    digest = hashlib.sha256(name.encode()).hexdigest()[:16]
    new_key = _key_expression(table, columns, "NEW")
    old_key = _key_expression(table, columns, "OLD")
    quoted, literal = _qident(name), _qliteral(name)
    now = "strftime('%Y-%m-%dT%H:%M:%fZ','now')"
    for suffix, event, key, operation in (
        ("ai", "INSERT", new_key, "upsert"),
        ("ad", "DELETE", old_key, "delete"),
    ):
        connection.execute(
            f"CREATE TRIGGER IF NOT EXISTS {_qident(TRIGGER_PREFIX + digest + '_' + suffix)} "
            f"AFTER {event} ON {quoted} BEGIN "
            f"INSERT INTO {_qident(CHANGE_TABLE)}(table_name,key_sql,operation,changed_at) "
            f"VALUES ({literal},{key},'{operation}',{now}); END"
        )
    connection.execute(
        f"CREATE TRIGGER IF NOT EXISTS {_qident(TRIGGER_PREFIX + digest + '_au')} AFTER UPDATE ON {quoted} BEGIN "
        f"INSERT INTO {_qident(CHANGE_TABLE)}(table_name,key_sql,operation,changed_at) "
        f"VALUES ({literal},{old_key},'delete',{now}); "
        f"INSERT INTO {_qident(CHANGE_TABLE)}(table_name,key_sql,operation,changed_at) "
        f"VALUES ({literal},{new_key},'upsert',{now}); END"
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
    connection.execute(f"CREATE TABLE IF NOT EXISTS {_qident(META_TABLE)}(key TEXT PRIMARY KEY,value TEXT NOT NULL)")
    meta = _meta(connection)
    observed = int(connection.execute("PRAGMA schema_version").fetchone()[0])
    if (
        meta.get("replication_version") == REPLICATION_VERSION
        and meta.get("epoch")
        and meta.get("schema_fingerprint")
        and int(meta.get("configured_schema_version", "-1")) == observed
    ):
        return meta, False

    fingerprint = _schema_fingerprint(connection)
    identity_changed = (
        not meta
        or meta.get("replication_version") != REPLICATION_VERSION
        or meta.get("schema_fingerprint") != fingerprint
    )
    if identity_changed:
        _drop_tracking_triggers(connection)
        connection.execute(f"DELETE FROM {_qident(CHANGE_TABLE)}")
        _set_meta(connection, "epoch", secrets.token_urlsafe(24))
        _set_meta(connection, "schema_fingerprint", fingerprint)
        _set_meta(connection, "replication_version", REPLICATION_VERSION)
    for table in _ordinary_tables(connection):
        _install_table_triggers(connection, table)
    _set_meta(connection, "configured_schema_version", str(int(connection.execute("PRAGMA schema_version").fetchone()[0])))
    return _meta(connection), identity_changed


def prepare_bootstrap(store: Any) -> dict[str, Any]:
    with store._lock, store.db:
        meta, changed = _ensure_tracking_locked(store.db)
        watermark = _current_watermark(store.db)
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
    columns = [column for column in _columns(connection, name) if int(column["hidden"]) == 0]
    names = [str(column["name"]) for column in columns]
    if _primary_columns(columns):
        projection, insert_names = ",".join(_qident(name) for name in names), names
    else:
        projection, insert_names = "rowid," + ",".join(_qident(name) for name in names), ["rowid", *names]
    row = connection.execute(
        f"SELECT {projection} FROM {_qident(name)} WHERE {key_sql} LIMIT 1"
    ).fetchone()
    if row is None:
        return None
    return (
        f"INSERT OR REPLACE INTO {_qident(name)}("
        + ",".join(_qident(column) for column in insert_names)
        + ") VALUES ("
        + ",".join(_sql_value(value) for value in row)
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
    reader = sqlite3.connect(f"file:{source_path.resolve()}?mode=ro", uri=True, timeout=5.0)
    try:
        reader.execute("PRAGMA query_only=ON")
        reader.execute("PRAGMA busy_timeout=5000")
        reader.execute("BEGIN")
        if _schema_fingerprint(reader) != schema_fingerprint:
            raise HTTPException(status_code=409, detail="certification_replica_bootstrap_required:schema_changed")
        to_watermark = _current_watermark(reader)
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
        tables = {str(table["name"]): table for table in _ordinary_tables(reader)}
        changes: list[dict[str, str]] = []
        payload_bytes = 0
        for (table_name, key_sql), operation in coalesced.items():
            table = tables.get(table_name)
            if table is None:
                raise HTTPException(status_code=409, detail="certification_replica_bootstrap_required:table_missing")
            sql = (
                f"DELETE FROM {_qident(table_name)} WHERE {key_sql}"
                if operation == "delete"
                else _upsert_sql(reader, table, key_sql)
            )
            if sql is None:
                sql = f"DELETE FROM {_qident(table_name)} WHERE {key_sql}"
            payload_bytes += len(sql.encode())
            if payload_bytes > _max_delta_bytes():
                raise HTTPException(status_code=409, detail="certification_replica_bootstrap_required:delta_bytes_exceeded")
            changes.append({"table": table_name, "sql": sql})
    finally:
        reader.close()

    # from_watermark is a durable acknowledgement: the client advances it only
    # after its prior transaction and state sidecar have committed.  Prune only
    # rows already acknowledged.  sqlite_sequence keeps the watermark monotonic.
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


def _wrap_bootstrap_snapshot_builder() -> None:
    current = split._snapshot_store_to_file
    if bool(getattr(current, "_roi_replication_bootstrap_prepare", False)):
        return
    def snapshot_with_replication_identity(store: Any, snapshot: Path) -> tuple[int, int]:
        prepare_bootstrap(store)
        return current(store, snapshot)
    setattr(snapshot_with_replication_identity, "_roi_replication_bootstrap_prepare", True)
    setattr(snapshot_with_replication_identity, "_roi_original_snapshot_store_to_file", current)
    split._snapshot_store_to_file = snapshot_with_replication_identity  # type: ignore[assignment]


def install_certification_incremental_replication(app: Any, runtime_provider: Callable[[], Any]) -> None:
    # Route composition must remain safe before the runtime store exists (several
    # canonical tests intentionally compose a dummy runtime).  Actual replication
    # requests remain fail-closed until a canonical store is present.  The full
    # bootstrap builder is wrapped unconditionally so a later-available store gets
    # tracking metadata before its first exceptional snapshot.
    _wrap_bootstrap_snapshot_builder()
    try:
        initial_runtime = runtime_provider()
    except Exception:
        initial_runtime = None
    initial_store = getattr(initial_runtime, "store", None) if initial_runtime is not None else None
    if initial_store is not None:
        prepare_bootstrap(initial_store)

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
            try:
                current_runtime = runtime_provider()
            except Exception as exc:
                raise HTTPException(status_code=503, detail="canonical certification runtime unavailable") from exc
            store = getattr(current_runtime, "store", None)
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
            try:
                current_runtime = runtime_provider()
            except Exception:
                current_runtime = None
            store = getattr(current_runtime, "store", None) if current_runtime is not None else None
            if store is None:
                return {
                    "replication_version": REPLICATION_VERSION,
                    "ready": False,
                    "reason": "canonical_store_unavailable",
                    "full_snapshot_normal_cycle": False,
                    "paper_only": True,
                    "live_money_authority": False,
                }
            state = prepare_bootstrap(store)
            return {
                **state,
                "ready": True,
                "full_snapshot_normal_cycle": False,
                "full_snapshot_role": "bootstrap_recovery_reconciliation_only",
                "normal_cycle_transport": "bounded_incremental_delta",
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


__all__ = [
    "CHANGE_TABLE",
    "META_TABLE",
    "REPLICATION_VERSION",
    "TRIGGER_PREFIX",
    "_delta_payload",
    "install_certification_incremental_replication",
    "prepare_bootstrap",
]
