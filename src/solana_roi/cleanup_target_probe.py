from __future__ import annotations

"""Bounded read-only metadata probe for cleanup-target dependency proof.

The probe intentionally avoids payload scans and mutation. It reports only schema,
index/foreign-key topology, schema-level trigger/view dependencies, sqlite_stat1
estimates when already available, and rowid/sequence bounds that SQLite can resolve
from B-tree edges. This makes it safe to run after canonical runtime readiness while
the production disk lease is held.
"""

import sqlite3
from pathlib import Path
from typing import Any

PROBE_VERSION = "cleanup-target-probe-v3-schema-dependencies"
TARGET_TABLE = "anonymous_candidate_latency_failures"


def _quote(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _connect_read_only(path: Path) -> sqlite3.Connection:
    uri = f"file:{path.resolve()}?mode=ro"
    connection = sqlite3.connect(uri, uri=True, timeout=2.0)
    connection.execute("PRAGMA query_only=ON")
    connection.execute("PRAGMA busy_timeout=1500")
    return connection


def _user_tables(connection: sqlite3.Connection) -> list[str]:
    return [
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ).fetchall()
    ]


def _foreign_keys(connection: sqlite3.Connection, table: str) -> list[dict[str, Any]]:
    rows = connection.execute(f"PRAGMA foreign_key_list({_quote(table)})").fetchall()
    return [
        {
            "id": int(row[0]),
            "seq": int(row[1]),
            "referenced_table": str(row[2]),
            "from_column": str(row[3]),
            "to_column": str(row[4]),
            "on_update": str(row[5]),
            "on_delete": str(row[6]),
            "match": str(row[7]),
        }
        for row in rows
    ]


def _schema_dependents(connection: sqlite3.Connection, table: str) -> list[dict[str, Any]]:
    # sqlite_master is schema metadata, not payload history. Searching it proves
    # whether any trigger/view SQL names the cleanup target without scanning rows.
    pattern = f"%{table.lower()}%"
    rows = connection.execute(
        "SELECT type,name,tbl_name,sql FROM sqlite_master "
        "WHERE type IN ('trigger','view') AND lower(COALESCE(sql,'')) LIKE ? "
        "ORDER BY type,name",
        (pattern,),
    ).fetchall()
    return [
        {
            "type": str(row[0]),
            "name": str(row[1]),
            "owner_table": str(row[2]) if row[2] is not None else None,
            "sql": str(row[3]) if row[3] is not None else None,
        }
        for row in rows
    ]


def _single_edge(connection: sqlite3.Connection, table: str, aggregate: str) -> int | None:
    # SQLite's MIN/MAX optimization applies when the statement contains a single
    # MIN or MAX aggregate. Keep the two edges separate so this cannot devolve into
    # a history-scaled table scan on a large cleanup target.
    if aggregate not in {"MIN", "MAX"}:
        raise ValueError("unsupported rowid aggregate")
    row = connection.execute(
        f"SELECT {aggregate}(rowid) FROM {_quote(table)}"
    ).fetchone()
    return int(row[0]) if row and row[0] is not None else None


def probe_cleanup_target(database_path: Path, table: str = TARGET_TABLE) -> dict[str, Any]:
    database_path = Path(database_path)
    result: dict[str, Any] = {
        "probe_version": PROBE_VERSION,
        "database_path": str(database_path),
        "table": table,
        "read_only": True,
        "payload_rows_scanned": False,
        "rowid_bounds_use_single_aggregate_btree_edges": True,
    }
    if not database_path.exists() or not database_path.is_file():
        return {**result, "status": "database_missing"}

    connection = _connect_read_only(database_path)
    try:
        table_row = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()
        if table_row is None:
            return {**result, "status": "table_missing"}

        create_sql = str(table_row[0] or "")
        columns = connection.execute(f"PRAGMA table_info({_quote(table)})").fetchall()
        indexes = []
        for row in connection.execute(f"PRAGMA index_list({_quote(table)})").fetchall():
            index_name = str(row[1])
            info = connection.execute(f"PRAGMA index_info({_quote(index_name)})").fetchall()
            sql_row = connection.execute(
                "SELECT sql FROM sqlite_master WHERE type='index' AND name=?",
                (index_name,),
            ).fetchone()
            indexes.append(
                {
                    "name": index_name,
                    "unique": bool(row[2]),
                    "origin": str(row[3]),
                    "partial": bool(row[4]),
                    "columns": [str(item[2]) for item in info],
                    "sql": str(sql_row[0]) if sql_row and sql_row[0] is not None else None,
                }
            )

        reverse_foreign_keys: list[dict[str, Any]] = []
        for other_table in _user_tables(connection):
            if other_table == table:
                continue
            for fk in _foreign_keys(connection, other_table):
                if fk["referenced_table"] == table:
                    reverse_foreign_keys.append({"table": other_table, **fk})

        sequence = None
        sqlite_sequence_exists = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='sqlite_sequence'"
        ).fetchone()
        if sqlite_sequence_exists:
            seq_row = connection.execute(
                "SELECT seq FROM sqlite_sequence WHERE name=?",
                (table,),
            ).fetchone()
            sequence = int(seq_row[0]) if seq_row and seq_row[0] is not None else None

        stat1: list[dict[str, Any]] = []
        stat1_exists = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='sqlite_stat1'"
        ).fetchone()
        if stat1_exists:
            for row in connection.execute(
                "SELECT idx,stat FROM sqlite_stat1 WHERE tbl=? ORDER BY idx",
                (table,),
            ).fetchall():
                stat = str(row[1] or "")
                first = stat.split()[0] if stat else ""
                try:
                    estimated_rows = int(first)
                except ValueError:
                    estimated_rows = None
                stat1.append(
                    {
                        "index": str(row[0]) if row[0] is not None else None,
                        "stat": stat,
                        "estimated_rows": estimated_rows,
                    }
                )

        rowid_bounds = None
        without_rowid = "WITHOUT ROWID" in create_sql.upper()
        if not without_rowid:
            rowid_bounds = {
                "min": _single_edge(connection, table, "MIN"),
                "max": _single_edge(connection, table, "MAX"),
            }

        return {
            **result,
            "status": "ok",
            "database_bytes": int(database_path.stat().st_size),
            "page_size": int(connection.execute("PRAGMA page_size").fetchone()[0]),
            "page_count": int(connection.execute("PRAGMA page_count").fetchone()[0]),
            "freelist_count": int(connection.execute("PRAGMA freelist_count").fetchone()[0]),
            "create_sql": create_sql,
            "without_rowid": without_rowid,
            "columns": [
                {
                    "cid": int(row[0]),
                    "name": str(row[1]),
                    "type": str(row[2]),
                    "not_null": bool(row[3]),
                    "default": row[4],
                    "primary_key_position": int(row[5]),
                }
                for row in columns
            ],
            "indexes": indexes,
            "foreign_keys": _foreign_keys(connection, table),
            "reverse_foreign_keys": reverse_foreign_keys,
            "schema_dependents": _schema_dependents(connection, table),
            "sqlite_sequence": sequence,
            "sqlite_stat1": stat1,
            "rowid_bounds": rowid_bounds,
        }
    finally:
        connection.close()


__all__ = ["PROBE_VERSION", "TARGET_TABLE", "probe_cleanup_target"]
