from __future__ import annotations

"""Bound transient row materialization during the active-storage shadow copy.

This changes only copy transport.  The source queries, rows selected, schemas,
INSERT OR REPLACE behavior, and post-copy semantic verification remain unchanged.
"""

import sqlite3
from typing import Any, Mapping, Sequence

from . import storage_manifest
from . import storage_shadow_migration as migration

REPAIR_VERSION = "storage-shadow-copy-bounded-v1"
COPY_BATCH_ROWS = 2_048

PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
STRATEGY_THRESHOLDS_CHANGED = False
RETENTION_SEMANTICS_CHANGED = False

_INSTALLED = False
_ORIGINAL_COPY_DICT_ROWS = migration._copy_dict_rows
_ORIGINAL_COPY_QUERY = migration._copy_query
_ORIGINAL_COPY_LATEST_250 = migration._copy_latest_250_features


def _value(row: Any, column: str) -> Any:
    if isinstance(row, sqlite3.Row):
        return row[column]
    if isinstance(row, Mapping):
        return row.get(column)
    return row[column]


def _row_keys(row: Any) -> list[str]:
    if isinstance(row, sqlite3.Row):
        return list(row.keys())
    if isinstance(row, Mapping):
        return list(row.keys())
    raise RuntimeError("migration blocked: unsupported row shape")


def _normalize_value(value: Any) -> Any:
    if isinstance(value, dict) and set(value) == {"hex"}:
        return bytes.fromhex(str(value["hex"]))
    return value


def _bounded_copy_dict_rows(
    source: sqlite3.Connection,
    dest: sqlite3.Connection,
    table: str,
    rows: Sequence[Mapping[str, Any]] | Sequence[sqlite3.Row],
) -> int:
    if not rows:
        return 0
    storage_manifest.contract_for(table)
    source_columns = migration._columns(source, table)
    if migration._exists(dest, table):
        destination_columns = migration._columns(dest, table)
        if destination_columns != source_columns:
            raise RuntimeError(
                f"migration blocked: compatibility schema mismatch:{table}:"
                f"source={source_columns}:destination={destination_columns}"
            )
    else:
        dest.execute(migration._table_ddl(source, table))

    columns = _row_keys(rows[0])
    real = set(source_columns)
    if set(columns) - real:
        raise RuntimeError(
            f"migration blocked: synthetic columns selected for {table}:"
            f"{sorted(set(columns)-real)}"
        )
    qcols = ",".join('"' + column.replace('"', '""') + '"' for column in columns)
    placeholders = ",".join("?" for _ in columns)
    statement = f'INSERT OR REPLACE INTO "{table}"({qcols}) VALUES({placeholders})'

    copied = 0
    for offset in range(0, len(rows), COPY_BATCH_ROWS):
        batch = rows[offset : offset + COPY_BATCH_ROWS]
        values: list[tuple[Any, ...]] = []
        for row in batch:
            if _row_keys(row) != columns:
                raise RuntimeError(f"migration blocked: inconsistent row shape:{table}")
            values.append(tuple(_normalize_value(_value(row, column)) for column in columns))
        dest.executemany(statement, values)
        copied += len(values)
        # Drop the transformed tuples before the next source-sized batch is built.
        del values
    return copied


def _bounded_copy_query(
    source: sqlite3.Connection,
    dest: sqlite3.Connection,
    table: str,
    sql: str,
    args: Sequence[Any] = (),
) -> int:
    cursor = source.execute(sql, tuple(args))
    copied = 0
    try:
        while True:
            batch = cursor.fetchmany(COPY_BATCH_ROWS)
            if not batch:
                return copied
            copied += _bounded_copy_dict_rows(source, dest, table, batch)
            del batch
    finally:
        cursor.close()


def _bounded_copy_latest_250_features(source: sqlite3.Connection, dest: sqlite3.Connection) -> int:
    table = "v52_market_validation_features"
    if not migration._exists(source, table):
        return 0
    columns = migration._columns(source, table)
    selected = ",".join('ranked."' + column.replace('"', '""') + '"' for column in columns)
    return _bounded_copy_query(
        source,
        dest,
        table,
        "SELECT "
        + selected
        + " FROM (SELECT f.*,ROW_NUMBER() OVER(PARTITION BY lane,feature ORDER BY id DESC) AS _roi_rank "
        "FROM v52_market_validation_features AS f) AS ranked "
        "WHERE ranked._roi_rank<=250 ORDER BY ranked.id",
    )


def configure_storage_shadow_copy_bounded_repair() -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    migration._copy_dict_rows = _bounded_copy_dict_rows
    migration._copy_query = _bounded_copy_query
    migration._copy_latest_250_features = _bounded_copy_latest_250_features
    _INSTALLED = True


def status() -> dict[str, Any]:
    return {
        "repair_version": REPAIR_VERSION,
        "installed": _INSTALLED,
        "copy_batch_rows": COPY_BATCH_ROWS,
        "source_selection_changed": False,
        "semantic_verification_preserved": True,
        "retention_semantics_changed": RETENTION_SEMANTICS_CHANGED,
        "strategy_thresholds_changed": STRATEGY_THRESHOLDS_CHANGED,
        "paper_only": PAPER_ONLY,
        "live_money_authority": LIVE_MONEY_AUTHORITY,
    }


__all__ = ["REPAIR_VERSION", "configure_storage_shadow_copy_bounded_repair", "status"]
