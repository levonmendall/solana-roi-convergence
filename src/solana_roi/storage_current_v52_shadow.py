from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import storage_current_v52_reconciliation as current_v52
from . import storage_manifest
from .active_storage import ActiveStorage


def _connect_ro(path: Path) -> sqlite3.Connection:
    uri = f"file:{path.resolve()}?mode=ro&cache=private"
    connection = sqlite3.connect(uri, uri=True, timeout=30.0)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    connection.execute("PRAGMA busy_timeout=30000")
    return connection


def _columns(connection: sqlite3.Connection, table: str) -> list[str]:
    return [str(row[1]) for row in connection.execute(f'PRAGMA table_info("{table}")')]


def _exists(connection: sqlite3.Connection, table: str) -> bool:
    return connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (table,)
    ).fetchone() is not None


def _ddl(connection: sqlite3.Connection, table: str) -> str:
    row = connection.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    if row is None or not str(row[0] or "").strip():
        raise RuntimeError(f"current-v5.2 shadow blocked: table DDL unavailable:{table}")
    return str(row[0])


def _copy_query(
    source: sqlite3.Connection,
    destination: sqlite3.Connection,
    table: str,
    sql: str,
    args: Sequence[Any] = (),
) -> int:
    storage_manifest.contract_for(table)
    rows = source.execute(sql, tuple(args)).fetchall()
    if not rows:
        return 0
    source_columns = _columns(source, table)
    if _exists(destination, table):
        destination_columns = _columns(destination, table)
        if destination_columns != source_columns:
            raise RuntimeError(
                f"current-v5.2 shadow blocked: compatibility schema mismatch:{table}:"
                f"source={source_columns}:destination={destination_columns}"
            )
    else:
        destination.execute(_ddl(source, table))
    row_columns = [str(name) for name in rows[0].keys()]
    if set(row_columns) - set(source_columns):
        raise RuntimeError(f"current-v5.2 shadow blocked: synthetic columns:{table}")
    quoted = ",".join('"' + name.replace('"', '""') + '"' for name in row_columns)
    placeholders = ",".join("?" for _ in row_columns)
    values = [tuple(row[name] for name in row_columns) for row in rows]
    destination.executemany(
        f'INSERT OR REPLACE INTO "{table}"({quoted}) VALUES({placeholders})', values
    )
    return len(values)


def reconcile_current_v52_shadow(
    legacy_path: Path | str,
    active_path: Path | str,
) -> dict[str, Any]:
    """Add bounded current-main evidence after the core exact-state shadow build.

    The transition checkpoint already seals current strategy/wallet/portfolio
    truth. This pass preserves only recent or unresolved validation evidence that
    current v5.2 needs for genuine 24h/7d/30d evaluation. It never imports all
    historical evidence merely because it exists.
    """
    legacy = Path(legacy_path)
    active = Path(active_path)
    source = _connect_ro(legacy)
    destination = sqlite3.connect(active, timeout=30.0)
    destination.row_factory = sqlite3.Row
    destination.execute("PRAGMA foreign_keys=ON")
    destination.execute("PRAGMA busy_timeout=30000")
    counts: dict[str, int] = {}
    try:
        source.execute("BEGIN")
        destination.execute("BEGIN IMMEDIATE")
        current_v52.copy_bounded_current_v52(
            source,
            destination,
            copy_query=_copy_query,
            counts=counts,
        )
        destination.commit()
        source.execute("COMMIT")
    except Exception:
        destination.rollback()
        try:
            source.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise
    finally:
        destination.close()
        source.close()

    # The active database, not the legacy source, owns ongoing pruning.
    pruned = current_v52.prune_current_v52_database(active)
    storage = ActiveStorage(active)
    storage.assert_positive_schema()
    storage.checkpoint_wal()
    storage.enforce_hard_budget()
    sizes = storage.storage_bytes()
    return {
        "copied_rows": counts,
        "pruned_rows": pruned,
        "active_size_bytes": int(sizes["main"]),
        "active_wal_bytes": int(sizes["wal"]),
        "registered_datasets": list(current_v52.registered_dataset_names()),
        "paper_only": True,
        "live_money_authority": False,
    }


__all__ = ["reconcile_current_v52_shadow"]
