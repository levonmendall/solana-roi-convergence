from __future__ import annotations

import os
import sqlite3
import stat
from pathlib import Path
from typing import Any


DEFAULT_MAX_OBJECTS = 4096
DEFAULT_CACHE_KIB = 2048


def _release_file_cache(path: Path) -> bool:
    fadvise = getattr(os, "posix_fadvise", None)
    advice = getattr(os, "POSIX_FADV_DONTNEED", None)
    if fadvise is None or advice is None:
        return False
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return False
    try:
        try:
            fadvise(fd, 0, 0, advice)
        except OSError:
            return False
        return True
    finally:
        os.close(fd)


def inventory_sqlite_pages(
    database_path: str | os.PathLike[str],
    *,
    max_objects: int = DEFAULT_MAX_OBJECTS,
    cache_kib: int = DEFAULT_CACHE_KIB,
) -> dict[str, Any]:
    """Return exact SQLite b-tree page ownership without reading application rows.

    This is an operator-only diagnostic. It opens the database read-only, uses the
    SQLite ``dbstat`` virtual table in aggregate mode, keeps SQLite's private page
    cache deliberately small, performs no writes/retention/cleanup, and releases
    touched kernel file-cache pages best-effort after the connection closes.

    ``dbstat`` reports one aggregate row per table/index b-tree, so the result size
    is bounded by schema object count rather than historical row count. If dbstat is
    unavailable, the function reports that explicitly instead of falling back to a
    row scan.
    """
    path = Path(database_path).expanduser().resolve()
    result: dict[str, Any] = {
        "database_path": str(path),
        "status": "ok",
        "read_only": True,
        "application_rows_read": False,
        "writes_performed": False,
        "retention_changed": False,
        "cleanup_performed": False,
        "dbstat_aggregate": True,
        "max_objects": max(1, int(max_objects)),
        "sqlite_private_cache_kib": max(256, int(cache_kib)),
        "objects": [],
        "tables": [],
    }

    try:
        metadata = path.lstat()
    except FileNotFoundError:
        result["status"] = "missing"
        return result
    except OSError as exc:
        result["status"] = "unavailable"
        result["error_type"] = type(exc).__name__
        return result
    if stat.S_ISLNK(metadata.st_mode):
        result["status"] = "refused_symlink"
        return result
    if not stat.S_ISREG(metadata.st_mode):
        result["status"] = "not_regular_file"
        return result

    result["database_bytes"] = int(metadata.st_size)
    connection: sqlite3.Connection | None = None
    try:
        uri = f"file:{path}?mode=ro&cache=private"
        connection = sqlite3.connect(uri, uri=True, timeout=5.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        connection.execute("PRAGMA busy_timeout=5000")
        connection.execute(f"PRAGMA cache_size=-{max(256, int(cache_kib))}")
        page_size = int(connection.execute("PRAGMA page_size").fetchone()[0])
        page_count = int(connection.execute("PRAGMA page_count").fetchone()[0])
        result["page_size_bytes"] = page_size
        result["page_count"] = page_count
        result["logical_database_bytes"] = page_size * page_count

        try:
            rows = connection.execute(
                "SELECT name,pageno AS pages,pgsize AS bytes,payload,unused,mx_payload "
                "FROM dbstat WHERE aggregate=TRUE ORDER BY pgsize DESC,name LIMIT ?",
                (max(1, int(max_objects)) + 1,),
            ).fetchall()
        except sqlite3.DatabaseError as exc:
            result["status"] = "dbstat_unavailable"
            result["error_type"] = type(exc).__name__
            return result

        if len(rows) > max(1, int(max_objects)):
            result["status"] = "schema_object_limit_exceeded"
            result["observed_at_least"] = len(rows)
            return result

        # Include SQLite-generated autoindexes in ownership mapping. They are named
        # ``sqlite_autoindex_*`` and therefore must not be dropped by a sqlite_%
        # filter or their bytes would appear as an unowned internal object.
        schema_objects = connection.execute(
            "SELECT name,tbl_name,type FROM sqlite_master WHERE type IN ('table','index')"
        ).fetchall()
        owners = {
            str(row[0]): (str(row[1]) if row[1] is not None else str(row[0]))
            for row in schema_objects
        }
        object_types = {str(row[0]): str(row[2]) for row in schema_objects}

        objects: list[dict[str, Any]] = []
        table_totals: dict[str, dict[str, int]] = {}
        for row in rows:
            name = str(row["name"])
            pages = int(row["pages"] or 0)
            size = int(row["bytes"] or 0)
            payload = int(row["payload"] or 0)
            unused = int(row["unused"] or 0)
            owner = owners.get(name, name)
            kind = object_types.get(name, "internal")
            objects.append(
                {
                    "name": name,
                    "owner_table": owner,
                    "type": kind,
                    "pages": pages,
                    "bytes": size,
                    "payload_bytes": payload,
                    "unused_bytes": unused,
                    "max_payload_bytes": int(row["mx_payload"] or 0),
                }
            )
            bucket = table_totals.setdefault(
                owner,
                {
                    "table_bytes": 0,
                    "index_bytes": 0,
                    "other_bytes": 0,
                    "pages": 0,
                    "payload_bytes": 0,
                    "unused_bytes": 0,
                },
            )
            key = "table_bytes" if kind == "table" else "index_bytes" if kind == "index" else "other_bytes"
            bucket[key] += size
            bucket["pages"] += pages
            bucket["payload_bytes"] += payload
            bucket["unused_bytes"] += unused

        result["objects"] = objects
        result["tables"] = [
            {
                "name": name,
                **values,
                "total_bytes": values["table_bytes"] + values["index_bytes"] + values["other_bytes"],
            }
            for name, values in sorted(
                table_totals.items(),
                key=lambda item: (
                    -(item[1]["table_bytes"] + item[1]["index_bytes"] + item[1]["other_bytes"]),
                    item[0],
                ),
            )
        ]
        result["schema_object_count"] = len(objects)
        result["accounted_btree_bytes"] = sum(int(item["bytes"]) for item in objects)
        result["largest_table"] = result["tables"][0] if result["tables"] else None
        return result
    finally:
        if connection is not None:
            connection.close()
        result["file_cache_release_attempted"] = True
        result["file_cache_release_succeeded"] = _release_file_cache(path)


__all__ = [
    "DEFAULT_CACHE_KIB",
    "DEFAULT_MAX_OBJECTS",
    "inventory_sqlite_pages",
]
