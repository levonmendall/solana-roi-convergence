from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Any

SCHEMA_CACHE_VERSION = "v51-schema-introspection-cache-v1"


@dataclass
class _StoreSchemaState:
    schema_version: int
    tables: dict[str, bool] = field(default_factory=dict)
    columns: dict[str, frozenset[str]] = field(default_factory=dict)


_LOCK = threading.RLock()
_STATES: dict[int, _StoreSchemaState] = {}
_STATS = {
    "schema_version_reads": 0,
    "table_cache_hits": 0,
    "table_cache_misses": 0,
    "column_cache_hits": 0,
    "column_cache_misses": 0,
    "schema_invalidations": 0,
}


def _schema_version(store: Any) -> int:
    with store._lock:
        row = store.db.execute("PRAGMA schema_version").fetchone()
    with _LOCK:
        _STATS["schema_version_reads"] += 1
    if row is None:
        return 0
    try:
        return int(row[0])
    except (TypeError, ValueError, KeyError):
        try:
            return int(row["schema_version"])
        except Exception:
            return 0


def _state(store: Any) -> _StoreSchemaState:
    version = _schema_version(store)
    key = id(store)
    with _LOCK:
        current = _STATES.get(key)
        if current is None or current.schema_version != version:
            if current is not None:
                _STATS["schema_invalidations"] += 1
            current = _StoreSchemaState(schema_version=version)
            _STATES[key] = current
        return current


def table_exists(store: Any, table: str) -> bool:
    name = str(table)
    state = _state(store)
    with _LOCK:
        if name in state.tables:
            _STATS["table_cache_hits"] += 1
            return state.tables[name]
        _STATS["table_cache_misses"] += 1
    try:
        with store._lock:
            exists = store.db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                (name,),
            ).fetchone() is not None
    except Exception:
        exists = False
    with _LOCK:
        # If the schema changed during the query, the next call will invalidate.
        state.tables[name] = exists
    return exists


def columns(store: Any, table: str) -> set[str]:
    name = str(table)
    state = _state(store)
    with _LOCK:
        cached = state.columns.get(name)
        if cached is not None:
            _STATS["column_cache_hits"] += 1
            return set(cached)
        _STATS["column_cache_misses"] += 1
    if not table_exists(store, name):
        result: frozenset[str] = frozenset()
    else:
        with store._lock:
            rows = store.db.execute(f"PRAGMA table_info({name})").fetchall()
        result = frozenset(str(row["name"]) for row in rows)
    with _LOCK:
        state.columns[name] = result
    return set(result)


def invalidate(store: Any | None = None) -> None:
    with _LOCK:
        if store is None:
            _STATES.clear()
        else:
            _STATES.pop(id(store), None)
        _STATS["schema_invalidations"] += 1


def stats() -> dict[str, Any]:
    with _LOCK:
        return {
            "schema_cache_version": SCHEMA_CACHE_VERSION,
            **_STATS,
            "tracked_store_count": len(_STATES),
            "schema_version_auto_invalidates_on_ddl": True,
            "paper_only": True,
            "live_money_authority": False,
        }


def install_v51_schema_cache() -> None:
    """Replace repeated v5.1 sqlite_master/table_info scans at the proof hot path.

    The target helpers are implementation details in existing v5.1 proof modules.
    They keep their historical names/contracts; only their introspection primitive is
    replaced. PRAGMA schema_version invalidates cached results whenever DDL changes.
    """
    from . import v51_candidate_ledger as candidate_ledger
    from . import v51_evidence_analytics as evidence_analytics

    candidate_ledger._table_exists = table_exists  # type: ignore[assignment]
    candidate_ledger._columns = columns  # type: ignore[assignment]
    evidence_analytics._table_exists = table_exists  # type: ignore[assignment]
    evidence_analytics._columns = columns  # type: ignore[assignment]


__all__ = [
    "SCHEMA_CACHE_VERSION",
    "columns",
    "install_v51_schema_cache",
    "invalidate",
    "stats",
    "table_exists",
]
