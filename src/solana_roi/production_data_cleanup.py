from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from .certification_epoch import release_commit_from_env
from .safe_retention_cleanup import cleanup_stale_certification_exports

CLEANUP_VERSION = "production-data-cleanup-v3-bounded-preflight"
ENABLED_ENV = "SOLANA_ROI_PRODUCTION_CLEANUP_ENABLED"
RUN_ID_ENV = "SOLANA_ROI_PRODUCTION_CLEANUP_RUN_ID"
ROLE_ENV = "SOLANA_ROI_PRODUCTION_CLEANUP_ROLE"
ACK_WATERMARK_ENV = "SOLANA_ROI_PRODUCTION_CLEANUP_ACK_WATERMARK"
TELEMETRY_HOURS_ENV = "SOLANA_ROI_PRODUCTION_CLEANUP_TELEMETRY_HOURS"

LATENCY_FAILURE_TABLE = "anonymous_candidate_latency_failures"
CERTIFICATION_EPOCH_TABLE = "certification_release_epochs"

# These tables are current authority, current portfolio/accounting, wallet
# intelligence, live evidence, release/certification state, or immutable lineage.
# Any table not explicitly covered by a proven deletion predicate is protected too.
PROTECTED_TABLES = frozenset(
    {
        "events",
        "paper_engine_checkpoint",
        "wallet_profiles",
        "wallet_entity_links",
        "normalized_swaps",
        "wallet_first_touches",
        "wallet_first_touch_candidates",
        "wallet_risk_evidence",
        "wallet_intelligence_snapshots",
        "adaptive_wallet_cohorts",
        "program_coverage_observations",
        "price_marks",
        "_certification_replica_meta",
        CERTIFICATION_EPOCH_TABLE,
    }
)
SYNTHETIC_ROOT = "v51_synthetic_provenance"
SYNTHETIC_TABLES = (
    "v51_candidate_stage_events",
    "v51_candidate_pipeline_audit",
    "v51_candidate_current_state",
    "v51_candidates",
)
PREDICATE_TABLES = frozenset(
    {
        "certification_replication_changes",
        "risk_refresh_measurements",
        LATENCY_FAILURE_TABLE,
        SYNTHETIC_ROOT,
        *SYNTHETIC_TABLES,
    }
)


@dataclass(frozen=True)
class TableShape:
    name: str
    columns: tuple[str, ...]
    primary_key: tuple[str, ...]
    foreign_keys: tuple[tuple[str, str, str], ...]
    rows: str = "not_scanned"


class CleanupBlocked(RuntimeError):
    pass


def _env_true(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _validate_run_id(value: str) -> str:
    value = value.strip()
    if not value or len(value) > 96 or any(
        char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_." for char in value
    ):
        raise CleanupBlocked("cleanup run id is missing or unsafe")
    return value


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _connect(path: Path) -> sqlite3.Connection:
    db = sqlite3.connect(str(path), timeout=30.0)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout=30000")
    db.execute("PRAGMA foreign_keys=ON")
    return db


def _user_tables(db: sqlite3.Connection) -> list[str]:
    return [
        str(row[0])
        for row in db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ).fetchall()
    ]


def _shape(db: sqlite3.Connection, table: str) -> TableShape:
    quoted = _quote_identifier(table)
    info = db.execute(f"PRAGMA table_info({quoted})").fetchall()
    columns = tuple(str(row[1]) for row in info)
    primary_key = tuple(
        str(row[1]) for row in sorted((row for row in info if int(row[5]) > 0), key=lambda row: int(row[5]))
    )
    foreign_keys = tuple(
        (str(row[3]), str(row[2]), str(row[4]))
        for row in db.execute(f"PRAGMA foreign_key_list({quoted})").fetchall()
    )
    # Deliberately do not COUNT(*) here. Schema extraction is part of cleanup
    # preflight and must remain O(number of schema objects), not O(history).
    return TableShape(table, columns, primary_key, foreign_keys)


def extract_schema(db: sqlite3.Connection) -> dict[str, TableShape]:
    return {table: _shape(db, table) for table in _user_tables(db)}


def _required_columns(schema: dict[str, TableShape], table: str, required: Iterable[str]) -> None:
    shape = schema.get(table)
    if shape is None:
        raise CleanupBlocked(f"required cleanup table missing: {table}")
    missing = set(required).difference(shape.columns)
    if missing:
        raise CleanupBlocked(f"cleanup schema mismatch for {table}: missing {sorted(missing)}")


def _inbound_foreign_keys(schema: dict[str, TableShape], target: str) -> list[tuple[str, str, str]]:
    result: list[tuple[str, str, str]] = []
    for shape in schema.values():
        for source_column, referenced_table, referenced_column in shape.foreign_keys:
            if referenced_table == target:
                result.append((shape.name, source_column, referenced_column))
    return result


def _pragma_metrics(db: sqlite3.Connection) -> dict[str, Any]:
    def one(name: str) -> Any:
        row = db.execute(f"PRAGMA {name}").fetchone()
        return row[0] if row else None

    return {
        "page_size": int(one("page_size") or 0),
        "page_count": int(one("page_count") or 0),
        "freelist_count": int(one("freelist_count") or 0),
        "journal_mode": str(one("journal_mode") or ""),
        "auto_vacuum": int(one("auto_vacuum") or 0),
        "foreign_keys": int(one("foreign_keys") or 0),
    }


def _file_metrics(database_path: Path) -> dict[str, int]:
    usage = shutil.disk_usage(database_path.parent)

    def size(path: Path) -> int:
        try:
            return int(path.stat().st_size)
        except FileNotFoundError:
            return 0

    return {
        "database_bytes": size(database_path),
        "wal_bytes": size(Path(str(database_path) + "-wal")),
        "shm_bytes": size(Path(str(database_path) + "-shm")),
        "filesystem_total_bytes": int(usage.total),
        "filesystem_used_bytes": int(usage.used),
        "filesystem_free_bytes": int(usage.free),
    }


def _full_integrity(db: sqlite3.Connection) -> dict[str, Any]:
    integrity_rows = [str(row[0]) for row in db.execute("PRAGMA integrity_check").fetchall()]
    fk_rows = [tuple(row) for row in db.execute("PRAGMA foreign_key_check").fetchall()]
    return {
        "checked": True,
        "integrity_check": integrity_rows,
        "foreign_key_violations": fk_rows,
        "ok": integrity_rows == ["ok"] and not fk_rows,
    }


def _deferred_integrity() -> dict[str, Any]:
    return {
        "checked": False,
        "ok": None,
        "reason": "full integrity and foreign-key scans deferred until after deletion and compaction",
    }


def _schema_fingerprint(schema: dict[str, TableShape]) -> str:
    payload = {name: asdict(shape) for name, shape in sorted(schema.items())}
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode()).hexdigest()


def _edge_value(db: sqlite3.Connection, table: str, column: str) -> Any:
    quoted_table = _quote_identifier(table)
    quoted_column = _quote_identifier(column)
    row = db.execute(
        f"SELECT {quoted_column} FROM {quoted_table} ORDER BY {quoted_column} DESC LIMIT 1"
    ).fetchone()
    return row[0] if row else None


def _bounded_anchors(db: sqlite3.Connection, schema: dict[str, TableShape]) -> dict[str, Any]:
    anchors: dict[str, Any] = {}
    if "events" in schema:
        _required_columns(schema, "events", ("id",))
        anchors["event_head_id"] = _edge_value(db, "events", "id")
    if "paper_engine_checkpoint" in schema:
        _required_columns(
            schema,
            "paper_engine_checkpoint",
            ("id", "last_engine_event_id", "state_sha256"),
        )
        row = db.execute(
            "SELECT id,last_engine_event_id,state_sha256 FROM paper_engine_checkpoint WHERE id=1 LIMIT 1"
        ).fetchone()
        anchors["paper_engine_checkpoint"] = (
            {
                "id": row[0],
                "last_engine_event_id": row[1],
                "state_sha256": row[2],
            }
            if row is not None
            else None
        )
    if "certification_replication_changes" in schema:
        _required_columns(schema, "certification_replication_changes", ("id",))
        anchors["replication_head_id"] = _edge_value(
            db, "certification_replication_changes", "id"
        )
    return anchors


def _measure(
    database_path: Path,
    db: sqlite3.Connection,
    *,
    full_integrity: bool,
) -> dict[str, Any]:
    schema = extract_schema(db)
    return {
        "captured_at": _utcnow(),
        "files": _file_metrics(database_path),
        "pragma": _pragma_metrics(db),
        "integrity": _full_integrity(db) if full_integrity else _deferred_integrity(),
        "schema_fingerprint": _schema_fingerprint(schema),
        "anchors": _bounded_anchors(db, schema),
        "tables": {
            name: {
                "rows": shape.rows,
                "columns": list(shape.columns),
                "primary_key": list(shape.primary_key),
                "foreign_keys": [list(item) for item in shape.foreign_keys],
            }
            for name, shape in schema.items()
        },
    }


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    raw = json.dumps(payload, sort_keys=True, indent=2, default=str) + "\n"
    with temporary.open("w", encoding="utf-8") as handle:
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _durable_certifier_watermark(db: sqlite3.Connection, schema: dict[str, TableShape]) -> int | None:
    table = "_certification_replica_meta"
    if table not in schema:
        return None
    _required_columns(schema, table, ("key", "value", "updated_at"))
    row = db.execute(
        f"SELECT value FROM {_quote_identifier(table)} WHERE key='source_watermark' LIMIT 1"
    ).fetchone()
    if row is None:
        return None
    try:
        value = int(str(row[0]))
    except (TypeError, ValueError) as exc:
        raise CleanupBlocked("certifier source watermark is not an integer") from exc
    if value < 0:
        raise CleanupBlocked("certifier source watermark cannot be negative")
    return value


def _delete_batched(
    db: sqlite3.Connection,
    sql: str,
    args: tuple[Any, ...],
    *,
    batch_size: int = 2000,
) -> int:
    total = 0
    while True:
        cursor = db.execute(sql, (*args, int(batch_size)))
        deleted = int(cursor.rowcount if cursor.rowcount >= 0 else 0)
        db.commit()
        total += deleted
        if deleted < batch_size:
            return total


def _delete_synthetic_candidates(
    db: sqlite3.Connection, schema: dict[str, TableShape]
) -> dict[str, int]:
    if SYNTHETIC_ROOT not in schema:
        return {}
    _required_columns(schema, SYNTHETIC_ROOT, ("surface", "candidate_id", "synthetic"))
    for table in SYNTHETIC_TABLES:
        if table not in schema:
            continue
        _required_columns(schema, table, ("surface", "candidate_id"))
        inbound = [
            fk
            for fk in _inbound_foreign_keys(schema, table)
            if fk[0] not in SYNTHETIC_TABLES and fk[0] != SYNTHETIC_ROOT
        ]
        if inbound:
            raise CleanupBlocked(
                f"synthetic deletion has unmodelled inbound foreign keys for {table}: {inbound}"
            )

    db.execute("DROP TABLE IF EXISTS temp._roi_cleanup_synthetic")
    db.execute(
        "CREATE TEMP TABLE _roi_cleanup_synthetic("
        "surface TEXT NOT NULL,candidate_id TEXT NOT NULL,"
        "PRIMARY KEY(surface,candidate_id)) WITHOUT ROWID"
    )
    root = _quote_identifier(SYNTHETIC_ROOT)
    db.execute(
        "INSERT OR IGNORE INTO _roi_cleanup_synthetic(surface,candidate_id) "
        f"SELECT surface,candidate_id FROM {root} WHERE synthetic=1"
    )
    found = db.execute("SELECT 1 FROM _roi_cleanup_synthetic LIMIT 1").fetchone()
    if found is None:
        return {table: 0 for table in SYNTHETIC_TABLES if table in schema} | {SYNTHETIC_ROOT: 0}

    deleted: dict[str, int] = {}
    for table in SYNTHETIC_TABLES:
        if table not in schema:
            continue
        quoted = _quote_identifier(table)
        cursor = db.execute(
            f"DELETE FROM {quoted} WHERE EXISTS ("
            "SELECT 1 FROM _roi_cleanup_synthetic s "
            f"WHERE s.surface={quoted}.surface AND s.candidate_id={quoted}.candidate_id)"
        )
        deleted[table] = int(cursor.rowcount if cursor.rowcount >= 0 else 0)
    cursor = db.execute(
        f"DELETE FROM {root} WHERE synthetic=1 AND EXISTS ("
        "SELECT 1 FROM _roi_cleanup_synthetic s "
        f"WHERE s.surface={root}.surface AND s.candidate_id={root}.candidate_id)"
    )
    deleted[SYNTHETIC_ROOT] = int(cursor.rowcount if cursor.rowcount >= 0 else 0)
    db.commit()
    return deleted


def _delete_acknowledged_replication(
    db: sqlite3.Connection,
    schema: dict[str, TableShape],
    *,
    acknowledged_watermark: int | None,
) -> int:
    table = "certification_replication_changes"
    if table not in schema or acknowledged_watermark is None:
        return 0
    if acknowledged_watermark < 0:
        raise CleanupBlocked("acknowledged watermark cannot be negative")
    _required_columns(
        schema,
        table,
        ("id", "table_name", "change_type", "row_json", "primary_key_json", "created_at"),
    )
    inbound = _inbound_foreign_keys(schema, table)
    if inbound:
        raise CleanupBlocked(f"replication journal has unexpected inbound foreign keys: {inbound}")
    quoted = _quote_identifier(table)
    head = db.execute(f"SELECT id FROM {quoted} ORDER BY id DESC LIMIT 1").fetchone()
    source_head = int(head[0] if head else 0)
    if acknowledged_watermark > source_head:
        raise CleanupBlocked(
            f"acknowledged watermark {acknowledged_watermark} exceeds source journal head {source_head}"
        )
    deleted = _delete_batched(
        db,
        f"DELETE FROM {quoted} WHERE rowid IN ("
        f"SELECT rowid FROM {quoted} WHERE id<=? ORDER BY id LIMIT ?)",
        (int(acknowledged_watermark),),
    )
    if db.execute(f"SELECT 1 FROM {quoted} WHERE id<=? LIMIT 1", (int(acknowledged_watermark),)).fetchone():
        raise CleanupBlocked("acknowledged replication rows remain after bounded deletion")
    return deleted


def _delete_stale_measurements(
    db: sqlite3.Connection, schema: dict[str, TableShape], *, cutoff: str
) -> int:
    table = "risk_refresh_measurements"
    if table not in schema:
        return 0
    _required_columns(schema, table, ("id", "completed_at", "token_mint", "complete", "fresh"))
    inbound = _inbound_foreign_keys(schema, table)
    if inbound:
        raise CleanupBlocked(f"risk refresh measurements have unexpected inbound foreign keys: {inbound}")
    quoted = _quote_identifier(table)
    return _delete_batched(
        db,
        f"DELETE FROM {quoted} WHERE rowid IN ("
        f"SELECT rowid FROM {quoted} WHERE completed_at<? ORDER BY id LIMIT ?)",
        (cutoff,),
    )


def _first_indexed_column(db: sqlite3.Connection, table: str, column: str) -> str | None:
    quoted = _quote_identifier(table)
    for index_row in db.execute(f"PRAGMA index_list({quoted})").fetchall():
        index_name = str(index_row[1])
        info = db.execute(f"PRAGMA index_info({_quote_identifier(index_name)})").fetchall()
        if info and str(info[0][2]) == column:
            return index_name
    return None


def _certification_boundary(
    db: sqlite3.Connection, schema: dict[str, TableShape]
) -> tuple[str, str]:
    _required_columns(schema, CERTIFICATION_EPOCH_TABLE, ("release_commit", "started_at"))
    release_commit = release_commit_from_env()
    if release_commit is None:
        raise CleanupBlocked("current release commit is unavailable; certification boundary cannot be proven")
    row = db.execute(
        f"SELECT started_at FROM {_quote_identifier(CERTIFICATION_EPOCH_TABLE)} "
        "WHERE release_commit=? LIMIT 1",
        (release_commit,),
    ).fetchone()
    if row is None:
        raise CleanupBlocked(
            f"current release certification epoch is missing for {release_commit}"
        )
    raw = str(row[0])
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise CleanupBlocked("current release certification epoch is not ISO-8601") from exc
    if parsed.tzinfo is None:
        raise CleanupBlocked("current release certification epoch is timezone-naive")
    return release_commit, parsed.astimezone(timezone.utc).isoformat()


def _replication_only_triggers(db: sqlite3.Connection, table: str) -> list[str]:
    rows = db.execute(
        "SELECT name,sql FROM sqlite_master WHERE type='trigger' AND tbl_name=? ORDER BY name",
        (table,),
    ).fetchall()
    names: list[str] = []
    for row in rows:
        name = str(row[0])
        sql = str(row[1] or "").lower()
        if "certification_replication_changes" not in sql:
            raise CleanupBlocked(
                f"{table} has non-replication trigger dependency: {name}"
            )
        names.append(name)
    views = db.execute(
        "SELECT name FROM sqlite_master WHERE type='view' AND lower(sql) LIKE ? ORDER BY name",
        (f"%{table.lower()}%",),
    ).fetchall()
    if views:
        raise CleanupBlocked(
            f"{table} has view dependencies: {[str(row[0]) for row in views]}"
        )
    return names


def _delete_obsolete_latency_failures(
    db: sqlite3.Connection,
    schema: dict[str, TableShape],
) -> tuple[int, dict[str, Any] | None]:
    table = LATENCY_FAILURE_TABLE
    if table not in schema:
        return 0, None
    _required_columns(
        schema,
        table,
        ("id", "failed_at", "reason", "outcome", "count", "max_age_ms"),
    )
    inbound = _inbound_foreign_keys(schema, table)
    if inbound:
        raise CleanupBlocked(f"{table} has unexpected inbound foreign keys: {inbound}")
    index_name = _first_indexed_column(db, table, "failed_at")
    if index_name is None:
        raise CleanupBlocked(f"{table}.failed_at is not indexed")
    triggers = _replication_only_triggers(db, table)
    release_commit, prospective_start_at = _certification_boundary(db, schema)
    quoted = _quote_identifier(table)
    deleted = _delete_batched(
        db,
        f"DELETE FROM {quoted} WHERE rowid IN ("
        f"SELECT rowid FROM {quoted} WHERE failed_at<? ORDER BY failed_at LIMIT ?)",
        (prospective_start_at,),
    )
    if db.execute(
        f"SELECT 1 FROM {quoted} WHERE failed_at<? LIMIT 1",
        (prospective_start_at,),
    ).fetchone():
        raise CleanupBlocked("obsolete anonymous candidate latency failures remain after cleanup")
    return deleted, {
        "release_commit": release_commit,
        "prospective_start_at": prospective_start_at,
        "failed_at_index": index_name,
        "replication_triggers": triggers,
        "predicate": "failed_at < prospective_start_at",
    }


def _protected_plan(schema: dict[str, TableShape], *, role: str) -> dict[str, str]:
    plan: dict[str, str] = {}
    for table in sorted(schema):
        if role == "certifier":
            plan[table] = "protected:certifier-replica-equivalence"
        elif table in PREDICATE_TABLES:
            plan[table] = "predicate-controlled"
        elif table in PROTECTED_TABLES:
            plan[table] = "protected:current-authority-or-lineage"
        else:
            plan[table] = "protected:unknown-or-not-yet-proven-disposable"
    return plan


def _compact(database_path: Path, db: sqlite3.Connection) -> dict[str, Any]:
    checkpoint = db.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
    db.commit()
    before = _file_metrics(database_path)
    db_size = int(before["database_bytes"])
    free = int(before["filesystem_free_bytes"])
    required_headroom = max(256 * 1024 * 1024, db_size + 64 * 1024 * 1024)
    if free < required_headroom:
        auto_vacuum = int(db.execute("PRAGMA auto_vacuum").fetchone()[0])
        if auto_vacuum == 2:
            db.execute("PRAGMA incremental_vacuum")
            db.execute("PRAGMA optimize")
            db.commit()
            return {
                "wal_checkpoint": list(checkpoint or ()),
                "mode": "incremental",
                "required_headroom": required_headroom,
                "free_before": free,
            }
        raise CleanupBlocked(
            f"physical compaction blocked: free={free} required={required_headroom}"
        )
    db.execute("VACUUM")
    db.execute("PRAGMA optimize")
    return {
        "wal_checkpoint": list(checkpoint or ()),
        "mode": "vacuum",
        "required_headroom": required_headroom,
        "free_before": free,
    }


def execute_cleanup(
    database_path: Path,
    *,
    role: str,
    run_id: str,
    acknowledged_watermark: int | None = None,
    telemetry_hours: float = 24.0,
) -> dict[str, Any]:
    role = role.strip().lower()
    if role not in {"authoritative", "certifier"}:
        raise CleanupBlocked("cleanup role must be authoritative or certifier")
    run_id = _validate_run_id(run_id)
    database_path = database_path.resolve()
    if not database_path.exists() or not database_path.is_file():
        raise CleanupBlocked(f"database path does not exist: {database_path}")

    marker_dir = database_path.parent / ".production-cleanup"
    report_path = marker_dir / f"{run_id}.json"
    if report_path.exists():
        previous = json.loads(report_path.read_text(encoding="utf-8"))
        if previous.get("status") == "success":
            return {**previous, "idempotent_replay": True}
        raise CleanupBlocked(f"cleanup run id already has non-success durable state: {run_id}")

    report: dict[str, Any] = {
        "version": CLEANUP_VERSION,
        "run_id": run_id,
        "role": role,
        "database_path": str(database_path),
        "status": "running",
        "started_at": _utcnow(),
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }
    _atomic_json(report_path, report)

    db = _connect(database_path)
    try:
        # Acquiring and releasing an immediate transaction proves no other writer is
        # active. The command runs under the service-wide persistent-disk lease before
        # normal database-writing workers are released.
        db.execute("BEGIN IMMEDIATE")
        db.commit()
        before = _measure(database_path, db, full_integrity=False)
        schema = extract_schema(db)
        report["protected_set"] = _protected_plan(schema, role=role)
        report["durable_source_watermark"] = _durable_certifier_watermark(db, schema)
        report["before"] = before
        _atomic_json(report_path, report)

        if role == "certifier":
            # Certifier maintenance preserves logical replica equivalence. It may
            # reclaim WAL/freelist space but does not independently discard source
            # rows or certification evidence.
            synthetic: dict[str, int] = {}
            replication = 0
            stale_measurements = 0
            latency_failures = 0
            latency_boundary = None
        else:
            synthetic = _delete_synthetic_candidates(db, schema)
            replication = _delete_acknowledged_replication(
                db, schema, acknowledged_watermark=acknowledged_watermark
            )
            cutoff = (
                datetime.now(timezone.utc)
                - timedelta(hours=max(1.0, float(telemetry_hours)))
            ).isoformat()
            stale_measurements = _delete_stale_measurements(db, schema, cutoff=cutoff)
            report["telemetry_cutoff"] = cutoff
            latency_failures, latency_boundary = _delete_obsolete_latency_failures(db, schema)

        report["deleted_rows"] = {
            "synthetic_candidate_closure": synthetic,
            "acknowledged_replication_changes": replication,
            "stale_risk_refresh_measurements": stale_measurements,
            "obsolete_anonymous_candidate_latency_failures": latency_failures,
        }
        report["latency_certification_boundary"] = latency_boundary
        report["acknowledged_watermark"] = acknowledged_watermark
        report["orphan_files"] = cleanup_stale_certification_exports(database_path)
        _atomic_json(report_path, report)

        compaction = _compact(database_path, db)
        after = _measure(database_path, db, full_integrity=True)
        if not after["integrity"]["ok"]:
            raise CleanupBlocked("post-cleanup SQLite integrity/foreign-key check failed")
        report["compaction"] = compaction
        report["after"] = after
        report["reclaimed"] = {
            "database_bytes": int(before["files"]["database_bytes"])
            - int(after["files"]["database_bytes"]),
            "wal_bytes": int(before["files"]["wal_bytes"])
            - int(after["files"]["wal_bytes"]),
            "filesystem_free_bytes": int(after["files"]["filesystem_free_bytes"])
            - int(before["files"]["filesystem_free_bytes"]),
            "freelist_pages": int(before["pragma"]["freelist_count"])
            - int(after["pragma"]["freelist_count"]),
        }
        report["status"] = "success"
        report["completed_at"] = _utcnow()
        _atomic_json(report_path, report)
        return report
    except Exception as exc:
        try:
            db.rollback()
        except Exception:
            pass
        report["status"] = "blocked"
        report["completed_at"] = _utcnow()
        report["error"] = f"{type(exc).__name__}:{exc}"
        _atomic_json(report_path, report)
        raise
    finally:
        db.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Fail-closed one-shot production SQLite cleanup"
    )
    parser.add_argument(
        "--database",
        default=os.getenv("SOLANA_ROI_DATABASE_PATH") or os.getenv("SOLANA_ROI_DB_PATH"),
    )
    parser.add_argument("--role", default=os.getenv(ROLE_ENV, ""))
    parser.add_argument("--run-id", default=os.getenv(RUN_ID_ENV, ""))
    parser.add_argument(
        "--ack-watermark",
        type=int,
        default=int(os.environ[ACK_WATERMARK_ENV]) if os.getenv(ACK_WATERMARK_ENV) else None,
    )
    parser.add_argument(
        "--telemetry-hours",
        type=float,
        default=float(os.getenv(TELEMETRY_HOURS_ENV, "24")),
    )
    args = parser.parse_args(argv)
    if not _env_true(ENABLED_ENV):
        print(
            json.dumps(
                {"version": CLEANUP_VERSION, "status": "disabled", "enabled": False},
                sort_keys=True,
            )
        )
        return 0
    if not args.database:
        raise CleanupBlocked("cleanup enabled but database path is not configured")
    result = execute_cleanup(
        Path(args.database),
        role=args.role,
        run_id=args.run_id,
        acknowledged_watermark=args.ack_watermark,
        telemetry_hours=args.telemetry_hours,
    )
    print("ROI_PRODUCTION_DATA_CLEANUP " + json.dumps(result, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(
            f"ROI_PRODUCTION_DATA_CLEANUP_BLOCKED {type(exc).__name__}:{exc}",
            file=sys.stderr,
        )
        raise
