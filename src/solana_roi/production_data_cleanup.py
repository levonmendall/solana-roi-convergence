from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

CLEANUP_VERSION = "production-data-cleanup-v1"
ENABLED_ENV = "SOLANA_ROI_PRODUCTION_CLEANUP_ENABLED"
RUN_ID_ENV = "SOLANA_ROI_PRODUCTION_CLEANUP_RUN_ID"
ROLE_ENV = "SOLANA_ROI_PRODUCTION_CLEANUP_ROLE"
ACK_WATERMARK_ENV = "SOLANA_ROI_PRODUCTION_CLEANUP_ACK_WATERMARK"
TELEMETRY_HOURS_ENV = "SOLANA_ROI_PRODUCTION_CLEANUP_TELEMETRY_HOURS"

# These tables carry current authority/state or immutable lineage and are never
# deletion targets in v1. Unknown tables are also protected by default.
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
        "v51_candidate_current_state",
        "_certification_replica_meta",
        "sqlite_sequence",
    }
)

SYNTHETIC_ROOT = "v51_synthetic_provenance"
SYNTHETIC_TABLES = (
    "v51_candidate_stage_events",
    "v51_candidate_pipeline_audit",
    "v51_candidate_current_state",
    "v51_candidates",
)


@dataclass(frozen=True)
class TableShape:
    name: str
    columns: tuple[str, ...]
    primary_key: tuple[str, ...]
    foreign_keys: tuple[tuple[str, str, str], ...]
    rows: int


class CleanupBlocked(RuntimeError):
    pass


def _env_true(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _validate_run_id(run_id: str) -> str:
    value = run_id.strip()
    if not value or len(value) > 96 or any(ch not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_." for ch in value):
        raise CleanupBlocked("cleanup run id is missing or unsafe")
    return value


def _connect(path: Path) -> sqlite3.Connection:
    db = sqlite3.connect(str(path), timeout=30.0)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout=30000")
    db.execute("PRAGMA foreign_keys=ON")
    return db


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _user_tables(db: sqlite3.Connection) -> list[str]:
    return [
        str(row[0])
        for row in db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ).fetchall()
    ]


def _shape(db: sqlite3.Connection, table: str) -> TableShape:
    q = _quote_identifier(table)
    info = db.execute(f"PRAGMA table_info({q})").fetchall()
    columns = tuple(str(row[1]) for row in info)
    primary_key = tuple(str(row[1]) for row in sorted((r for r in info if int(r[5]) > 0), key=lambda r: int(r[5])))
    foreign_keys = tuple(
        (str(row[3]), str(row[2]), str(row[4]))
        for row in db.execute(f"PRAGMA foreign_key_list({q})").fetchall()
    )
    rows = int(db.execute(f"SELECT COUNT(*) FROM {q}").fetchone()[0])
    return TableShape(table, columns, primary_key, foreign_keys, rows)


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


def _file_metrics(database_path: Path) -> dict[str, Any]:
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


def _integrity(db: sqlite3.Connection) -> dict[str, Any]:
    integrity_rows = [str(row[0]) for row in db.execute("PRAGMA integrity_check").fetchall()]
    fk_rows = [tuple(row) for row in db.execute("PRAGMA foreign_key_check").fetchall()]
    return {"integrity_check": integrity_rows, "foreign_key_violations": fk_rows, "ok": integrity_rows == ["ok"] and not fk_rows}


def _schema_fingerprint(schema: dict[str, TableShape]) -> str:
    payload = {name: asdict(shape) for name, shape in sorted(schema.items())}
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


def _measure(database_path: Path, db: sqlite3.Connection) -> dict[str, Any]:
    schema = extract_schema(db)
    return {
        "captured_at": _utcnow(),
        "files": _file_metrics(database_path),
        "pragma": _pragma_metrics(db),
        "integrity": _integrity(db),
        "schema_fingerprint": _schema_fingerprint(schema),
        "tables": {name: {"rows": shape.rows, "columns": list(shape.columns), "primary_key": list(shape.primary_key), "foreign_keys": [list(x) for x in shape.foreign_keys]} for name, shape in schema.items()},
    }


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    raw = json.dumps(payload, sort_keys=True, indent=2, default=str) + "\n"
    with tmp.open("w", encoding="utf-8") as handle:
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)


def _delete_batched(db: sqlite3.Connection, sql: str, args: tuple[Any, ...], *, batch_size: int = 2000) -> int:
    total = 0
    while True:
        cursor = db.execute(sql, (*args, int(batch_size)))
        deleted = int(cursor.rowcount if cursor.rowcount >= 0 else 0)
        db.commit()
        total += deleted
        if deleted < batch_size:
            return total


def _delete_synthetic_candidates(db: sqlite3.Connection, schema: dict[str, TableShape]) -> dict[str, int]:
    if SYNTHETIC_ROOT not in schema:
        return {}
    _required_columns(schema, SYNTHETIC_ROOT, ("surface", "candidate_id", "synthetic"))
    for table in SYNTHETIC_TABLES:
        if table in schema:
            _required_columns(schema, table, ("surface", "candidate_id"))
            inbound = [fk for fk in _inbound_foreign_keys(schema, table) if fk[0] not in SYNTHETIC_TABLES and fk[0] != SYNTHETIC_ROOT]
            if inbound:
                raise CleanupBlocked(f"synthetic deletion has unmodelled inbound foreign keys for {table}: {inbound}")

    db.execute("DROP TABLE IF EXISTS temp._roi_cleanup_synthetic")
    db.execute(
        "CREATE TEMP TABLE _roi_cleanup_synthetic(surface TEXT NOT NULL, candidate_id TEXT NOT NULL, PRIMARY KEY(surface,candidate_id)) WITHOUT ROWID"
    )
    db.execute(
        "INSERT OR IGNORE INTO _roi_cleanup_synthetic(surface,candidate_id) "
        f"SELECT surface,candidate_id FROM {_quote_identifier(SYNTHETIC_ROOT)} WHERE synthetic=1"
    )
    count = int(db.execute("SELECT COUNT(*) FROM _roi_cleanup_synthetic").fetchone()[0])
    if count == 0:
        return {table: 0 for table in SYNTHETIC_TABLES if table in schema} | {SYNTHETIC_ROOT: 0}

    deleted: dict[str, int] = {}
    for table in SYNTHETIC_TABLES:
        if table not in schema:
            continue
        q = _quote_identifier(table)
        cursor = db.execute(
            f"DELETE FROM {q} WHERE EXISTS (SELECT 1 FROM _roi_cleanup_synthetic s WHERE s.surface={q}.surface AND s.candidate_id={q}.candidate_id)"
        )
        deleted[table] = int(cursor.rowcount if cursor.rowcount >= 0 else 0)
    cursor = db.execute(
        f"DELETE FROM {_quote_identifier(SYNTHETIC_ROOT)} WHERE synthetic=1 AND EXISTS (SELECT 1 FROM _roi_cleanup_synthetic s WHERE s.surface={_quote_identifier(SYNTHETIC_ROOT)}.surface AND s.candidate_id={_quote_identifier(SYNTHETIC_ROOT)}.candidate_id)"
    )
    deleted[SYNTHETIC_ROOT] = int(cursor.rowcount if cursor.rowcount >= 0 else 0)
    db.commit()
    return deleted


def _delete_acknowledged_replication(db: sqlite3.Connection, schema: dict[str, TableShape], *, role: str, acknowledged_watermark: int | None) -> int:
    table = "certification_replication_changes"
    if table not in schema or acknowledged_watermark is None:
        return 0
    if role != "authoritative":
        raise CleanupBlocked("replication journal pruning is authoritative-only")
    if acknowledged_watermark < 0:
        raise CleanupBlocked("acknowledged watermark cannot be negative")
    _required_columns(schema, table, ("id", "table_name", "change_type", "row_json", "primary_key_json", "created_at"))
    inbound = _inbound_foreign_keys(schema, table)
    if inbound:
        raise CleanupBlocked(f"replication journal has unexpected inbound foreign keys: {inbound}")
    row = db.execute(f"SELECT MAX(id) FROM {_quote_identifier(table)}").fetchone()
    source_head = int(row[0] or 0)
    if acknowledged_watermark > source_head:
        raise CleanupBlocked(f"acknowledged watermark {acknowledged_watermark} exceeds source journal head {source_head}")
    return _delete_batched(
        db,
        f"DELETE FROM {_quote_identifier(table)} WHERE rowid IN (SELECT rowid FROM {_quote_identifier(table)} WHERE id<=? ORDER BY id LIMIT ?)",
        (int(acknowledged_watermark),),
    )


def _delete_stale_measurements(db: sqlite3.Connection, schema: dict[str, TableShape], *, cutoff: str) -> int:
    table = "risk_refresh_measurements"
    if table not in schema:
        return 0
    _required_columns(schema, table, ("id", "completed_at", "token_mint", "complete", "fresh"))
    inbound = _inbound_foreign_keys(schema, table)
    if inbound:
        raise CleanupBlocked(f"risk refresh measurements have unexpected inbound foreign keys: {inbound}")
    return _delete_batched(
        db,
        f"DELETE FROM {_quote_identifier(table)} WHERE rowid IN (SELECT rowid FROM {_quote_identifier(table)} WHERE completed_at<? ORDER BY id LIMIT ?)",
        (cutoff,),
    )


def _protected_plan(schema: dict[str, TableShape]) -> dict[str, str]:
    plan: dict[str, str] = {}
    known_deletable = {"certification_replication_changes", "risk_refresh_measurements", SYNTHETIC_ROOT, *SYNTHETIC_TABLES}
    for table in sorted(schema):
        if table in PROTECTED_TABLES:
            plan[table] = "protected:current-authority-or-lineage"
        elif table in known_deletable:
            plan[table] = "predicate-controlled"
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
        if auto_vacuum in (1, 2):
            db.execute("PRAGMA incremental_vacuum")
            db.execute("PRAGMA optimize")
            db.commit()
            return {"wal_checkpoint": list(checkpoint or ()), "mode": "incremental", "required_headroom": required_headroom, "free_before": free}
        raise CleanupBlocked(f"physical compaction blocked: free={free} required={required_headroom}")
    db.execute("VACUUM")
    db.execute("PRAGMA optimize")
    return {"wal_checkpoint": list(checkpoint or ()), "mode": "vacuum", "required_headroom": required_headroom, "free_before": free}


def execute_cleanup(database_path: Path, *, role: str, run_id: str, acknowledged_watermark: int | None = None, telemetry_hours: float = 24.0) -> dict[str, Any]:
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
        db.execute("BEGIN IMMEDIATE")
        db.commit()
        before = _measure(database_path, db)
        if not before["integrity"]["ok"]:
            raise CleanupBlocked("pre-cleanup SQLite integrity/foreign-key check failed")
        schema = extract_schema(db)
        report["protected_set"] = _protected_plan(schema)
        report["before"] = before
        _atomic_json(report_path, report)

        synthetic = _delete_synthetic_candidates(db, schema)
        replication = _delete_acknowledged_replication(
            db, schema, role=role, acknowledged_watermark=acknowledged_watermark
        )
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=max(1.0, float(telemetry_hours)))).isoformat()
        stale_measurements = _delete_stale_measurements(db, schema, cutoff=cutoff)
        report["deleted_rows"] = {
            "synthetic_candidate_closure": synthetic,
            "acknowledged_replication_changes": replication,
            "stale_risk_refresh_measurements": stale_measurements,
        }
        report["acknowledged_watermark"] = acknowledged_watermark
        report["telemetry_cutoff"] = cutoff
        _atomic_json(report_path, report)

        compaction = _compact(database_path, db)
        after = _measure(database_path, db)
        if not after["integrity"]["ok"]:
            raise CleanupBlocked("post-cleanup SQLite integrity/foreign-key check failed")
        report["compaction"] = compaction
        report["after"] = after
        report["reclaimed"] = {
            "database_bytes": int(before["files"]["database_bytes"]) - int(after["files"]["database_bytes"]),
            "wal_bytes": int(before["files"]["wal_bytes"]) - int(after["files"]["wal_bytes"]),
            "filesystem_free_bytes": int(after["files"]["filesystem_free_bytes"]) - int(before["files"]["filesystem_free_bytes"]),
            "freelist_pages": int(before["pragma"]["freelist_count"]) - int(after["pragma"]["freelist_count"]),
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
    parser = argparse.ArgumentParser(description="Fail-closed one-shot production SQLite cleanup")
    parser.add_argument("--database", default=os.getenv("SOLANA_ROI_DATABASE_PATH") or os.getenv("SOLANA_ROI_DB_PATH"))
    parser.add_argument("--role", default=os.getenv(ROLE_ENV, ""))
    parser.add_argument("--run-id", default=os.getenv(RUN_ID_ENV, ""))
    parser.add_argument("--ack-watermark", type=int, default=int(os.environ[ACK_WATERMARK_ENV]) if os.getenv(ACK_WATERMARK_ENV) else None)
    parser.add_argument("--telemetry-hours", type=float, default=float(os.getenv(TELEMETRY_HOURS_ENV, "24")))
    args = parser.parse_args(argv)

    if not _env_true(ENABLED_ENV):
        print(json.dumps({"version": CLEANUP_VERSION, "status": "disabled", "enabled": False}, sort_keys=True))
        return 0
    if not args.database:
        raise CleanupBlocked("cleanup enabled but database path is not configured")
    result = execute_cleanup(Path(args.database), role=args.role, run_id=args.run_id, acknowledged_watermark=args.ack_watermark, telemetry_hours=args.telemetry_hours)
    print("ROI_PRODUCTION_DATA_CLEANUP " + json.dumps(result, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ROI_PRODUCTION_DATA_CLEANUP_BLOCKED {type(exc).__name__}:{exc}", file=sys.stderr)
        raise
