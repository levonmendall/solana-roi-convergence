from __future__ import annotations

"""Fail-closed exact-dependency production cleanup executor.

V4 is intentionally narrower than the earlier prototype: only a category whose
production dependency is proven may be deleted.  At this release boundary the
only database-row deletion with a complete proof contract is the authoritative
certification replication journal through an externally proven certifier-applied
watermark.  Synthetic candidate rows and old risk-refresh measurements are
reported as protected/pending rather than guessed disposable.

The certifier replica is never row-pruned by this executor.  Its durable
``.state.json`` sidecar is the applied-through truth and must remain unchanged
across compaction.
"""

import hashlib
import json
import os
import sqlite3
from pathlib import Path
from typing import Any

from . import production_data_cleanup as base
from .safe_retention_cleanup import cleanup_stale_certification_exports

CLEANUP_VERSION = "production-data-cleanup-v4"
ENABLED_ENV = base.ENABLED_ENV
RUN_ID_ENV = base.RUN_ID_ENV
ROLE_ENV = base.ROLE_ENV
ACK_WATERMARK_ENV = base.ACK_WATERMARK_ENV
TELEMETRY_HOURS_ENV = base.TELEMETRY_HOURS_ENV
CleanupBlocked = base.CleanupBlocked

REPLICATION_TABLE = "certification_replication_changes"
REPLICATION_COLUMNS = ("id", "table_name", "key_sql", "operation", "changed_at")
BATCH_SIZE = 5000

# These tables carry current authority, portfolio/accounting state, current wallet
# intelligence, active evidence, checkpoints, or lineage.  Unknown tables are also
# protected automatically until a concrete deletion dependency is proven.
PROTECTED_TABLES = frozenset(
    set(base.PROTECTED_TABLES)
    | {
        "events",
        "wallet_profiles",
        "normalized_swaps",
        "token_first_touches",
        "risk_evidence",
        "entity_links",
        "source_cursors",
        "paper_engine_checkpoint",
        "wallet_intelligence_snapshots",
        "adaptive_wallet_cohorts",
        "price_marks",
        "program_coverage_observations",
        "risk_refresh_measurements",
        "certification_replication_meta",
        "direct_solana_storage_maintenance",
        "v51_candidates",
        "v51_candidate_stage_events",
        "v51_candidate_current_state",
        "v51_candidate_pipeline_audit",
        "v51_synthetic_provenance",
        "robinhood_paper_trials",
    }
)


def _certifier_state_path(database_path: Path) -> Path:
    return database_path.with_suffix(database_path.suffix + ".state.json")


def _read_certifier_state(database_path: Path) -> dict[str, Any]:
    path = _certifier_state_path(database_path)
    if not path.exists() or not path.is_file() or path.is_symlink():
        raise CleanupBlocked(f"certifier durable replica state unavailable: {path}")
    try:
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise CleanupBlocked("certifier durable replica state is unreadable") from exc
    if not isinstance(payload, dict):
        raise CleanupBlocked("certifier durable replica state is invalid")
    required = (
        "release_commit",
        "replication_version",
        "epoch",
        "schema_fingerprint",
        "watermark",
        "bootstrap_complete",
    )
    missing = [key for key in required if key not in payload]
    if missing:
        raise CleanupBlocked(f"certifier durable replica state missing keys: {missing}")
    try:
        watermark = int(payload["watermark"])
    except (TypeError, ValueError) as exc:
        raise CleanupBlocked("certifier durable watermark is not an integer") from exc
    if watermark < 0:
        raise CleanupBlocked("certifier durable watermark cannot be negative")
    if not bool(payload.get("bootstrap_complete")):
        raise CleanupBlocked("certifier durable replica bootstrap is incomplete")
    for key in ("release_commit", "replication_version", "epoch", "schema_fingerprint"):
        if not str(payload.get(key) or "").strip():
            raise CleanupBlocked(f"certifier durable replica {key} is empty")
    return {
        "path": str(path),
        "sha256": hashlib.sha256(raw).hexdigest(),
        "watermark": watermark,
        "release_commit": str(payload["release_commit"]),
        "replication_version": str(payload["replication_version"]),
        "epoch": str(payload["epoch"]),
        "schema_fingerprint": str(payload["schema_fingerprint"]),
        "bootstrap_complete": True,
        "catchup_complete": bool(payload.get("catchup_complete", False)),
        "last_transport": payload.get("last_transport"),
    }


def _structural_schema_fingerprint(schema: dict[str, base.TableShape]) -> str:
    payload = {
        name: {
            "columns": list(shape.columns),
            "primary_key": list(shape.primary_key),
            "foreign_keys": [list(item) for item in shape.foreign_keys],
        }
        for name, shape in sorted(schema.items())
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _protected_plan(schema: dict[str, base.TableShape], *, role: str) -> dict[str, str]:
    plan: dict[str, str] = {}
    for table in sorted(schema):
        if role == "certifier":
            plan[table] = "protected:certifier-replica-equivalence"
        elif table == REPLICATION_TABLE:
            plan[table] = "predicate-controlled:externally-proven-certifier-watermark"
        elif table == "v51_synthetic_provenance":
            plan[table] = "protected:synthetic-provenance-contract-not-yet-proven-on-release"
        elif table == "risk_refresh_measurements":
            plan[table] = "protected:reader-dependency-not-yet-proven-disposable"
        elif table in PROTECTED_TABLES:
            plan[table] = "protected:current-authority-or-lineage"
        else:
            plan[table] = "protected:unknown-or-not-yet-proven-disposable"
    return plan


def _paper_checkpoint_snapshot(
    db: sqlite3.Connection, schema: dict[str, base.TableShape]
) -> dict[str, Any] | None:
    table = "paper_engine_checkpoint"
    if table not in schema:
        return None
    base._required_columns(
        schema,
        table,
        ("id", "saved_at", "last_engine_event_id", "state_json", "state_sha256"),
    )
    rows = db.execute(
        "SELECT id,saved_at,last_engine_event_id,state_json,state_sha256 "
        "FROM paper_engine_checkpoint ORDER BY id"
    ).fetchall()
    result: list[dict[str, Any]] = []
    for row in rows:
        raw = str(row[3])
        expected = str(row[4])
        if hashlib.sha256(raw.encode()).hexdigest() != expected:
            raise CleanupBlocked("paper-engine checkpoint digest is invalid before cleanup")
        event_id = int(row[2])
        marker = None
        if event_id:
            if "events" not in schema:
                raise CleanupBlocked("paper-engine checkpoint references missing event ledger")
            event = db.execute(
                "SELECT id,event_type,lineage_hash FROM events WHERE id=?", (event_id,)
            ).fetchone()
            if event is None:
                raise CleanupBlocked("paper-engine checkpoint event marker is missing")
            marker = [int(event[0]), str(event[1]), str(event[2])]
        result.append(
            {
                "id": int(row[0]),
                "saved_at": str(row[1]),
                "last_engine_event_id": event_id,
                "state_sha256": expected,
                "event_marker": marker,
            }
        )
    return {"rows": result}


def _event_head_snapshot(
    db: sqlite3.Connection, schema: dict[str, base.TableShape]
) -> dict[str, Any] | None:
    if "events" not in schema:
        return None
    base._required_columns(
        schema,
        "events",
        ("id", "event_type", "observed_at", "payload_json", "previous_hash", "lineage_hash"),
    )
    counts = db.execute("SELECT COUNT(*),MIN(id),MAX(id) FROM events").fetchone()
    head = db.execute(
        "SELECT id,event_type,observed_at,previous_hash,lineage_hash "
        "FROM events ORDER BY id DESC LIMIT 1"
    ).fetchone()
    return {
        "count": int(counts[0] or 0),
        "min_id": int(counts[1]) if counts[1] is not None else None,
        "max_id": int(counts[2]) if counts[2] is not None else None,
        "head": (
            [int(head[0]), str(head[1]), str(head[2]), head[3], str(head[4])]
            if head is not None
            else None
        ),
    }


def _protection_snapshot(
    db: sqlite3.Connection,
    schema: dict[str, base.TableShape],
    plan: dict[str, str],
) -> dict[str, Any]:
    protected_rows = {
        table: int(shape.rows)
        for table, shape in schema.items()
        if str(plan.get(table, "")).startswith("protected:")
    }
    return {
        "structural_schema_sha256": _structural_schema_fingerprint(schema),
        "protected_row_counts": protected_rows,
        "paper_engine_checkpoint": _paper_checkpoint_snapshot(db, schema),
        "event_ledger_head": _event_head_snapshot(db, schema),
    }


def _assert_protection_unchanged(before: dict[str, Any], after: dict[str, Any]) -> None:
    for key in (
        "structural_schema_sha256",
        "protected_row_counts",
        "paper_engine_checkpoint",
        "event_ledger_head",
    ):
        if before.get(key) != after.get(key):
            raise CleanupBlocked(f"protected production invariant changed during cleanup: {key}")


def _replication_sequence_head(
    db: sqlite3.Connection, schema: dict[str, base.TableShape]
) -> int:
    if REPLICATION_TABLE not in schema:
        return 0
    base._required_columns(schema, REPLICATION_TABLE, REPLICATION_COLUMNS)
    inbound = base._inbound_foreign_keys(schema, REPLICATION_TABLE)
    if inbound:
        raise CleanupBlocked(f"replication journal has unexpected inbound foreign keys: {inbound}")
    sequence_row = db.execute(
        "SELECT seq FROM sqlite_sequence WHERE name=?", (REPLICATION_TABLE,)
    ).fetchone()
    sequence = int(sequence_row[0]) if sequence_row is not None else 0
    if sequence < 0:
        raise CleanupBlocked("replication AUTOINCREMENT sequence cannot be negative")
    max_row = db.execute(
        f"SELECT MAX(id) FROM {base._quote_identifier(REPLICATION_TABLE)}"
    ).fetchone()
    existing_max = int(max_row[0] or 0)
    if existing_max > sequence:
        raise CleanupBlocked(
            f"replication journal max id {existing_max} exceeds AUTOINCREMENT sequence {sequence}"
        )
    unexpected = db.execute(
        f"SELECT COUNT(*) FROM {base._quote_identifier(REPLICATION_TABLE)} "
        "WHERE operation<>'UPSERT'"
    ).fetchone()
    if int(unexpected[0] or 0) != 0:
        raise CleanupBlocked("replication journal contains unsupported operations")
    return sequence


def _delete_acknowledged_replication(
    db: sqlite3.Connection,
    schema: dict[str, base.TableShape],
    *,
    acknowledged_watermark: int | None,
    batch_size: int = BATCH_SIZE,
) -> dict[str, Any]:
    if REPLICATION_TABLE not in schema:
        return {
            "status": "table_absent",
            "deleted": 0,
            "acknowledged_watermark": acknowledged_watermark,
            "sequence_before": 0,
            "sequence_after": 0,
        }
    sequence_before = _replication_sequence_head(db, schema)
    if acknowledged_watermark is None:
        return {
            "status": "protected_no_acknowledged_watermark",
            "deleted": 0,
            "acknowledged_watermark": None,
            "sequence_before": sequence_before,
            "sequence_after": sequence_before,
        }
    try:
        acknowledged = int(acknowledged_watermark)
    except (TypeError, ValueError) as exc:
        raise CleanupBlocked("acknowledged certifier watermark is not an integer") from exc
    if acknowledged < 0:
        raise CleanupBlocked("acknowledged certifier watermark cannot be negative")
    if acknowledged > sequence_before:
        raise CleanupBlocked(
            f"acknowledged certifier watermark {acknowledged} exceeds source AUTOINCREMENT sequence {sequence_before}"
        )
    if batch_size <= 0:
        raise CleanupBlocked("replication deletion batch size must be positive")

    quoted = base._quote_identifier(REPLICATION_TABLE)
    total = 0
    while True:
        cursor = db.execute(
            f"DELETE FROM {quoted} WHERE id IN ("
            f"SELECT id FROM {quoted} WHERE id<=? ORDER BY id LIMIT ?)",
            (acknowledged, int(batch_size)),
        )
        deleted = int(cursor.rowcount if cursor.rowcount >= 0 else 0)
        db.commit()
        total += deleted
        if deleted < batch_size:
            break

    sequence_after = _replication_sequence_head(db, schema)
    if sequence_after != sequence_before:
        raise CleanupBlocked(
            "replication journal AUTOINCREMENT sequence changed during pruning"
        )
    remaining = db.execute(
        f"SELECT COUNT(*),MIN(id),MAX(id) FROM {quoted}"
    ).fetchone()
    stale = db.execute(
        f"SELECT COUNT(*) FROM {quoted} WHERE id<=?", (acknowledged,)
    ).fetchone()
    if int(stale[0] or 0) != 0:
        raise CleanupBlocked("acknowledged replication rows remain after bounded pruning")
    return {
        "status": "pruned_to_externally_proven_certifier_watermark",
        "deleted": total,
        "acknowledged_watermark": acknowledged,
        "sequence_before": sequence_before,
        "sequence_after": sequence_after,
        "remaining_rows": int(remaining[0] or 0),
        "remaining_min_id": int(remaining[1]) if remaining[1] is not None else None,
        "remaining_max_id": int(remaining[2]) if remaining[2] is not None else None,
        "bounded_batch_size": int(batch_size),
    }


def _unproven_categories(schema: dict[str, base.TableShape]) -> dict[str, Any]:
    return {
        "synthetic_candidate_history": {
            "status": "protected_pending_exact_provenance_contract",
            "provenance_table_present": "v51_synthetic_provenance" in schema,
            "deleted": 0,
        },
        "risk_refresh_measurements": {
            "status": "protected_pending_reader_dependency_proof",
            "table_present": "risk_refresh_measurements" in schema,
            "deleted": 0,
        },
    }


def _deleted_total(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return max(0, value)
    if isinstance(value, dict):
        return sum(_deleted_total(item) for item in value.values())
    return 0


def execute_cleanup(
    database_path: Path,
    *,
    role: str,
    run_id: str,
    acknowledged_watermark: int | None = None,
    telemetry_hours: float = 24.0,
) -> dict[str, Any]:
    del telemetry_hours  # retained for environment/CLI compatibility; V4 does not delete this telemetry.
    role = role.strip().lower()
    if role not in {"authoritative", "certifier"}:
        raise CleanupBlocked("cleanup role must be authoritative or certifier")
    run_id = base._validate_run_id(run_id)
    database_path = Path(database_path).resolve()
    if not database_path.exists() or not database_path.is_file() or database_path.is_symlink():
        raise CleanupBlocked(f"database path is not a regular file: {database_path}")

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
        "started_at": base._utcnow(),
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }
    base._atomic_json(report_path, report)

    db = base._connect(database_path)
    try:
        # The outer runtime owns the persistent-disk flock. BEGIN IMMEDIATE is an
        # independent SQLite-level assertion that no uncoordinated writer remains.
        db.execute("BEGIN IMMEDIATE")
        db.commit()
        before = base._measure(database_path, db)
        if not before["integrity"]["ok"]:
            raise CleanupBlocked("pre-cleanup SQLite integrity/foreign-key check failed")
        schema = base.extract_schema(db)
        plan = _protected_plan(schema, role=role)
        protected_before = _protection_snapshot(db, schema, plan)
        certifier_state_before = _read_certifier_state(database_path) if role == "certifier" else None

        report["protected_set"] = plan
        report["protection_before"] = protected_before
        report["certifier_replica_state_before"] = certifier_state_before
        report["durable_source_watermark"] = (
            int(certifier_state_before["watermark"])
            if certifier_state_before is not None
            else None
        )
        report["before"] = before
        report["unproven_categories"] = _unproven_categories(schema)
        base._atomic_json(report_path, report)

        if role == "certifier":
            replication = {
                "status": "protected_certifier_replica",
                "deleted": 0,
                "acknowledged_watermark": None,
            }
        else:
            replication = _delete_acknowledged_replication(
                db,
                schema,
                acknowledged_watermark=acknowledged_watermark,
            )

        report["deleted_rows"] = {
            "certification_replication_changes": replication,
            "synthetic_candidate_history": 0,
            "risk_refresh_measurements": 0,
        }
        report["deleted_rows_total"] = _deleted_total(report["deleted_rows"])
        report["externally_proven_certifier_acknowledged_watermark"] = acknowledged_watermark
        report["orphan_files"] = cleanup_stale_certification_exports(database_path)
        base._atomic_json(report_path, report)

        compaction = base._compact(database_path, db)
        after = base._measure(database_path, db)
        if not after["integrity"]["ok"]:
            raise CleanupBlocked("post-cleanup SQLite integrity/foreign-key check failed")
        schema_after = base.extract_schema(db)
        plan_after = _protected_plan(schema_after, role=role)
        if plan_after != plan:
            raise CleanupBlocked("table protection plan changed during cleanup")
        protected_after = _protection_snapshot(db, schema_after, plan_after)
        _assert_protection_unchanged(protected_before, protected_after)

        certifier_state_after = _read_certifier_state(database_path) if role == "certifier" else None
        if certifier_state_before != certifier_state_after:
            raise CleanupBlocked("certifier durable replica state changed during cleanup")

        report["compaction"] = compaction
        report["after"] = after
        report["protection_after"] = protected_after
        report["certifier_replica_state_after"] = certifier_state_after
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
        report["completed_at"] = base._utcnow()
        base._atomic_json(report_path, report)
        return report
    except Exception as exc:
        try:
            db.rollback()
        except Exception:
            pass
        report["status"] = "blocked"
        report["completed_at"] = base._utcnow()
        report["error"] = f"{type(exc).__name__}:{exc}"
        base._atomic_json(report_path, report)
        raise
    finally:
        db.close()


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Fail-closed one-shot production SQLite cleanup v4")
    parser.add_argument(
        "--database",
        default=os.getenv("SOLANA_ROI_DATABASE_PATH") or os.getenv("SOLANA_ROI_DB_PATH"),
    )
    parser.add_argument("--role", default=os.getenv(ROLE_ENV, ""))
    parser.add_argument("--run-id", default=os.getenv(RUN_ID_ENV, ""))
    parser.add_argument(
        "--ack-watermark",
        type=int,
        default=(int(os.environ[ACK_WATERMARK_ENV]) if os.getenv(ACK_WATERMARK_ENV) else None),
    )
    parser.add_argument("--telemetry-hours", type=float, default=float(os.getenv(TELEMETRY_HOURS_ENV, "24")))
    args = parser.parse_args(argv)
    if not base._env_true(ENABLED_ENV):
        print(json.dumps({"version": CLEANUP_VERSION, "status": "disabled", "enabled": False}, sort_keys=True))
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
    raise SystemExit(main())


__all__ = [
    "ACK_WATERMARK_ENV",
    "BATCH_SIZE",
    "CLEANUP_VERSION",
    "CleanupBlocked",
    "ENABLED_ENV",
    "REPLICATION_TABLE",
    "ROLE_ENV",
    "RUN_ID_ENV",
    "TELEMETRY_HOURS_ENV",
    "_delete_acknowledged_replication",
    "_read_certifier_state",
    "_replication_sequence_head",
    "execute_cleanup",
    "main",
]
