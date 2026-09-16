from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from .active_storage import payload_hash
from .config import BASELINE
from .storage_current_v52_reconciliation import augment_current_state_truth
from .storage_runtime_persistence_reconciliation import augment_runtime_current_state_truth


TERMINAL_CANDIDATE_STATES = {
    "closed","expired","rejected","settled","failed","complete","completed","cancelled","canceled","terminal"
}
_ENGINE_EVENT_TYPES = ("first_touch", "confirmation", "price", "trade_intent", "trade_outcome")

# Current-state tables are read in a bounded way.  If a table that is expected
# to be current state has grown beyond its contract, extraction fails closed
# instead of silently turning migration into a whole-history scan.
SECTION_TABLES: dict[str, tuple[tuple[str,int],...]] = {
    "strategy": (
        ("strategy_controls", 256),
        ("v52_lane_gate_state", 128),
        ("v52_wallet_forward_validation", 256),
        ("v51_release_compatibility", 4096),
        ("v52_tournament_exact_evidence", 100_000),
        ("candidate_execution_plane_snapshots", 100_000),
    ),
    "wallet": (
        ("wallet_profiles", 250_000),
        ("wallet_performance", 250_000),
        ("wallet_scorecard", 250_000),
        ("wallet_universe_state", 4096),
        ("wallet_seed_source", 4096),
        ("wallet_seed_candidates", 250_000),
        ("wallet_seed_eligibility", 250_000),
        ("wallet_runtime_state", 250_000),
    ),
    "provider_source": (
        ("provider_health", 4096),
        ("provider_runtime_state", 4096),
        ("system_status", 256),
        ("system_runtime_state", 4096),
        ("discovery_universe_state", 4096),
        ("stream_cursor_state", 16_384),
    ),
    "active_candidates": (("discovery_candidates", 250_000),),
    "active_lifecycles": (("candidate_lifecycle", 250_000),),
    "continuity": (
        ("continuity_epoch", 4096),
        ("graceful_shutdown_state", 128),
    ),
    "certification": (
        ("certification_release", 128),
        ("certification_state", 4096),
        ("replication_handshake", 128),
        ("replication_handshake_ack", 128),
        ("parity_check", 256),
        ("certification_cycle_evidence", 1024),
        ("certification_release_evidence", 1024),
    ),
    "replication_watermarks": (("replication_watermark", 4096),),
}


@dataclass(frozen=True)
class ExtractionResult:
    truth: dict[str, Any]
    source_path: str
    source_size_bytes: int
    schema_fingerprint: str
    table_row_counts: dict[str,int]


def _connect_ro(path: Path) -> sqlite3.Connection:
    if not path.is_file():
        raise RuntimeError(f"legacy database unavailable: {path}")
    uri = f"file:{path.resolve()}?mode=ro&immutable=0&cache=private"
    conn = sqlite3.connect(uri, uri=True, timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def _release_source_file_cache(path: Path) -> None:
    """Best-effort release of legacy pages touched by one-shot migration reads."""
    fadvise = getattr(os, "posix_fadvise", None)
    advice = getattr(os, "POSIX_FADV_DONTNEED", None)
    if fadvise is None or advice is None:
        return
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        try:
            fadvise(fd, 0, 0, advice)
        except OSError:
            pass
    finally:
        os.close(fd)


def _tables(conn: sqlite3.Connection) -> set[str]:
    return {str(row[0]) for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")}


def _columns(conn: sqlite3.Connection, table: str) -> list[str]:
    return [str(row[1]) for row in conn.execute(f'PRAGMA table_info("{table}")')]


def _pk_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    rows = list(conn.execute(f'PRAGMA table_info("{table}")'))
    return [str(row[1]) for row in sorted((r for r in rows if int(r[5]) > 0), key=lambda r:int(r[5]))]


def _row_dict(row: sqlite3.Row) -> dict[str,Any]:
    out: dict[str,Any] = {}
    for key in row.keys():
        value = row[key]
        if isinstance(value, bytes):
            out[str(key)] = {"hex": value.hex()}
        else:
            out[str(key)] = value
    return out


def _bounded_rows(conn: sqlite3.Connection, table: str, max_rows: int, *, newest_first: bool = False) -> list[dict[str,Any]]:
    cols = _columns(conn, table)
    if not cols:
        return []
    pk = _pk_columns(conn, table)
    order = pk or (["id"] if "id" in cols else [])
    sql = f'SELECT * FROM "{table}"'
    if order:
        sql += " ORDER BY " + ",".join(f'"{name}"' for name in order) + (" DESC" if newest_first and len(order)==1 else "")
    sql += " LIMIT ?"
    rows = list(conn.execute(sql, (int(max_rows)+1,)))
    if len(rows) > max_rows:
        raise RuntimeError(f"current-state extraction blocked: {table} exceeds bounded row contract {max_rows}")
    return [_row_dict(row) for row in rows]


def _latest_rows(conn: sqlite3.Connection, table: str, limit: int) -> list[dict[str,Any]]:
    cols = _columns(conn, table)
    order = "id" if "id" in cols else ("rowid" if table not in {"sqlite_sequence"} else "")
    if order:
        rows = conn.execute(f'SELECT * FROM "{table}" ORDER BY {order} DESC LIMIT ?', (int(limit),)).fetchall()
    else:
        rows = conn.execute(f'SELECT * FROM "{table}" LIMIT ?', (int(limit),)).fetchall()
    return [_row_dict(row) for row in rows]


def _active_rows(conn: sqlite3.Connection, table: str, max_rows: int) -> list[dict[str,Any]]:
    cols = set(_columns(conn, table))
    state_col = next((name for name in ("status","state","lifecycle_state","candidate_state") if name in cols), None)
    if state_col is None:
        return _bounded_rows(conn, table, max_rows)
    placeholders = ",".join("?" for _ in TERMINAL_CANDIDATE_STATES)
    sql = f'SELECT * FROM "{table}" WHERE lower(CAST("{state_col}" AS TEXT)) NOT IN ({placeholders}) LIMIT ?'
    rows = list(conn.execute(sql, (*sorted(TERMINAL_CANDIDATE_STATES), int(max_rows)+1)))
    if len(rows) > max_rows:
        raise RuntimeError(f"active-state extraction blocked: {table} exceeds {max_rows} unresolved rows")
    return [_row_dict(row) for row in rows]


def _genesis_paper_state() -> dict[str, Any]:
    """Exact state DurablePaperTradingEngine uses before its first engine event."""
    return {
        "schema": "roi-convergence-paper-engine-checkpoint.v1",
        "strategy_version": BASELINE.version,
        "initial_capital_usd": BASELINE.initial_capital_usd,
        "cash_usd": BASELINE.initial_capital_usd,
        "marks": {},
        "trade_start_nav": {},
        "candidates": {},
        "positions": {},
        "closed": [],
    }


def _latest_engine_event(conn: sqlite3.Connection, tables: set[str]) -> sqlite3.Row | None:
    if "events" not in tables:
        raise RuntimeError("migration blocked: events ledger missing")
    placeholders = ",".join("?" for _ in _ENGINE_EVENT_TYPES)
    # This is the same authority boundary used by DurablePaperTradingEngine.  It
    # selects only the event PK/type columns and never materializes history.
    return conn.execute(
        f"SELECT id,event_type FROM events WHERE event_type IN ({placeholders}) ORDER BY id DESC LIMIT 1",
        _ENGINE_EVENT_TYPES,
    ).fetchone()


def _extract_paper_checkpoint(conn: sqlite3.Connection, tables: set[str]) -> dict[str,Any]:
    if "paper_engine_checkpoint" not in tables:
        raise RuntimeError("migration blocked: paper_engine_checkpoint missing")
    row = conn.execute("SELECT saved_at,last_engine_event_id,state_json,state_sha256 FROM paper_engine_checkpoint WHERE id=1").fetchone()
    if row is None:
        engine_event = _latest_engine_event(conn, tables)
        if engine_event is not None:
            raise RuntimeError(
                "migration blocked: engine history exists without a durable checkpoint:"
                f"{int(engine_event['id'])}:{str(engine_event['event_type'])}"
            )
        state = _genesis_paper_state()
        raw = json.dumps(state, sort_keys=True, separators=(",", ":"), allow_nan=False)
        digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
        return {
            "saved_at": None,
            "last_engine_event_id": 0,
            "state_sha256": digest,
            "state": state,
            "source_checkpoint_present": False,
            "genesis_materialized": True,
        }
    raw = str(row["state_json"])
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    if digest != str(row["state_sha256"]):
        raise RuntimeError("migration blocked: paper_engine_checkpoint digest mismatch")
    try:
        state = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError("migration blocked: paper_engine_checkpoint JSON invalid") from exc
    if not isinstance(state, dict) or state.get("schema") != "roi-convergence-paper-engine-checkpoint.v1":
        raise RuntimeError("migration blocked: unsupported paper_engine_checkpoint schema")
    return {
        "saved_at": str(row["saved_at"]),
        "last_engine_event_id": int(row["last_engine_event_id"]),
        "state_sha256": digest,
        "state": state,
    }


def _extract_event_heads(conn: sqlite3.Connection, tables: set[str], paper: Mapping[str,Any]) -> dict[str,Any]:
    result: dict[str,Any] = {"paper_engine_event_id": int(paper["last_engine_event_id"])}
    if "events" in tables:
        row = conn.execute("SELECT id,event_type,observed_at,lineage_hash,previous_hash FROM events ORDER BY id DESC LIMIT 1").fetchone()
        if row is not None:
            result["events"] = _row_dict(row)
    if "observation_events" in tables:
        cols = set(_columns(conn,"observation_events"))
        wanted = [name for name in ("id","event_id","kind","source","source_event_id","lineage_hash") if name in cols]
        if wanted:
            order = "id" if "id" in cols else wanted[0]
            row = conn.execute(f'SELECT {",".join(wanted)} FROM observation_events ORDER BY {order} DESC LIMIT 1').fetchone()
            if row is not None:
                result["observation_events"] = _row_dict(row)
    if "incremental_event_integrity_repair_state" in tables:
        result["integrity_repair_state"] = _latest_rows(conn,"incremental_event_integrity_repair_state",1)
    return result


def _extract_freshness(conn: sqlite3.Connection, tables: set[str]) -> dict[str,Any]:
    result: dict[str,Any] = {}
    for table in ("market_quotes","risk_refresh_measurements","price_marks"):
        if table not in tables:
            continue
        cols = set(_columns(conn,table))
        time_col = next((c for c in ("received_at","observed_at","completed_at","updated_at") if c in cols), None)
        if time_col:
            row = conn.execute(f'SELECT MAX("{time_col}") AS max_time FROM "{table}"').fetchone()
            result[table] = {"max_time": row[0] if row is not None else None}
    return result


def _schema_fingerprint(conn: sqlite3.Connection) -> str:
    rows = conn.execute("SELECT type,name,tbl_name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name").fetchall()
    payload = [[row[0],row[1],row[2],row[3]] for row in rows]
    return payload_hash(payload)


class LegacyCurrentStateExtractor:
    """Read-only, fail-closed extractor for the state needed to resume production.

    Historical event/swap/risk bodies are never copied wholesale.  Portfolio
    truth comes from the durable checkpoint, except for the one exact state the
    durable engine itself permits without a checkpoint: genesis with no engine
    event history.
    """

    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)

    def extract(self) -> ExtractionResult:
        conn = _connect_ro(self.path)
        try:
            conn.execute("BEGIN")
            tables = _tables(conn)
            paper = _extract_paper_checkpoint(conn,tables)
            counts: dict[str,int] = {}
            truth: dict[str,Any] = {}
            for section, specs in SECTION_TABLES.items():
                section_payload: dict[str,Any] = {}
                for table,max_rows in specs:
                    if table not in tables:
                        continue
                    if section in {"active_candidates","active_lifecycles"}:
                        rows = _active_rows(conn,table,max_rows)
                    elif section == "certification" and table in {"certification_release_evidence","certification_cycle_evidence","parity_check"}:
                        rows = _latest_rows(conn,table,min(max_rows,1024))
                    else:
                        rows = _bounded_rows(conn,table,max_rows)
                    section_payload[table] = rows
                    counts[table] = len(rows)
                truth[section] = section_payload
            augment_current_state_truth(conn, tables, truth, counts)
            augment_runtime_current_state_truth(conn, tables, truth, counts)
            truth["portfolio"] = paper
            truth["latest_event_ids"] = _extract_event_heads(conn,tables,paper)
            truth["freshness"] = _extract_freshness(conn,tables)
            # Preserve the exact wallet evidence frontier separately so a future
            # wallet score cannot silently reuse data beyond the transition.
            truth["wallet_evidence_watermarks"] = {
                "stream_cursor_state": truth.get("provider_source",{}).get("stream_cursor_state",[]),
                "event_head": truth["latest_event_ids"],
            }
            required = {
                "strategy","wallet","wallet_evidence_watermarks","provider_source","freshness",
                "latest_event_ids","active_candidates","active_lifecycles","portfolio",
                "replication_watermarks","certification","continuity",
            }
            if set(truth) != required:
                missing = sorted(required-set(truth)); extra = sorted(set(truth)-required)
                raise RuntimeError(f"current-state extractor shape invalid missing={missing} extra={extra}")
            conn.execute("COMMIT")
            return ExtractionResult(
                truth=truth,
                source_path=str(self.path),
                source_size_bytes=int(self.path.stat().st_size),
                schema_fingerprint=_schema_fingerprint(conn),
                table_row_counts=counts,
            )
        finally:
            conn.close()
            _release_source_file_cache(self.path)


def exact_truth_hash(truth: Mapping[str,Any]) -> str:
    return payload_hash(truth)
