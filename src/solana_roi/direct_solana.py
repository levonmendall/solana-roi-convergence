from __future__ import annotations

import asyncio
import hashlib
import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

import httpx
import websockets

from .deployment import FROZEN_PROGRAM_ADDRESSES
from .direct_transaction import normalize_standard_transaction
from .ingestion import NormalizedSwap
from .solana_rpc import RpcEndpoint, SolanaRpcPool, rpc_endpoints_from_env
from .source_coverage import FROZEN_SUPPORTED_PROGRAM_IDS_BY_SOURCE


PROGRAM_SOURCE_BY_ID: dict[str, str] = {
    program_id: source
    for source, program_ids in FROZEN_SUPPORTED_PROGRAM_IDS_BY_SOURCE
    for program_id in program_ids
}


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes"}


@dataclass(frozen=True, slots=True)
class WatchTarget:
    kind: str
    address: str
    source_hint: str | None


class DirectSolanaJournal:
    """Compact durable full-market receipt journal and hydration queue."""

    def __init__(self, store: Any, *, raw_retention_seconds: float = 900.0):
        self.store = store
        self.raw_retention_seconds = max(120.0, float(raw_retention_seconds))
        now = utcnow().isoformat()
        with store._lock, store.db:
            store.db.execute(
                "CREATE TABLE IF NOT EXISTS direct_solana_recent_receipts ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, signature TEXT NOT NULL, source_key TEXT NOT NULL, "
                "slot INTEGER NOT NULL, received_at TEXT NOT NULL, launch_like INTEGER NOT NULL, "
                "expires_at TEXT NOT NULL, UNIQUE(signature, source_key))"
            )
            store.db.execute(
                "CREATE INDEX IF NOT EXISTS ix_direct_recent_source_received "
                "ON direct_solana_recent_receipts(source_key, received_at)"
            )
            store.db.execute(
                "CREATE TABLE IF NOT EXISTS direct_solana_minute_receipts ("
                "bucket TEXT NOT NULL, source TEXT NOT NULL, receipt_count INTEGER NOT NULL, "
                "first_received_at TEXT NOT NULL, last_received_at TEXT NOT NULL, last_slot INTEGER NOT NULL, "
                "rolling_sha256 TEXT NOT NULL, PRIMARY KEY(bucket, source))"
            )
            store.db.execute(
                "CREATE TABLE IF NOT EXISTS direct_solana_hydration_queue ("
                "signature TEXT PRIMARY KEY, slot INTEGER NOT NULL, trigger_received_at TEXT NOT NULL, "
                "source_hint TEXT, priority INTEGER NOT NULL, reason TEXT NOT NULL, status TEXT NOT NULL, "
                "attempts INTEGER NOT NULL DEFAULT 0, last_error TEXT, updated_at TEXT NOT NULL)"
            )
            store.db.execute(
                "CREATE INDEX IF NOT EXISTS ix_direct_hydration_pending "
                "ON direct_solana_hydration_queue(status, priority, updated_at)"
            )
            store.db.execute(
                "CREATE TABLE IF NOT EXISTS direct_solana_hydration_metrics ("
                "signature TEXT PRIMARY KEY, source TEXT, trigger_received_at TEXT NOT NULL, "
                "hydrated_at TEXT NOT NULL, rpc_provider TEXT, rpc_latency_ms REAL, total_hydration_ms REAL NOT NULL, "
                "normalized INTEGER NOT NULL, candidate_context_prefilled INTEGER NOT NULL DEFAULT 0, "
                "historical_recovery INTEGER NOT NULL DEFAULT 0)"
            )
            columns = {
                str(row["name"])
                for row in store.db.execute("PRAGMA table_info(direct_solana_hydration_metrics)").fetchall()
            }
            if "historical_recovery" not in columns:
                store.db.execute(
                    "ALTER TABLE direct_solana_hydration_metrics ADD COLUMN historical_recovery INTEGER NOT NULL DEFAULT 0"
                )
            store.db.execute(
                "CREATE TABLE IF NOT EXISTS direct_solana_provider_state ("
                "provider TEXT PRIMARY KEY, connected INTEGER NOT NULL, connected_at TEXT, last_message_at TEXT, "
                "reconnect_count INTEGER NOT NULL DEFAULT 0, last_error_type TEXT)"
            )
            store.db.execute(
                "CREATE TABLE IF NOT EXISTS direct_solana_global_state ("
                "id INTEGER PRIMARY KEY CHECK(id=1), outage_started_at TEXT, unresolved_gap INTEGER NOT NULL DEFAULT 0, "
                "last_backfill_complete_at TEXT, last_backfill_error TEXT)"
            )
            store.db.execute(
                "INSERT OR IGNORE INTO direct_solana_global_state(id, unresolved_gap) VALUES (1, 0)"
            )
            store.db.execute("UPDATE direct_solana_provider_state SET connected=0")
            store.db.execute(
                "UPDATE direct_solana_hydration_queue SET status='pending', updated_at=? WHERE status='processing'",
                (now,),
            )
        self._receipt_inserts = 0

    def record_receipt(self, *, signature: str, source_key: str, slot: int, received_at: datetime, launch_like: bool) -> bool:
        expires_at = received_at + timedelta(seconds=self.raw_retention_seconds)
        with self.store._lock, self.store.db:
            cur = self.store.db.execute(
                "INSERT OR IGNORE INTO direct_solana_recent_receipts("
                "signature, source_key, slot, received_at, launch_like, expires_at) VALUES (?, ?, ?, ?, ?, ?)",
                (signature, source_key, int(slot), received_at.isoformat(), 1 if launch_like else 0, expires_at.isoformat()),
            )
            inserted = cur.rowcount == 1
            if inserted and source_key in {"PUMP_FUN", "PUMP_AMM", "RAYDIUM"}:
                bucket = received_at.replace(second=0, microsecond=0).isoformat()
                row = self.store.db.execute(
                    "SELECT receipt_count, rolling_sha256 FROM direct_solana_minute_receipts WHERE bucket=? AND source=?",
                    (bucket, source_key),
                ).fetchone()
                previous = str(row["rolling_sha256"]) if row is not None else ""
                digest = hashlib.sha256(
                    f"{previous}|{signature}|{int(slot)}|{received_at.isoformat()}".encode("utf-8")
                ).hexdigest()
                if row is None:
                    self.store.db.execute(
                        "INSERT INTO direct_solana_minute_receipts("
                        "bucket, source, receipt_count, first_received_at, last_received_at, last_slot, rolling_sha256) "
                        "VALUES (?, ?, 1, ?, ?, ?, ?)",
                        (bucket, source_key, received_at.isoformat(), received_at.isoformat(), int(slot), digest),
                    )
                else:
                    self.store.db.execute(
                        "UPDATE direct_solana_minute_receipts SET receipt_count=receipt_count+1, "
                        "last_received_at=?, last_slot=?, rolling_sha256=? WHERE bucket=? AND source=?",
                        (received_at.isoformat(), int(slot), digest, bucket, source_key),
                    )
            if inserted:
                self._receipt_inserts += 1
                if self._receipt_inserts % 500 == 0:
                    self.store.db.execute(
                        "DELETE FROM direct_solana_recent_receipts WHERE expires_at<?", (received_at.isoformat(),)
                    )
        return inserted

    def enqueue(self, *, signature: str, slot: int, trigger_received_at: datetime, source_hint: str | None, priority: int, reason: str) -> None:
        now = utcnow().isoformat()
        with self.store._lock, self.store.db:
            row = self.store.db.execute(
                "SELECT priority, source_hint, status FROM direct_solana_hydration_queue WHERE signature=?", (signature,)
            ).fetchone()
            if row is None:
                self.store.db.execute(
                    "INSERT INTO direct_solana_hydration_queue("
                    "signature, slot, trigger_received_at, source_hint, priority, reason, status, attempts, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, 'pending', 0, ?)",
                    (signature, int(slot), trigger_received_at.isoformat(), source_hint, int(priority), reason, now),
                )
                return
            new_priority = min(int(row["priority"]), int(priority))
            existing_hint = str(row["source_hint"] or "") or None
            status = str(row["status"])
            self.store.db.execute(
                "UPDATE direct_solana_hydration_queue SET source_hint=?, priority=?, reason=?, status=?, updated_at=? "
                "WHERE signature=?",
                (
                    existing_hint or source_hint,
                    new_priority,
                    reason,
                    "pending" if status == "failed" and priority <= 2 else status,
                    now,
                    signature,
                ),
            )

    def claim(self) -> dict[str, Any] | None:
        now = utcnow().isoformat()
        with self.store._lock, self.store.db:
            row = self.store.db.execute(
                "SELECT signature, slot, trigger_received_at, source_hint, priority, reason, attempts "
                "FROM direct_solana_hydration_queue WHERE status='pending' "
                "ORDER BY priority, updated_at, signature LIMIT 1"
            ).fetchone()
            if row is None:
                return None
            self.store.db.execute(
                "UPDATE direct_solana_hydration_queue SET status='processing', attempts=attempts+1, updated_at=? "
                "WHERE signature=? AND status='pending'", (now, str(row["signature"]))
            )
            return dict(row)

    def finish(self, signature: str, *, error: str | None = None, retry: bool = False) -> None:
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "UPDATE direct_solana_hydration_queue SET status=?, last_error=?, updated_at=? WHERE signature=?",
                ("pending" if retry else ("failed" if error else "complete"), error, utcnow().isoformat(), signature),
            )

    def record_hydration(
        self, *, signature: str, source: str | None, trigger_received_at: datetime, hydrated_at: datetime,
        rpc_provider: str | None, rpc_latency_ms: float | None, normalized: bool,
        candidate_context_prefilled: bool = False, historical_recovery: bool = False,
    ) -> None:
        total_ms = max(0.0, (hydrated_at - trigger_received_at).total_seconds() * 1000.0)
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "INSERT INTO direct_solana_hydration_metrics("
                "signature, source, trigger_received_at, hydrated_at, rpc_provider, rpc_latency_ms, total_hydration_ms, "
                "normalized, candidate_context_prefilled, historical_recovery) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(signature) DO UPDATE SET source=excluded.source, hydrated_at=excluded.hydrated_at, "
                "rpc_provider=excluded.rpc_provider, rpc_latency_ms=excluded.rpc_latency_ms, "
                "total_hydration_ms=excluded.total_hydration_ms, normalized=excluded.normalized, "
                "candidate_context_prefilled=MAX(candidate_context_prefilled, excluded.candidate_context_prefilled), "
                "historical_recovery=MAX(historical_recovery, excluded.historical_recovery)",
                (signature, source, trigger_received_at.isoformat(), hydrated_at.isoformat(), rpc_provider, rpc_latency_ms,
                 total_ms, 1 if normalized else 0, 1 if candidate_context_prefilled else 0, 1 if historical_recovery else 0),
            )

    def recent_source_signatures(self, source: str, *, start: datetime, end: datetime, exclude_signature: str | None = None, limit: int = 600) -> list[dict[str, Any]]:
        sql = "SELECT signature, slot, received_at FROM direct_solana_recent_receipts WHERE source_key=? AND received_at>=? AND received_at<=?"
        args: list[Any] = [source, start.isoformat(), end.isoformat()]
        if exclude_signature:
            sql += " AND signature<>?"
            args.append(exclude_signature)
        sql += " ORDER BY received_at, id LIMIT ?"
        args.append(max(1, int(limit)))
        with self.store._lock:
            rows = self.store.db.execute(sql, tuple(args)).fetchall()
        return [dict(row) for row in rows]

    def set_provider(self, provider: str, *, connected: bool, error_type: str | None = None) -> None:
        now = utcnow().isoformat()
        with self.store._lock, self.store.db:
            row = self.store.db.execute(
                "SELECT connected, reconnect_count FROM direct_solana_provider_state WHERE provider=?", (provider,)
            ).fetchone()
            reconnects = int(row["reconnect_count"]) if row is not None else 0
            was_connected = bool(row["connected"]) if row is not None else False
            if connected and row is not None and not was_connected:
                reconnects += 1
            self.store.db.execute(
                "INSERT INTO direct_solana_provider_state("
                "provider, connected, connected_at, last_message_at, reconnect_count, last_error_type) "
                "VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(provider) DO UPDATE SET "
                "connected=excluded.connected, connected_at=CASE WHEN excluded.connected=1 THEN excluded.connected_at ELSE connected_at END, "
                "last_message_at=CASE WHEN excluded.connected=1 THEN excluded.last_message_at ELSE last_message_at END, "
                "reconnect_count=excluded.reconnect_count, last_error_type=excluded.last_error_type",
                (provider, 1 if connected else 0, now if connected else None, now if connected else None, reconnects, error_type),
            )

    def touch_provider(self, provider: str, received_at: datetime) -> None:
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "UPDATE direct_solana_provider_state SET last_message_at=? WHERE provider=?", (received_at.isoformat(), provider)
            )

    def connected_provider_count(self) -> int:
        with self.store._lock:
            row = self.store.db.execute("SELECT COUNT(*) AS n FROM direct_solana_provider_state WHERE connected=1").fetchone()
        return int(row["n"]) if row is not None else 0

    def mark_outage(self, started_at: datetime) -> None:
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "UPDATE direct_solana_global_state SET outage_started_at=COALESCE(outage_started_at, ?) WHERE id=1",
                (started_at.isoformat(),),
            )

    def outage_started_at(self) -> datetime | None:
        with self.store._lock:
            row = self.store.db.execute("SELECT outage_started_at FROM direct_solana_global_state WHERE id=1").fetchone()
        raw = str(row["outage_started_at"] or "") if row is not None else ""
        return datetime.fromisoformat(raw) if raw else None

    def close_outage(self, *, complete: bool, error: str | None = None) -> None:
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "UPDATE direct_solana_global_state SET outage_started_at=NULL, unresolved_gap=?, "
                "last_backfill_complete_at=?, last_backfill_error=? WHERE id=1",
                (0 if complete else 1, utcnow().isoformat() if complete else None, error),
            )

    def status(self) -> dict[str, Any]:
        now = utcnow()
        with self.store._lock:
            providers = [dict(row) for row in self.store.db.execute(
                "SELECT provider, connected, connected_at, last_message_at, reconnect_count, last_error_type FROM direct_solana_provider_state ORDER BY provider"
            ).fetchall()]
            global_row = self.store.db.execute(
                "SELECT outage_started_at, unresolved_gap, last_backfill_complete_at, last_backfill_error FROM direct_solana_global_state WHERE id=1"
            ).fetchone()
            queue_rows = self.store.db.execute("SELECT status, COUNT(*) AS n FROM direct_solana_hydration_queue GROUP BY status").fetchall()
            source_rows = self.store.db.execute(
                "SELECT source, SUM(receipt_count) AS n FROM direct_solana_minute_receipts WHERE bucket>=? GROUP BY source",
                ((now - timedelta(hours=1)).replace(second=0, microsecond=0).isoformat(),),
            ).fetchall()
            metrics = [dict(row) for row in self.store.db.execute(
                "SELECT total_hydration_ms, normalized FROM direct_solana_hydration_metrics WHERE historical_recovery=0 ORDER BY hydrated_at DESC LIMIT 500"
            ).fetchall()]
        values = sorted(float(row["total_hydration_ms"]) for row in metrics)
        p95 = values[min(len(values) - 1, int((len(values) - 1) * 0.95))] if values else None
        queue = {str(row["status"]): int(row["n"]) for row in queue_rows}
        sources = {str(row["source"]): int(row["n"]) for row in source_rows}
        unresolved = bool(global_row["unresolved_gap"]) if global_row is not None else True
        connected = sum(1 for row in providers if bool(row["connected"]))
        outage_started = str(global_row["outage_started_at"] or "") if global_row is not None else ""
        backfill_at = str(global_row["last_backfill_complete_at"] or "") if global_row is not None else ""
        backfill_error = str(global_row["last_backfill_error"] or "") if global_row is not None else ""
        return {
            "durable": True,
            "connected_provider_count": connected,
            "provider_states": providers,
            "continuity_ok": connected >= 1 and not unresolved,
            "unresolved_gap": unresolved,
            "outage_started_at": outage_started or None,
            "last_backfill_complete_at": backfill_at or None,
            "last_backfill_error": backfill_error or None,
            "hydration_queue": queue,
            "raw_receipts_last_hour_by_source": sources,
            "hydration_sample_count": len(metrics),
            "hydration_normalized_count": sum(1 for row in metrics if bool(row["normalized"])),
            "p95_hydration_ms": p95,
        }


class DirectSolanaIngestionPlane:
    """Full-scope processed stream with prioritized confirmed hydration."""

    def __init__(
        self, *, store: Any, service: Any, scout_wallets: tuple[str, ...], rpc_pool: SolanaRpcPool | None = None,
        endpoints: tuple[RpcEndpoint, ...] | None = None, coverage_status_fn: Callable[[], dict[str, Any]] | None = None,
        worker_count: int = 12, market_sample_modulus: int = 20, audit_sample_modulus: int = 200,
        candidate_context_deadline_seconds: float = 3.0, candidate_context_max_signatures: int = 600,
        gap_backfill_max_pages: int = 5,
    ):
        self.store = store
        self.service = service
        self.scout_wallets = tuple(dict.fromkeys(scout_wallets))
        self.endpoints = endpoints or rpc_endpoints_from_env()
        self.rpc = rpc_pool or SolanaRpcPool(self.endpoints)
        self.coverage_status_fn = coverage_status_fn
        self.worker_count = max(2, int(worker_count))
        self.market_sample_modulus = max(1, int(market_sample_modulus))
        self.audit_sample_modulus = max(self.market_sample_modulus, int(audit_sample_modulus))
        self.candidate_context_deadline_seconds = max(0.5, float(candidate_context_deadline_seconds))
        self.candidate_context_max_signatures = max(50, int(candidate_context_max_signatures))
        self.gap_backfill_max_pages = max(1, int(gap_backfill_max_pages))
        self.journal = DirectSolanaJournal(store, raw_retention_seconds=float(os.getenv("SOLANA_ROI_DIRECT_RAW_RETENTION_SECONDS", "900")))
        self._connection_lock = asyncio.Lock()
        self._connected: set[str] = set()
        self._initial_connection_observed = False
        self._recovering = False
        self._dex = httpx.AsyncClient(timeout=1.25, headers={"Accept": "application/json"})
        self.enabled = _truthy(os.getenv("SOLANA_ROI_DIRECT_SOLANA_ENABLED", "true"))

    @property
    def watch_targets(self) -> tuple[WatchTarget, ...]:
        programs = tuple(WatchTarget("program", address, PROGRAM_SOURCE_BY_ID[address]) for address in FROZEN_PROGRAM_ADDRESSES)
        scouts = tuple(WatchTarget("scout", wallet, None) for wallet in self.scout_wallets)
        return programs + scouts

    @staticmethod
    def _launch_like(logs: Any) -> bool:
        if not isinstance(logs, list):
            return False
        text = " ".join(str(row).lower() for row in logs)
        return any(token in text for token in ("instruction: create", "initialize", "launch", "create_pool", "createpool"))

    @staticmethod
    def _sample(signature: str, modulus: int) -> bool:
        digest = hashlib.sha256(signature.encode("utf-8")).digest()
        return int.from_bytes(digest[:8], "big") % max(1, modulus) == 0

    def _coverage_needs_more(self, source: str) -> bool:
        if self.coverage_status_fn is None:
            return True
        try:
            status = self.coverage_status_fn()
        except Exception:
            return True
        counts = status.get("program_source_counts") if isinstance(status, dict) else None
        requirements = status.get("requirements") if isinstance(status, dict) else None
        if not isinstance(counts, dict) or not isinstance(requirements, dict):
            return True
        required = int(requirements.get("min_normalized_swaps_per_source") or 10)
        return not bool(status.get("certified")) or int(counts.get(source, 0)) < required

    async def _connection_state(self, provider: str, connected: bool, error_type: str | None = None) -> None:
        async with self._connection_lock:
            before = len(self._connected)
            if connected:
                self._connected.add(provider)
            else:
                self._connected.discard(provider)
            after = len(self._connected)
            self.journal.set_provider(provider, connected=connected, error_type=error_type)
            if self._initial_connection_observed and before > 0 and after == 0:
                self.journal.mark_outage(utcnow())
            if connected and not self._initial_connection_observed:
                self._initial_connection_observed = True
            if before == 0 and after > 0:
                outage = self.journal.outage_started_at()
                if outage is not None and not self._recovering:
                    self._recovering = True
                    asyncio.create_task(self._recover_gap(outage), name="direct-solana-gap-recovery")

    async def _recover_gap(self, outage_started_at: datetime) -> None:
        complete = True
        error: str | None = None
        try:
            for target in self.watch_targets:
                before: str | None = None
                reached_boundary = False
                for _ in range(self.gap_backfill_max_pages):
                    rows, _provider, _latency = await self.rpc.get_signatures_for_address(target.address, before=before, limit=1000, hedge=False)
                    if not rows:
                        reached_boundary = True
                        break
                    for row in rows:
                        signature = str(row.get("signature") or "")
                        try:
                            block_time = int(row.get("blockTime") or 0)
                            slot = int(row.get("slot") or 0)
                        except (TypeError, ValueError):
                            continue
                        if block_time and datetime.fromtimestamp(block_time, tz=timezone.utc) < outage_started_at:
                            reached_boundary = True
                            break
                        if not signature or row.get("err") is not None:
                            continue
                        chain_time = datetime.fromtimestamp(block_time, tz=timezone.utc) if block_time else utcnow()
                        self.journal.enqueue(
                            signature=signature, slot=slot, trigger_received_at=chain_time, source_hint=target.source_hint,
                            priority=2 if target.kind == "scout" else 15, reason="gap_backfill",
                        )
                    if reached_boundary:
                        break
                    before = str(rows[-1].get("signature") or "")
                    if not before:
                        break
                if not reached_boundary:
                    complete = False
                    error = "gap backfill exceeded bounded pagination before reaching outage boundary"
                    break
        except Exception as exc:
            complete = False
            error = f"{type(exc).__name__}: gap backfill failed closed"
        finally:
            self.journal.close_outage(complete=complete, error=error)
            self._recovering = False

    async def _stream_endpoint(self, endpoint: RpcEndpoint, stop: asyncio.Event) -> None:
        backoff = 0.25
        while not stop.is_set():
            try:
                async with websockets.connect(endpoint.ws_url, ping_interval=15, ping_timeout=15, close_timeout=2, max_queue=8192, max_size=4 * 1024 * 1024) as ws:
                    await self._connection_state(endpoint.name, True)
                    backoff = 0.25
                    request_targets: dict[int, WatchTarget] = {}
                    subscription_targets: dict[int, WatchTarget] = {}
                    for request_id, target in enumerate(self.watch_targets, start=1):
                        request_targets[request_id] = target
                        await ws.send(json.dumps({
                            "jsonrpc": "2.0", "id": request_id, "method": "logsSubscribe",
                            "params": [{"mentions": [target.address]}, {"commitment": "processed"}],
                        }))
                    pending_acks = set(request_targets)
                    buffered: list[dict[str, Any]] = []
                    while pending_acks and not stop.is_set():
                        message = json.loads(await asyncio.wait_for(ws.recv(), timeout=20.0))
                        if isinstance(message, dict) and message.get("id") in pending_acks:
                            request_id = int(message["id"])
                            subscription = message.get("result")
                            if not isinstance(subscription, int):
                                raise RuntimeError("Solana logsSubscribe acknowledgement is invalid")
                            subscription_targets[subscription] = request_targets[request_id]
                            pending_acks.discard(request_id)
                        elif isinstance(message, dict):
                            buffered.append(message)
                    for message in buffered:
                        await self._handle_notification(endpoint.name, subscription_targets, message)
                    while not stop.is_set():
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=30.0)
                        except asyncio.TimeoutError:
                            pong = await ws.ping()
                            await asyncio.wait_for(pong, timeout=5.0)
                            continue
                        await self._handle_notification(endpoint.name, subscription_targets, json.loads(raw))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                await self._connection_state(endpoint.name, False, type(exc).__name__)
                if not stop.is_set():
                    await asyncio.sleep(backoff)
                    backoff = min(10.0, backoff * 2.0)
            else:
                await self._connection_state(endpoint.name, False, None)

    async def _handle_notification(self, provider: str, subscription_targets: dict[int, WatchTarget], message: dict[str, Any]) -> None:
        if message.get("method") != "logsNotification":
            return
        params = message.get("params")
        if not isinstance(params, dict):
            return
        try:
            subscription = int(params["subscription"])
            result = params["result"]
            slot = int(result["context"]["slot"])
            value = result["value"]
            signature = str(value["signature"])
        except (KeyError, TypeError, ValueError):
            return
        target = subscription_targets.get(subscription)
        if target is None or not signature:
            return
        received_at = utcnow()
        self.journal.touch_provider(provider, received_at)
        launch_like = self._launch_like(value.get("logs") if isinstance(value, dict) else [])
        source_key = target.source_hint or f"SCOUT:{target.address}"
        inserted = self.journal.record_receipt(signature=signature, source_key=source_key, slot=slot, received_at=received_at, launch_like=launch_like)
        if not inserted or value.get("err") is not None:
            return
        if target.kind == "scout":
            self.journal.enqueue(signature=signature, slot=slot, trigger_received_at=received_at, source_hint=None, priority=0, reason="frozen_scout_processed_trigger")
            return
        source = str(target.source_hint)
        modulus = self.market_sample_modulus if self._coverage_needs_more(source) else self.audit_sample_modulus
        if launch_like or self._sample(signature, modulus):
            self.journal.enqueue(
                signature=signature, slot=slot, trigger_received_at=received_at, source_hint=source,
                priority=10 if launch_like else 20, reason="prospective_launch" if launch_like else "deterministic_market_sample",
            )

    async def _get_transaction_ready(self, signature: str, *, hedge: bool, attempts: int) -> tuple[Any, str | None, float | None]:
        last_error: Exception | None = None
        for index in range(max(1, attempts)):
            try:
                result, provider, latency = await self.rpc.get_transaction(signature, hedge=hedge)
                if result is not None:
                    return result, provider, latency
            except Exception as exc:
                last_error = exc
            await asyncio.sleep(min(0.75, 0.075 * (2 ** index)))
        if last_error is not None:
            raise last_error
        return None, None, None

    async def _pair_created_at(self, mint: str) -> datetime | None:
        response = await self._dex.get(f"https://api.dexscreener.com/token-pairs/v1/solana/{mint}")
        response.raise_for_status()
        rows = response.json()
        if not isinstance(rows, list):
            return None
        candidates: list[tuple[float, datetime]] = []
        for row in rows:
            if not isinstance(row, dict) or row.get("chainId") != "solana" or row.get("pairCreatedAt") is None:
                continue
            liquidity = row.get("liquidity")
            try:
                liquidity_usd = float(liquidity.get("usd") or 0.0) if isinstance(liquidity, dict) else 0.0
                created_at = datetime.fromtimestamp(float(row["pairCreatedAt"]) / 1000.0, tz=timezone.utc)
            except (TypeError, ValueError, OSError):
                continue
            if liquidity_usd > 0:
                candidates.append((liquidity_usd, created_at))
        return max(candidates, key=lambda item: item[0])[1] if candidates else None

    def _persist_context_swap(self, swap: NormalizedSwap) -> None:
        inserted = self.store.record_swap(
            signature=swap.signature, slot=swap.slot, observed_at=swap.observed_at.isoformat(), received_at=swap.received_at.isoformat(),
            wallet=swap.wallet, token_mint=swap.token_mint, side=swap.side, token_amount=swap.token_amount,
            native_amount_sol=swap.native_amount_sol, reference_price_sol=swap.reference_price_sol,
            ingestion_latency_ms=swap.ingestion_latency_ms, source=swap.source,
        )
        if inserted:
            self.store.append("normalized_swap", swap.received_at.isoformat(), asdict(swap))

    async def _prefill_launch_context(self, candidate: NormalizedSwap) -> bool:
        parts = candidate.source.split(":")
        if len(parts) < 3:
            return False
        source = parts[1].upper()
        try:
            created_at = await self._pair_created_at(candidate.token_mint)
        except Exception:
            return False
        if created_at is None:
            return False
        launch_window_end = created_at + timedelta(seconds=8.0)
        if candidate.received_at < launch_window_end:
            return False
        rows = self.journal.recent_source_signatures(
            source, start=created_at - timedelta(seconds=1.0), end=launch_window_end,
            exclude_signature=candidate.signature, limit=self.candidate_context_max_signatures,
        )
        if not rows:
            return False
        sem = asyncio.Semaphore(24)

        async def hydrate(row: dict[str, Any]) -> None:
            async with sem:
                signature = str(row["signature"])
                trigger = datetime.fromisoformat(str(row["received_at"]))
                try:
                    result, provider, latency = await self._get_transaction_ready(signature, hedge=False, attempts=3)
                    swap = normalize_standard_transaction(result, signature=signature, trigger_received_at=trigger, source_hint=source)
                    if swap is not None and swap.token_mint == candidate.token_mint:
                        self._persist_context_swap(swap)
                        self.journal.record_hydration(
                            signature=signature, source=source, trigger_received_at=trigger, hydrated_at=utcnow(),
                            rpc_provider=provider, rpc_latency_ms=latency, normalized=True, candidate_context_prefilled=True,
                        )
                except Exception:
                    return

        tasks = [asyncio.create_task(hydrate(row)) for row in rows]
        try:
            await asyncio.wait_for(asyncio.gather(*tasks), timeout=self.candidate_context_deadline_seconds)
        except asyncio.TimeoutError:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            return False
        return True

    async def _hydrate_one(self, row: dict[str, Any]) -> None:
        signature = str(row["signature"])
        trigger = datetime.fromisoformat(str(row["trigger_received_at"]))
        priority = int(row["priority"])
        reason = str(row["reason"])
        source_hint = str(row["source_hint"] or "") or None
        historical_recovery = reason == "gap_backfill"
        try:
            result, provider, latency = await self._get_transaction_ready(
                signature, hedge=priority <= 2 and not historical_recovery, attempts=8 if priority <= 2 else 4
            )
            if result is None:
                attempts = int(row["attempts"]) + 1
                self.journal.finish(signature, error="confirmed transaction not yet available", retry=priority <= 2 and attempts < 5)
                return
            swap = normalize_standard_transaction(result, signature=signature, trigger_received_at=trigger, source_hint=source_hint)
            context_prefilled = False
            if swap is not None:
                if historical_recovery:
                    self._persist_context_swap(swap)
                else:
                    profile = self.service.registry.get(swap.wallet)
                    needs_context = bool(
                        (profile is not None and swap.side == "buy") or reason == "prospective_launch"
                        or (source_hint is not None and self._coverage_needs_more(source_hint))
                    )
                    if needs_context:
                        context_prefilled = await self._prefill_launch_context(swap)
                    await self.service.ingest_swap(swap)
            source = swap.source.split(":")[1] if swap is not None and ":" in swap.source else source_hint
            self.journal.record_hydration(
                signature=signature, source=source, trigger_received_at=trigger, hydrated_at=utcnow(), rpc_provider=provider,
                rpc_latency_ms=latency, normalized=swap is not None, candidate_context_prefilled=context_prefilled,
                historical_recovery=historical_recovery,
            )
            self.journal.finish(signature)
        except Exception as exc:
            attempts = int(row["attempts"]) + 1
            self.journal.finish(
                signature, error=f"{type(exc).__name__}: direct hydration failed closed",
                retry=priority <= 2 and attempts < 5,
            )

    async def _worker(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            row = self.journal.claim()
            if row is None:
                await asyncio.sleep(0.025)
                continue
            await self._hydrate_one(row)

    async def run(self, stop: asyncio.Event) -> None:
        if not self.enabled:
            await stop.wait()
            return
        tasks = [asyncio.create_task(self._stream_endpoint(endpoint, stop), name=f"direct-solana-ws:{endpoint.name}") for endpoint in self.endpoints]
        tasks.extend(asyncio.create_task(self._worker(stop), name=f"direct-solana-hydrator:{index}") for index in range(self.worker_count))
        try:
            await stop.wait()
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            for endpoint in self.endpoints:
                self.journal.set_provider(endpoint.name, connected=False)

    def status(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "transport": "standard-solana-json-rpc+logsSubscribe",
            "commitment_fast_path": "processed",
            "authoritative_hydration_commitment": "confirmed",
            "full_program_scope": list(FROZEN_PROGRAM_ADDRESSES),
            "scout_wallet_count": len(self.scout_wallets),
            "strategy_scope_reduced": False,
            "provider_enhanced_webhook_required": False,
            "signing_available": False,
            "transaction_submission_available": False,
            "rpc_pool": self.rpc.status(),
            **self.journal.status(),
        }
