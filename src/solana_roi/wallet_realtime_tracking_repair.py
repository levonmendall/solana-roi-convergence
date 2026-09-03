from __future__ import annotations

import asyncio
import hashlib
import json
import os
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

import websockets

from .direct_solana import DirectSolanaIngestionPlane
from .direct_transaction import normalize_standard_transaction
from .ingestion import NormalizedSwap
from .solana_rpc import RpcEndpoint, SolanaRpcPool
from .wallet_discovery import ContinuousWalletDiscovery


REALTIME_REFRESH_SECONDS = 2.0
REALTIME_RECEIPT_WORKERS = 6
REALTIME_MAX_RECOVERY_PAGES = 3
REALTIME_RECOVERY_PAGE_SIZE = 1000
REALTIME_HYDRATION_ATTEMPTS = 12
REALTIME_MARK_DELAY_LIMIT_SECONDS = 20.0
REALTIME_WS_MAX_QUEUE = 128
REALTIME_WS_MAX_SIZE_BYTES = 256 * 1024

_ORIGINAL_DISCOVERY_RUN = ContinuousWalletDiscovery.run
_ORIGINAL_DISCOVERY_POLL_WALLET = ContinuousWalletDiscovery.poll_wallet
_ORIGINAL_DISCOVERY_STATUS = ContinuousWalletDiscovery.status
_ORIGINAL_DIRECT_STATUS = DirectSolanaIngestionPlane.status


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _endpoint_host(endpoint: RpcEndpoint) -> str:
    try:
        return endpoint.http_url.split("/", 3)[2].lower()
    except Exception:
        return ""


def _select_research_endpoints(endpoints: tuple[RpcEndpoint, ...]) -> tuple[RpcEndpoint, ...]:
    """Prefer already-configured non-official capacity for the small wallet lane.

    No provider is created here. If Alchemy is already configured it may be used;
    otherwise this lane remains on the existing public endpoints. The burst-limited
    official public endpoint is excluded whenever two independent alternatives are
    already available.
    """

    rows = list(endpoints)
    non_official = [row for row in rows if _endpoint_host(row) != "api.mainnet.solana.com"]
    if len(non_official) >= 2:
        rows = non_official
    rows.sort(
        key=lambda endpoint: (
            0 if endpoint.name.lower() == "alchemy" else 1,
            1 if _endpoint_host(endpoint) == "api.mainnet.solana.com" else 0,
            endpoint.name,
        )
    )
    return tuple(rows[:2] if len(rows) > 2 else rows)


class RealtimeWalletTracker:
    """Dedicated prospective lane for already-enrolled wallets.

    Broad discovery may yield whenever core ingestion is pressured. This tracker
    does not: it subscribes only the three incumbents plus the bounded challenger
    shortlist, timestamps notifications at the WebSocket boundary, durably queues
    those signatures, and hydrates them with an independent read-only RPC pool.
    """

    def __init__(self, discovery: ContinuousWalletDiscovery):
        self.discovery = discovery
        self.store = discovery.store
        core_endpoints = tuple(getattr(discovery.rpc, "endpoints", ()) or ())
        self.endpoints = _select_research_endpoints(core_endpoints)
        timeout_seconds = float(os.getenv("SOLANA_ROI_WALLET_REALTIME_RPC_TIMEOUT_SECONDS", "2.5"))
        hedge_delay_seconds = float(os.getenv("SOLANA_ROI_WALLET_REALTIME_HEDGE_DELAY_SECONDS", "0.10"))
        self.rpc = SolanaRpcPool(
            self.endpoints or core_endpoints,
            timeout_seconds=timeout_seconds,
            hedge_delay_seconds=hedge_delay_seconds,
        )
        self.refresh_seconds = max(
            0.5,
            float(os.getenv("SOLANA_ROI_WALLET_REALTIME_REFRESH_SECONDS", str(REALTIME_REFRESH_SECONDS))),
        )
        self.worker_count = max(
            1,
            int(os.getenv("SOLANA_ROI_WALLET_REALTIME_HYDRATION_WORKERS", str(REALTIME_RECEIPT_WORKERS))),
        )
        self._wallets: tuple[str, ...] = ()
        self._generation = 0
        self._provider_tasks: list[asyncio.Task[None]] = []
        self._connected: set[str] = set()
        self._connection_lock = asyncio.Lock()
        self._outage_started_at: datetime | None = None
        self._ever_connected = False
        self._planned_reconfigure = False
        self._recovery_task: asyncio.Task[None] | None = None
        self._startup_recovery_required = False
        self._last_error: str | None = None
        self._notifications = 0
        self._duplicates = 0
        self._hydrated = 0
        self._normalized = 0
        self._copyable = 0
        self._epoch_resets = 0
        self._recovery_runs = 0
        self._recovery_failures = 0
        self._mark_delay_ms: list[float] = []
        self._bootstrap_schema()

    def _bootstrap_schema(self) -> None:
        now = utcnow().isoformat()
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "CREATE TABLE IF NOT EXISTS wallet_realtime_state ("
                "wallet TEXT PRIMARY KEY, epoch_started_at TEXT NOT NULL, anchor_signature TEXT, "
                "last_live_signature TEXT, last_live_slot INTEGER NOT NULL DEFAULT 0, last_live_received_at TEXT, "
                "active INTEGER NOT NULL DEFAULT 1, epoch_resets INTEGER NOT NULL DEFAULT 0, last_error TEXT)"
            )
            self.store.db.execute(
                "CREATE TABLE IF NOT EXISTS wallet_realtime_receipts ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, signature TEXT NOT NULL UNIQUE, wallet TEXT NOT NULL, "
                "slot INTEGER NOT NULL, received_at TEXT NOT NULL, provider TEXT NOT NULL, status TEXT NOT NULL, "
                "attempts INTEGER NOT NULL DEFAULT 0, last_error TEXT, updated_at TEXT NOT NULL)"
            )
            self.store.db.execute(
                "CREATE INDEX IF NOT EXISTS ix_wallet_realtime_receipt_status "
                "ON wallet_realtime_receipts(status, updated_at, id)"
            )
            self.store.db.execute(
                "CREATE TABLE IF NOT EXISTS wallet_realtime_runtime ("
                "id INTEGER PRIMARY KEY CHECK(id=1), last_cycle_at TEXT, last_error TEXT, "
                "last_provider_change_at TEXT, last_recovery_at TEXT)"
            )
            self.store.db.execute(
                "INSERT OR IGNORE INTO wallet_realtime_runtime(id, last_cycle_at) VALUES (1, ?)", (now,)
            )
            self.store.db.execute(
                "UPDATE wallet_realtime_receipts SET status='pending', updated_at=? WHERE status='processing'",
                (now,),
            )
            row = self.store.db.execute(
                "SELECT COUNT(*) AS n FROM wallet_realtime_state WHERE active=1"
            ).fetchone()
            self._startup_recovery_required = bool(row is not None and int(row["n"]) > 0)

        # Transport-quality metadata is additive and does not change the canonical
        # wallet-intelligence schema contract.
        with self.store._lock, self.store.db:
            columns = {
                str(row["name"])
                for row in self.store.db.execute(
                    "PRAGMA table_info(wallet_discovery_forward_observations)"
                ).fetchall()
            }
            if "processing_delay_ms" not in columns:
                self.store.db.execute(
                    "ALTER TABLE wallet_discovery_forward_observations "
                    "ADD COLUMN processing_delay_ms REAL"
                )
            if "tracking_transport" not in columns:
                self.store.db.execute(
                    "ALTER TABLE wallet_discovery_forward_observations "
                    "ADD COLUMN tracking_transport TEXT"
                )

    async def _head_signature(self, wallet: str) -> tuple[str | None, int]:
        rows, _provider, _latency = await self.rpc.get_signatures_for_address(
            wallet, limit=1, hedge=True
        )
        if not rows:
            return None, 0
        row = rows[0]
        signature = str(row.get("signature") or "") or None
        try:
            slot = int(row.get("slot") or 0)
        except (TypeError, ValueError):
            slot = 0
        return signature, slot

    async def _begin_epoch(self, wallet: str, *, reason: str, reset_count: bool) -> None:
        now = utcnow()
        signature: str | None = None
        slot = 0
        try:
            signature, slot = await self._head_signature(wallet)
        except Exception as exc:
            self._last_error = f"{type(exc).__name__}: realtime epoch anchor failed"
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "DELETE FROM wallet_discovery_forward_observations WHERE wallet=?", (wallet,)
            )
            self.store.db.execute(
                "DELETE FROM wallet_realtime_receipts WHERE wallet=?", (wallet,)
            )
            self.store.db.execute(
                "UPDATE wallet_discovery_candidates SET forward_started_at=?, last_signature=?, "
                "last_polled_at=?, last_error=NULL WHERE wallet=?",
                (now.isoformat(), signature, now.isoformat(), wallet),
            )
            existing = self.store.db.execute(
                "SELECT epoch_resets FROM wallet_realtime_state WHERE wallet=?", (wallet,)
            ).fetchone()
            resets = int(existing["epoch_resets"]) if existing is not None else 0
            if reset_count:
                resets += 1
            self.store.db.execute(
                "INSERT INTO wallet_realtime_state("
                "wallet, epoch_started_at, anchor_signature, last_live_signature, last_live_slot, "
                "last_live_received_at, active, epoch_resets, last_error) "
                "VALUES (?, ?, ?, ?, ?, ?, 1, ?, NULL) "
                "ON CONFLICT(wallet) DO UPDATE SET epoch_started_at=excluded.epoch_started_at, "
                "anchor_signature=excluded.anchor_signature, last_live_signature=excluded.last_live_signature, "
                "last_live_slot=excluded.last_live_slot, last_live_received_at=excluded.last_live_received_at, "
                "active=1, epoch_resets=excluded.epoch_resets, last_error=NULL",
                (
                    wallet,
                    now.isoformat(),
                    signature,
                    signature,
                    slot,
                    now.isoformat(),
                    resets,
                ),
            )
        if reset_count:
            self._epoch_resets += 1
        self.store.append(
            "wallet_realtime_epoch_started",
            now.isoformat(),
            {
                "wallet": wallet,
                "reason": reason,
                "anchor_signature": signature,
                "anchor_slot": slot,
                "historical_or_old_forward_evidence_has_promotion_authority": False,
                "paper_only": True,
            },
        )

    async def _sync_wallets(self) -> tuple[str, ...]:
        desired = tuple(self.discovery._tracked_wallets())
        desired_set = set(desired)
        with self.store._lock:
            existing = {
                str(row["wallet"]): dict(row)
                for row in self.store.db.execute(
                    "SELECT wallet, active, epoch_started_at FROM wallet_realtime_state"
                ).fetchall()
            }
        for wallet in desired:
            row = existing.get(wallet)
            if row is None:
                await self._begin_epoch(wallet, reason="realtime_transport_cutover", reset_count=False)
            elif not bool(row["active"]):
                await self._begin_epoch(wallet, reason="reselected_for_realtime_tracking", reset_count=True)
        with self.store._lock, self.store.db:
            if desired_set:
                placeholders = ",".join("?" for _ in desired_set)
                self.store.db.execute(
                    f"UPDATE wallet_realtime_state SET active=0 WHERE active=1 AND wallet NOT IN ({placeholders})",
                    tuple(sorted(desired_set)),
                )
            else:
                self.store.db.execute("UPDATE wallet_realtime_state SET active=0 WHERE active=1")
        return desired

    async def _set_provider(self, provider: str, connected: bool) -> None:
        recovery_needed = False
        async with self._connection_lock:
            before = len(self._connected)
            if connected:
                self._connected.add(provider)
            else:
                self._connected.discard(provider)
            after = len(self._connected)
            now = utcnow()
            with self.store._lock, self.store.db:
                self.store.db.execute(
                    "UPDATE wallet_realtime_runtime SET last_provider_change_at=? WHERE id=1",
                    (now.isoformat(),),
                )
            if self._planned_reconfigure:
                return
            if before > 0 and after == 0:
                self._outage_started_at = now
            if before == 0 and after > 0:
                if not self._ever_connected:
                    self._ever_connected = True
                    recovery_needed = self._startup_recovery_required
                    self._startup_recovery_required = False
                elif self._outage_started_at is not None:
                    recovery_needed = True
                self._outage_started_at = None
        if recovery_needed and (self._recovery_task is None or self._recovery_task.done()):
            self._recovery_task = asyncio.create_task(
                self._recover_all(), name="wallet-realtime-gap-recovery"
            )

    def _enqueue_receipt(
        self,
        *,
        wallet: str,
        signature: str,
        slot: int,
        received_at: datetime,
        provider: str,
    ) -> bool:
        with self.store._lock, self.store.db:
            state = self.store.db.execute(
                "SELECT epoch_started_at, active, last_live_slot FROM wallet_realtime_state WHERE wallet=?",
                (wallet,),
            ).fetchone()
            if state is None or not bool(state["active"]):
                return False
            epoch_started_at = datetime.fromisoformat(str(state["epoch_started_at"]))
            if received_at < epoch_started_at:
                return False
            cursor = self.store.db.execute(
                "INSERT OR IGNORE INTO wallet_realtime_receipts("
                "signature, wallet, slot, received_at, provider, status, attempts, updated_at) "
                "VALUES (?, ?, ?, ?, ?, 'pending', 0, ?)",
                (
                    signature,
                    wallet,
                    int(slot),
                    received_at.isoformat(),
                    provider,
                    received_at.isoformat(),
                ),
            )
            if cursor.rowcount != 1:
                self._duplicates += 1
                return False
            if int(slot) >= int(state["last_live_slot"] or 0):
                self.store.db.execute(
                    "UPDATE wallet_realtime_state SET last_live_signature=?, last_live_slot=?, "
                    "last_live_received_at=?, last_error=NULL WHERE wallet=?",
                    (signature, int(slot), received_at.isoformat(), wallet),
                )
                self.store.db.execute(
                    "UPDATE wallet_discovery_candidates SET last_signature=?, last_polled_at=? WHERE wallet=?",
                    (signature, received_at.isoformat(), wallet),
                )
        self._notifications += 1
        return True

    async def _handle_message(
        self,
        provider: str,
        subscription_wallets: dict[int, str],
        message: dict[str, Any],
    ) -> None:
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
        wallet = subscription_wallets.get(subscription)
        if wallet is None or not signature or not isinstance(value, dict) or value.get("err") is not None:
            return
        self._enqueue_receipt(
            wallet=wallet,
            signature=signature,
            slot=slot,
            received_at=utcnow(),
            provider=provider,
        )

    async def _stream_endpoint(
        self,
        endpoint: RpcEndpoint,
        wallets: tuple[str, ...],
        generation: int,
        stop: asyncio.Event,
    ) -> None:
        backoff = 0.25
        while not stop.is_set() and generation == self._generation:
            try:
                async with websockets.connect(
                    endpoint.ws_url,
                    ping_interval=15,
                    ping_timeout=15,
                    close_timeout=2,
                    max_queue=REALTIME_WS_MAX_QUEUE,
                    max_size=REALTIME_WS_MAX_SIZE_BYTES,
                ) as ws:
                    request_wallets: dict[int, str] = {}
                    subscription_wallets: dict[int, str] = {}
                    for request_id, wallet in enumerate(wallets, start=1):
                        request_wallets[request_id] = wallet
                        await ws.send(
                            json.dumps(
                                {
                                    "jsonrpc": "2.0",
                                    "id": request_id,
                                    "method": "logsSubscribe",
                                    "params": [
                                        {"mentions": [wallet]},
                                        {"commitment": "processed"},
                                    ],
                                }
                            )
                        )
                    pending_acks = set(request_wallets)
                    buffered: list[dict[str, Any]] = []
                    while pending_acks and not stop.is_set() and generation == self._generation:
                        message = json.loads(await asyncio.wait_for(ws.recv(), timeout=20.0))
                        if isinstance(message, dict) and message.get("id") in pending_acks:
                            request_id = int(message["id"])
                            subscription = message.get("result")
                            if not isinstance(subscription, int):
                                raise RuntimeError("realtime wallet logsSubscribe acknowledgement invalid")
                            subscription_wallets[subscription] = request_wallets[request_id]
                            pending_acks.discard(request_id)
                        elif isinstance(message, dict):
                            buffered.append(message)
                    if pending_acks:
                        raise RuntimeError("realtime wallet subscription set incomplete")
                    await self._set_provider(endpoint.name, True)
                    backoff = 0.25
                    for message in buffered:
                        await self._handle_message(endpoint.name, subscription_wallets, message)
                    while not stop.is_set() and generation == self._generation:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=20.0)
                        except asyncio.TimeoutError:
                            pong = await ws.ping()
                            await asyncio.wait_for(pong, timeout=5.0)
                            continue
                        await self._handle_message(
                            endpoint.name, subscription_wallets, json.loads(raw)
                        )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._last_error = f"{type(exc).__name__}: realtime wallet stream failed"
                await self._set_provider(endpoint.name, False)
                if not stop.is_set() and generation == self._generation:
                    await asyncio.sleep(backoff)
                    backoff = min(10.0, backoff * 2.0)
            else:
                await self._set_provider(endpoint.name, False)

    def _claim_receipt(self) -> dict[str, Any] | None:
        now = utcnow().isoformat()
        with self.store._lock, self.store.db:
            row = self.store.db.execute(
                "SELECT id, signature, wallet, slot, received_at, provider, attempts "
                "FROM wallet_realtime_receipts WHERE status='pending' "
                "ORDER BY received_at, id LIMIT 1"
            ).fetchone()
            if row is None:
                return None
            self.store.db.execute(
                "UPDATE wallet_realtime_receipts SET status='processing', attempts=attempts+1, updated_at=? "
                "WHERE id=? AND status='pending'",
                (now, int(row["id"])),
            )
        return dict(row)

    def _finish_receipt(
        self,
        row_id: int,
        *,
        status: str,
        error: str | None = None,
    ) -> None:
        with self.store._lock, self.store.db:
            self.store.db.execute(
                "UPDATE wallet_realtime_receipts SET status=?, last_error=?, updated_at=? WHERE id=?",
                (status, error, utcnow().isoformat(), int(row_id)),
            )

    async def _hydrate_transaction(self, signature: str) -> Any:
        last_error: Exception | None = None
        for index in range(5):
            try:
                result, _provider, _latency = await self.rpc.get_transaction(signature, hedge=True)
                if result is not None:
                    return result
            except Exception as exc:
                last_error = exc
            await asyncio.sleep(min(0.8, 0.05 * (2**index)))
        if last_error is not None:
            raise last_error
        return None

    async def _record_quick_forward_swap(self, swap: NormalizedSwap) -> bool:
        mark: dict[str, Any] | None = None
        started = time.perf_counter()
        try:
            mark = await self.discovery.mark_provider.mark(swap.token_mint)
        except Exception:
            mark = None
        processing_delay_ms = max(
            0.0,
            (utcnow() - swap.received_at).total_seconds() * 1000.0,
        )
        self._mark_delay_ms.append(processing_delay_ms)
        if len(self._mark_delay_ms) > 500:
            del self._mark_delay_ms[:-500]
        copyable_price: float | None = None
        chase_fraction: float | None = None
        if isinstance(mark, dict):
            try:
                copyable_price = float(mark.get("price_sol") or 0.0)
            except (TypeError, ValueError):
                copyable_price = None
            if copyable_price is not None and copyable_price > 0.0:
                observed = mark.get("observed_at", utcnow())
                received = mark.get("received_at", utcnow())
                self.store.record_price_mark(
                    token_mint=swap.token_mint,
                    observed_at=observed.isoformat(),
                    received_at=received.isoformat(),
                    price_sol=copyable_price,
                    source="wallet-realtime:" + str(mark.get("source") or "current-mark"),
                    source_ref=str(mark.get("source_ref") or "") or None,
                )
                if swap.side == "buy":
                    chase_fraction = max(0.0, copyable_price / swap.reference_price_sol - 1.0)
                else:
                    chase_fraction = max(0.0, 1.0 - copyable_price / swap.reference_price_sol)
        lag_ms = swap.ingestion_latency_ms
        copyable = bool(
            copyable_price is not None
            and copyable_price > 0.0
            and chase_fraction is not None
            and chase_fraction <= self.discovery.policy.max_chase_fraction
            and lag_ms <= self.discovery.policy.max_observation_lag_seconds * 1000.0
            and processing_delay_ms <= REALTIME_MARK_DELAY_LIMIT_SECONDS * 1000.0
        )
        # Risk enrichment is deliberately asynchronous. It is still required for
        # promotion, but it cannot delay the observation-time mark or the next live
        # receipt. Until enrichment completes, buys fail closed as high risk.
        risk_complete = swap.side != "buy"
        manipulation_flag = swap.side == "buy"
        side_wallet_flag = swap.side == "buy"
        with self.store._lock, self.store.db:
            cursor = self.store.db.execute(
                "INSERT OR IGNORE INTO wallet_discovery_forward_observations("
                "signature, wallet, token_mint, side, token_amount, observed_at, received_at, wallet_price_sol, "
                "copyable_price_sol, chase_fraction, copyable, observation_lag_ms, risk_complete, manipulation_flag, "
                "side_wallet_flag, source, processing_delay_ms, tracking_transport) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    swap.signature,
                    swap.wallet,
                    swap.token_mint,
                    swap.side,
                    float(swap.token_amount),
                    swap.observed_at.isoformat(),
                    swap.received_at.isoformat(),
                    float(swap.reference_price_sol),
                    copyable_price,
                    chase_fraction,
                    1 if copyable else 0,
                    float(lag_ms),
                    1 if risk_complete else 0,
                    1 if manipulation_flag else 0,
                    1 if side_wallet_flag else 0,
                    "wallet-realtime-forward:" + swap.source,
                    processing_delay_ms,
                    "logsSubscribe",
                ),
            )
        if cursor.rowcount != 1:
            return False
        self._normalized += 1
        if copyable:
            self._copyable += 1
        self.store.append(
            "wallet_realtime_forward_observation",
            swap.received_at.isoformat(),
            {
                "wallet": swap.wallet,
                "token_mint": swap.token_mint,
                "signature": swap.signature,
                "copyable": copyable,
                "observation_lag_ms": lag_ms,
                "processing_delay_ms": processing_delay_ms,
                "chase_fraction": chase_fraction,
                "risk_enrichment_pending": swap.side == "buy",
            },
        )
        _ = time.perf_counter() - started
        if swap.side != "buy":
            self.discovery.refresh_wallet_snapshot(swap.wallet)
        return True

    async def _receipt_worker(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            row = self._claim_receipt()
            if row is None:
                try:
                    await asyncio.wait_for(stop.wait(), timeout=0.05)
                except asyncio.TimeoutError:
                    continue
                return
            row_id = int(row["id"])
            signature = str(row["signature"])
            wallet = str(row["wallet"])
            received_at = datetime.fromisoformat(str(row["received_at"]))
            try:
                result = await self._hydrate_transaction(signature)
                self._hydrated += 1
                if result is None:
                    raise RuntimeError("confirmed transaction unavailable")
                swap = normalize_standard_transaction(
                    result,
                    signature=signature,
                    trigger_received_at=received_at,
                    source_hint=None,
                )
                if swap is not None and swap.wallet == wallet:
                    swap = NormalizedSwap(
                        signature=swap.signature,
                        slot=swap.slot,
                        observed_at=swap.observed_at,
                        received_at=received_at,
                        wallet=swap.wallet,
                        token_mint=swap.token_mint,
                        side=swap.side,
                        token_amount=swap.token_amount,
                        native_amount_sol=swap.native_amount_sol,
                        reference_price_sol=swap.reference_price_sol,
                        source="wallet-realtime:" + swap.source,
                    )
                    await self._record_quick_forward_swap(swap)
                self._finish_receipt(row_id, status="complete")
            except asyncio.CancelledError:
                self._finish_receipt(row_id, status="pending", error="cancelled")
                raise
            except Exception as exc:
                attempts = int(row.get("attempts") or 0) + 1
                if attempts >= REALTIME_HYDRATION_ATTEMPTS:
                    self._finish_receipt(
                        row_id,
                        status="failed",
                        error=f"{type(exc).__name__}: realtime hydration continuity lost",
                    )
                    await self._begin_epoch(
                        wallet,
                        reason="terminal_realtime_hydration_failure",
                        reset_count=True,
                    )
                else:
                    self._finish_receipt(
                        row_id,
                        status="pending",
                        error=f"{type(exc).__name__}: realtime hydration retry",
                    )
                    await asyncio.sleep(min(1.0, 0.05 * attempts))

    async def _risk_worker(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            with self.store._lock:
                row = self.store.db.execute(
                    "SELECT signature, wallet, token_mint, side, token_amount, observed_at, received_at, "
                    "wallet_price_sol, source FROM wallet_discovery_forward_observations "
                    "WHERE tracking_transport='logsSubscribe' AND side='buy' AND risk_complete=0 "
                    "ORDER BY received_at LIMIT 1"
                ).fetchone()
            if row is None:
                try:
                    await asyncio.wait_for(stop.wait(), timeout=0.25)
                except asyncio.TimeoutError:
                    continue
                return
            swap = NormalizedSwap(
                signature=str(row["signature"]),
                slot=0,
                observed_at=datetime.fromisoformat(str(row["observed_at"])),
                received_at=datetime.fromisoformat(str(row["received_at"])),
                wallet=str(row["wallet"]),
                token_mint=str(row["token_mint"]),
                side="buy",
                token_amount=float(row["token_amount"]),
                native_amount_sol=float(row["token_amount"]) * float(row["wallet_price_sol"]),
                reference_price_sol=float(row["wallet_price_sol"]),
                source=str(row["source"]),
            )
            try:
                complete, manipulation, side_wallet = await self.discovery._risk_flags(swap)
            except asyncio.CancelledError:
                raise
            except Exception:
                complete, manipulation, side_wallet = False, True, True
            if not complete:
                try:
                    await asyncio.wait_for(stop.wait(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                return
            with self.store._lock, self.store.db:
                self.store.db.execute(
                    "UPDATE wallet_discovery_forward_observations SET risk_complete=1, "
                    "manipulation_flag=?, side_wallet_flag=? WHERE signature=?",
                    (
                        1 if manipulation else 0,
                        1 if side_wallet else 0,
                        swap.signature,
                    ),
                )
            self.discovery.refresh_wallet_snapshot(swap.wallet)
            try:
                self.discovery.maybe_propose_adaptive_cohort()
            except Exception:
                pass

    async def _recover_wallet(self, wallet: str) -> bool:
        with self.store._lock:
            state = self.store.db.execute(
                "SELECT last_live_signature, epoch_started_at FROM wallet_realtime_state "
                "WHERE wallet=? AND active=1",
                (wallet,),
            ).fetchone()
        if state is None:
            return True
        anchor = str(state["last_live_signature"] or "") or None
        started_at = datetime.fromisoformat(str(state["epoch_started_at"]))
        if anchor is None:
            await self._begin_epoch(wallet, reason="missing_realtime_anchor", reset_count=True)
            return False
        before: str | None = None
        collected: list[dict[str, Any]] = []
        anchor_found = False
        for _ in range(REALTIME_MAX_RECOVERY_PAGES):
            rows, _provider, _latency = await self.rpc.get_signatures_for_address(
                wallet,
                before=before,
                limit=REALTIME_RECOVERY_PAGE_SIZE,
                hedge=True,
            )
            if not rows:
                break
            stop = False
            for row in rows:
                signature = str(row.get("signature") or "")
                if signature == anchor:
                    anchor_found = True
                    stop = True
                    break
                try:
                    block_time = int(row.get("blockTime") or 0)
                except (TypeError, ValueError):
                    block_time = 0
                if block_time and datetime.fromtimestamp(block_time, tz=timezone.utc) < started_at:
                    anchor_found = True
                    stop = True
                    break
                if row.get("err") is None and signature:
                    collected.append(row)
            if stop or len(rows) < REALTIME_RECOVERY_PAGE_SIZE:
                break
            before = str(rows[-1].get("signature") or "") or None
            if before is None:
                break
        if not anchor_found:
            await self._begin_epoch(
                wallet,
                reason="realtime_gap_exceeded_bounded_3x1000_recovery",
                reset_count=True,
            )
            return False
        recovery_received_at = utcnow()
        for row in reversed(collected):
            signature = str(row.get("signature") or "")
            try:
                slot = int(row.get("slot") or 0)
            except (TypeError, ValueError):
                slot = 0
            if signature and slot > 0:
                self._enqueue_receipt(
                    wallet=wallet,
                    signature=signature,
                    slot=slot,
                    received_at=recovery_received_at,
                    provider="bounded-realtime-recovery",
                )
        return True

    async def _recover_all(self) -> None:
        self._recovery_runs += 1
        self._recovery_task = asyncio.current_task()
        try:
            wallets = tuple(self._wallets)
            if wallets:
                results = await asyncio.gather(
                    *(self._recover_wallet(wallet) for wallet in wallets),
                    return_exceptions=True,
                )
                if any(value is False or isinstance(value, Exception) for value in results):
                    self._recovery_failures += 1
            with self.store._lock, self.store.db:
                self.store.db.execute(
                    "UPDATE wallet_realtime_runtime SET last_recovery_at=? WHERE id=1",
                    (utcnow().isoformat(),),
                )
        finally:
            self._recovery_task = None

    async def _replace_provider_tasks(self, wallets: tuple[str, ...], stop: asyncio.Event) -> None:
        self._planned_reconfigure = True
        for task in self._provider_tasks:
            task.cancel()
        if self._provider_tasks:
            await asyncio.gather(*self._provider_tasks, return_exceptions=True)
        async with self._connection_lock:
            self._connected.clear()
        self._wallets = wallets
        self._generation += 1
        generation = self._generation
        self._provider_tasks = [
            asyncio.create_task(
                self._stream_endpoint(endpoint, wallets, generation, stop),
                name=f"wallet-realtime-stream:{endpoint.name}",
            )
            for endpoint in self.endpoints
            if wallets
        ]
        self._planned_reconfigure = False

    async def run(self, stop: asyncio.Event) -> None:
        receipt_workers = [
            asyncio.create_task(self._receipt_worker(stop), name=f"wallet-realtime-hydrator:{index}")
            for index in range(self.worker_count)
        ]
        risk_worker = asyncio.create_task(self._risk_worker(stop), name="wallet-realtime-risk")
        try:
            while not stop.is_set():
                desired = await self._sync_wallets()
                if desired != self._wallets:
                    await self._replace_provider_tasks(desired, stop)
                with self.store._lock, self.store.db:
                    self.store.db.execute(
                        "UPDATE wallet_realtime_runtime SET last_cycle_at=?, last_error=? WHERE id=1",
                        (utcnow().isoformat(), self._last_error),
                    )
                try:
                    await asyncio.wait_for(stop.wait(), timeout=self.refresh_seconds)
                except asyncio.TimeoutError:
                    continue
        finally:
            self._planned_reconfigure = True
            for task in self._provider_tasks:
                task.cancel()
            for task in receipt_workers:
                task.cancel()
            risk_worker.cancel()
            tasks = [*self._provider_tasks, *receipt_workers, risk_worker]
            if self._recovery_task is not None:
                self._recovery_task.cancel()
                tasks.append(self._recovery_task)
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            self._provider_tasks = []

    @staticmethod
    def _percentile(values: list[float], q: float) -> float | None:
        if not values:
            return None
        ordered = sorted(values)
        index = min(len(ordered) - 1, int((len(ordered) - 1) * q))
        return ordered[index]

    def status(self) -> dict[str, Any]:
        with self.store._lock:
            rows = self.store.db.execute(
                "SELECT status, COUNT(*) AS n FROM wallet_realtime_receipts GROUP BY status"
            ).fetchall()
            states = self.store.db.execute(
                "SELECT wallet, epoch_started_at, last_live_signature, last_live_slot, last_live_received_at, "
                "active, epoch_resets, last_error FROM wallet_realtime_state ORDER BY wallet"
            ).fetchall()
            runtime = self.store.db.execute(
                "SELECT last_cycle_at, last_error, last_provider_change_at, last_recovery_at "
                "FROM wallet_realtime_runtime WHERE id=1"
            ).fetchone()
        queue = {str(row["status"]): int(row["n"]) for row in rows}
        total_observed = max(1, self._normalized)
        return {
            "enabled": True,
            "transport": "logsSubscribe",
            "polling_disabled_while_realtime_active": True,
            "tracked_wallet_count": len(self._wallets),
            "tracked_wallets": list(self._wallets),
            "endpoint_count": len(self.endpoints),
            "endpoints": [
                {"name": row.name, "http_host": _endpoint_host(row), "ws_host": row.ws_url.split("/", 3)[2]}
                for row in self.endpoints
            ],
            "connected_providers": sorted(self._connected),
            "connected_provider_count": len(self._connected),
            "durable_receipt_queue": queue,
            "notifications": self._notifications,
            "duplicate_notifications": self._duplicates,
            "hydrated_transactions": self._hydrated,
            "normalized_forward_swaps": self._normalized,
            "copyable_forward_swaps": self._copyable,
            "session_copyable_fraction": self._copyable / total_observed if self._normalized else 0.0,
            "mark_processing_delay_p50_ms": self._percentile(self._mark_delay_ms, 0.50),
            "mark_processing_delay_p95_ms": self._percentile(self._mark_delay_ms, 0.95),
            "max_allowed_mark_delay_seconds": REALTIME_MARK_DELAY_LIMIT_SECONDS,
            "bounded_recovery_pages": REALTIME_MAX_RECOVERY_PAGES,
            "bounded_recovery_page_size": REALTIME_RECOVERY_PAGE_SIZE,
            "recovery_runs": self._recovery_runs,
            "recovery_failures": self._recovery_failures,
            "session_epoch_resets": self._epoch_resets,
            "wallet_states": [dict(row) for row in states],
            "rpc": self.rpc.status(),
            "last_cycle_at": str(runtime["last_cycle_at"] or "") if runtime is not None else None,
            "last_provider_change_at": str(runtime["last_provider_change_at"] or "") if runtime is not None else None,
            "last_recovery_at": str(runtime["last_recovery_at"] or "") if runtime is not None else None,
            "last_error": self._last_error or (str(runtime["last_error"] or "") if runtime is not None else None),
            "paper_only": True,
            "live_money_authority": False,
            "signing_or_submission_available": False,
            "promotion_thresholds_unchanged": True,
        }


def _set_based_background_batch(self: Any, items: list[Any]) -> int:
    """Reduce ordinary receipt SQL from per-receipt read/update to grouped writes.

    Every unique receipt remains durable. Critical launch/scout receipts never call
    this path. Minute counters retain the canonical rolling hash in queue order, but
    the minute table is read/written only once per (minute, source) group.
    """

    from . import production_capacity_repair as capacity

    journal = self.journal
    parsed: list[tuple[str, str, int, datetime, str]] = []
    provider_last: dict[str, datetime] = {}
    for item in items:
        _priority, _mono, _sequence, received_at, provider, _targets, _message = capacity._parse_dispatch_item(item)
        fields = capacity._dispatch_fields(item)
        if fields is None:
            raise RuntimeError("invalid raw receipt dispatch item")
        target, slot, signature, _failed, source = fields
        source_key = source or f"SCOUT:{str(getattr(target, 'address', '') or '')}"
        parsed.append((signature, source_key, int(slot), received_at, provider))
        previous = provider_last.get(provider)
        if previous is None or received_at > previous:
            provider_last[provider] = received_at

    inserted_keys: set[tuple[str, str]] = set()
    with self.store._lock, self.store.db:
        if parsed:
            values_sql = ",".join("(?,?,?,?,0,?)" for _ in parsed)
            params: list[Any] = []
            for signature, source_key, slot, received_at, _provider in parsed:
                params.extend(
                    [
                        signature,
                        source_key,
                        slot,
                        received_at.isoformat(),
                        (received_at + timedelta(seconds=float(journal.raw_retention_seconds))).isoformat(),
                    ]
                )
            try:
                returned = self.store.db.execute(
                    "INSERT OR IGNORE INTO direct_solana_recent_receipts("
                    "signature, source_key, slot, received_at, launch_like, expires_at) VALUES "
                    + values_sql
                    + " RETURNING signature, source_key",
                    tuple(params),
                ).fetchall()
                inserted_keys = {(str(row["signature"]), str(row["source_key"])) for row in returned}
            except Exception:
                # Compatibility fallback retains one transaction and no per-row
                # minute-table read/update.
                for signature, source_key, slot, received_at, _provider in parsed:
                    cursor = self.store.db.execute(
                        "INSERT OR IGNORE INTO direct_solana_recent_receipts("
                        "signature, source_key, slot, received_at, launch_like, expires_at) "
                        "VALUES (?, ?, ?, ?, 0, ?)",
                        (
                            signature,
                            source_key,
                            slot,
                            received_at.isoformat(),
                            (received_at + timedelta(seconds=float(journal.raw_retention_seconds))).isoformat(),
                        ),
                    )
                    if cursor.rowcount == 1:
                        inserted_keys.add((signature, source_key))

        inserted_rows = [row for row in parsed if (row[0], row[1]) in inserted_keys]
        groups: dict[tuple[str, str], list[tuple[str, str, int, datetime, str]]] = defaultdict(list)
        for row in inserted_rows:
            signature, source_key, _slot, received_at, _provider = row
            if source_key in {"PUMP_FUN", "PUMP_AMM", "RAYDIUM"}:
                bucket = received_at.replace(second=0, microsecond=0).isoformat()
                groups[(bucket, source_key)].append(row)

        for (bucket, source_key), rows in groups.items():
            current = self.store.db.execute(
                "SELECT rolling_sha256 FROM direct_solana_minute_receipts WHERE bucket=? AND source=?",
                (bucket, source_key),
            ).fetchone()
            rolling = str(current["rolling_sha256"]) if current is not None else ""
            for signature, _source, slot, received_at, _provider in rows:
                rolling = hashlib.sha256(
                    f"{rolling}|{signature}|{slot}|{received_at.isoformat()}".encode("utf-8")
                ).hexdigest()
            first_at = rows[0][3].isoformat()
            last_at = rows[-1][3].isoformat()
            last_slot = rows[-1][2]
            self.store.db.execute(
                "INSERT INTO direct_solana_minute_receipts("
                "bucket, source, receipt_count, first_received_at, last_received_at, last_slot, rolling_sha256) "
                "VALUES (?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(bucket, source) DO UPDATE SET "
                "receipt_count=direct_solana_minute_receipts.receipt_count+excluded.receipt_count, "
                "last_received_at=excluded.last_received_at, last_slot=excluded.last_slot, "
                "rolling_sha256=excluded.rolling_sha256",
                (bucket, source_key, len(rows), first_at, last_at, last_slot, rolling),
            )

        for provider, received_at in provider_last.items():
            self.store.db.execute(
                "UPDATE direct_solana_provider_state SET last_message_at=? WHERE provider=?",
                (received_at.isoformat(), provider),
            )

        old_count = int(getattr(journal, "_receipt_inserts", 0) or 0)
        new_count = old_count + len(inserted_rows)
        journal._receipt_inserts = new_count
        if new_count // 500 > old_count // 500 and parsed:
            newest = max(row[3] for row in parsed)
            self.store.db.execute(
                "DELETE FROM direct_solana_recent_receipts WHERE expires_at<?",
                (newest.isoformat(),),
            )

    setattr(
        self,
        "_roi_set_based_batch_rows",
        int(getattr(self, "_roi_set_based_batch_rows", 0) or 0) + len(inserted_rows),
    )
    setattr(
        self,
        "_roi_set_based_batch_groups",
        int(getattr(self, "_roi_set_based_batch_groups", 0) or 0) + len(groups),
    )
    return len(inserted_rows)


async def _realtime_discovery_run(self: ContinuousWalletDiscovery, stop: asyncio.Event) -> None:
    tracker = getattr(self, "_roi_realtime_tracker", None)
    if tracker is None:
        tracker = RealtimeWalletTracker(self)
        setattr(self, "_roi_realtime_tracker", tracker)
    tracker_task = asyncio.create_task(tracker.run(stop), name="wallet-realtime-tracking")
    try:
        await _ORIGINAL_DISCOVERY_RUN(self, stop)
    finally:
        if not tracker_task.done():
            tracker_task.cancel()
        await asyncio.gather(tracker_task, return_exceptions=True)


async def _realtime_poll_guard(self: ContinuousWalletDiscovery, wallet: str) -> int:
    tracker = getattr(self, "_roi_realtime_tracker", None)
    if tracker is not None:
        # The dedicated live lane owns prospective acquisition. Polling would
        # reintroduce minute-scale delays and routine false epoch resets.
        return 0
    return await _ORIGINAL_DISCOVERY_POLL_WALLET(self, wallet)


def _realtime_discovery_status(self: ContinuousWalletDiscovery) -> dict[str, Any]:
    payload = _ORIGINAL_DISCOVERY_STATUS(self)
    tracker = getattr(self, "_roi_realtime_tracker", None)
    payload["realtime_tracking"] = (
        tracker.status()
        if tracker is not None
        else {
            "enabled": True,
            "state": "initializing_after_research_bootstrap",
            "transport": "logsSubscribe",
            "polling_disabled_while_realtime_active": True,
            "paper_only": True,
        }
    )
    payload["broad_discovery_can_yield_without_pausing_tracked_wallets"] = True
    payload["prospective_tracking_transport"] = "dedicated-logsSubscribe"
    return payload


def _direct_status_with_set_based_writer(original: Callable[[Any], dict[str, Any]]) -> Callable[[Any], dict[str, Any]]:
    def status(self: Any) -> dict[str, Any]:
        payload = original(self)
        dispatch = payload.get("raw_receipt_dispatch")
        if isinstance(dispatch, dict):
            dispatch.update(
                {
                    "set_based_background_writer": True,
                    "minute_table_grouped_by_bucket_source": True,
                    "set_based_rows": int(getattr(self, "_roi_set_based_batch_rows", 0) or 0),
                    "set_based_minute_groups": int(getattr(self, "_roi_set_based_batch_groups", 0) or 0),
                    "unique_receipt_durability_unchanged": True,
                }
            )
        return payload

    setattr(status, "_roi_wallet_realtime_tracking_repair", True)
    return status


def install_wallet_realtime_tracking_repair() -> None:
    """Install realtime wallet acquisition without changing strategy authority."""

    from . import production_capacity_repair as capacity

    capacity._persist_background_batch = _set_based_background_batch  # type: ignore[assignment]

    if not bool(getattr(ContinuousWalletDiscovery.run, "_roi_wallet_realtime_tracking_repair", False)):
        setattr(_realtime_discovery_run, "_roi_wallet_realtime_tracking_repair", True)
        setattr(_realtime_poll_guard, "_roi_wallet_realtime_tracking_repair", True)
        setattr(_realtime_discovery_status, "_roi_wallet_realtime_tracking_repair", True)
        ContinuousWalletDiscovery.run = _realtime_discovery_run  # type: ignore[method-assign]
        ContinuousWalletDiscovery.poll_wallet = _realtime_poll_guard  # type: ignore[method-assign]
        ContinuousWalletDiscovery.status = _realtime_discovery_status  # type: ignore[method-assign]

    current_status = DirectSolanaIngestionPlane.status
    if not bool(getattr(current_status, "_roi_wallet_realtime_tracking_repair", False)):
        DirectSolanaIngestionPlane.status = _direct_status_with_set_based_writer(current_status)  # type: ignore[method-assign]


__all__ = [
    "REALTIME_MAX_RECOVERY_PAGES",
    "REALTIME_RECOVERY_PAGE_SIZE",
    "RealtimeWalletTracker",
    "_select_research_endpoints",
    "_set_based_background_batch",
    "install_wallet_realtime_tracking_repair",
]
