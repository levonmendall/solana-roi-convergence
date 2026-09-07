from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Any, Callable

from . import v51_atomic_paper_capital as capital


LIFECYCLE_RUNTIME_VERSION = "v51-paper-lifecycle-runtime-v1"
PAPER_ONLY = True
LIVE_MONEY_AUTHORITY = False
SIGNING_AVAILABLE = False
TRANSACTION_SUBMISSION_AVAILABLE = False
TICK_SECONDS = 1.0

_INSTALLED = False
_ORIGINAL_OBSERVE: Callable[..., Any] | None = None
_ORIGINAL_STATUS: Callable[..., dict[str, Any]] | None = None
_ORIGINAL_REALTIME_RUN: Callable[..., Any] | None = None
_ORIGINAL_ATTEMPT_LIQUIDATION: Callable[..., Any] | None = None

_LAST_TICK_AT: str | None = None
_LAST_ERROR: str | None = None
_TICK_COUNT = 0
_RETRY_TICK_COUNT = 0
_RESERVATION_SYNC_COUNT = 0
_SETTLEMENT_SYNC_COUNT = 0
_WORKER_RUNNING = False


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _table_exists(store: Any, table: str) -> bool:
    try:
        with store._lock:
            row = store.db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                (table,),
            ).fetchone()
        return row is not None
    except Exception:
        return False


def _ensure_schema(store: Any) -> None:
    capital.ensure_atomic_capital_schema(store)
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS v51_paper_execution_balance_artifacts ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, release_commit TEXT NOT NULL, "
            "position_scope TEXT NOT NULL, source_signature TEXT NOT NULL, attempt_number INTEGER NOT NULL, "
            "token_mint TEXT NOT NULL, actual_position_raw INTEGER NOT NULL, expected_output_lamports INTEGER NOT NULL, "
            "total_fee_lamports INTEGER NOT NULL, simulation_error_class TEXT, simulation_error TEXT, "
            "recorded_at TEXT NOT NULL, paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL, "
            "UNIQUE(release_commit,position_scope,source_signature,attempt_number))"
        )
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS v51_paper_lifecycle_runtime_state ("
            "release_commit TEXT PRIMARY KEY, runtime_version TEXT NOT NULL, last_tick_at TEXT, "
            "tick_count INTEGER NOT NULL, retry_tick_count INTEGER NOT NULL, reservation_sync_count INTEGER NOT NULL, "
            "settlement_sync_count INTEGER NOT NULL, worker_running INTEGER NOT NULL, last_error TEXT, "
            "paper_only INTEGER NOT NULL, live_money_authority INTEGER NOT NULL)"
        )


def _reservation_id(surface: str, candidate_id: str) -> str:
    return f"{surface.lower()}:{candidate_id}"


def _execution_epoch() -> str:
    try:
        from . import v51_exit_execution_terminal_fomo_followup as followup
        return str(followup.ACTIVE_EXECUTION_MODEL_EPOCH)
    except Exception:
        return "unknown"


def _record_open_event(
    adapter: Any,
    *,
    surface: str,
    candidate_id: str,
    lane: str,
    reserved_fraction: float,
    token_mint: str,
    token_raw: int,
    entry_cost_sol: float | None,
) -> None:
    capital.record_lifecycle_event(
        adapter.store,
        release_commit=adapter.release_commit,
        candidate_id=candidate_id,
        event_key=f"{surface}:OPEN",
        stage="OPEN",
        payload={
            "surface": surface,
            "lane": lane,
            "reservation_id": _reservation_id(surface, candidate_id),
            "reserved_fraction": float(reserved_fraction),
            "token_mint": token_mint,
            "entry_token_raw": int(token_raw),
            "entry_cost_sol": entry_cost_sol,
            "execution_model_epoch": _execution_epoch(),
            "paper_only": True,
            "live_money_authority": False,
        },
    )


def _record_rejected_event(adapter: Any, *, surface: str, candidate_id: str, reason: str) -> None:
    capital.record_lifecycle_event(
        adapter.store,
        release_commit=adapter.release_commit,
        candidate_id=candidate_id,
        event_key=f"{surface}:ENTRY_REJECTED",
        stage="ENTRY_REJECTED",
        payload={
            "surface": surface,
            "reason": reason,
            "paper_only": True,
            "live_money_authority": False,
        },
    )


def _sync_solana_entry(adapter: Any, signature: str) -> int:
    if not _table_exists(adapter.store, "risk_conditioned_alpha_v5_trials"):
        return 0
    with adapter.store._lock:
        row = adapter.store.db.execute(
            "SELECT * FROM risk_conditioned_alpha_v5_trials "
            "WHERE release_commit=? AND source_signature=? AND selected=1 "
            "AND decision LIKE 'paper_enter%' ORDER BY id DESC LIMIT 1",
            (adapter.release_commit, signature),
        ).fetchone()
    if row is None:
        return 0
    trial = dict(row)
    requested = max(0.0, float(trial.get("position_fraction") or 0.0))
    if requested <= 0.0:
        return 0
    reservation = capital.reserve_paper_capital(
        adapter.store,
        release_commit=adapter.release_commit,
        reservation_id=_reservation_id("SOLANA", signature),
        lane=f"SOLANA:{trial.get('lane') or 'unknown'}",
        candidate_id=signature,
        requested_fraction=requested,
    )
    reservation_status = str(reservation.get("status") or "")
    if reservation_status == "settled":
        return 0
    if reservation_status != "active":
        with adapter.store._lock, adapter.store.db:
            adapter.store.db.execute(
                "UPDATE risk_conditioned_alpha_v5_trials SET selected=0,"
                "decision='paper_observe_shared_capital_unavailable',"
                "decision_reason='shared_atomic_paper_capital_unavailable',position_fraction=0 "
                "WHERE release_commit=? AND source_signature=? AND id=?",
                (adapter.release_commit, signature, int(trial["id"])),
            )
        _record_rejected_event(
            adapter,
            surface="SOLANA",
            candidate_id=signature,
            reason="shared_atomic_paper_capital_unavailable",
        )
        return 0
    _record_open_event(
        adapter,
        surface="SOLANA",
        candidate_id=signature,
        lane=str(trial.get("lane") or "unknown"),
        reserved_fraction=float(reservation.get("reserved_fraction") or 0.0),
        token_mint=str(trial.get("token_mint") or ""),
        token_raw=int(trial.get("entry_token_raw") or 0),
        entry_cost_sol=float(trial["entry_cost_sol"]) if trial.get("entry_cost_sol") is not None else None,
    )
    return 1


def _sync_fomo_entry(adapter: Any, signature: str) -> int:
    if not _table_exists(adapter.store, "fomo_paper_trials"):
        return 0
    with adapter.store._lock:
        row = adapter.store.db.execute(
            "SELECT * FROM fomo_paper_trials WHERE release_commit=? AND source_signature=? "
            "AND decision LIKE 'paper_enter%' ORDER BY id DESC LIMIT 1",
            (adapter.release_commit, signature),
        ).fetchone()
    if row is None:
        return 0
    trial = dict(row)
    requested = max(0.0, float(trial.get("position_fraction") or 0.0))
    if requested <= 0.0:
        return 0
    reservation = capital.reserve_paper_capital(
        adapter.store,
        release_commit=adapter.release_commit,
        reservation_id=_reservation_id("FOMO", signature),
        lane="FOMO",
        candidate_id=signature,
        requested_fraction=requested,
    )
    reservation_status = str(reservation.get("status") or "")
    if reservation_status == "settled":
        return 0
    if reservation_status != "active":
        with adapter.store._lock, adapter.store.db:
            adapter.store.db.execute(
                "UPDATE fomo_paper_trials SET decision='no_entry_shared_paper_capital_unavailable',"
                "decision_reason='shared_atomic_paper_capital_unavailable',position_fraction=0 "
                "WHERE release_commit=? AND source_signature=? AND id=?",
                (adapter.release_commit, signature, int(trial["id"])),
            )
        _record_rejected_event(
            adapter,
            surface="FOMO",
            candidate_id=signature,
            reason="shared_atomic_paper_capital_unavailable",
        )
        return 0
    _record_open_event(
        adapter,
        surface="FOMO",
        candidate_id=signature,
        lane="FOMO",
        reserved_fraction=float(reservation.get("reserved_fraction") or 0.0),
        token_mint=str(trial.get("token_mint") or ""),
        token_raw=int(trial.get("entry_token_raw") or 0),
        entry_cost_sol=float(trial["entry_cost_sol"]) if trial.get("entry_cost_sol") is not None else None,
    )
    return 1


def _candidate_signatures(adapter: Any) -> set[str]:
    values: set[str] = set()
    if _table_exists(adapter.store, "risk_conditioned_alpha_v5_trials"):
        with adapter.store._lock:
            rows = adapter.store.db.execute(
                "SELECT DISTINCT source_signature FROM risk_conditioned_alpha_v5_trials "
                "WHERE release_commit=? AND selected=1 AND decision LIKE 'paper_enter%' "
                "ORDER BY id DESC LIMIT 512",
                (adapter.release_commit,),
            ).fetchall()
        values.update(str(row["source_signature"]) for row in rows if row["source_signature"])
    if _table_exists(adapter.store, "fomo_paper_trials"):
        with adapter.store._lock:
            rows = adapter.store.db.execute(
                "SELECT DISTINCT source_signature FROM fomo_paper_trials "
                "WHERE release_commit=? AND decision LIKE 'paper_enter%' "
                "ORDER BY id DESC LIMIT 512",
                (adapter.release_commit,),
            ).fetchall()
        values.update(str(row["source_signature"]) for row in rows if row["source_signature"])
    return values


def sync_entry_reservations(adapter: Any, signature: str | None = None) -> int:
    global _RESERVATION_SYNC_COUNT
    _ensure_schema(adapter.store)
    signatures = {signature} if signature else _candidate_signatures(adapter)
    changed = 0
    for candidate in sorted(value for value in signatures if value):
        changed += _sync_solana_entry(adapter, candidate)
        changed += _sync_fomo_entry(adapter, candidate)
    _RESERVATION_SYNC_COUNT += 1
    return changed


def _settle_one(
    adapter: Any,
    *,
    surface: str,
    source_signature: str,
    exit_signature: str,
    net_return: float,
    settled_at: str | None,
) -> bool:
    reservation_id = _reservation_id(surface, source_signature)
    try:
        result = capital.settle_paper_capital(
            adapter.store,
            release_commit=adapter.release_commit,
            reservation_id=reservation_id,
            settlement_id=f"{surface.lower()}:{source_signature}:{exit_signature}",
            net_return=float(net_return),
        )
    except KeyError:
        return False
    except RuntimeError as exc:
        if "not_active:settled" in str(exc):
            return False
        raise
    capital.record_lifecycle_event(
        adapter.store,
        release_commit=adapter.release_commit,
        candidate_id=source_signature,
        event_key=f"{surface}:CLOSED",
        stage="CLOSED",
        payload={
            "surface": surface,
            "reservation_id": reservation_id,
            "settlement_id": result.get("settlement_id"),
            "exit_signature": exit_signature,
            "net_return": float(net_return),
            "settled_at": settled_at,
            "execution_model_epoch": _execution_epoch(),
            "paper_only": True,
            "live_money_authority": False,
        },
    )
    return not bool(result.get("idempotent_replay"))


def sync_settlements(adapter: Any) -> int:
    global _SETTLEMENT_SYNC_COUNT
    _ensure_schema(adapter.store)
    changed = 0
    if _table_exists(adapter.store, "risk_conditioned_alpha_v5_outcomes"):
        with adapter.store._lock:
            rows = adapter.store.db.execute(
                "SELECT source_signature,exit_signature,net_return,settled_at "
                "FROM risk_conditioned_alpha_v5_outcomes WHERE release_commit=? ORDER BY id DESC LIMIT 1024",
                (adapter.release_commit,),
            ).fetchall()
        seen: set[str] = set()
        for row in rows:
            signature = str(row["source_signature"] or "")
            if not signature or signature in seen:
                continue
            seen.add(signature)
            changed += int(
                _settle_one(
                    adapter,
                    surface="SOLANA",
                    source_signature=signature,
                    exit_signature=str(row["exit_signature"] or "paper-exit"),
                    net_return=float(row["net_return"]),
                    settled_at=str(row["settled_at"] or "") or None,
                )
            )
    if _table_exists(adapter.store, "fomo_paper_outcomes"):
        with adapter.store._lock:
            rows = adapter.store.db.execute(
                "SELECT source_signature,exit_signature,net_return,settled_at "
                "FROM fomo_paper_outcomes WHERE release_commit=? ORDER BY id DESC LIMIT 1024",
                (adapter.release_commit,),
            ).fetchall()
        seen = set()
        for row in rows:
            signature = str(row["source_signature"] or "")
            if not signature or signature in seen:
                continue
            seen.add(signature)
            changed += int(
                _settle_one(
                    adapter,
                    surface="FOMO",
                    source_signature=signature,
                    exit_signature=str(row["exit_signature"] or "paper-exit"),
                    net_return=float(row["net_return"]),
                    settled_at=str(row["settled_at"] or "") or None,
                )
            )
    _SETTLEMENT_SYNC_COUNT += 1
    return changed


def _proven_paper_balance_artifact(evidence: dict[str, Any]) -> bool:
    error = str(evidence.get("error") or "").lower()
    balance_marker = "insufficient funds" in error or "insufficientfunds" in error
    expected = int(evidence.get("expected_output_lamports") or 0)
    fees = int(evidence.get("total_fee_lamports") or 0)
    return bool(
        balance_marker
        and evidence.get("simulation_error_class") == "account_failure"
        and evidence.get("amount_match")
        and evidence.get("transaction_built")
        and evidence.get("route_valid")
        and expected > fees
        and not evidence.get("token_restriction")
        and not evidence.get("transfer_failure")
    )


def _record_balance_artifact(
    adapter: Any,
    liquidation: dict[str, Any],
    evidence: dict[str, Any],
    *,
    attempt_number: int,
) -> None:
    _ensure_schema(adapter.store)
    with adapter.store._lock, adapter.store.db:
        adapter.store.db.execute(
            "INSERT OR IGNORE INTO v51_paper_execution_balance_artifacts("
            "release_commit,position_scope,source_signature,attempt_number,token_mint,actual_position_raw,"
            "expected_output_lamports,total_fee_lamports,simulation_error_class,simulation_error,recorded_at,"
            "paper_only,live_money_authority) VALUES (?,?,?,?,?,?,?,?,?,?,?,1,0)",
            (
                adapter.release_commit,
                str(liquidation["position_scope"]),
                str(liquidation["source_signature"]),
                int(attempt_number),
                str(liquidation["token_mint"]),
                int(liquidation["actual_position_raw"]),
                int(evidence.get("expected_output_lamports") or 0),
                int(evidence.get("total_fee_lamports") or 0),
                evidence.get("simulation_error_class"),
                str(evidence.get("error") or "")[:1000] or None,
                _utcnow().isoformat(),
            ),
        )


async def _attempt_liquidation_with_paper_inventory(adapter: Any, liquidation: dict[str, Any]) -> None:
    from . import v51_exact_exit_execution as exact
    from .quote import LAMPORTS_PER_SOL

    attempt_number = int(liquidation.get("attempt_count") or 0) + 1
    first_due = datetime.fromisoformat(str(liquidation["first_exit_due_at"]))
    evidence = await exact.observe_exact_exit_order(
        adapter,
        token_mint=str(liquidation["token_mint"]),
        actual_position_raw=int(liquidation["actual_position_raw"]),
    )
    artifact = _proven_paper_balance_artifact(evidence)
    if artifact:
        expected = int(evidence.get("expected_output_lamports") or 0)
        fees = int(evidence.get("total_fee_lamports") or 0)
        evidence["exit_net_sol"] = (expected - fees) / LAMPORTS_PER_SOL
        evidence["paper_wallet_balance_artifact"] = True
        evidence["market_executable"] = True
        evidence["chain_simulation_ok"] = False
    else:
        evidence["paper_wallet_balance_artifact"] = False
        evidence["market_executable"] = bool(evidence.get("simulation_ok"))

    executable = bool(
        evidence.get("amount_match")
        and evidence.get("transaction_built")
        and evidence.get("route_valid")
        and (evidence.get("simulation_ok") or artifact)
        and evidence.get("exit_net_sol") is not None
        and float(evidence["exit_net_sol"]) > 0.0
    )
    next_retry = None if executable else exact._retry_at(first_due, attempt_number)
    terminal = bool(not executable and next_retry is None)
    status = (
        "paper_exit_executed"
        if executable
        else ("paper_exit_terminal_unexitable" if terminal else "paper_exit_execution_failed")
    )
    attempt_id = exact._record_attempt(
        adapter,
        liquidation,
        evidence,
        attempt_number=attempt_number,
        next_retry_at=next_retry,
        status=status,
        terminal_assumption=exact.TERMINAL_LIQUIDATION_ASSUMPTION if terminal else None,
    )
    if artifact:
        _record_balance_artifact(adapter, liquidation, evidence, attempt_number=attempt_number)

    settled_at = _utcnow().isoformat() if executable or terminal else None
    eventual_exit = float(evidence["exit_net_sol"]) if executable else (0.0 if terminal else None)
    with adapter.store._lock, adapter.store.db:
        adapter.store.db.execute(
            "UPDATE profit_first_final_exit_liquidations SET last_attempt_at=?,attempt_count=?,next_retry_at=?,status=?,"
            "eventual_exit_net_sol=?,settled_at=?,terminal_assumption=? "
            "WHERE epoch_id=? AND position_scope=? AND source_signature=?",
            (
                str(evidence.get("attempted_at") or _utcnow().isoformat()),
                attempt_number,
                next_retry.isoformat() if next_retry else None,
                status,
                eventual_exit,
                settled_at,
                exact.TERMINAL_LIQUIDATION_ASSUMPTION if terminal else None,
                adapter.epoch_id,
                str(liquidation["position_scope"]),
                str(liquidation["source_signature"]),
            ),
        )
    updated = dict(liquidation)
    updated["attempt_count"] = attempt_number
    if executable or terminal:
        if str(updated["position_scope"]) == "fomo":
            exact._settle_fomo(
                adapter,
                updated,
                attempt_id=attempt_id,
                exit_net_sol=float(eventual_exit or 0.0),
                terminal=terminal,
            )
        else:
            exact._settle_final(
                adapter,
                updated,
                attempt_id=attempt_id,
                exit_net_sol=float(eventual_exit or 0.0),
                terminal=terminal,
            )
        sync_settlements(adapter)
    try:
        adapter.store.append(
            status,
            str(evidence.get("attempted_at") or _utcnow().isoformat()),
            {
                "execution_model_epoch": exact.EXACT_EXIT_EXECUTION_MODEL_EPOCH,
                "position_scope": str(updated["position_scope"]),
                "source_signature": str(updated["source_signature"]),
                "token_mint": str(updated["token_mint"]),
                "actual_position_raw": int(updated["actual_position_raw"]),
                "exit_quote_amount": int(evidence.get("quote_input_raw") or 0),
                "amount_match": bool(evidence.get("amount_match")),
                "attempt_number": attempt_number,
                "next_retry_at": next_retry.isoformat() if next_retry else None,
                "chain_simulation_ok": bool(evidence.get("simulation_ok")),
                "paper_wallet_balance_artifact": artifact,
                "market_executable": bool(evidence.get("market_executable")),
                "terminal_assumption": exact.TERMINAL_LIQUIDATION_ASSUMPTION if terminal else None,
                "paper_only": True,
                "live_money_authority": False,
                "signing_available": False,
                "transaction_submission_available": False,
            },
        )
    except Exception:
        pass


def _persist_runtime_state(adapter: Any) -> None:
    _ensure_schema(adapter.store)
    with adapter.store._lock, adapter.store.db:
        adapter.store.db.execute(
            "INSERT INTO v51_paper_lifecycle_runtime_state("
            "release_commit,runtime_version,last_tick_at,tick_count,retry_tick_count,reservation_sync_count,"
            "settlement_sync_count,worker_running,last_error,paper_only,live_money_authority) "
            "VALUES (?,?,?,?,?,?,?,?,?,1,0) "
            "ON CONFLICT(release_commit) DO UPDATE SET "
            "runtime_version=excluded.runtime_version,last_tick_at=excluded.last_tick_at,"
            "tick_count=excluded.tick_count,retry_tick_count=excluded.retry_tick_count,"
            "reservation_sync_count=excluded.reservation_sync_count,"
            "settlement_sync_count=excluded.settlement_sync_count,"
            "worker_running=excluded.worker_running,last_error=excluded.last_error,paper_only=1,live_money_authority=0",
            (
                adapter.release_commit,
                LIFECYCLE_RUNTIME_VERSION,
                _LAST_TICK_AT,
                int(_TICK_COUNT),
                int(_RETRY_TICK_COUNT),
                int(_RESERVATION_SYNC_COUNT),
                int(_SETTLEMENT_SYNC_COUNT),
                1 if _WORKER_RUNNING else 0,
                _LAST_ERROR,
            ),
        )


async def lifecycle_tick(adapter: Any) -> dict[str, int]:
    global _LAST_TICK_AT, _LAST_ERROR, _TICK_COUNT, _RETRY_TICK_COUNT
    from . import v51_exact_exit_execution as exact

    entries = sync_entry_reservations(adapter)
    await exact._retry_due(adapter)
    _RETRY_TICK_COUNT += 1
    settlements = sync_settlements(adapter)
    _TICK_COUNT += 1
    _LAST_TICK_AT = _utcnow().isoformat()
    _LAST_ERROR = None
    _persist_runtime_state(adapter)
    return {"entry_sync": entries, "settlement_sync": settlements}


async def _lifecycle_loop(tracker: Any, stop: asyncio.Event) -> None:
    global _WORKER_RUNNING, _LAST_ERROR, _LAST_TICK_AT, _TICK_COUNT
    _WORKER_RUNNING = True
    try:
        while not stop.is_set():
            adapter = getattr(
                getattr(tracker, "discovery", None),
                "_roi_profit_first_entity_final_research",
                None,
            )
            if adapter is not None:
                try:
                    await lifecycle_tick(adapter)
                except Exception as exc:
                    _LAST_ERROR = f"{type(exc).__name__}:{exc}"[:1000]
                    _TICK_COUNT += 1
                    _LAST_TICK_AT = _utcnow().isoformat()
                    try:
                        _persist_runtime_state(adapter)
                    except Exception:
                        pass
            try:
                await asyncio.wait_for(stop.wait(), timeout=TICK_SECONDS)
            except asyncio.TimeoutError:
                pass
    finally:
        _WORKER_RUNNING = False
        adapter = getattr(
            getattr(tracker, "discovery", None),
            "_roi_profit_first_entity_final_research",
            None,
        )
        if adapter is not None:
            try:
                _persist_runtime_state(adapter)
            except Exception:
                pass


async def _run_with_lifecycle(self: Any, stop: asyncio.Event) -> None:
    if _ORIGINAL_REALTIME_RUN is None:
        raise RuntimeError("paper lifecycle runtime missing realtime owner")
    task = asyncio.create_task(_lifecycle_loop(self, stop), name="v51-paper-lifecycle-runtime")
    try:
        await _ORIGINAL_REALTIME_RUN(self, stop)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def _observe_with_lifecycle(self: Any, signature: str) -> None:
    if _ORIGINAL_OBSERVE is None:
        raise RuntimeError("paper lifecycle runtime missing final observe owner")
    await _ORIGINAL_OBSERVE(self, signature)
    sync_entry_reservations(self, signature)
    sync_settlements(self)


def _status_payload(store: Any | None = None, release_commit: str | None = None) -> dict[str, Any]:
    result: dict[str, Any] = {
        "runtime_version": LIFECYCLE_RUNTIME_VERSION,
        "installed": _INSTALLED,
        "worker_running": _WORKER_RUNNING,
        "last_tick_at": _LAST_TICK_AT,
        "last_error": _LAST_ERROR,
        "tick_count": _TICK_COUNT,
        "retry_tick_count": _RETRY_TICK_COUNT,
        "reservation_sync_count": _RESERVATION_SYNC_COUNT,
        "settlement_sync_count": _SETTLEMENT_SYNC_COUNT,
        "exit_retry_clock": "independent_realtime_worker_tick",
        "research_trials_are_portfolio_positions": False,
        "atomic_capital_reservation_defines_open_position": True,
        "paper_wallet_balance_artifact_policy": "insufficient_funds_only_plus_exact_route_amount_and_no_restriction",
        "paper_only": True,
        "live_money_authority": False,
        "signing_available": False,
        "transaction_submission_available": False,
    }
    if store is None or not release_commit:
        return result
    _ensure_schema(store)
    reconciliation = capital.capital_reconciliation(store, release_commit=release_commit)
    with store._lock:
        opens = store.db.execute(
            "SELECT lane,COUNT(*) AS n FROM v51_paper_capital_reservations "
            "WHERE release_commit=? AND status='active' GROUP BY lane",
            (release_commit,),
        ).fetchall()
        artifacts = store.db.execute(
            "SELECT COUNT(*) FROM v51_paper_execution_balance_artifacts WHERE release_commit=?",
            (release_commit,),
        ).fetchone()[0]
        lifecycle = store.db.execute(
            "SELECT stage,COUNT(*) AS n FROM v51_paper_lifecycle_events "
            "WHERE release_commit=? GROUP BY stage",
            (release_commit,),
        ).fetchall()
    result.update(
        {
            "open_positions_by_lane": {str(row["lane"]): int(row["n"]) for row in opens},
            "open_position_count": sum(int(row["n"]) for row in opens),
            "balance_artifact_execution_count": int(artifacts or 0),
            "lifecycle_event_counts": {str(row["stage"]): int(row["n"]) for row in lifecycle},
            "capital_reconciliation": reconciliation,
            "lifecycle_proven": bool(
                reconciliation.get("settlement_count", 0) > 0
                and any(str(row["stage"]) == "OPEN" for row in lifecycle)
                and any(str(row["stage"]) == "CLOSED" for row in lifecycle)
            ),
        }
    )
    return result


def status(store: Any | None = None, release_commit: str | None = None) -> dict[str, Any]:
    return _status_payload(store, release_commit)


def _status_with_lifecycle(self: Any) -> dict[str, Any]:
    if _ORIGINAL_STATUS is None:
        raise RuntimeError("paper lifecycle runtime missing status owner")
    payload = _ORIGINAL_STATUS(self)
    payload["paper_execution_lifecycle"] = _status_payload(self.store, self.release_commit)
    return payload


def install_paper_lifecycle_runtime() -> None:
    global _INSTALLED, _ORIGINAL_OBSERVE, _ORIGINAL_STATUS, _ORIGINAL_REALTIME_RUN
    global _ORIGINAL_ATTEMPT_LIQUIDATION
    if _INSTALLED:
        return

    from . import v51_exact_exit_execution as exact
    from .profit_first_entity_final_research import FinalProfitFirstResearchAdapter
    from .wallet_realtime_tracking_repair import RealtimeWalletTracker

    if not bool(getattr(exact, "_INSTALLED", False)):
        raise RuntimeError("canonical_exact_exit_engine_must_be_installed_first")

    _ORIGINAL_ATTEMPT_LIQUIDATION = exact._attempt_liquidation
    exact._attempt_liquidation = _attempt_liquidation_with_paper_inventory  # type: ignore[assignment]

    _ORIGINAL_OBSERVE = FinalProfitFirstResearchAdapter.observe
    _observe_with_lifecycle.__dict__.update(getattr(_ORIGINAL_OBSERVE, "__dict__", {}))
    setattr(_observe_with_lifecycle, "_roi_v51_paper_lifecycle_runtime", True)
    FinalProfitFirstResearchAdapter.observe = _observe_with_lifecycle  # type: ignore[method-assign]

    _ORIGINAL_STATUS = FinalProfitFirstResearchAdapter.status
    _status_with_lifecycle.__dict__.update(getattr(_ORIGINAL_STATUS, "__dict__", {}))
    setattr(_status_with_lifecycle, "_roi_v51_paper_lifecycle_runtime", True)
    FinalProfitFirstResearchAdapter.status = _status_with_lifecycle  # type: ignore[method-assign]

    _ORIGINAL_REALTIME_RUN = RealtimeWalletTracker.run
    _run_with_lifecycle.__dict__.update(getattr(_ORIGINAL_REALTIME_RUN, "__dict__", {}))
    setattr(_run_with_lifecycle, "_roi_v51_paper_lifecycle_runtime", True)
    RealtimeWalletTracker.run = _run_with_lifecycle  # type: ignore[method-assign]

    _INSTALLED = True


__all__ = [
    "LIFECYCLE_RUNTIME_VERSION",
    "LIVE_MONEY_AUTHORITY",
    "PAPER_ONLY",
    "SIGNING_AVAILABLE",
    "TRANSACTION_SUBMISSION_AVAILABLE",
    "install_paper_lifecycle_runtime",
    "lifecycle_tick",
    "status",
    "sync_entry_reservations",
    "sync_settlements",
]
