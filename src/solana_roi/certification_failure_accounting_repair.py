from __future__ import annotations

import asyncio
import json
import math
import sqlite3
import time
from collections import Counter
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import ephemeral_candidate_retention as retention
from .observation import LatencyCertificationGate
from .quote import QuoteCertificationGate
from .shadow_execution import (
    JupiterShadowTransactionSimulator,
    ShadowExecutionObservation,
    ShadowWalletExecutableQuoteHandoff,
)


SCOUT_REASONS = frozenset({"frozen_scout_processed_trigger", "frozen_scout_live_poll_trigger"})
_ORIGINAL_DISCARD = retention._discard_hydration_row
_ORIGINAL_REAP = retention._reap_sqlite
_ORIGINAL_LATENCY_STATUS = LatencyCertificationGate.status
_ORIGINAL_QUOTE_STATUS = QuoteCertificationGate.status
_ORIGINAL_QUOTE_OBSERVE = ShadowWalletExecutableQuoteHandoff.observe
_ORIGINAL_SHADOW_OBSERVE = JupiterShadowTransactionSimulator.observe


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * q
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def _ensure_failure_schema(store: Any) -> None:
    with store._lock, store.db:
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS anonymous_candidate_latency_failures ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, failed_at TEXT NOT NULL, reason TEXT NOT NULL, "
            "outcome TEXT NOT NULL, count INTEGER NOT NULL, max_age_ms REAL NOT NULL)"
        )
        store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_anonymous_candidate_latency_failed_at "
            "ON anonymous_candidate_latency_failures(failed_at)"
        )
        store.db.execute(
            "CREATE TABLE IF NOT EXISTS execution_quote_failures ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, token_mint TEXT NOT NULL, stage TEXT NOT NULL, "
            "trigger_observed_at TEXT NOT NULL, started_at TEXT NOT NULL, failed_at TEXT NOT NULL, "
            "quote_latency_ms REAL NOT NULL, chain_to_quote_ms REAL NOT NULL, "
            "error_type TEXT NOT NULL, error TEXT NOT NULL)"
        )
        store.db.execute(
            "CREATE INDEX IF NOT EXISTS ix_execution_quote_failures_failed_at "
            "ON execution_quote_failures(failed_at)"
        )


def _ensure_failure_schema_conn(db: sqlite3.Connection) -> None:
    db.execute(
        "CREATE TABLE IF NOT EXISTS anonymous_candidate_latency_failures ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, failed_at TEXT NOT NULL, reason TEXT NOT NULL, "
        "outcome TEXT NOT NULL, count INTEGER NOT NULL, max_age_ms REAL NOT NULL)"
    )
    db.execute(
        "CREATE INDEX IF NOT EXISTS ix_anonymous_candidate_latency_failed_at "
        "ON anonymous_candidate_latency_failures(failed_at)"
    )


def _record_anonymous_candidate_failure(
    store: Any,
    *,
    reason: str,
    outcome: str,
    count: int,
    max_age_ms: float,
    failed_at: datetime | None = None,
) -> None:
    if int(count) <= 0:
        return
    at = failed_at or _utcnow()
    _ensure_failure_schema(store)
    with store._lock, store.db:
        store.db.execute(
            "INSERT INTO anonymous_candidate_latency_failures("
            "failed_at, reason, outcome, count, max_age_ms) VALUES (?, ?, ?, ?, ?)",
            (at.isoformat(), reason, outcome, int(count), float(max_age_ms)),
        )


def _normalized_candidate_for_signature(store: Any, signature: str) -> dict[str, Any] | None:
    if not signature:
        return None
    try:
        with store._lock:
            row = store.db.execute(
                "SELECT s.token_mint, s.observed_at, s.received_at, s.ingestion_latency_ms, s.side, "
                "s.wallet, w.historically_eligible, w.tier FROM normalized_swaps s "
                "LEFT JOIN wallet_profiles w ON w.wallet=s.wallet WHERE s.signature=? "
                "ORDER BY s.id DESC LIMIT 1",
                (signature,),
            ).fetchone()
    except Exception:
        return None
    return dict(row) if row is not None else None


def _risk_refresh_already_recorded(store: Any, row: dict[str, Any]) -> bool:
    try:
        with store._lock:
            found = store.db.execute(
                "SELECT 1 FROM risk_refresh_measurements WHERE token_mint=? AND trigger_received_at=? LIMIT 1",
                (str(row["token_mint"]), str(row["received_at"])),
            ).fetchone()
    except Exception:
        return False
    return found is not None


def _account_scout_expiry(
    store: Any,
    row: dict[str, Any],
    *,
    outcome: str,
    failed_at: datetime | None = None,
) -> str:
    """Retain certification truth while allowing strategy work itself to expire.

    If the transaction had already normalized, its eligibility can be determined
    from canonical evidence. A qualifying S/A buy that never completed its timed
    risk refresh is persisted as an explicit failed risk-refresh measurement. If
    the transaction never normalized before expiry, identity remains ephemeral and
    only an anonymous unresolved-scout-trigger failure is retained. Sells and
    otherwise ineligible transactions do not contaminate the candidate denominator.
    """

    reason = str(row.get("reason") or "")
    if reason not in SCOUT_REASONS:
        return "not_scout"
    now = failed_at or _utcnow()
    trigger = retention._parse_dt(row["trigger_received_at"])
    age_ms = max(0.0, (now - trigger).total_seconds() * 1000.0)
    candidate = _normalized_candidate_for_signature(store, str(row.get("signature") or ""))
    if candidate is None:
        _record_anonymous_candidate_failure(
            store,
            reason=reason,
            outcome=outcome,
            count=1,
            max_age_ms=age_ms,
            failed_at=now,
        )
        return "anonymous_unclassified"

    eligible = bool(candidate.get("historically_eligible")) and str(candidate.get("tier") or "").upper() in {"S", "A"}
    if str(candidate.get("side") or "").lower() != "buy" or not eligible:
        return "classified_non_candidate"
    if _risk_refresh_already_recorded(store, candidate):
        return "risk_already_accounted"

    try:
        observed = retention._parse_dt(candidate["observed_at"])
        received = retention._parse_dt(candidate["received_at"])
        ingestion_latency_ms = float(candidate.get("ingestion_latency_ms") or 0.0)
        store.record_risk_refresh(
            token_mint=str(candidate["token_mint"]),
            trigger_observed_at=observed.isoformat(),
            trigger_received_at=received.isoformat(),
            started_at=received.isoformat(),
            completed_at=now.isoformat(),
            elapsed_ms=max(0.0, (now - received).total_seconds() * 1000.0),
            ingestion_latency_ms=ingestion_latency_ms,
            end_to_end_ms=max(0.0, (now - observed).total_seconds() * 1000.0),
            complete=False,
            fresh=False,
            readiness={
                "complete": False,
                "fresh": False,
                "failure": "candidate_entry_window_expired",
                "retention_outcome": outcome,
                "certification_failure_accounting": True,
            },
        )
    except Exception:
        _record_anonymous_candidate_failure(
            store,
            reason=reason,
            outcome="eligible_candidate_failure_accounting_error",
            count=1,
            max_age_ms=age_ms,
            failed_at=now,
        )
        return "anonymous_accounting_fallback"
    return "eligible_candidate_failed_latency"


def _discard_with_failure_accounting(self: Any, row: dict[str, Any], *, outcome: str) -> None:
    _account_scout_expiry(self.store, row, outcome=outcome)
    _ORIGINAL_DISCARD(self, row, outcome=outcome)


def _anonymous_expired_before_entry_count(path: Path) -> tuple[int, float]:
    db = sqlite3.connect(path, timeout=0.0, isolation_level=None, check_same_thread=False)
    db.row_factory = sqlite3.Row
    try:
        if not retention._sqlite_table_exists(db, "anonymous_certification_outcomes"):
            return 0, 0.0
        row = db.execute(
            "SELECT COALESCE(SUM(count),0) AS n, COALESCE(MAX(max_age_ms),0) AS max_age_ms "
            "FROM anonymous_certification_outcomes WHERE reason IN (?,?) AND outcome='expired_before_entry'",
            tuple(sorted(SCOUT_REASONS)),
        ).fetchone()
        return (int(row["n"] or 0), float(row["max_age_ms"] or 0.0)) if row is not None else (0, 0.0)
    finally:
        db.close()


def _record_reaper_delta(path: Path, *, count: int, max_age_ms: float, failed_at: datetime) -> None:
    if count <= 0:
        return
    db = sqlite3.connect(path, timeout=0.0, isolation_level=None, check_same_thread=False)
    try:
        db.execute("BEGIN IMMEDIATE")
        _ensure_failure_schema_conn(db)
        db.execute(
            "INSERT INTO anonymous_candidate_latency_failures("
            "failed_at, reason, outcome, count, max_age_ms) VALUES (?, ?, ?, ?, ?)",
            (
                failed_at.isoformat(),
                "frozen_scout_unclassified_pending_trigger",
                "expired_before_entry",
                int(count),
                float(max_age_ms),
            ),
        )
        db.execute("COMMIT")
    except sqlite3.OperationalError as exc:
        try:
            db.execute("ROLLBACK")
        except Exception:
            pass
        if "locked" not in str(exc).lower() and "busy" not in str(exc).lower():
            raise
    finally:
        db.close()


def _reap_with_failure_accounting(path: Path, now: datetime | None = None) -> dict[str, Any]:
    at = (now or _utcnow()).astimezone(timezone.utc)
    before_count, _before_max = _anonymous_expired_before_entry_count(path)
    result = _ORIGINAL_REAP(path, at)
    if result.get("busy"):
        return result
    after_count, after_max = _anonymous_expired_before_entry_count(path)
    delta = max(0, int(after_count) - int(before_count))
    if delta:
        _record_reaper_delta(path, count=delta, max_age_ms=after_max, failed_at=at)
    return result


def _candidate_failure_rows(store: Any, *, since: datetime | None) -> list[dict[str, Any]]:
    _ensure_failure_schema(store)
    sql = "SELECT failed_at, reason, outcome, count, max_age_ms FROM anonymous_candidate_latency_failures"
    args: list[Any] = []
    if since is not None:
        sql += " WHERE failed_at>=?"
        args.append(since.isoformat())
    sql += " ORDER BY id DESC LIMIT 500"
    with store._lock:
        rows = store.db.execute(sql, tuple(args)).fetchall()
    return [dict(row) for row in rows]


def _latency_status_with_failure_accounting(self: LatencyCertificationGate, *, limit: int = 500) -> dict[str, object]:
    payload = _ORIGINAL_LATENCY_STATUS(self, limit=limit)
    rows = _candidate_failure_rows(self.store, since=self.prospective_start_at)
    unresolved_count = sum(int(row.get("count") or 0) for row in rows)
    reasons = Counter(str(row.get("outcome") or "unknown") for row in rows for _ in range(max(1, int(row.get("count") or 0))))
    sampling_complete = unresolved_count == 0
    payload["certified"] = bool(payload.get("certified") and sampling_complete)
    payload["candidate_sampling_complete"] = sampling_complete
    payload["unclassified_scout_trigger_expiry_count"] = unresolved_count
    payload["unclassified_scout_trigger_expiry_outcomes"] = dict(reasons)
    requirements = payload.setdefault("requirements", {})
    if isinstance(requirements, dict):
        requirements["all_frozen_scout_triggers_must_be_classified_within_entry_window"] = True
        requirements["candidate_entry_window_seconds_unchanged"] = float(retention.ENTRY_WINDOW_SECONDS)
    return payload


def _ensure_quote_failure_schema(store: Any) -> None:
    _ensure_failure_schema(store)


def _record_quote_failure(
    store: Any,
    *,
    token_mint: str,
    stage: str,
    trigger_observed_at: datetime,
    started_at: datetime,
    failed_at: datetime,
    elapsed_ms: float,
    error_type: str,
    error: str,
) -> None:
    _ensure_quote_failure_schema(store)
    chain_ms = max(0.0, (failed_at - trigger_observed_at).total_seconds() * 1000.0)
    with store._lock, store.db:
        store.db.execute(
            "INSERT INTO execution_quote_failures("
            "token_mint, stage, trigger_observed_at, started_at, failed_at, quote_latency_ms, "
            "chain_to_quote_ms, error_type, error) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                token_mint,
                stage,
                trigger_observed_at.isoformat(),
                started_at.isoformat(),
                failed_at.isoformat(),
                float(elapsed_ms),
                float(chain_ms),
                error_type,
                error[:500],
            ),
        )
    store.append(
        "execution_quote_failure_measurement",
        failed_at.isoformat(),
        {
            "token_mint": token_mint,
            "stage": stage,
            "trigger_observed_at": trigger_observed_at.isoformat(),
            "started_at": started_at.isoformat(),
            "failed_at": failed_at.isoformat(),
            "quote_latency_ms": elapsed_ms,
            "chain_to_quote_ms": chain_ms,
            "error_type": error_type,
            "error": error[:500],
            "usable": False,
            "certification_failure_accounting": True,
        },
    )


def _quote_failures(store: Any, *, limit: int) -> list[dict[str, Any]]:
    _ensure_quote_failure_schema(store)
    with store._lock:
        rows = store.db.execute(
            "SELECT token_mint, stage, failed_at, quote_latency_ms, chain_to_quote_ms, error_type, error "
            "FROM execution_quote_failures ORDER BY id DESC LIMIT ?",
            (max(int(limit) * 2, 500),),
        ).fetchall()
    return [dict(row) for row in rows]


def _quote_status_with_failure_accounting(self: QuoteCertificationGate, limit: int = 500) -> dict[str, object]:
    success_rows = self.ledger.recent(max(int(limit) * 2, 500))
    failure_rows = _quote_failures(self.ledger.store, limit=limit)
    attempts: list[dict[str, Any]] = []
    for row in success_rows:
        attempts.append(
            {
                "received_at": str(row["received_at"]),
                "usable": bool(row["usable"]),
                "quote_latency_ms": float(row["quote_latency_ms"]),
                "chain_to_quote_ms": float(row["chain_to_quote_ms"]),
                "failure": False,
                "error_type": None,
            }
        )
    for row in failure_rows:
        attempts.append(
            {
                "received_at": str(row["failed_at"]),
                "usable": False,
                "quote_latency_ms": float(row["quote_latency_ms"]),
                "chain_to_quote_ms": float(row["chain_to_quote_ms"]),
                "failure": True,
                "error_type": str(row.get("error_type") or "unknown"),
            }
        )
    attempts.sort(key=lambda row: str(row["received_at"]), reverse=True)
    attempts = attempts[: int(limit)]
    if self.prospective_start_at is not None:
        attempts = [
            row for row in attempts
            if datetime.fromisoformat(str(row["received_at"])) >= self.prospective_start_at
        ]

    usable = [row for row in attempts if bool(row["usable"])]
    failures = [row for row in attempts if bool(row["failure"])]
    success_fraction = len(usable) / len(attempts) if attempts else 0.0
    quote_latencies = [float(row["quote_latency_ms"]) for row in attempts]
    chain_to_quote = [float(row["chain_to_quote_ms"]) for row in attempts]
    p95_quote = _percentile(quote_latencies, 0.95)
    p95_chain = _percentile(chain_to_quote, 0.95)
    p99_chain = _percentile(chain_to_quote, 0.99)
    certified = bool(
        len(attempts) >= self.policy.min_samples
        and success_fraction >= self.policy.min_quote_success_fraction
        and p95_quote is not None and p95_quote <= self.policy.max_p95_quote_latency_ms
        and p95_chain is not None and p95_chain <= self.policy.max_p95_chain_to_quote_ms
        and p99_chain is not None and p99_chain <= self.policy.max_p99_chain_to_quote_ms
    )
    error_counts = Counter(str(row.get("error_type") or "unknown") for row in failures)
    return {
        "certified": certified,
        "automatic_activation": False,
        "sample_count": len(attempts),
        "usable_count": len(usable),
        "usable_fraction": success_fraction,
        "failed_attempt_count": len(failures),
        "failed_attempt_error_types": dict(error_counts),
        "p95_quote_latency_ms": p95_quote,
        "p95_chain_to_quote_ms": p95_chain,
        "p99_chain_to_quote_ms": p99_chain,
        "prospective_start_at": self.prospective_start_at.isoformat() if self.prospective_start_at else None,
        "measurement_path": "chain event -> complete/fresh risk decision -> amount-specific executable quote available",
        "failure_attempts_included_in_denominator": True,
        "requirements": {
            "min_samples": self.policy.min_samples,
            "min_quote_success_fraction": self.policy.min_quote_success_fraction,
            "max_p95_quote_latency_ms": self.policy.max_p95_quote_latency_ms,
            "max_p95_chain_to_quote_ms": self.policy.max_p95_chain_to_quote_ms,
            "max_p99_chain_to_quote_ms": self.policy.max_p99_chain_to_quote_ms,
            "prospective_release_boundary_required": True,
        },
    }


def _shadow_cancelled_observation(self: JupiterShadowTransactionSimulator, quote: Any, elapsed_ms: float) -> ShadowExecutionObservation:
    input_lamports = max(1, int(round(float(quote.input_sol) * 1_000_000_000)))
    now = _utcnow()
    return ShadowExecutionObservation(
        token_mint=str(quote.token_mint),
        stage=str(quote.stage),
        shadow_wallet=str(self.shadow_wallet),
        observed_at=now,
        completed_at=now,
        input_lamports=input_lamports,
        transaction_built=False,
        transaction_sha256=None,
        transaction_size_bytes=None,
        last_valid_block_height=None,
        router=str(getattr(quote, "router", "unknown") or "unknown"),
        order_out_token_units=None,
        order_effective_price_sol=None,
        order_drift_fraction=None,
        signature_fee_lamports=None,
        prioritization_fee_lamports=None,
        rent_fee_lamports=None,
        simulation_ok=False,
        units_consumed=None,
        simulation_slot=None,
        logs_count=0,
        total_latency_ms=float(elapsed_ms),
        error="CancelledError:candidate entry window expired during unsigned shadow simulation",
    )


async def _shadow_observe_with_cancellation_accounting(self: JupiterShadowTransactionSimulator, quote: Any) -> Any:
    started = time.perf_counter()
    try:
        return await _ORIGINAL_SHADOW_OBSERVE(self, quote)
    except asyncio.CancelledError:
        observation = _shadow_cancelled_observation(
            self,
            quote,
            max(0.0, (time.perf_counter() - started) * 1000.0),
        )
        pending = getattr(self, "_roi_cancelled_shadow_observations", None)
        if not isinstance(pending, list):
            pending = []
            setattr(self, "_roi_cancelled_shadow_observations", pending)
        pending.append(observation)
        raise


def _pop_cancelled_shadow(self: ShadowWalletExecutableQuoteHandoff, token_mint: str, stage: str) -> ShadowExecutionObservation | None:
    simulator = self.simulator
    if simulator is None:
        return None
    pending = getattr(simulator, "_roi_cancelled_shadow_observations", None)
    if not isinstance(pending, list):
        return None
    for index in range(len(pending) - 1, -1, -1):
        row = pending[index]
        if str(getattr(row, "token_mint", "")) == token_mint and str(getattr(row, "stage", "")) == stage:
            return pending.pop(index)
    return None


async def _quote_observe_with_failure_accounting(
    self: ShadowWalletExecutableQuoteHandoff,
    *,
    token_mint: str,
    stage: str,
    fraction_of_full_position: float,
    scout_reference_price_sol: float,
    trigger_observed_at: datetime,
) -> Any:
    started_at = _utcnow()
    started_perf = time.perf_counter()
    try:
        result = await _ORIGINAL_QUOTE_OBSERVE(
            self,
            token_mint=token_mint,
            stage=stage,
            fraction_of_full_position=fraction_of_full_position,
            scout_reference_price_sol=scout_reference_price_sol,
            trigger_observed_at=trigger_observed_at,
        )
    except asyncio.CancelledError:
        failed_at = _utcnow()
        shadow = _pop_cancelled_shadow(self, token_mint, stage)
        if shadow is not None:
            self.shadow_ledger.record(shadow)
        _record_quote_failure(
            self.store,
            token_mint=token_mint,
            stage=stage,
            trigger_observed_at=trigger_observed_at,
            started_at=started_at,
            failed_at=failed_at,
            elapsed_ms=max(0.0, (time.perf_counter() - started_perf) * 1000.0),
            error_type="CancelledError",
            error="candidate entry window expired during quote/shadow observation",
        )
        raise

    if result is None:
        try:
            notional = max(0.0, float(self.full_position_notional_fn()) * float(fraction_of_full_position))
        except Exception:
            notional = 0.0
        if notional > 0.0:
            failed_at = _utcnow()
            _record_quote_failure(
                self.store,
                token_mint=token_mint,
                stage=stage,
                trigger_observed_at=trigger_observed_at,
                started_at=started_at,
                failed_at=failed_at,
                elapsed_ms=max(0.0, (time.perf_counter() - started_perf) * 1000.0),
                error_type="QuoteObservationUnavailable",
                error="quote or unsigned shadow simulation returned no usable observation",
            )
    return result


def install_certification_failure_accounting_repair() -> None:
    """Make expired/failed certification attempts visible without retaining strategy state."""

    retention._discard_hydration_row = _discard_with_failure_accounting  # type: ignore[assignment]
    retention._reap_sqlite = _reap_with_failure_accounting  # type: ignore[assignment]

    current_latency = LatencyCertificationGate.status
    if not bool(getattr(current_latency, "_roi_failure_accounting", False)):
        try:
            _latency_status_with_failure_accounting.__dict__.update(getattr(current_latency, "__dict__", {}))
        except Exception:
            pass
        setattr(_latency_status_with_failure_accounting, "_roi_failure_accounting", True)
        LatencyCertificationGate.status = _latency_status_with_failure_accounting  # type: ignore[method-assign]

    current_quote_status = QuoteCertificationGate.status
    if not bool(getattr(current_quote_status, "_roi_failure_accounting", False)):
        try:
            _quote_status_with_failure_accounting.__dict__.update(getattr(current_quote_status, "__dict__", {}))
        except Exception:
            pass
        setattr(_quote_status_with_failure_accounting, "_roi_failure_accounting", True)
        QuoteCertificationGate.status = _quote_status_with_failure_accounting  # type: ignore[method-assign]

    current_shadow = JupiterShadowTransactionSimulator.observe
    if not bool(getattr(current_shadow, "_roi_failure_accounting", False)):
        setattr(_shadow_observe_with_cancellation_accounting, "_roi_failure_accounting", True)
        JupiterShadowTransactionSimulator.observe = _shadow_observe_with_cancellation_accounting  # type: ignore[method-assign]

    current_handoff = ShadowWalletExecutableQuoteHandoff.observe
    if not bool(getattr(current_handoff, "_roi_failure_accounting", False)):
        setattr(_quote_observe_with_failure_accounting, "_roi_failure_accounting", True)
        ShadowWalletExecutableQuoteHandoff.observe = _quote_observe_with_failure_accounting  # type: ignore[method-assign]


__all__ = [
    "SCOUT_REASONS",
    "_account_scout_expiry",
    "_latency_status_with_failure_accounting",
    "_quote_status_with_failure_accounting",
    "_record_quote_failure",
    "install_certification_failure_accounting_repair",
]
